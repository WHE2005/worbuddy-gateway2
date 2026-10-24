package main

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	defaultAPIPort = 8317
	defaultWebPort = 8316
)

// Key 按用户配置以明文落盘；APIKeyEnabled 只影响模型 API，绝不能关闭管理鉴权。
type gatewayFileConfig struct {
	APIPort       int           `json:"apiPort"`
	WebPort       int           `json:"webPort"`
	AdminKey      string        `json:"adminKey"`
	APIKeyEnabled bool          `json:"apiKeyEnabled"`
	APIKey        string        `json:"apiKey"` // 旧单 Key 字段：读取时自动迁移为一把「默认」Key，仅作回滚兼容保留
	Keys          []APIKeyEntry `json:"keys"`
}

// APIKeyEntry 是一把可独立启停、可限制可用模型范围的模型 API Key。
type APIKeyEntry struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Key       string   `json:"key"`
	CreatedAt int64    `json:"createdAt"`
	Enabled   bool     `json:"enabled"`
	Models    []string `json:"models,omitempty"` // 空/缺省 = 不限制；非空 = 只允许这些模型
}

type gatewayValidationError string

func (e gatewayValidationError) Error() string { return string(e) }

var (
	gatewayConfigMu sync.RWMutex // 串行配置写入及运行时 Key 切换；不持锁调用 HTTP handler
	gatewaySettings gatewayFileConfig
	errSetupDone    = errors.New("管理 Key 已配置；外部修改后请等待配置热加载并重新登录")
	errConfigChange = errors.New("管理 Key 已被外部修改，请等待配置热加载并使用新 Key 重新登录")
)

const (
	maxAPIKeys        = 50
	maxKeyModelsScope = 100
)

func normalizeGatewayConfig(g gatewayFileConfig) (gatewayFileConfig, error) {
	if g.APIPort == 0 {
		g.APIPort = defaultAPIPort
	}
	if g.WebPort == 0 {
		g.WebPort = defaultWebPort
	}
	if g.APIPort < 1 || g.APIPort > 65535 || g.WebPort < 1 || g.WebPort > 65535 {
		return g, gatewayValidationError("gateway.apiPort/webPort 必须为 1～65535，省略或 0 使用默认值")
	}
	if g.APIPort == g.WebPort {
		return g, gatewayValidationError("API 端口和 Web 端口不能相同")
	}
	for name, value := range map[string]string{"adminKey": g.AdminKey, "apiKey": g.APIKey} {
		for _, c := range value {
			if c < 33 || c > 126 {
				return g, gatewayValidationError(fmt.Sprintf("gateway.%s 只能包含非空白可打印 ASCII 字符", name))
			}
		}
	}
	// 旧单 Key 配置自动迁移为一把「默认」Key；ID 由 Key 内容派生，重复加载结果一致。
	if len(g.Keys) == 0 && g.APIKey != "" {
		sum := sha256.Sum256([]byte(g.APIKey))
		g.Keys = []APIKeyEntry{{ID: "legacy-" + hex.EncodeToString(sum[:4]), Name: "默认",
			Key: g.APIKey, CreatedAt: time.Now().Unix(), Enabled: true}}
	}
	seenIDs := map[string]bool{}
	seenKeys := map[string]bool{}
	for i := range g.Keys {
		k := &g.Keys[i]
		if k.ID == "" {
			return g, gatewayValidationError(fmt.Sprintf("gateway.keys[%d].id 不能为空", i))
		}
		if seenIDs[k.ID] {
			return g, gatewayValidationError(fmt.Sprintf("gateway.keys[%d].id 重复: %s", i, k.ID))
		}
		seenIDs[k.ID] = true
		if strings.TrimSpace(k.Name) == "" {
			k.Name = "未命名 Key"
		}
		if len([]rune(k.Name)) > 64 {
			return g, gatewayValidationError(fmt.Sprintf("gateway.keys[%d].name 最长 64 字符", i))
		}
		for _, c := range k.Key {
			if c < 33 || c > 126 {
				return g, gatewayValidationError(fmt.Sprintf("gateway.keys[%d].key 只能包含非空白可打印 ASCII 字符", i))
			}
		}
		if k.Key == "" || len(k.Key) > 128 {
			return g, gatewayValidationError(fmt.Sprintf("gateway.keys[%d].key 必须为 1～128 个字符", i))
		}
		if seenKeys[k.Key] {
			return g, gatewayValidationError(fmt.Sprintf("gateway.keys[%d].key 与其他 Key 重复", i))
		}
		seenKeys[k.Key] = true
		models := make([]string, 0, len(k.Models))
		seenModels := map[string]bool{}
		for _, m := range k.Models {
			if m = normalizeModelName(m); m == "" || seenModels[m] {
				continue
			}
			seenModels[m] = true
			models = append(models, m)
		}
		if len(models) > maxKeyModelsScope {
			return g, gatewayValidationError(fmt.Sprintf("gateway.keys[%d].models 超过 %d 个上限", i, maxKeyModelsScope))
		}
		k.Models = models
	}
	if len(g.Keys) > maxAPIKeys {
		return g, gatewayValidationError(fmt.Sprintf("gateway.keys 超过 %d 把上限", maxAPIKeys))
	}
	if g.APIKeyEnabled && !hasEnabledAPIKey(g) {
		return g, gatewayValidationError("启用 API Key 校验时至少需要一把「启用」状态的 Key（或旧版 gateway.apiKey）")
	}
	return g, nil
}

func hasEnabledAPIKey(g gatewayFileConfig) bool {
	for _, k := range g.Keys {
		if k.Enabled {
			return true
		}
	}
	return g.APIKey != ""
}

func applyGatewayConfig(g gatewayFileConfig) error {
	g, err := normalizeGatewayConfig(g)
	if err != nil {
		return err
	}
	apiPort := g.APIPort
	if cfg.PortExplicit {
		apiPort = cfg.Port
	}
	if apiPort < 1 || apiPort > 65535 || cfg.WebUI && apiPort == g.WebPort {
		return errors.New("API 监听端口无效或与 Web 监听端口冲突")
	}
	gatewayConfigMu.Lock()
	defer gatewayConfigMu.Unlock()
	gatewaySettings = g
	cfg.Port, cfg.WebPort = apiPort, g.WebPort
	setGatewayKeysLocked(g)
	return nil
}

func setGatewayKeysLocked(g gatewayFileConfig) {
	registerSecrets(g.AdminKey, g.APIKey)
	cfg.AdminKey = g.AdminKey
	cfg.APIKeys = append([]APIKeyEntry(nil), g.Keys...)
	cfg.APIKey = ""
	if g.APIKeyEnabled {
		// 兼容 probe CLI 等旧调用方：取第一把「启用」的 Key 作为当前 Key。
		for _, k := range cfg.APIKeys {
			if k.Enabled {
				cfg.APIKey = k.Key
				break
			}
		}
		if cfg.APIKey == "" && g.APIKey != "" {
			cfg.APIKey = g.APIKey
		}
	}
	for _, k := range cfg.APIKeys {
		registerSecrets(k.Key)
	}
}

// matchAPIKey 恒时匹配请求令牌；命中返回该把 Key（Models 非空表示限制了可用模型范围）。
// 旧单 Key 配置在 normalize 阶段已迁移进 Keys，这里保留 legacy 兜底以防极端时序。
func matchAPIKey(token string) (APIKeyEntry, bool) {
	gatewayConfigMu.RLock()
	defer gatewayConfigMu.RUnlock()
	if token == "" {
		return APIKeyEntry{}, false
	}
	for _, k := range cfg.APIKeys {
		if k.Enabled && subtle.ConstantTimeCompare([]byte(token), []byte(k.Key)) == 1 {
			return k, true
		}
	}
	if len(cfg.APIKeys) == 0 && cfg.APIKey != "" && subtle.ConstantTimeCompare([]byte(token), []byte(cfg.APIKey)) == 1 {
		return APIKeyEntry{Name: "默认"}, true
	}
	return APIKeyEntry{}, false
}

func currentAPIKey() string {
	gatewayConfigMu.RLock()
	defer gatewayConfigMu.RUnlock()
	return cfg.APIKey
}

func currentAdminKey() string {
	gatewayConfigMu.RLock()
	defer gatewayConfigMu.RUnlock()
	return cfg.AdminKey
}

// adminKeyMatches 恒时比较两个管理 Key，统一替代各处散落的 != 直接比较，
// 与鉴权中间件的 subtle.ConstantTimeCompare 保持同一标准。
func adminKeyMatches(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// readGatewayDocumentLocked 保留其他配置段，不将提示词/账号规则重新构造为默认值。
func readGatewayDocumentLocked() (map[string]json.RawMessage, gatewayFileConfig, []byte, error) {
	data, err := os.ReadFile(runtimeConfigFile)
	if errors.Is(err, os.ErrNotExist) {
		g, _ := normalizeGatewayConfig(gatewayFileConfig{})
		return map[string]json.RawMessage{}, g, nil, nil
	}
	if err != nil {
		return nil, gatewayFileConfig{}, nil, err
	}
	typed, err := parseRuntimeConfig(data)
	if err != nil {
		return nil, gatewayFileConfig{}, nil, fmt.Errorf("配置文件不可解析，拒绝覆盖: %w", err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(data, &doc); err != nil || doc == nil {
		return nil, gatewayFileConfig{}, nil, errors.New("config.json 必须是 JSON 对象")
	}
	g, err := normalizeGatewayConfig(typed.Gateway)
	return doc, g, data, err
}

// writeConfigPreservingFile 原子替换 config.json：同目录临时文件 + fsync + 改名，
// 断电或进程被强杀时文件要么是完整旧内容、要么是完整新内容；替换失败时原文件不变，
// 因此失败恢复路径（调用方把 previous 写回）通常不再需要，但保持兼容。
// 临时文件以 config.json.tmp 命名且权限 0600，与原文件一致；新增文件继承目录 ACL。
func writeConfigPreservingFile(data, previous []byte) error {
	_ = previous
	return atomicWriteFile(runtimeConfigFile, data, 0600)
}

func persistGatewayConfigLocked(doc map[string]json.RawMessage, g gatewayFileConfig, old []byte, trace string) error {
	raw, err := json.Marshal(g)
	if err != nil {
		return err
	}
	doc["gateway"] = raw
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	log.Printf("[管理配置] traceId=%s 阶段=写入配置 文件=%s API鉴权启用=%t Key已隐藏=true 说明=先落盘再切换运行时配置，其他配置段保留", trace, runtimeConfigFile, g.APIKeyEnabled)
	if err := writeConfigPreservingFile(append(data, '\n'), old); err != nil {
		log.Printf("[管理配置] traceId=%s 阶段=写入配置 结果=失败 原因=%v 业务影响=运行时配置未切换", trace, err)
		return err
	}
	gatewaySettings = g
	setGatewayKeysLocked(g)
	// 本函数只应用 gateway 段；强制下轮完整重载其他外部编辑的配置。
	runtimeHasAppliedHash = false
	log.Printf("[管理配置] traceId=%s 结果=保存成功 API鉴权启用=%t 说明=后续请求使用新鉴权状态，管理鉴权始终独立启用，监听端口不热切换", trace, g.APIKeyEnabled)
	return nil
}

func initializeAdminKey(trace string) (string, error) {
	runtimeConfigMu.Lock()
	defer runtimeConfigMu.Unlock()
	gatewayConfigMu.Lock()
	defer gatewayConfigMu.Unlock()
	if cfg.AdminKey != "" {
		return "", errSetupDone
	}
	doc, g, old, err := readGatewayDocumentLocked()
	if err != nil {
		return "", err
	}
	if g.AdminKey != "" {
		return "", errSetupDone
	}
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("生成管理 Key 失败: %w", err)
	}
	g.AdminKey = hex.EncodeToString(entropy[:]) // 精确 32 字符，128 位随机熵。
	registerSecrets(g.AdminKey)
	if err := persistGatewayConfigLocked(doc, g, old, trace); err != nil {
		return "", err
	}
	return g.AdminKey, nil
}

func saveAPIKeySettings(enabled bool, key, trace string) error {
	return saveGatewaySettings(&enabled, key, nil, nil, trace)
}

func saveGatewaySettings(enabled *bool, key string, apiPort, webPort *int, trace string) error {
	runtimeConfigMu.Lock()
	defer runtimeConfigMu.Unlock()
	gatewayConfigMu.Lock()
	defer gatewayConfigMu.Unlock()
	doc, g, old, err := readGatewayDocumentLocked()
	if err != nil {
		return err
	}
	if cfg.AdminKey == "" || !adminKeyMatches(g.AdminKey, cfg.AdminKey) {
		return errConfigChange
	}
	if enabled != nil {
		g.APIKeyEnabled = *enabled
	}
	if apiPort != nil {
		g.APIPort = *apiPort
	}
	if webPort != nil {
		g.WebPort = *webPort
	}
	// 空输入表示保留已配置 Key；关闭校验不删除原 Key。
	if key != "" {
		g.APIKey = key
	}
	g, err = normalizeGatewayConfig(g)
	if err != nil {
		return err
	}
	effectiveAPIPort := g.APIPort
	if cfg.PortExplicit {
		effectiveAPIPort = cfg.Port
	}
	if cfg.WebUI && effectiveAPIPort == g.WebPort {
		return gatewayValidationError("Web 端口不能与实际 API 端口冲突（含 -port 命令行覆盖）")
	}
	log.Printf("[管理端口设置] traceId=%s 配置API端口=%d 配置Web端口=%d 当前API端口=%d 当前Web端口=%d 说明=保存等效于编辑gateway配置，监听端口重启后改变",
		trace, g.APIPort, g.WebPort, cfg.Port, cfg.WebPort)
	registerSecrets(g.APIKey)
	return persistGatewayConfigLocked(doc, g, old, trace)
}

func gatewaySettingsView() map[string]any {
	gatewayConfigMu.RLock()
	defer gatewayConfigMu.RUnlock()
	return map[string]any{
		"apiKeyEnabled":      cfg.APIKey != "",
		"apiKeyConfigured":   gatewaySettings.APIKey != "",
		"adminKeyConfigured": cfg.AdminKey != "",
		"apiPort":            cfg.Port, "webPort": cfg.WebPort,
		"configuredApiPort": gatewaySettings.APIPort,
		"configuredWebPort": gatewaySettings.WebPort,
		"apiPortOverridden": cfg.PortExplicit,
		"restartRequired":   gatewaySettings.WebPort != cfg.WebPort || !cfg.PortExplicit && gatewaySettings.APIPort != cfg.Port,
	}
}

// ignoreLegacyAPIKeyFlag 只是吞掉旧 systemd 单元/脚本里残留的 -api-key，避免升级后启动失败。
//
// 该参数在新版本中**完全静默、不产生任何作用**：不写日志、不写盘、不修改 config.json、
// 不影响模型 API 鉴权；模型 API 鉴权一律只由 config.json 的 gateway.apiKeyEnabled / apiKey 决定。
//
// TODO(remove): 这是纯兼容占位，计划在后续版本彻底删除该参数与 main.go 中的注册代码；
// 删除后旧启动命令会因未知参数而启动失败，届时应在发布说明中提示用户先移除该参数。
func ignoreLegacyAPIKeyFlag(_ string) {
}
