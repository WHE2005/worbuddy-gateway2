package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
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

// -----------------------------------------------------------------------------
// 以下内容原位于 runtimeconfig.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

// 所有进程内配置文件读改写共用此锁，避免网页写入与热加载互相覆盖。
// 锁序：runtimeConfigMu -> gatewayConfigMu；调度不持有此锁。
var runtimeConfigMu sync.Mutex
var upstreamConfigMu sync.RWMutex
var statusSnapshotWriteMu sync.Mutex

// 与运行时状态同步更新，不能仅由扫描协程缓存：网页写入后立即撤回旧文件也必须生效。
var runtimeAppliedHash [32]byte
var runtimeHasAppliedHash bool

func parseRuntimeConfig(data []byte) (runtimeFileConfig, error) {
	var c runtimeFileConfig
	if len(bytes.TrimSpace(data)) == 0 || bytes.TrimSpace(data)[0] != '{' {
		return c, errors.New("config.json 必须是 JSON 对象，不能是空文件或 null")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return c, err
	}
	if err := ensureJSONEOF(dec); err != nil {
		return c, err
	}
	g, err := normalizeGatewayConfig(c.Gateway)
	if err != nil {
		return c, err
	}
	apiPort := g.APIPort
	if cfg.PortExplicit {
		apiPort = cfg.Port
	}
	if apiPort < 1 || apiPort > 65535 || cfg.WebUI && apiPort == g.WebPort {
		return c, errors.New("API 监听端口无效或与 Web 监听端口冲突")
	}
	if _, err := buildModelAccountRules(c.Models.Accounts); err != nil {
		return c, err
	}
	for _, seconds := range []int{c.Upstream.HeaderTimeoutSeconds, c.Upstream.IdleTimeoutSeconds} {
		if int64(seconds) > int64((1<<63-1)/time.Second) {
			return c, errors.New("upstream 超时秒数过大，超出 time.Duration 可表示范围")
		}
	}
	// 兼容旧示例同时携带两个空数组，但非空黑白名单不能同时生效。
	if hasModelNames(c.Models.Blocklist) && hasModelNames(c.Models.Allowlist) {
		return c, errors.New("models.allowlist 与 models.blocklist 不能同时非空，请只保留一种模型名单")
	}
	if c.Logs.RetentionDays < 0 || c.Logs.RetentionDays > 36500 {
		return c, gatewayValidationError("logs.retentionDays 必须为 0～36500 的整数；0 表示不自动清理")
	}
	return c, nil
}

func hasModelNames(names []string) bool {
	for _, name := range names {
		if normalizeModelName(name) != "" {
			return true
		}
	}
	return false
}

func loadRuntimeConfig(path string) error {
	runtimeConfigMu.Lock()
	defer runtimeConfigMu.Unlock()
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		data = []byte("{}")
	} else if err != nil {
		return fmt.Errorf("读取配置文件 %s: %w", path, err)
	}
	c, err := parseRuntimeConfig(data)
	if err != nil {
		return fmt.Errorf("解析配置文件 %s: %w", path, err)
	}
	if err := applyRuntimeConfig(c, false, ""); err != nil {
		return err
	}
	runtimeAppliedHash, runtimeHasAppliedHash = sha256.Sum256(data), true
	return nil
}

// 调用前已完整校验。热加载不修改监听端口，不中断正在输出的流。
func applyRuntimeConfig(c runtimeFileConfig, hot bool, trace string) error {
	if hot {
		if err := switchDebugLogging(c.Debug.Enabled); err != nil {
			return err // 日志文件不可写时，整份配置仍保留上一版本。
		}
		g, _ := normalizeGatewayConfig(c.Gateway)
		gatewayConfigMu.Lock()
		gatewaySettings = g
		setGatewayKeysLocked(g)
		gatewayConfigMu.Unlock()
	} else if err := applyGatewayConfig(c.Gateway); err != nil {
		return err
	}
	if !hot {
		cfg.DebugEnabled = c.Debug.Enabled // 仅启动时写入 cfg，运行期间由 sink 判断。
	}
	setModelFilter(c.Models.Blocklist, c.Models.Allowlist)
	_ = setModelAccountFilter(c.Models.Accounts)
	setSystemPromptConfig(c.SystemPrompt.Fallback, c.SystemPrompt.Force)
	logRetentionDays.Store(int64(c.Logs.RetentionDays))
	header, idle, retries := upstreamHeaderTimeoutDefault, upstreamIdleTimeoutDefault, upstreamTransientRetriesDefault
	if n := c.Upstream.HeaderTimeoutSeconds; n > 0 {
		header = time.Duration(n) * time.Second
	}
	if n := c.Upstream.IdleTimeoutSeconds; n > 0 {
		idle = time.Duration(n) * time.Second
	}
	if n := c.Upstream.TransientRetries; n != nil && *n >= 0 {
		retries = *n
	}
	upstreamConfigMu.Lock()
	upstreamHeaderTimeout, upstreamIdleTimeout, upstreamTransientRetries = header, idle, retries
	upstreamConfigMu.Unlock()
	if hot {
		log.Printf("[配置热加载] traceId=%s 结果=成功 模型黑名单=%d 模型白名单=%d 账号规则=%d 调试日志=%t 日志保留天数=%d 响应头超时=%v 流空闲超时=%v 重试次数=%d 说明=后续请求使用新配置，凭据及历史账本保留，端口变更需重启",
			trace, len(c.Models.Blocklist), len(c.Models.Allowlist), len(c.Models.Accounts), c.Debug.Enabled, c.Logs.RetentionDays, header, idle, retries)
	}
	return nil
}

func upstreamSettings() (time.Duration, time.Duration, int) {
	upstreamConfigMu.RLock()
	defer upstreamConfigMu.RUnlock()
	return upstreamHeaderTimeout, upstreamIdleTimeout, upstreamTransientRetries
}

func currentIdleTimeout() time.Duration {
	_, idle, _ := upstreamSettings()
	return idle
}

func switchDebugLogging(enabled bool) error {
	debugSinkMu.Lock()
	defer debugSinkMu.Unlock()
	if enabled == (debugSink != nil) {
		return nil
	}
	if enabled {
		f, err := newDailyLogWriter("debug", ".jsonl")
		if err != nil {
			return err
		}
		debugSink = &debugJSONSink{writer: f, closer: f}
	} else {
		sink := debugSink
		debugSink = nil
		sink.mu.Lock()
		if sink.closer != nil {
			_ = sink.closer.Close()
		}
		sink.mu.Unlock()
	}
	return nil
}

// 根据内容摘要检测变化，避免相同大小/相同时间戳的编辑被漏掉。
// 缺失/半写/错误配置保留上一份有效配置；同一坏内容只告警一次。
type runtimeConfigWatcher struct {
	failed    [32]byte
	hasFailed bool
	lastError error
}

func (watcher *runtimeConfigWatcher) reload() bool {
	watcher.lastError = nil
	runtimeConfigMu.Lock()
	defer runtimeConfigMu.Unlock()
	data, err := os.ReadFile(runtimeConfigFile)
	sum := sha256.Sum256(data)
	if err != nil {
		watcher.lastError = err
		sum = sha256.Sum256([]byte(err.Error()))
	}
	if err == nil && runtimeHasAppliedHash && runtimeAppliedHash == sum {
		watcher.hasFailed = false
		return false
	}
	trace := newTraceID()
	var c runtimeFileConfig
	if err == nil {
		c, err = parseRuntimeConfig(data)
	}
	if err == nil {
		err = applyRuntimeConfig(c, true, trace)
	}
	if err != nil {
		if !watcher.hasFailed || watcher.failed != sum {
			log.Printf("[配置热加载] traceId=%s 结果=拒绝 原因=%s 业务影响=配置未切换，保留上一份有效配置；请修正文件后再保存", trace, safeDebugError(err))
		}
		watcher.lastError = err
		watcher.failed, watcher.hasFailed = sum, true
		return false
	}
	runtimeAppliedHash, runtimeHasAppliedHash, watcher.hasFailed = sum, true, false
	return true
}

func runtimeConfigLoop(ctx context.Context) {
	watcher := runtimeConfigWatcher{}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastLogged time.Time
	var previousStatus string
	logScheduledTask("config_reload", "scheduled", "", newTraceID(), time.Time{}, time.Now().Add(time.Second), "")
	if logRetentionDays.Load() == 0 {
		logScheduledTask("log_cleanup", "disabled", "disabled", newTraceID(), time.Time{}, time.Time{}, "保留天数为0，不自动清理")
	}
	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-ticker.C:
			next := tick.Add(time.Second)
			started := time.Now()
			changed := watcher.reload()
			status, detail := "success", "检查完成，配置没有变化"
			if changed {
				detail = "新配置已校验并应用"
			}
			if watcher.lastError != nil {
				status, detail = "failed", safeDebugError(watcher.lastError)
			}
			logFrequentTask("config_reload", status, detail, started, next, &lastLogged, &previousStatus, changed)
			scheduledLogCleanup(time.Now())
		}
	}
}

// Transport 不可在活跃请求期间原地改字段。仅超时变化时克隆，
// 新请求用新 Transport；旧流继续读取，闲置连接回收。
type hotConfigTransport struct {
	mu        sync.Mutex
	transport *http.Transport
}

func (t *hotConfigTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	header, _, _ := upstreamSettings()
	t.mu.Lock()
	if t.transport.ResponseHeaderTimeout != header {
		old := t.transport
		t.transport = old.Clone()
		t.transport.ResponseHeaderTimeout = header
		old.CloseIdleConnections()
	}
	current := t.transport
	t.mu.Unlock()
	return current.RoundTrip(r)
}

func (t *hotConfigTransport) CloseIdleConnections() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.transport.CloseIdleConnections()
}

// -----------------------------------------------------------------------------
// 以下内容原位于 configedit.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

const configEditBudget = 1 << 20

func configRevision(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// 仅编辑固定的 config.json；不接受客户端指定路径，也不跟随符号链接。
// 调用方持有 runtimeConfigMu，与网页设置、名单开关和配置热加载串行。
func readConfigEditorDocument() ([]byte, error) {
	info, err := os.Lstat(runtimeConfigFile)
	if err != nil {
		return nil, errors.New("config.json 不可读取，请检查文件是否存在和文件权限")
	}
	if !info.Mode().IsRegular() {
		return nil, gatewayValidationError("config.json 必须是普通文件，网页编辑不支持符号链接或目录")
	}
	f, err := os.Open(runtimeConfigFile)
	if err != nil {
		return nil, errors.New("config.json 不可读取，请检查文件权限")
	}
	defer f.Close()
	if opened, err := f.Stat(); err != nil || !os.SameFile(info, opened) {
		return nil, errConfigChange
	}
	data, err := io.ReadAll(io.LimitReader(f, configEditBudget+1))
	if err != nil {
		return nil, errors.New("config.json 读取失败，未返回配置原文")
	}
	if len(data) > configEditBudget || !utf8.Valid(data) {
		return nil, gatewayValidationError("网页编辑仅支持最大 1 MiB 的 UTF-8 配置文件")
	}
	return data, nil
}

func validateConfigEditorDocument(content string) (runtimeFileConfig, error) {
	if len(content) > configEditBudget || !utf8.ValidString(content) {
		return runtimeFileConfig{}, gatewayValidationError("配置必须是最大 1 MiB 的 UTF-8 JSON 对象")
	}
	c, err := parseRuntimeConfig([]byte(content))
	if err != nil {
		// 解析错误可能携带用户输入的字段名或值，不把其原文写入响应或审计日志。
		return c, gatewayValidationError("配置校验失败：必须是单个 JSON 对象；字段名、类型、端口、日志天数及账号/模型名单须符合配置规则")
	}
	for _, key := range []string{c.Gateway.AdminKey, c.Gateway.APIKey} {
		if key == "[REDACTED]" {
			return c, gatewayValidationError("不能把脱敏占位符 [REDACTED] 保存为 Key，请重新加载编辑原文")
		}
	}
	return c, nil
}

func handleWebUIConfigEdit(w http.ResponseWriter, r *http.Request) {
	trace := debugTraceID(r)
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		writeJSONError(w, http.StatusMethodNotAllowed, "仅支持 GET/POST")
		return
	}
	var input struct {
		Revision               string `json:"revision"`
		Content                string `json:"content"`
		Acknowledged           bool   `json:"acknowledged"`
		ConfirmAdminKeyRemoval bool   `json:"confirmAdminKeyRemoval"`
	}
	if r.Method == http.MethodGet {
		input.Acknowledged = r.URL.Query().Get("acknowledged") == "1"
	} else if err := decodeAdminJSONLimit(w, r, &input, configEditBudget*6+4096); err != nil {
		// JSON 字符串中的转义字符可能扩大请求长度；配置原文仍单独限制 1 MiB。
		log.Printf("[配置编辑] traceId=%s 阶段=参数校验 结果=拒绝 状态码=400 原因=请求格式或长度无效 正文已省略=true", trace)
		writeJSONError(w, http.StatusBadRequest, "配置编辑请求格式或长度无效")
		return
	}
	if !input.Acknowledged {
		log.Printf("[配置编辑] traceId=%s 阶段=风险确认 结果=拒绝 状态码=400 说明=未确认编辑警告，不返回明文、不写入配置", trace)
		writeJSONError(w, http.StatusBadRequest, "请先确认配置编辑风险警告")
		return
	}
	runtimeConfigMu.Lock()
	defer runtimeConfigMu.Unlock()
	// 请求可能在等待配置锁时遇到管理 Key 轮换，锁内必须再次校验。
	key := currentAdminKey()
	if key == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+key)) != 1 {
		log.Printf("[配置编辑] traceId=%s 阶段=锁内鉴权 结果=拒绝 状态码=401 说明=管理Key已变化，未读取或写入配置", trace)
		writeJSONError(w, http.StatusUnauthorized, "管理 Key 已变化，请重新登录")
		return
	}
	old, err := readConfigEditorDocument()
	if err != nil {
		writeAdminOperationError(w, r, "配置编辑加载", err)
		return
	}
	// 有效磁盘配置已修改管理 Key、但热加载尚未执行时，不能用旧会话覆盖它。
	if onDisk, err := parseRuntimeConfig(old); err == nil && !adminKeyMatches(onDisk.Gateway.AdminKey, key) {
		writeAdminOperationError(w, r, "配置编辑", errConfigChange)
		return
	}
	if r.Method == http.MethodGet {
		log.Printf("[配置编辑] traceId=%s 阶段=加载 结果=成功 字节=%d 风险已确认=true 说明=仅此管理接口返回可编辑原文，配置正文及Key不进入日志、不在浏览器持久保存", trace, len(old))
		writeJSON(w, http.StatusOK, map[string]any{
			"path": runtimeConfigFile, "content": string(old), "revision": configRevision(old),
		})
		return
	}
	if input.Revision == "" || input.Revision != configRevision(old) {
		log.Printf("[配置编辑] traceId=%s 阶段=版本校验 结果=拒绝 状态码=409 说明=配置已被其他操作修改，未覆盖新内容", trace)
		writeJSONError(w, http.StatusConflict, "配置已被其他页面或服务器编辑，请重新加载后再保存；当前修改未写入")
		return
	}
	c, err := validateConfigEditorDocument(input.Content)
	if err != nil {
		writeAdminOperationError(w, r, "配置编辑校验", err)
		return
	}
	adminKeyChanged := !adminKeyMatches(c.Gateway.AdminKey, key)
	if c.Gateway.AdminKey == "" && !input.ConfirmAdminKeyRemoval {
		writeAdminOperationError(w, r, "配置编辑校验", gatewayValidationError("清空 gateway.adminKey 会使管理登录不可用，必须额外确认；可在服务器本机重新设置"))
		return
	}
	if r.Context().Err() != nil {
		log.Printf("[配置编辑] traceId=%s 阶段=保存前检查 结果=取消 说明=请求已取消，配置未写入", trace)
		writeJSONError(w, http.StatusRequestTimeout, "请求已取消，配置未写入")
		return
	}
	// 外部编辑不受进程内配置锁约束；完整校验后再次核对磁盘，
	// 避免校验较大配置期间发生的服务器编辑被覆盖。
	current, err := readConfigEditorDocument()
	if err != nil {
		writeAdminOperationError(w, r, "配置编辑保存前检查", err)
		return
	}
	if configRevision(current) != input.Revision {
		log.Printf("[配置编辑] traceId=%s 阶段=保存前版本复查 结果=拒绝 状态码=409 说明=校验期间磁盘配置已变化，未覆盖新内容", trace)
		writeJSONError(w, http.StatusConflict, "配置在校验期间被修改，请重新加载后再保存；当前修改未写入")
		return
	}
	var doc map[string]json.RawMessage
	_ = json.Unmarshal([]byte(input.Content), &doc) // 已由完整配置校验保证单个对象和字段类型。
	log.Printf("[配置编辑] traceId=%s 阶段=保存 旧字节=%d 新字节=%d 管理Key变化=%t API鉴权启用=%t 配置API端口=%d 配置Web端口=%d 说明=校验通过，先落盘再热应用；不记录JSON正文或Key", trace, len(old), len(input.Content), adminKeyChanged, c.Gateway.APIKeyEnabled, c.Gateway.APIPort, c.Gateway.WebPort)
	if err := persistRuntimeDocument(doc, old, trace); err != nil {
		log.Printf("[配置编辑] traceId=%s 阶段=落盘或应用 结果=失败 异常类型=%T 原因=%s 正文已省略=true", trace, err, safeDebugError(err))
		writeAdminOperationError(w, r, "配置编辑保存", errors.New("配置保存或应用失败，请检查文件权限及服务器日志；未完成更新"))
		return
	}
	settings := gatewaySettingsView()
	log.Printf("[配置编辑] traceId=%s 结果=保存成功 管理Key变化=%t 管理Key清空=%t 需重启=%t 说明=后续请求使用新配置；当前监听端口及已有请求不变，原文未记录", trace, adminKeyChanged, c.Gateway.AdminKey == "", settings["restartRequired"])
	writeJSON(w, http.StatusOK, map[string]any{
		"saved": true, "revision": hex.EncodeToString(runtimeAppliedHash[:]),
		"adminKeyChanged": adminKeyChanged, "adminKeyCleared": c.Gateway.AdminKey == "",
		"settings": settings,
	})
}
