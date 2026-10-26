package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
)

// handleWebUIAPIKeys 管理台的模型 API Key 列表管理。
// GET 返回全部 Key（含明文——与配置编辑同一安全模型：仅管理鉴权下可达，no-store，不入审计正文）；
// POST 全量替换保存（前端整表编辑后提交），经 normalize 校验后原子落盘并热生效。
func handleWebUIAPIKeys(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeAPIKeysView(w, r)
	case http.MethodPost:
		var input struct {
			Keys []APIKeyEntry `json:"keys"`
		}
		if err := decodeAdminJSONLimit(w, r, &input, 256<<10); err != nil {
			log.Printf("[API Keys] traceId=%s 阶段=参数校验 结果=拒绝 状态码=400 说明=列表格式无效，正文未记录", debugTraceID(r))
			writeJSONError(w, http.StatusBadRequest, "API Key 列表格式无效")
			return
		}
		trace := debugTraceID(r)
		if err := saveAPIKeys(input.Keys, trace); err != nil {
			status := http.StatusInternalServerError
			var validation gatewayValidationError
			if errors.As(err, &validation) {
				status = http.StatusBadRequest
			}
			if errors.Is(err, errConfigChange) {
				status = http.StatusConflict
			}
			log.Printf("[API Keys] traceId=%s 结果=保存失败 状态码=%d 原因=%v 业务影响=运行时 Key 保持不变", trace, status, safeDebugError(err))
			writeJSONError(w, status, safeDebugError(err))
			return
		}
		log.Printf("[API Keys] traceId=%s 结果=保存成功 数量=%d 说明=立即生效，已有流式请求不受影响，Key 原文不进入日志", trace, len(input.Keys))
		writeAPIKeysView(w, r)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "仅支持 GET/POST")
	}
}

// apiKeyListView 管理台视图：全量 Key（含明文）+ 生效计数。
func apiKeyListView() map[string]any {
	gatewayConfigMu.RLock()
	defer gatewayConfigMu.RUnlock()
	keys := append([]APIKeyEntry(nil), cfg.APIKeys...)
	enabled := 0
	for _, k := range keys {
		if k.Enabled {
			enabled++
		}
	}
	return map[string]any{"apiKeyEnabled": cfg.APIKey != "", "enabledCount": enabled, "keys": keys}
}

// writeAPIKeysView 明文返回 Key 列表。绝不能走 writeAdminData：它的统一脱敏会把
// 已注册为日志秘密的 Key 值替换成字面量 "[REDACTED]"，前端把占位符存回就会毁掉真实 Key。
// 明文出口与凭据编辑接口同一安全模型：仅管理鉴权可达，no-store，不写日志正文。
func writeAPIKeysView(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, apiKeyListView())
}

// saveAPIKeys 全量替换 gateway.keys 并原子落盘；保存后旧单 Key 字段置空，
// 运行时 Key 立即切换（persistGatewayConfigLocked 内同步内存）。
func saveAPIKeys(keys []APIKeyEntry, trace string) error {
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
	// 防占位符污染：脱敏视图回存会把 "[REDACTED]"/"****" 当成真实 Key 写盘，毁掉原值。
	for i := range keys {
		if strings.Contains(keys[i].Key, "[REDACTED]") || strings.Contains(keys[i].Key, "****") {
			return gatewayValidationError(fmt.Sprintf("keys[%d]（%s）的 key 是脱敏占位符，不能保存；请填写真实 Key 或留空由网页自动生成", i, keys[i].Name))
		}
	}
	g.Keys = keys
	// 显式保存列表后旧单 Key 字段完成迁移使命：置空避免双入口混淆；
	// 对从未保存过列表的旧配置，读取时的自动迁移仍然生效。
	g.APIKey = ""
	if _, err := normalizeGatewayConfig(g); err != nil {
		return err
	}
	return persistGatewayConfigLocked(doc, g, old, trace)
}

// -----------------------------------------------------------------------------
// 以下内容原位于 adminoperations.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

// 调用方持有 runtimeConfigMu；先校验、落盘，再应用，失败恢复旧配置。
func persistRuntimeDocument(doc map[string]json.RawMessage, old []byte, trace string) error {
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	c, err := parseRuntimeConfig(data)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := writeConfigPreservingFile(data, old); err != nil {
		return err
	}
	if err := applyRuntimeConfig(c, true, trace); err != nil {
		if restoreErr := writeConfigPreservingFile(old, data); restoreErr != nil {
			return errors.New("配置应用失败且旧配置无法恢复，请检查服务器日志")
		}
		return err
	}
	runtimeAppliedHash, runtimeHasAppliedHash = sha256.Sum256(data), true
	return nil
}

func writeAdminOperationError(w http.ResponseWriter, r *http.Request, operation string, err error) {
	status := http.StatusInternalServerError
	var validation gatewayValidationError
	if errors.As(err, &validation) {
		status = http.StatusBadRequest
	}
	if errors.Is(err, errConfigChange) || errors.Is(err, errCredentialChanged) {
		status = http.StatusConflict
	}
	log.Printf("[管理操作失败] traceId=%s 操作=%s 状态码=%d 原因=%s 说明=未成功修改，页面应保留用户输入供修正或重试", debugTraceID(r), operation, status, safeDebugError(err))
	writeJSONError(w, status, safeDebugError(err))
}
