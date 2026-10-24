package main

import (
	"errors"
	"log"
	"net/http"
)

// handleWebUIAPIKeys 管理台的模型 API Key 列表管理。
// GET 返回全部 Key（含明文——与配置编辑同一安全模型：仅管理鉴权下可达，no-store，不入审计正文）；
// POST 全量替换保存（前端整表编辑后提交），经 normalize 校验后原子落盘并热生效。
func handleWebUIAPIKeys(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeAdminData(w, r, apiKeyListView())
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
		writeAdminData(w, r, apiKeyListView())
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
	g.Keys = keys
	// 显式保存列表后旧单 Key 字段完成迁移使命：置空避免双入口混淆；
	// 对从未保存过列表的旧配置，读取时的自动迁移仍然生效。
	g.APIKey = ""
	if _, err := normalizeGatewayConfig(g); err != nil {
		return err
	}
	return persistGatewayConfigLocked(doc, g, old, trace)
}
