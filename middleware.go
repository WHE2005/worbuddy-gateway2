package main

import (
	"context"
	"github.com/google/uuid"
	"log"
	"net/http"
	"strings"
	"time"
)

type auditResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *auditResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *auditResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(data)
}

func (w *auditResponseWriter) Flush() {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func requestAuditMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		traceID := r.Header.Get("X-Trace-ID")
		if traceID == "" {
			traceID = uuid.NewString()
		}
		w.Header().Set("X-Trace-ID", traceID)
		// 普通落盘审计也需要稳定的 TraceID / requestID；不能依赖可选 JSON 日志。
		ensureDebugRequestContext(r, traceID, start)
		r = r.WithContext(context.WithValue(r.Context(), statsRequestContextKey{}, &statsAttempt{}))
		if debugLoggingEnabled() {
			debugEvent(r, "info", "request_received", map[string]any{
				"message": "请求已进入网关，准备执行中间件与路由处理",
			})
		}
		aw := &auditResponseWriter{ResponseWriter: w}
		log.Printf("[请求到达] traceId=%s 方法=%s 路径=%s 来源=%s 说明=请求已进入网关", traceID, r.Method, r.URL.Path, r.RemoteAddr)
		next.ServeHTTP(aw, r)
		if aw.status == 0 {
			aw.status = http.StatusOK
		}
		statsFinishAttempt(r, aw.status)
		debugEvent(r, "info", "response_returned", map[string]any{
			"status_code": aw.status,
			"result":      http.StatusText(aw.status),
		})
		log.Printf("[响应返回] traceId=%s 状态码=%d 耗时=%v 结果=%s", traceID, aw.status, time.Since(start), http.StatusText(aw.status))
	})
}

// -----------------------------------------------------------------------------
// 中间件: CORS 与可选 APIKey 校验
// -----------------------------------------------------------------------------

// corsMiddleware 不再下发 Access-Control-Allow-Origin: *：本网关的客户端是 CLI/SDK，
// 不依赖浏览器 CORS；放开通配等于让任意网页获得跨域读取本机网关响应的能力。
// 预检请求仍直接短路与业务隔离；将来如需从浏览器应用跨域调用，再按来源白名单显式开放。
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			debugEvent(r, "info", "cors_preflight_completed", map[string]any{
				"result":  "short_circuited",
				"message": "预检请求未进入业务逻辑；网关不下发通配 CORS 头",
			})
			log.Printf("[中间件-CORS] traceId=%s 结果=短路 说明=预检请求未进入业务方法，未下发通配CORS头", w.Header().Get("X-Trace-ID"))
			w.WriteHeader(http.StatusOK)
			return
		}
		debugEvent(r, "debug", "cors_check_passed", map[string]any{"result": "no_wildcard_cors"})
		log.Printf("[中间件-CORS] traceId=%s 结果=通过 说明=本机网关不下发通配CORS头", w.Header().Get("X-Trace-ID"))
		next.ServeHTTP(w, r)
	})
}

// API 端口只保留原有健康与根路径免鉴权；管理端口使用独立鉴权链。
func isAuthExemptPath(p string) bool {
	return p == "/health" || p == "/ping" || p == "/"
}

// apiKeyScopeContextKey 携带命中 Key 的可用模型范围（nil/缺省 = 不限制）。
type apiKeyScopeContextKey struct{}

// apiKeyModelScope 返回当前请求命中的 API Key 的可用模型范围；nil/空 = 不限制。
func apiKeyModelScope(r *http.Request) []string {
	scope, _ := r.Context().Value(apiKeyScopeContextKey{}).([]string)
	return scope
}

// modelInScope 判断模型是否在 Key 的可用范围内（忽略大小写与常见别名差异）。
func modelInScope(scope []string, model string) bool {
	m := normalizeModelName(model)
	for _, allowed := range scope {
		if normalizeModelName(allowed) == m {
			return true
		}
	}
	return false
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := currentAPIKey()
		if apiKey != "" && !isAuthExemptPath(r.URL.Path) {
			authHeader := r.Header.Get("Authorization")
			token := strings.TrimPrefix(authHeader, "Bearer ")
			// Anthropic 客户端（Claude Code 等）用 x-api-key 头携带密钥。
			if token == "" {
				token = r.Header.Get("x-api-key")
			}
			entry, ok := matchAPIKey(token)
			if !ok {
				debugEvent(r, "warn", "authentication_rejected", map[string]any{
					"status_code":     http.StatusUnauthorized,
					"reason":          "invalid_api_key",
					"business_impact": "请求未进入业务逻辑",
				})
				log.Printf("[请求被拦截] traceId=%s 拦截层=API鉴权 结果=拒绝 原因=未提供有效API密钥 返回状态码=401 业务影响=请求未进入业务方法", w.Header().Get("X-Trace-ID"))
				writeUpstreamError(w, r, http.StatusUnauthorized, "invalid_api_key", "未提供有效 API 密钥")
				return
			}
			if len(entry.Models) > 0 {
				r = r.WithContext(context.WithValue(r.Context(), apiKeyScopeContextKey{}, entry.Models))
			}
		}
		debugEvent(r, "debug", "authentication_passed", map[string]any{"result": "allowed"})
		log.Printf("[中间件-鉴权] traceId=%s 结果=通过", w.Header().Get("X-Trace-ID"))
		next.ServeHTTP(w, r)
	})
}
