package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"time"

	"github.com/google/uuid"
)

// atomicWriteFile 以「同目录临时文件 + fsync + 改名」原子落盘：进程被强杀或断电时，
// 目标文件要么是完整旧内容、要么是完整新内容，绝不出现写一半的中间态。
// 凭据文件是唯一 refreshToken 副本、config.json 解析失败会导致启动退出，两者都必须原子替换。
// 临时文件固定用 path+".tmp" 命名，reset 与启动清理按该后缀回收孤儿临时文件；
// 调用方需自行串行化对同一路径的并发写入（各写盘点已有各自的锁）。
// 注意：替换前调用方必须关闭目标文件自身的句柄——Go 在 Windows 上打开的句柄
// 不含 FILE_SHARE_DELETE，句柄未关时 rename 会因共享冲突失败。
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	if closeErr := f.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr == nil {
		writeErr = os.Rename(tmp, path)
	}
	if writeErr != nil {
		_ = os.Remove(tmp)
		return writeErr
	}
	return nil
}

// -----------------------------------------------------------------------------
// 以下内容原位于 safego.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

// runUntilPanic 同步运行 fn 并捕获 panic：正常返回 false；panic 时记录任务名与完整堆栈后返回 true。
// HTTP handler 的 panic 由 net/http 兜底，但后台协程 panic 会直接击穿进程——NSSM 拉起服务、
// 所有进行中的流式响应中断。这里是后台任务对进程的唯一防线。
func runUntilPanic(name string, fn func()) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[后台任务异常] 任务=%s 结果=捕获panic 说明=进程继续运行，进行中的模型请求不受影响 堆栈=%s", name, debug.Stack())
			panicked = true
		}
	}()
	fn()
	return false
}

// safeGoLoop 运行常驻后台循环：panic 时退避 15s 重启该循环；正常返回（如功能被配置关闭、
// shutdown 时 ctx 取消）则退出，不重启。闭包捕获的变量必须是调用点的快照。
func safeGoLoop(name string, fn func()) {
	go func() {
		for {
			if !runUntilPanic(name, fn) {
				return
			}
			time.Sleep(15 * time.Second)
		}
	}()
}

// safeGo 运行一次性后台任务并捕获 panic，进程不因后台任务异常而退出。
func safeGo(name string, fn func()) {
	go func() {
		runUntilPanic(name, fn)
	}()
}

// -----------------------------------------------------------------------------
// 以下内容原位于 upstream.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

// isAuthFailure 判断上游响应是否为授权失效（401/403 / invalid token / 登录过期等）。
func isAuthFailure(statusCode int, body string) bool {
	if statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden {
		return true
	}
	low := strings.ToLower(body)
	if strings.Contains(low, "invalid token") ||
		strings.Contains(low, "unauthorized") ||
		strings.Contains(low, "登录已过期") ||
		strings.Contains(low, "登录失效") ||
		strings.Contains(low, "token 已失效") ||
		strings.Contains(low, "authentication required") {
		return true
	}
	return false
}

var resetTimeRe = regexp.MustCompile(`将在\s*(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s*(UTC[+-]\d+(?::\d{2})?)?`)

// resetTimeGenericRe 兜底匹配任意「日期 + 时间(可选 UTC 偏移)」片段，
// 用于国际站等英文限流消息（如 "will reset at 2026-09-05 01:57:00 UTC+8"）。
var resetTimeGenericRe = regexp.MustCompile(`(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\s*(UTC[+-]\d+(?::\d{2})?))?`)

// parseResetTime 从上游 429 错误消息中解析频率限制重置时间。
// 国内站示例: "您的使用量已超出频率限制，将在 2026-09-04 07:48:15 UTC+8 重置"
// 国际站示例: "Your usage has exceeded the rate limit. It will reset at 2026-09-05 01:57:00 UTC+8."
func parseResetTime(s string) (time.Time, bool) {
	m := resetTimeRe.FindStringSubmatch(s)
	if m == nil {
		m = resetTimeGenericRe.FindStringSubmatch(s)
	}
	if m == nil {
		return time.Time{}, false
	}
	return parseDateTimeTZ(m[1], m[2], m[3])
}

// parseDateTimeTZ 按「日期 时间 (可选 UTC 偏移)」解析时间；未提供时区时默认按 UTC+8。
func parseDateTimeTZ(date, clock, zone string) (time.Time, bool) {
	offset := 8 * 3600 // 默认按 UTC+8 解析
	if zone != "" {
		z := zone
		z = strings.TrimPrefix(z, "UTC")
		z = strings.TrimPrefix(z, "utc")
		var h, mi int
		if _, err := fmt.Sscanf(z, "%d:%d", &h, &mi); err == nil {
			offset = h*3600 + mi*60
		} else if _, err := fmt.Sscanf(z, "%d", &h); err == nil {
			offset = h * 3600
		}
	}
	loc := time.FixedZone("UTC", offset)
	t, err := time.ParseInLocation("2006-01-02 15:04:05", date+" "+clock, loc)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// isRateLimited 判断上游响应是否属于频率限制（429 或 code 6004 / 频率限制提示，含中英文）。
func isRateLimited(statusCode int, body string) bool {
	if statusCode == http.StatusTooManyRequests {
		return true
	}
	if strings.Contains(body, `"code":6004`) || strings.Contains(body, "频率限制") || strings.Contains(body, "frequency limit") {
		return true
	}
	low := strings.ToLower(body)
	if strings.Contains(low, "rate limit") || strings.Contains(low, "ratelimit") || strings.Contains(low, "too many requests") {
		return true
	}
	return false
}

// isModelUnavailable 判断上游是否明确表示“该模型在当前站点不存在/不可用”。
// 这类错误常带“请切换模型”的提示，不能当成模型级限流，否则会反复探测并误判为暂时限流。
func isModelUnavailable(body string) bool {
	low := strings.ToLower(body)
	return strings.Contains(body, `"code":11102`) ||
		strings.Contains(low, "service info not found") ||
		strings.Contains(low, "model unavailable") ||
		strings.Contains(body, "模型不可用") ||
		strings.Contains(body, "模型不存在")
}

func isModelRateLimited(body string) bool {
	if isModelUnavailable(body) {
		return false
	}
	low := strings.ToLower(body)
	return strings.Contains(body, `"code":6004`) || strings.Contains(body, "切换其他模型") || strings.Contains(low, "switch to another model")
}

func isQuotaExhausted(statusCode int, body string) bool {
	if statusCode != http.StatusTooManyRequests && !strings.Contains(body, `"code":14018`) {
		return false
	}
	low := strings.ToLower(body)
	return strings.Contains(body, `"code":14018`) || strings.Contains(body, "额度已用尽") || strings.Contains(low, "credits exhausted")
}

// -----------------------------------------------------------------------------
// 以下内容原位于 middleware.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

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
