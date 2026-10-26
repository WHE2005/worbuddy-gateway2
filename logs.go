package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"
)

var logRetentionDays atomic.Int64
var activeLogsMu sync.Mutex
var activeLogs = map[string]int{}
var logCleanupMu sync.Mutex
var cleanupScheduleMu sync.Mutex
var lastLogCleanup struct {
	time time.Time
	days int
}

// 写入时按本地日期切换文件。与清理共用打开文件登记，不能删除正在写入的旧日志。
type dailyLogWriter struct {
	mu                sync.Mutex
	prefix, extension string
	file              *os.File
	path, day         string
	closed            bool
}

func newDailyLogWriter(prefix, extension string) (*dailyLogWriter, error) {
	w := &dailyLogWriter{prefix: prefix, extension: extension}
	if err := w.rotate(time.Now()); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *dailyLogWriter) rotate(now time.Time) error {
	day := now.Format("2006-01-02")
	if w.file != nil && day == w.day {
		return nil
	}
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	path, err := filepath.Abs(filepath.Join(logDir, w.prefix+"-"+day+w.extension))
	if err != nil {
		return err
	}
	activeLogsMu.Lock()
	defer activeLogsMu.Unlock()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	activeLogs[path]++
	if w.file != nil {
		_ = w.file.Close()
		activeLogs[w.path]--
		if activeLogs[w.path] == 0 {
			delete(activeLogs, w.path)
		}
	}
	w.file, w.path, w.day = f, path, day
	return nil
}

func (w *dailyLogWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, os.ErrClosed
	}
	if err := w.rotate(time.Now()); err != nil {
		return 0, err
	}
	return w.file.Write(data)
}

func (w *dailyLogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	activeLogsMu.Lock()
	defer activeLogsMu.Unlock()
	activeLogs[w.path]--
	if activeLogs[w.path] == 0 {
		delete(activeLogs, w.path)
	}
	return w.file.Close()
}

// managedLogPattern 受管日志白名单：网关自有日志之外，同时覆盖 build 审计日志与
// NSSM 按大小轮转出的 service-stdout/stderr 历史（NSSM 只轮转不删除，没有代码兜底会无限堆积）。
var managedLogPattern = regexp.MustCompile(`^(gateway-\d{4}-\d{2}-\d{2}\.log|debug-\d{4}-\d{2}-\d{2}\.jsonl|deployment-\d{4}-\d{2}-\d{2}\.jsonl|frontend-\d{4}-\d{2}-\d{2}\.log|build-\d{4}-\d{2}-\d{2}\.jsonl|service-(?:stdout|stderr)-\d{8}T\d{6}\.\d{3}\.log)$`)
var logDatePattern = regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)

// NSSM 轮转文件名的时间戳形如 20261019T122350（本地时间）。
var nssmLogDatePattern = regexp.MustCompile(`\d{8}T\d{6}`)

type logCleanupResult struct {
	RetentionDays int      `json:"retentionDays"`
	Deleted       int      `json:"deleted"`
	DeletedBytes  int64    `json:"deletedBytes"`
	SkippedActive int      `json:"skippedActive"`
	Failed        int      `json:"failed"`
	Files         []string `json:"files"`
}

func cleanupLogs(now time.Time, days int, trace string, schedule ...time.Time) (result logCleanupResult, resultErr error) {
	next := time.Time{}
	if len(schedule) > 0 {
		next = schedule[0]
	} else {
		cleanupScheduleMu.Lock()
		if !lastLogCleanup.time.IsZero() {
			next = lastLogCleanup.time.Add(time.Hour)
		}
		cleanupScheduleMu.Unlock()
	}
	started := time.Now()
	logScheduledTask("log_cleanup", "started", "running", trace, started, next, "")
	result = logCleanupResult{RetentionDays: days, Files: []string{}}
	defer func() {
		status := "success"
		if resultErr != nil {
			status = "failed"
		} else if days == 0 {
			status = "skipped"
		} else if result.Failed > 0 {
			status = "partial"
		}
		detail := fmt.Sprintf("保留天数=%d，删除=%d，失败=%d，跳过正在写入=%d", days, result.Deleted, result.Failed, result.SkippedActive)
		if resultErr != nil {
			detail = safeDebugError(resultErr)
		}
		logScheduledTask("log_cleanup", "completed", status, trace, started, next, detail)
	}()
	if days < 0 || days > 36500 {
		return result, gatewayValidationError("日志保留天数无效")
	}
	if days == 0 {
		log.Printf("[日志清理] traceId=%s 结果=跳过 保留天数=0 说明=未删除任何日志，0表示保留全部", trace)
		return result, nil
	}
	logCleanupMu.Lock()
	defer logCleanupMu.Unlock()
	cutoff := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()).AddDate(0, 0, -(days - 1))
	directoryInfo, err := os.Lstat(logDir)
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	if !directoryInfo.IsDir() || directoryInfo.Mode()&os.ModeSymlink != 0 {
		return result, errors.New("日志目录不是普通目录或是符号链接，拒绝清理")
	}
	entries, err := os.ReadDir(logDir)
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	root, err := os.OpenRoot(logDir)
	if err != nil {
		return result, err
	}
	defer root.Close()
	log.Printf("[日志清理] traceId=%s 阶段=扫描 保留天数=%d 最早保留日期=%s 文件数=%d 说明=仅清理命名受限的历史日志，不跟随符号链接，不触及配置和凭据",
		trace, days, cutoff.Format("2006-01-02"), len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !entry.Type().IsRegular() || !managedLogPattern.MatchString(name) {
			continue
		}
		var date time.Time
		var parseErr error
		if stamp := logDatePattern.FindString(name); stamp != "" {
			date, parseErr = time.ParseInLocation("2006-01-02", stamp, now.Location())
		} else if stamp := nssmLogDatePattern.FindString(name); stamp != "" {
			date, parseErr = time.ParseInLocation("20060102T150405", stamp, now.Location())
		} else {
			parseErr = errors.New("文件名不含可识别时间戳")
		}
		if parseErr != nil || !date.Before(cutoff) {
			continue
		}
		fullPath, _ := filepath.Abs(filepath.Join(logDir, name))
		activeLogsMu.Lock()
		if activeLogs[fullPath] > 0 {
			activeLogsMu.Unlock()
			result.SkippedActive++
			continue
		}
		info, statErr := entry.Info()
		if statErr == nil && !info.Mode().IsRegular() {
			activeLogsMu.Unlock()
			continue
		}
		removeErr := statErr
		if statErr == nil {
			removeErr = root.Remove(name)
		}
		activeLogsMu.Unlock()
		if removeErr != nil {
			result.Failed++
			log.Printf("[日志清理] traceId=%s 文件=%q 结果=删除失败 原因=%s", trace, name, safeDebugError(removeErr))
			continue
		}
		result.Deleted++
		result.DeletedBytes += info.Size()
		result.Files = append(result.Files, name)
		log.Printf("[日志清理] traceId=%s 文件=%q 文件日期=%s 字节=%d 结果=已删除 原因=超出保留天数", trace, name, date.Format("2006-01-02"), info.Size())
	}
	log.Printf("[日志清理] traceId=%s 结果=完成 已删除=%d 回收字节=%d 跳过正在写入=%d 失败=%d", trace, result.Deleted, result.DeletedBytes, result.SkippedActive, result.Failed)
	return result, nil
}

func scheduledLogCleanup(now time.Time) {
	days := int(logRetentionDays.Load())
	cleanupScheduleMu.Lock()
	previousDays := lastLogCleanup.days
	due := days > 0 && (lastLogCleanup.days != days || now.Sub(lastLogCleanup.time) >= time.Hour)
	lastLogCleanup.days = days
	if due {
		lastLogCleanup.time = now
	}
	cleanupScheduleMu.Unlock()
	if days == 0 && previousDays != 0 {
		logScheduledTask("log_cleanup", "disabled", "disabled", newTraceID(), time.Time{}, time.Time{}, "保留天数已改为0，自动清理关闭")
	}
	if !due {
		return
	}
	trace := newTraceID()
	if _, err := cleanupLogs(now, days, trace, now.Add(time.Hour)); err != nil {
		log.Printf("[日志清理] traceId=%s 阶段=自动任务 结果=失败 原因=%s 说明=保留现有日志，下个周期重试", trace, safeDebugError(err))
	}
}

func saveLogRetention(days int, trace string) error {
	if days < 0 || days > 36500 {
		return gatewayValidationError("日志保留天数必须为 0～36500，0 表示不自动清理")
	}
	runtimeConfigMu.Lock()
	defer runtimeConfigMu.Unlock()
	gatewayConfigMu.Lock()
	doc, gateway, old, err := readGatewayDocumentLocked()
	if err == nil && (cfg.AdminKey == "" || !adminKeyMatches(gateway.AdminKey, cfg.AdminKey)) {
		err = errConfigChange
	}
	gatewayConfigMu.Unlock()
	if err != nil {
		return err
	}
	doc["logs"], _ = json.Marshal(map[string]int{"retentionDays": days})
	log.Printf("[日志设置] traceId=%s 阶段=配置写入 旧保留天数=%d 新保留天数=%d 说明=其他配置段原样保留", trace, logRetentionDays.Load(), days)
	return persistRuntimeDocument(doc, old, trace)
}

func handleWebUILogSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		log.Printf("[日志设置] traceId=%s 阶段=读取 保留天数=%d 结果=成功", debugTraceID(r), logRetentionDays.Load())
		writeJSON(w, http.StatusOK, map[string]any{"retentionDays": logRetentionDays.Load()})
	case http.MethodPost:
		var input struct {
			Days *int `json:"retentionDays"`
		}
		if err := decodeAdminJSON(w, r, &input); err != nil || input.Days == nil {
			log.Printf("[日志设置] traceId=%s 阶段=参数校验 结果=拒绝 原因=保留天数缺失或字段格式无效 状态码=400", debugTraceID(r))
			writeJSONError(w, http.StatusBadRequest, "必须提供 retentionDays 整数")
			return
		}
		if err := saveLogRetention(*input.Days, debugTraceID(r)); err != nil {
			writeAdminOperationError(w, r, "日志设置", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"retentionDays": *input.Days, "saved": true})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "仅支持 GET/POST")
	}
}

func handleWebUILogCleanup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	var input map[string]json.RawMessage
	if err := decodeAdminJSON(w, r, &input); err != nil || input == nil || len(input) != 0 {
		log.Printf("[日志清理] traceId=%s 阶段=参数校验 结果=拒绝 原因=请求JSON无效 状态码=400", debugTraceID(r))
		writeJSONError(w, http.StatusBadRequest, "清理请求必须为空 JSON 对象")
		return
	}
	result, err := cleanupLogs(time.Now(), int(logRetentionDays.Load()), debugTraceID(r))
	if err != nil {
		writeAdminOperationError(w, r, "日志清理", err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// -----------------------------------------------------------------------------
// 以下内容原位于 logredact.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

// 脱敏仅用于日志/调试展示。不得把脱敏后的错误送回业务分类或上游响应解析。
var sensitiveSecrets sync.Map

var (
	namedSecretPattern  = regexp.MustCompile(`(?i)(\b(?:access[_-]?token|refresh[_-]?token|admin[_-]?key|api[_-]?key|x-api-key|x-refresh-token|authorization)\b["']?\s*[:=]\s*)("(?:\\.|[^"\\])*"|'[^']*'|(?:Bearer[ \t]+)?[^\s,;}\[\]]+)`)
	bearerSecretPattern = regexp.MustCompile(`(?i)(\bBearer[ \t]+)[A-Za-z0-9._~+/=-]+`)
	// Cookie 头与 URL 查询参数里的会话凭据：未经 registerSecrets 注册的不透明
	// token（如上游下发的 Set-Cookie、OAuth code）走结构化规则兜底。
	cookieHeaderPattern = regexp.MustCompile(`(?i)\b(set-cookie|cookie)\s*:\s*[^\r\n]+`)
	queryTokenPattern   = regexp.MustCompile(`(?i)([?&#](?:signature|sessionid|session|access_token|refresh_token|api_?key|token|ticket|code|sign|auth)=)[^&\s"']+`)
)

func registerSecrets(values ...string) {
	for _, value := range values {
		if value != "" {
			sensitiveSecrets.Store(value, struct{}{})
		}
	}
}

func registerCredentialSecrets(auth *StoredAuth) {
	if auth != nil {
		registerSecrets(auth.Auth.AccessToken, auth.Auth.RefreshToken)
	}
}

func redactSensitiveText(text string) string {
	var secrets []string
	sensitiveSecrets.Range(func(key, _ any) bool {
		value := key.(string)
		secrets = append(secrets, value)
		// 错误正文可能是 JSON 字符串；同时隐藏其转义后的表示。
		escaped := strconv.Quote(value)
		escaped = escaped[1 : len(escaped)-1]
		if escaped != value {
			secrets = append(secrets, escaped)
		}
		return true
	})
	// 长值优先、一次替换，避免重叠秘密值导致部分泄漏或改写替换标记。
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	var pairs []string
	for _, secret := range secrets {
		if len(secret) >= 8 {
			pairs = append(pairs, secret, "[REDACTED]")
		}
	}
	if len(pairs) > 0 {
		text = strings.NewReplacer(pairs...).Replace(text)
	}
	for _, secret := range secrets {
		if len(secret) < 8 {
			text = redactShortSecret(text, secret)
		}
	}
	text = namedSecretPattern.ReplaceAllStringFunc(text, func(match string) string {
		parts := namedSecretPattern.FindStringSubmatch(match)
		value := "[REDACTED]"
		if parts[2][0] == '"' {
			value = `"[REDACTED]"`
		} else if parts[2][0] == '\'' {
			value = "'[REDACTED]'"
		}
		return parts[1] + value
	})
	text = bearerSecretPattern.ReplaceAllString(text, "${1}[REDACTED]")
	text = cookieHeaderPattern.ReplaceAllString(text, "${1}: [REDACTED]")
	text = queryTokenPattern.ReplaceAllString(text, "${1}[REDACTED]")
	return bearerLikeRe.ReplaceAllString(text, "[REDACTED]")
}

// 短秘密值仅替换完整词，避免短测试密钥吞掉普通错误原因中的单个字母。
func redactShortSecret(text, secret string) string {
	word := func(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' }
	var out strings.Builder
	for {
		index := strings.Index(text, secret)
		if index < 0 {
			out.WriteString(text)
			return out.String()
		}
		end := index + len(secret)
		left, _ := utf8.DecodeLastRuneInString(text[:index])
		right, _ := utf8.DecodeRuneInString(text[end:])
		out.WriteString(text[:index])
		if (index == 0 || !word(left)) && (end == len(text) || !word(right)) {
			out.WriteString("[REDACTED]")
		} else {
			out.WriteString(secret)
		}
		text = text[end:]
	}
}

type redactingLogWriter struct{ writer io.Writer }

func (w *redactingLogWriter) Write(data []byte) (int, error) {
	redacted := redactSensitiveText(string(data))
	n, err := io.WriteString(w.writer, redacted)
	if err == nil && n != len(redacted) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return 0, err
	}
	// io.Writer 返回消耗的原始输入长度，而不是脱敏后的输出长度。
	return len(data), nil
}

func redactDebugValue(value any) any {
	switch v := value.(type) {
	case string:
		return redactSensitiveText(v)
	case error:
		return safeDebugError(v)
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			if secretLogField(key) {
				out[key] = "[REDACTED]"
			} else {
				out[key] = redactDebugValue(item)
			}
		}
		return out
	case map[string]string:
		out := make(map[string]string, len(v))
		for key, item := range v {
			if secretLogField(key) {
				out[key] = "[REDACTED]"
			} else {
				out[key] = redactSensitiveText(item)
			}
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = redactDebugValue(item)
		}
		return out
	case []string:
		out := make([]string, len(v))
		for i, item := range v {
			out[i] = redactSensitiveText(item)
		}
		return out
	default:
		return value
	}
}

func secretLogField(key string) bool {
	key = strings.ToLower(strings.NewReplacer("_", "", "-", "").Replace(key))
	switch key {
	case "accesstoken", "refreshtoken", "adminkey", "apikey", "xapikey", "xrefreshtoken", "authorization":
		return true
	default:
		return false
	}
}

// -----------------------------------------------------------------------------
// 以下内容原位于 logtail.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

const webUILogReadBudget int64 = 1 << 20 // 仅管理日志接口，不限制模型请求或上游响应。

// readTailLines 按块倒序定位，再一次性拼接；复杂度与扫描字节数线性相关。
// 预算截断时丢弃首个不完整行，避免显示无法可靠脱敏的秘密值片段。
func readTailLines(f *os.File, n int, budget int64) ([]string, bool, error) {
	if n <= 0 {
		return []string{}, false, nil
	}
	st, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	if !st.Mode().IsRegular() {
		return nil, false, fmt.Errorf("只允许读取普通日志文件")
	}
	const chunkSize int64 = 16 << 10
	pos, scanned := st.Size(), int64(0)
	var chunks [][]byte
	newlines := 0
	for pos > 0 && newlines <= n && (budget <= 0 || scanned < budget) {
		size := min(chunkSize, pos)
		if budget > 0 {
			size = min(size, budget-scanned)
		}
		pos -= size
		chunk := make([]byte, int(size))
		count, err := f.ReadAt(chunk, pos)
		if err != nil && err != io.EOF {
			return nil, false, err
		}
		chunk = chunk[:count]
		chunks = append(chunks, chunk)
		scanned += int64(count)
		newlines += bytes.Count(chunk, []byte{'\n'})
		if count != int(size) {
			return nil, false, fmt.Errorf("日志在读取期间发生截断，请重试")
		}
	}
	truncated := pos > 0 && budget > 0 && scanned >= budget && newlines <= n
	data := make([]byte, 0, int(scanned))
	for i := len(chunks) - 1; i >= 0; i-- {
		data = append(data, chunks[i]...)
	}
	if pos > 0 {
		if end := bytes.IndexByte(data, '\n'); end >= 0 {
			data = data[end+1:]
		} else {
			data = nil
		}
	}
	lines := make([]string, 0, n)
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(line) != 0 {
			lines = append(lines, string(line))
		}
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return lines, truncated, nil
}

// -----------------------------------------------------------------------------
// 以下内容原位于 weblogfocus.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

// parseLogOffset 解析告警带来的日志定位偏移；缺省或非法时返回 -1 表示按尾部读取。
func parseLogOffset(raw string) int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return -1
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return -1
	}
	return v
}

// readWebLogAround 读取覆盖指定字节偏移的日志窗口，供「告警 → 跳到那一行」使用。
// 返回窗口内的行、目标行下标、以及是否真的定位到了（文件被轮转或清理时为 false）。
func readWebLogAround(name string, offset int64, span int64) ([]string, int, bool, error) {
	root, err := os.OpenRoot(logDir)
	if err != nil {
		return nil, -1, false, err
	}
	defer root.Close()
	f, err := root.Open(name)
	if err != nil {
		return nil, -1, false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, -1, false, err
	}
	size := info.Size()
	resolved := offset <= size
	start, end := offset-span, offset+span
	if start < 0 {
		start = 0
	}
	if end > size {
		end = size
	}
	if end-start > webUILogReadBudget {
		end = start + webUILogReadBudget
	}
	if end <= start {
		return nil, -1, resolved, nil
	}
	buf := make([]byte, end-start)
	n, readErr := f.ReadAt(buf, start)
	if readErr != nil && readErr != io.EOF {
		return nil, -1, false, readErr
	}
	buf = buf[:n]
	// 记录窗口首行在文件中的真实起点，便于把偏移换算成行下标。
	cursor := start
	if start > 0 {
		if cut := strings.IndexByte(string(buf), '\n'); cut >= 0 {
			cursor += int64(cut) + 1
			buf = buf[cut+1:]
		} else {
			buf = nil
		}
	}
	lines := strings.Split(string(buf), "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	focus := -1
	for i, line := range lines {
		next := cursor + int64(len(line)) + 1
		if offset >= cursor && offset < next {
			focus = i
			break
		}
		cursor = next
	}
	return lines, focus, resolved && focus >= 0, nil
}
