package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// -----------------------------------------------------------------------------
// 模型级统计（/v1/models 附表）
//
// 记录每个模型被请求的次数、成功/失败、上游 usage token 累计、首字响应时间（TTFT）、平均耗时、
// 最近请求时间、最近状态，以及该模型当前可服务账号数。
// 统计只服务于 monitor 展示，不参与调度决策。
// -----------------------------------------------------------------------------

// statsWindow 是"平均首字 / 平均总耗时"的滚动统计窗口。
const statsWindow = 5 * time.Hour

// hourlyBucket 保存某个整点小时内累计的 TTFT / 耗时样本。
type hourlyBucket struct {
	Hour           int64 // 该小时起点（Unix 秒）
	TTFTSum        time.Duration
	TTFTSamples    int64
	LatencySum     time.Duration
	LatencySamples int64
}

type modelStat struct {
	Requests      int64
	Success       int64
	Failed        int64
	Tokens        int64 // 上游 usage 累计的 total_tokens（无 usage 的请求不计入）
	LastRequestAt time.Time
	LastStatus    string
	// 最近 statsWindow 内按小时分桶的延迟样本（自动淘汰过期桶）。
	Buckets []hourlyBucket
	// 站点维度的免费/收费观测：同一个模型名在 cn 与 intl 可能一个免费一个收费。
	CNFreeSeen   bool
	CNPaidSeen   bool
	IntlFreeSeen bool
	IntlPaidSeen bool
}

var (
	modelStatsMu sync.Mutex
	modelStats   = map[string]*modelStat{}
)

func hourStart(t time.Time) int64 {
	return t.Truncate(time.Hour).Unix()
}

// bucketLocked 返回当前小时的桶，并淘汰窗口外的旧桶。
func (st *modelStat) bucketLocked(now time.Time) *hourlyBucket {
	cur := hourStart(now)
	// 淘汰超出窗口的桶
	kept := st.Buckets[:0]
	for _, b := range st.Buckets {
		if now.Unix()-b.Hour < int64(statsWindow/time.Second) {
			kept = append(kept, b)
		}
	}
	st.Buckets = kept
	for i := range st.Buckets {
		if st.Buckets[i].Hour == cur {
			return &st.Buckets[i]
		}
	}
	st.Buckets = append(st.Buckets, hourlyBucket{Hour: cur})
	return &st.Buckets[len(st.Buckets)-1]
}

// windowAveragesLocked 汇总窗口内的平均首字与平均总耗时。
func (st *modelStat) windowAveragesLocked(now time.Time) (ttft time.Duration, hasTTFT bool, latency time.Duration, hasLatency bool) {
	var tSum, lSum time.Duration
	var tN, lN int64
	cutoff := now.Unix() - int64(statsWindow/time.Second)
	for _, b := range st.Buckets {
		if b.Hour < cutoff {
			continue
		}
		tSum += b.TTFTSum
		tN += b.TTFTSamples
		lSum += b.LatencySum
		lN += b.LatencySamples
	}
	if tN > 0 {
		ttft, hasTTFT = tSum/time.Duration(tN), true
	}
	if lN > 0 {
		latency, hasLatency = lSum/time.Duration(lN), true
	}
	return
}

func modelStatLocked(model string) *modelStat {
	model = normalizeModelName(model)
	if model == "" {
		model = "(默认)"
	}
	stat := modelStats[model]
	if stat == nil {
		stat = &modelStat{}
		modelStats[model] = stat
	}
	return stat
}

func recordModelRequest(model string) {
	modelStatsMu.Lock()
	stat := modelStatLocked(model)
	stat.Requests++
	stat.LastRequestAt = time.Now()
	modelStatsMu.Unlock()
}

func recordModelSuccess(model string) {
	modelStatsMu.Lock()
	stat := modelStatLocked(model)
	stat.Success++
	stat.LastStatus = "成功"
	modelStatsMu.Unlock()
}

func recordModelFailure(model, reason string) {
	modelStatsMu.Lock()
	stat := modelStatLocked(model)
	stat.Failed++
	stat.LastStatus = "失败:" + reason
	modelStatsMu.Unlock()
}

func recordModelTTFT(model string, d time.Duration) {
	if d <= 0 {
		return
	}
	modelStatsMu.Lock()
	stat := modelStatLocked(model)
	b := stat.bucketLocked(time.Now())
	b.TTFTSum += d
	b.TTFTSamples++
	modelStatsMu.Unlock()
}

func recordModelLatency(model string, d time.Duration) {
	if d <= 0 {
		return
	}
	modelStatsMu.Lock()
	stat := modelStatLocked(model)
	b := stat.bucketLocked(time.Now())
	b.LatencySum += d
	b.LatencySamples++
	modelStatsMu.Unlock()
}

// recordModelTokens 把一次成功响应的上游 usage token 数累加到该模型。
// 优先 usage.total_tokens；没有则用 prompt_tokens + completion_tokens。
// 上游没给 usage 时不估算，避免把猜的数字写进表里。
func recordModelTokens(model string, usage map[string]any, reqID uint64) {
	direct, hasDirect := usageNumber(usage, "total_tokens")
	prompt, hasPrompt := usageNumber(usage, "prompt_tokens")
	completion, hasCompletion := usageNumber(usage, "completion_tokens")
	n, ok := usageTotalTokens(usage)
	if !ok || n <= 0 {
		log.Printf("[模型Token累计] requestId=%d 模型=%s 结果=跳过 原因=上游usage无有效token 有total=%v 有prompt=%v 有completion=%v 业务影响=本次不计入总消耗",
			reqID, normalizeModelName(model), hasDirect, hasPrompt, hasCompletion)
		return
	}
	source := "usage.total_tokens"
	if !hasDirect {
		source = fmt.Sprintf("prompt_tokens(%d)+completion_tokens(%d)", prompt, completion)
	}
	modelStatsMu.Lock()
	stat := modelStatLocked(model)
	before := stat.Tokens
	stat.Tokens += n
	after := stat.Tokens
	modelStatsMu.Unlock()
	log.Printf("[模型Token累计] requestId=%d 模型=%s 来源=%s 本次token=%d 累计前=%d 累计后=%d 展示=%s 说明=已按上游usage计入总消耗",
		reqID, normalizeModelName(model), source, n, before, after, formatTokensM(after))
	if hasDirect && (hasPrompt || hasCompletion) && direct != prompt+completion {
		log.Printf("[模型Token累计] requestId=%d 模型=%s 分支=total与分项不一致 判断结果=仍采用total_tokens 原因=上游同时给了total和分项但不相等 total=%d prompt=%d completion=%d 合计=%d",
			reqID, normalizeModelName(model), direct, prompt, completion, prompt+completion)
	}
}

// recordModelCostClass 记录一次免费/收费观测，按站点分别累计。
func recordModelCostClass(model, edition string, free bool) {
	modelStatsMu.Lock()
	stat := modelStatLocked(model)
	if edition == "intl" {
		if free {
			stat.IntlFreeSeen = true
		} else {
			stat.IntlPaidSeen = true
		}
	} else {
		if free {
			stat.CNFreeSeen = true
		} else {
			stat.CNPaidSeen = true
		}
	}
	modelStatsMu.Unlock()
}

// TTFT v2：网关请求入口 → 首个有效内容/推理/工具调用增量被解析。
// 按 dsh 的 token-delta 规则，不计角色/心跳/usage/finish等控制帧。
const ttftMetric = "first_token_v2"

type ttftReader struct {
	r        io.Reader
	start    time.Time
	once     sync.Once
	d        time.Duration
	complete bool
}

func newTTFTReader(r io.Reader, start time.Time) *ttftReader {
	return &ttftReader{r: r, start: start}
}

func (t *ttftReader) Read(p []byte) (int, error) {
	return t.r.Read(p) // 原始字节不代表Token，禁止在这里计时。
}

func (t *ttftReader) duration() time.Duration { return t.d }

func (t *ttftReader) text() string {
	if t.d <= 0 {
		return "未知（无有效Token）"
	}
	return t.d.String()
}

func hasFirstTokenDelta(chunk map[string]any) bool {
	choices, _ := chunk["choices"].([]any)
	for _, raw := range choices {
		choice, _ := raw.(map[string]any)
		delta, _ := choice["delta"].(map[string]any)
		for _, field := range []string{"content", "reasoning_content", "reasoning"} {
			// 与dsh相同：空字符串不算，但非空空白字符串是有效增量。
			if text, ok := delta[field].(string); ok && text != "" {
				return true
			}
		}
		calls, _ := delta["tool_calls"].([]any)
		if legacy, ok := delta["function_call"].(map[string]any); ok {
			calls = append(calls, map[string]any{"function": legacy})
		}
		for _, rawCall := range calls {
			call, _ := rawCall.(map[string]any)
			fn, _ := call["function"].(map[string]any)
			if _, named := fn["name"].(string); named {
				return true
			}
			if args, ok := fn["arguments"].(string); ok && args != "" {
				return true
			}
		}
	}
	return false
}

func observeFirstToken(reader io.Reader, chunk map[string]any) {
	if t, ok := reader.(*ttftReader); ok && hasFirstTokenDelta(chunk) {
		t.once.Do(func() { t.d = time.Since(t.start) })
	}
}

func completeTokenStream(reader io.Reader, complete bool) {
	if t, ok := reader.(*ttftReader); ok {
		t.complete = complete
	}
}

// 三种协议、流式/非流式统一结算。完整性只由上游流本身决定：
// 读取出错或没有结束原因都不计；已确认完整结束后，客户端收到 [DONE] 就断开连接属于正常行为，
// 不得因为事后看到请求 Context 已取消而追溯抹掉首 Token 样本。
func finishFirstToken(r *http.Request, model string, reqID uint64, body *ttftReader) {
	complete := body.complete
	known := complete && body.duration() > 0
	if known {
		recordModelTTFT(model, body.duration())
	}
	if p := statsAttemptFor(r); p != nil {
		p.ttft = nil
		if known {
			v := durationMilliseconds(body.duration())
			p.ttft = &v
		}
	}
	log.Printf("[首Token统计] traceId=%s requestId=%d 模型=%s 口径=%s 完整=%t 有样本=%t 耗时=%s 起点=网关请求入口 终点=有效文本/推理/工具增量 说明=完整性只看上游流是否正常结束；已完整结束后客户端断开不抹除样本；不计心跳或角色声明，不记录正文",
		debugTraceID(r), reqID, model, ttftMetric, complete, known, body.text())
}

// modelStatusFromError 从错误响应中提取简短状态标签（业务码优先，其次 HTTP 状态码）。
func modelStatusFromError(statusCode int, body string) string {
	if code := extractBusinessCode(body); code != "" {
		return code
	}
	if statusCode > 0 {
		return strconv.Itoa(statusCode)
	}
	return "error"
}

// extractBusinessCode 从响应体提取 "code":NNNN 形式的业务码。
func extractBusinessCode(body string) string {
	const marker = `"code":`
	idx := strings.Index(body, marker)
	if idx < 0 {
		return ""
	}
	rest := body[idx+len(marker):]
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	if end == 0 {
		return ""
	}
	return rest[:end]
}

// modelStatSnapshot 是写入状态快照的单个模型统计。
type modelStatSnapshot struct {
	ID             string `json:"id"`
	Enabled        *bool  `json:"enabled,omitempty"`
	PolicyReason   string `json:"policyReason,omitempty"`
	Source         string `json:"source,omitempty"`
	CNFree         string `json:"cnFree,omitempty"`         // 国内站：是 | 否 | 混合 | -
	IntlFree       string `json:"intlFree,omitempty"`       // 国际站：是 | 否 | 混合 | -
	CNMultiplier   string `json:"cnMultiplier,omitempty"`   // 国内站倍率展示值，如 0.29x / 0.00x / -
	IntlMultiplier string `json:"intlMultiplier,omitempty"` // 国际站倍率展示值
	// SiteSupport 是当前上游目录的站点覆盖结论：both（双站）| cn（国内站）| intl（国际站）。
	// 空值表示目录尚未给出可用结论；只用于展示，不参与账号额度判断。
	SiteSupport string `json:"siteSupport,omitempty"`
	// AvailableAccounts 为两个站点合计的可服务账号数（已按模型账号黑白名单过滤）。
	AvailableAccounts int    `json:"availableAccounts"`
	Requests          int64  `json:"requests,omitempty"`
	Success           int64  `json:"success,omitempty"`
	Failed            int64  `json:"failed,omitempty"`
	Tokens            int64  `json:"tokens,omitempty"`
	AvgTTFTMs         int64  `json:"avgTtftMs,omitempty"`
	HasTTFT           bool   `json:"hasTtft,omitempty"`
	TTFTMetric        string `json:"ttftMetric,omitempty"`
	AvgLatencyMs      int64  `json:"avgLatencyMs,omitempty"`
	HasLatency        bool   `json:"hasLatency,omitempty"`
	LastRequestAt     int64  `json:"lastRequestAt,omitempty"`
	LastStatus        string `json:"lastStatus,omitempty"`
}

// modelServableLocked 只读判断账号当前能否服务该模型（不改变任何状态）。
// 与 nextAccountForModel 的可用性规则保持一致。
func modelServableLocked(acc *Account, model string, now time.Time) bool {
	if acc.Disabled || acc.CooldownUntil.After(now) {
		return false
	}
	if modelSiteSupport(acc.Profile().Key, model) == siteSupportUnsupported {
		return false
	}
	state := acc.ModelStates[normalizeModelName(model)]
	if state == nil {
		if acc.QuotaExhausted {
			return true // 未知模型允许一次受控探测
		}
		return true
	}
	if state.CooldownUntil.After(now) || state.QuotaBlocked {
		return false
	}
	if !acc.QuotaExhausted {
		return true
	}
	switch state.CostClass {
	case modelCostFree:
		return true
	case modelCostPaid:
		return false
	default:
		return !now.Before(state.NextProbeAt)
	}
}

// modelSourceFromSnapshots 从模型统计行中取出目录来源版本（用于 monitor 标题）。
func modelSourceFromSnapshots(rows []modelStatSnapshot) string {
	for _, row := range rows {
		if row.Source != "" {
			return row.Source
		}
	}
	return ""
}

func freeLabel(freeSeen, paidSeen bool) string {
	switch {
	case freeSeen && paidSeen:
		return "混合"
	case freeSeen:
		return "是"
	case paidSeen:
		return "否"
	default:
		return "-"
	}
}

// freeModelDisplayCounts 只复用模型统计表最终显示的站点倍率。
// 不能用这个展示数量补写账号实测账本，或判断账号能否请求某个模型。
func freeModelDisplayCounts(rows []modelStatSnapshot) (cn, intl int) {
	for _, row := range rows {
		if row.CNMultiplier == "0.00x" {
			cn++
		}
		if row.IntlMultiplier == "0.00x" {
			intl++
		}
	}
	return cn, intl
}

// 调用方持有 accountMu，仅在展示数量发生变化时记录，避免每 3 秒重复刷日志。
var lastFreeModelDisplay struct {
	initialized bool
	cn, intl    int
}

// buildModelStatSnapshots 汇总「官方/静态模型目录 ∪ 有统计的模型」的统计行。
func buildModelStatSnapshots(now time.Time, accs []*Account) []modelStatSnapshot {
	return buildModelRows(now, accs, false)
}

func buildAllModelStatSnapshots(now time.Time, accs []*Account) []modelStatSnapshot {
	return buildModelRows(now, accs, true)
}

func buildModelRows(now time.Time, accs []*Account, includeDisabled bool) []modelStatSnapshot {
	rememberServedSiteModels(accs)
	ids, source := collectModelIDs(includeDisabled)
	if includeDisabled {
		modelFilterMu.RLock()
		for id := range currentFilter.blocklist {
			ids = append(ids, id)
		}
		for id := range currentFilter.allowlist {
			ids = append(ids, id)
		}
		modelFilterMu.RUnlock()
	}
	seen := make(map[string]bool, len(ids))
	rows := make([]modelStatSnapshot, 0, len(ids))

	modelStatsMu.Lock()
	defer modelStatsMu.Unlock()

	appendRow := func(id string) {
		id = normalizeModelName(id)
		if id == "" || seen[id] {
			return
		}
		// 被配置禁用的模型在统计附表中直接隐藏（即使历史上有过请求）。
		disabled, reason := modelDisabled(id)
		if disabled && !includeDisabled {
			return
		}
		seen[id] = true
		row := modelStatSnapshot{ID: id, Source: source}
		enabled := !disabled
		row.Enabled, row.PolicyReason = &enabled, reason
		// 免费/收费按站点分别聚合：同一模型名在 cn 与 intl 结论可能不同，
		// 若混在一起会显示成无意义的“混合”。
		var cnFree, cnPaid, intlFree, intlPaid bool
		for _, acc := range accs {
			allowed, _ := modelAccountAllowed(id, acc, accs)
			if !disabled && allowed && modelServableLocked(acc, id, now) {
				row.AvailableAccounts++
			}
			state := acc.ModelStates[id]
			if state == nil {
				continue
			}
			isIntl := acc.Profile().Key == "intl"
			switch state.CostClass {
			case modelCostFree:
				if isIntl {
					intlFree = true
				} else {
					cnFree = true
				}
			case modelCostPaid:
				if isIntl {
					intlPaid = true
				} else {
					cnPaid = true
				}
			}
		}
		if stat := modelStats[id]; stat != nil {
			row.Requests = stat.Requests
			row.Success = stat.Success
			row.Failed = stat.Failed
			row.Tokens = stat.Tokens
			cnFree = cnFree || stat.CNFreeSeen
			cnPaid = cnPaid || stat.CNPaidSeen
			intlFree = intlFree || stat.IntlFreeSeen
			intlPaid = intlPaid || stat.IntlPaidSeen
			row.LastStatus = stat.LastStatus
			row.LastRequestAt = unixOrZero(stat.LastRequestAt)
			// 平均值按最近 statsWindow（5 小时）滚动窗口计算。
			if ttft, okT, latency, okL := stat.windowAveragesLocked(now); true {
				if okT {
					row.AvgTTFTMs = ttft.Milliseconds()
					row.HasTTFT = true
					row.TTFTMetric = ttftMetric
				}
				if okL {
					row.AvgLatencyMs = latency.Milliseconds()
					row.HasLatency = true
				}
			}
		}
		row.CNFree = freeLabel(cnFree, cnPaid)
		row.IntlFree = freeLabel(intlFree, intlPaid)
		row.CNMultiplier = modelDisplayMultiplier("cn", id, row.CNFree)
		row.IntlMultiplier = modelDisplayMultiplier("intl", id, row.IntlFree)
		row.SiteSupport = siteSupportBadge(modelSiteSupport("cn", id), modelSiteSupport("intl", id))
		rows = append(rows, row)
	}

	for _, id := range ids {
		appendRow(id)
	}
	for _, id := range modelAccountRuleIDs() {
		appendRow(id)
	}
	for id := range modelStats {
		appendRow(id)
	}
	return rows
}

func formatMilliseconds(ms int64, has bool) string {
	if !has {
		return "-"
	}
	if ms < 1000 {
		return fmt.Sprintf("%dms", ms)
	}
	return fmt.Sprintf("%.1fs", float64(ms)/1000)
}

// formatTokensM 把 token 数格式化为百万单位，供模型统计表展示。
func formatTokensM(n int64) string {
	if n <= 0 {
		return "0.00M"
	}
	m := float64(n) / 1_000_000
	if m < 0.01 {
		return fmt.Sprintf("%.4fM", m)
	}
	return fmt.Sprintf("%.2fM", m)
}

// renderModelTable 渲染 /v1/models 统计附表。
func renderModelTable(rows []modelStatSnapshot) string {
	widths := []int{26, 16, 16, 8, 8, 19, 15, 10}
	headers := []string{"模型", "国内倍率", "国际倍率", "可用账号", "请求", "平均首Token(5h)", "平均总耗时(5h)", "总Token(M)"}
	border := func() string {
		var b strings.Builder
		b.WriteByte('+')
		for _, width := range widths {
			b.WriteString(strings.Repeat("-", width+2))
			b.WriteByte('+')
		}
		return b.String()
	}
	row := func(cells []string) string {
		var b strings.Builder
		b.WriteByte('|')
		for i, cell := range cells {
			b.WriteByte(' ')
			b.WriteString(fitCell(cell, widths[i]))
			b.WriteString(" |")
		}
		return b.String()
	}

	var b strings.Builder
	b.WriteString(border() + "\n")
	b.WriteString(row(headers) + "\n")
	b.WriteString(border() + "\n")
	for _, r := range rows {
		cnMult := r.CNMultiplier
		if cnMult == "" {
			cnMult = "-"
		}
		intlMult := r.IntlMultiplier
		if intlMult == "" {
			intlMult = "-"
		}
		b.WriteString(row([]string{
			r.ID, cnMult, intlMult, strconv.Itoa(r.AvailableAccounts),
			strconv.FormatInt(r.Requests, 10),
			formatMilliseconds(r.AvgTTFTMs, r.HasTTFT && r.TTFTMetric == ttftMetric),
			formatMilliseconds(r.AvgLatencyMs, r.HasLatency),
			formatTokensM(r.Tokens),
		}) + "\n")
	}
	b.WriteString(border())
	return b.String()
}

// modelRequestCount 返回某模型的客户端请求次数（用于判断是否值得做价格探测）。
func modelRequestCount(model string) int64 {
	modelStatsMu.Lock()
	defer modelStatsMu.Unlock()
	if stat := modelStats[normalizeModelName(model)]; stat != nil {
		return stat.Requests
	}
	return 0
}

// -----------------------------------------------------------------------------
// 以下内容原位于 probe.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// 模型属性探测 (probe)
//
// 目的：让「从未被调度过」的账号（典型如余额耗尽的国际站账号）也能主动学习
// 某个模型对它是免费还是收费，而不必等客户端请求碰巧轮到它。
//
// 为什么要走 HTTP 而不是独立进程直连上游：
//   免费/收费账本保存在 serve 进程内存中（仅通过 workbuddy-status.json 周期快照），
//   独立进程写入的状态文件会被运行中的服务覆盖。因此 probe 子命令只作为客户端，
//   真正执行探测并更新账本的是运行中的服务端 /admin/probe 接口。
//
// 安全：/admin/probe 只接受回环地址来源，且要求已启用 API 鉴权——回环来源可能是
// 浏览器网页发起的免预检跨域请求，不能视为可信；探测会真实调用上游消耗积分。
// -----------------------------------------------------------------------------

const probePrompt = "Please write a factual paragraph of about 150 words about the water cycle, without markdown."

type probeRequest struct {
	Auth   string   `json:"auth,omitempty"`   // 凭据文件路径或文件名；空=全部账号
	Models []string `json:"models,omitempty"` // 模型列表；空=目录前 limit 个
	Limit  int      `json:"limit,omitempty"`  // Models 为空时的最大模型数，默认 5
}

type probeResult struct {
	Account string  `json:"account"`
	Edition string  `json:"edition"`
	Model   string  `json:"model"`
	Status  string  `json:"status"` // free | paid | unknown | quota | unsupported | rate_limited | auth_failed | skipped | error
	Credit  float64 `json:"credit"` // usage.credit（仅成功时有意义）
	Tokens  int64   `json:"tokens"` // usage.total_tokens
	HTTP    int     `json:"http"`   // 上游状态码
	Detail  string  `json:"detail,omitempty"`
}

type probeResponse struct {
	Results []probeResult  `json:"results"`
	Summary map[string]int `json:"summary"`
}

func isLoopbackRequest(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func handleAdminProbe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeOpenAIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "仅支持 POST 请求")
		return
	}
	if !isLoopbackRequest(r) {
		log.Printf("[Probe] 拒绝非回环来源的探测请求: %s", r.RemoteAddr)
		writeOpenAIError(w, http.StatusForbidden, "probe_local_only", "探测接口仅允许本机回环地址调用")
		return
	}
	if currentAPIKey() == "" {
		// 回环来源不等于可信来源：浏览器里任意网页都会以回环身份发起免预检跨域 POST。
		// 探测会真实调用上游并消耗积分，必须先启用 API 鉴权才允许触发。
		log.Printf("[Probe] 拒绝探测请求 原因=未启用API鉴权 说明=请先在 config.json 设置 gateway.apiKeyEnabled=true 与 gateway.apiKey")
		writeOpenAIError(w, http.StatusForbidden, "probe_requires_api_key", "探测接口要求先启用 API 鉴权（gateway.apiKeyEnabled + apiKey）；未鉴权时本机来源同样不可信")
		return
	}

	var req probeRequest
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeOpenAIError(w, http.StatusBadRequest, "invalid_json", "无效的 JSON 请求体")
			return
		}
	}
	if req.Limit <= 0 {
		req.Limit = 5
	}
	if req.Limit > 50 {
		req.Limit = 50
	}

	accountMu.Lock()
	candidates := append([]*Account(nil), accounts...)
	accountMu.Unlock()

	targets := make([]*Account, 0, len(candidates))
	for _, acc := range candidates {
		if acc.Disabled {
			continue
		}
		if req.Auth != "" && !probeAuthMatches(acc, req.Auth) {
			continue
		}
		targets = append(targets, acc)
	}
	if len(targets) == 0 {
		writeOpenAIError(w, http.StatusNotFound, "no_account", "没有匹配的账号（检查 -auth 路径/文件名）")
		return
	}

	models := req.Models
	if len(models) == 0 {
		all, _ := mergedModelIDs()
		if len(all) > req.Limit {
			all = all[:req.Limit]
		}
		models = all
	}

	log.Printf("[Probe] 开始探测：账号数=%d，模型数=%d，账号=%s，模型=%s",
		len(targets), len(models), probeAccountNames(targets), strings.Join(models, ","))

	resp := probeResponse{Summary: map[string]int{}}
	for _, acc := range targets {
		for _, model := range models {
			if r.Context().Err() != nil {
				break
			}
			if disabled, reason := modelDisabled(model); disabled {
				result := probeResult{Account: acc.Path, Edition: acc.Profile().Key, Model: model, Status: "skipped", Detail: reason}
				resp.Results = append(resp.Results, result)
				resp.Summary[result.Status]++
				log.Printf("[Probe] traceId=%s 模型=%s 结果=跳过 原因=模型已停用 说明=不会调用上游", debugTraceID(r), model)
				continue
			}
			if allowed, reason := modelAccountAllowed(model, acc, candidates); !allowed {
				result := probeResult{Account: acc.Path, Edition: acc.Profile().Key, Model: model, Status: "skipped", Detail: "账号名单排除：" + reason}
				resp.Results = append(resp.Results, result)
				resp.Summary[result.Status]++
				log.Printf("[Probe] traceId=%s 模型=%s 凭据=%s 结果=跳过 原因=%s", w.Header().Get("X-Trace-ID"), model, filepath.Base(acc.Path), reason)
				debugEvent(r, "debug", "model_account_policy_checked", map[string]any{
					"account_file": filepath.Base(acc.Path), "model": model, "allowed": false, "reason": reason,
				})
				continue
			}
			result := probeAccountModel(r.Context(), acc, model)
			resp.Results = append(resp.Results, result)
			resp.Summary[result.Status]++
			log.Printf("[Probe] 账号 %s [%s] 模型 %s -> %s (%s)", result.Account, result.Edition, model, result.Status, result.Detail)
		}
	}
	requestStatusSnapshotRefresh()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func probeAuthMatches(acc *Account, want string) bool {
	want = strings.TrimSpace(want)
	if want == "" {
		return true
	}
	return acc.Path == want || filepath.Base(acc.Path) == filepath.Base(want)
}

func probeAccountNames(accs []*Account) string {
	names := make([]string, 0, len(accs))
	for _, acc := range accs {
		names = append(names, filepath.Base(acc.Path))
	}
	return strings.Join(names, ",")
}

// probeAccountModel 对单个账号 + 模型发一次最小请求并学习免费/收费属性。
func probeAccountModel(ctx context.Context, acc *Account, model string) probeResult {
	result := probeResult{Account: filepath.Base(acc.Path), Edition: acc.Profile().Key, Model: model}

	if !lockAccountWithContext(ctx, &acc.lock) {
		result.Status = "error"
		result.Detail = "等待账号锁超时"
		return result
	}
	defer acc.lock.Unlock()

	accountMu.Lock()
	if allowed, reason := modelAccountAllowed(model, acc, accounts); !allowed {
		accountMu.Unlock()
		result.Status, result.Detail = "skipped", "账号名单排除："+reason
		log.Printf("[Probe] traceId=%s 模型=%s 凭据=%s 结果=跳过 原因=%s 说明=等待账号锁后重新检查热配置，未调用上游", newTraceID(), model, filepath.Base(acc.Path), reason)
		return result
	}
	if acc.Disabled || acc.Auth == nil || acc.Auth.Auth.AccessToken == "" {
		accountMu.Unlock()
		result.Status = "skipped"
		result.Detail = "账号失效或无凭据"
		return result
	}
	auth := *acc.Auth
	prof := acc.Profile()
	accountMu.Unlock()
	if disabled, reason := modelDisabled(model); disabled {
		result.Status, result.Detail = "skipped", reason
		return result
	}

	body := map[string]any{
		"model":      model,
		"stream":     true,
		"max_tokens": 300,
		"messages": []any{
			map[string]any{"role": "system", "content": defaultSystemPrompt},
			map[string]any{"role": "user", "content": probePrompt},
		},
	}
	payload, err := json.Marshal(body)
	if err != nil {
		result.Status = "error"
		result.Detail = err.Error()
		return result
	}

	reqCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, prof.chatURL(), bytes.NewReader(payload))
	if err != nil {
		result.Status = "error"
		result.Detail = err.Error()
		return result
	}
	backendHeaders(req, &auth, prof)

	resp, err := cfg.HttpClient.Do(req)
	if err != nil {
		result.Status = "error"
		result.Detail = "上游请求失败: " + err.Error()
		return result
	}
	defer resp.Body.Close()
	result.HTTP = resp.StatusCode

	if resp.StatusCode >= 400 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		errStr := string(errBody)
		switch {
		case isQuotaExhausted(resp.StatusCode, errStr):
			markModelQuotaBlocked(acc, model, errStr)
			result.Status = "quota"
			result.Detail = "额度耗尽（已记为该账号收费模型）"
		case isModelUnavailable(errStr):
			result.Status = "unsupported"
			result.Detail = "站点不支持该模型：" + truncate(errStr, 200)
		case isModelRateLimited(errStr):
			until, ok := parseResetTime(errStr)
			if !ok {
				until = time.Now().Add(60 * time.Second)
			}
			markModelCooldown(acc, model, until, errStr)
			result.Status = "rate_limited"
			result.Detail = "模型级限流至 " + until.Format("2006-01-02 15:04:05") + "：" + truncate(errStr, 200)
		case isAuthFailure(resp.StatusCode, errStr):
			result.Status = "auth_failed"
			result.Detail = "授权失效（未自动禁用，请重新登录）：" + truncate(errStr, 120)
		default:
			result.Status = "error"
			result.Detail = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, truncate(errStr, 120))
		}
		return result
	}

	// HTTP 2xx 已经证明该站点能接这个模型；即使 usage.credit 缺失也记下站点支持证据。
	markSiteModelServed(prof.Key, model)

	usage := readUsageFromSSE(resp.Body)
	credit, hasCredit := usageCredit(usage)
	tokens, _ := usageTotalTokens(usage)
	if !hasCredit {
		result.Status = "unknown"
		result.Detail = "上游未返回 usage.credit，无法判定"
		return result
	}
	result.Credit = credit
	result.Tokens = tokens

	if credit > 0 {
		observeModelCredit(acc, model, usage, 0)
		result.Status = "paid"
		result.Detail = fmt.Sprintf("usage.credit=%s，收费", formatQuota(credit))
		return result
	}
	if tokens < modelFreeMinTokens {
		result.Status = "unknown"
		result.Detail = fmt.Sprintf("credit=0 但样本过小（total_tokens=%d < %d），不判定免费", tokens, modelFreeMinTokens)
		return result
	}
	observeModelCredit(acc, model, usage, 0)
	result.Status = "free"
	result.Detail = fmt.Sprintf("usage.credit=0，total_tokens=%d，免费", tokens)
	return result
}

// readUsageFromSSE 从上游 SSE 流中读取最后一个 usage 对象。
func readUsageFromSSE(r io.Reader) map[string]any {
	var usage map[string]any
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		data := stripDataPrefix(scanner.Text())
		if data == "" || data == "[DONE]" {
			continue
		}
		var chunk map[string]any
		if json.Unmarshal([]byte(data), &chunk) != nil {
			continue
		}
		if u, ok := chunk["usage"].(map[string]any); ok {
			usage = u
		}
	}
	return usage
}

// runProbe 是 probe 子命令：作为客户端调用运行中服务的 /admin/probe。
func runProbe() {
	addr := fmt.Sprintf("%s:%d", cfg.Addr, cfg.Port)
	if cfg.Addr == "0.0.0.0" || cfg.Addr == "::" {
		addr = fmt.Sprintf("127.0.0.1:%d", cfg.Port)
	}
	url := "http://" + addr + "/admin/probe"

	reqBody := probeRequest{Limit: cfg.ProbeLimit}
	if cfg.AuthExplicit {
		reqBody.Auth = cfg.AuthFile
	}
	for _, m := range strings.Split(cfg.ProbeModels, ",") {
		if m = strings.TrimSpace(m); m != "" {
			reqBody.Models = append(reqBody.Models, m)
		}
	}
	payload, _ := json.Marshal(reqBody)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		fmt.Printf("构造请求失败: %v\n", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if key := currentAPIKey(); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	fmt.Printf("正在请求 %s（账号=%s，模型=%s）...\n", url, ifEmpty(reqBody.Auth, "全部"), ifEmpty(strings.Join(reqBody.Models, ","), "目录前若干"))

	resp, err := (&http.Client{Timeout: 15 * time.Minute}).Do(req)
	if err != nil {
		fmt.Printf("调用失败: %v\n请确认 serve 正在运行，且 -addr/-port 与之一致。\n", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("服务返回 HTTP %d: %s\n", resp.StatusCode, truncate(string(body), 300))
		return
	}
	var out probeResponse
	if err := json.Unmarshal(body, &out); err != nil {
		fmt.Printf("解析响应失败: %v\n", err)
		return
	}

	sort.SliceStable(out.Results, func(i, j int) bool {
		if out.Results[i].Account != out.Results[j].Account {
			return out.Results[i].Account < out.Results[j].Account
		}
		return out.Results[i].Model < out.Results[j].Model
	})
	fmt.Printf("\n%-22s %-8s %-24s %-12s %-8s %-8s %s\n", "账号", "站点", "模型", "结果", "credit", "tokens", "说明")
	for _, r := range out.Results {
		fmt.Printf("%-22s %-8s %-24s %-12s %-8s %-8d %s\n",
			r.Account, ifEmpty(r.Edition, "-"), r.Model, r.Status,
			ifEmpty(formatQuota(r.Credit), "-"), r.Tokens, r.Detail)
	}
	fmt.Printf("\n汇总: ")
	keys := make([]string, 0, len(out.Summary))
	for k := range out.Summary {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("%s=%d ", k, out.Summary[k])
	}
	fmt.Println()
}

// -----------------------------------------------------------------------------
// 以下内容原位于 maxtokens.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

// 上游按「输入 token + 输出预算」判断是否超出模型窗口：请求不带 max_tokens 时，
// 上游会为输出默认预留 384000，标称 1048576 窗口的 deepseek-v4.1-flash 实际只剩
// 约 664576 可用输入。长会话会在远未到窗口上限时被 400 code=11133
// （model_param_invalid，报错文案不指明具体参数）拒绝，表现为「聊着聊着就 400」。
//
// 实测（2026-10-10，deepseek-v4.1-flash，窗口 1048576）：
//
//	678912 输入 + 未声明输出预算       -> 400 code=11133
//	678912 输入 + max_tokens=128000    -> 200
//	678912 输入 + max_tokens=400000    -> 400 code=11133
//
// 因此仅在客户端未声明输出预算时，补上模型目录声明的输出上限，把被默认预留的
// 额度收回来。客户端显式传入的值一律不覆盖；目录未收录的模型保持原样，不猜测。
func ensureUpstreamMaxTokens(obj map[string]any, r *http.Request, requestID uint64, traceID, modelName string) {
	if clientDeclaredOutputBudget(obj) {
		return
	}
	limit := modelMaxOutputTokens(modelName)
	if limit <= 0 {
		return
	}
	obj["max_tokens"] = limit
	if traceID == "" {
		traceID = debugTraceID(r)
	}
	if traceID == "" {
		traceID = r.Header.Get("X-Trace-ID")
	}
	log.Printf("[输出预算] traceId=%s requestId=%d 模型=%s 阶段=上游请求序列化前 客户端已声明输出预算=false 结果=已补max_tokens=%d 业务影响=收回上游默认预留的输出额度，避免长会话被11133拒绝",
		traceID, requestID, modelName, limit)
	debugEvent(r, "debug", "max_tokens_budget_injected", map[string]any{
		"model": modelName, "injected_max_tokens": limit,
		"business_impact": "客户端未声明输出预算，网关按模型目录补入输出上限以收回上游默认预留额度；不覆盖客户端显式值",
	})
}

// clientDeclaredOutputBudget 判断客户端是否已声明输出预算。
// max_tokens 为 null 视为未声明；max_completion_tokens 是等价的新字段，
// 只要其中任意一个带有效值就保持请求原样。
func clientDeclaredOutputBudget(obj map[string]any) bool {
	for _, key := range []string{"max_tokens", "max_completion_tokens"} {
		if value, ok := obj[key]; ok && value != nil {
			return true
		}
	}
	return false
}

// modelMaxOutputTokens 返回模型在站点目录中声明的最大输出 token 数。
// 目录未收录该模型时返回 0，调用方应保持请求原样。
func modelMaxOutputTokens(modelID string) int {
	modelID = normalizeModelName(modelID)
	if modelID == "" {
		return 0
	}
	modelsMu.RLock()
	defer modelsMu.RUnlock()
	best := 0
	for _, site := range catalogSites {
		for _, m := range catalogModels[site] {
			if m.ID == modelID && m.MaxOutputTokens > best {
				best = m.MaxOutputTokens
			}
		}
	}
	return best
}
