package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

func parseQuotaCapacity(value string) (float64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	return strconv.ParseFloat(value, 64)
}

// summary 只有额度与历史标记，不能独立判定有效套餐。
// 由完整权益查询成功后替换“待确认”，禁止据试用标记猜成Pro试用。
func planLabelFromSummary(summary quotaSummaryData) string {
	return planUnknown
}

func parseQuotaSummary(data []byte) (total, used, remaining float64, paid bool, plan string, err error) {
	var summary quotaSummaryData
	if err = json.Unmarshal(data, &summary); err != nil {
		return 0, 0, 0, false, "", fmt.Errorf("解析额度响应失败: %w", err)
	}
	for _, pkg := range summary.Packages {
		pkgTotal, parseErr := parseQuotaCapacity(pkg.CycleTotalCapacity)
		if parseErr != nil {
			return 0, 0, 0, false, "", fmt.Errorf("解析总额度 %q 失败: %w", pkg.CycleTotalCapacity, parseErr)
		}
		pkgUsed, parseErr := parseQuotaCapacity(pkg.CycleUsedCapacity)
		if parseErr != nil {
			return 0, 0, 0, false, "", fmt.Errorf("解析已用额度 %q 失败: %w", pkg.CycleUsedCapacity, parseErr)
		}
		pkgRemaining, parseErr := parseQuotaCapacity(pkg.CycleRemainCapacity)
		if parseErr != nil {
			return 0, 0, 0, false, "", fmt.Errorf("解析剩余额度 %q 失败: %w", pkg.CycleRemainCapacity, parseErr)
		}
		total += pkgTotal
		used += pkgUsed
		remaining += pkgRemaining
	}
	return total, used, remaining, summary.IsPaidUser, planLabelFromSummary(summary), nil
}

// parseEnterpriseUsage 把企业分配额度换成网关的总额/已用/剩余。
// credit（或 used_num）是本周期已用，和个人口径的「剩余」相反；
// limitNum（或 limit_num）是分给这个成员的额度。limitNum=-1 表示不限量。
func parseEnterpriseUsage(data []byte) (total, used, remaining float64, unlimited bool, err error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		return 0, 0, 0, false, fmt.Errorf("企业额度响应为空")
	}
	var raw map[string]json.RawMessage
	if err = json.Unmarshal(trimmed, &raw); err != nil {
		return 0, 0, 0, false, fmt.Errorf("解析企业额度响应失败: %w", err)
	}
	limit, hasLimit, err := firstEnterpriseNumber(raw, "limitNum", "limit_num")
	if err != nil {
		return 0, 0, 0, false, err
	}
	if !hasLimit {
		return 0, 0, 0, false, fmt.Errorf("企业额度响应缺少 limitNum")
	}
	used, hasUsed, err := firstEnterpriseNumber(raw, "credit", "used_num", "usedNum")
	if err != nil {
		return 0, 0, 0, false, err
	}
	if !hasUsed {
		return 0, 0, 0, false, fmt.Errorf("企业额度响应缺少 credit/used_num")
	}
	if used < 0 || (limit < 0 && limit != -1) {
		return 0, 0, 0, false, fmt.Errorf("企业额度响应包含无效负数")
	}
	if limit == -1 {
		// 不限量没有「剩余<=0」这回事。总额/剩余用 -1 作哨兵，展示层显示「不限」。
		return -1, used, -1, true, nil
	}
	remaining = limit - used
	if remaining < 0 {
		remaining = 0
	}
	return limit, used, remaining, false, nil
}

func firstEnterpriseNumber(raw map[string]json.RawMessage, keys ...string) (float64, bool, error) {
	var value float64
	var present bool
	for _, key := range keys {
		val, ok := raw[key]
		if !ok || string(bytes.TrimSpace(val)) == "null" {
			continue
		}
		num, err := parseEnterpriseJSONNumber(val)
		if err != nil {
			return 0, true, fmt.Errorf("企业额度字段 %s 无法解析", key)
		}
		if present && value != num {
			return 0, true, fmt.Errorf("企业额度同义字段数值冲突")
		}
		value, present = num, true
	}
	return value, present, nil
}

func parseEnterpriseJSONNumber(raw json.RawMessage) (float64, error) {
	raw = bytes.TrimSpace(raw)
	var num float64
	if err := json.Unmarshal(raw, &num); err == nil && !math.IsNaN(num) && !math.IsInf(num, 0) {
		return num, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		num, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
		if err == nil && !math.IsNaN(num) && !math.IsInf(num, 0) {
			return num, nil
		}
	}
	return 0, fmt.Errorf("not a number")
}

// applyQuotaObservation 写入额度并在余额恢复或不限量时解除模型额度阻断。
// 调用方可以持有账号锁；这里只取账号池锁。
func applyQuotaObservation(acc *Account, total, used, remaining float64, unlimited, paid bool, plan string, planOK bool, now time.Time) (wasExhausted, exhausted bool, display string) {
	accountMu.Lock()
	defer accountMu.Unlock()
	wasExhausted = acc.QuotaExhausted
	acc.QuotaTotal = total
	acc.QuotaUsed = used
	acc.QuotaRemaining = remaining
	acc.QuotaUnlimited = unlimited
	acc.IsPaidUser = paid
	if planOK {
		acc.PlanLabel = plan
		acc.PlanCheckedAt = now.Unix()
		acc.PlanStale = false
	} else {
		if acc.PlanCheckedAt == 0 {
			acc.PlanLabel = planUnknown
		}
		acc.PlanStale = true
	}
	display = planDisplayLabel(acc.PlanLabel, acc.PlanStale)
	acc.QuotaKnown = true
	exhausted = !unlimited && remaining <= 0
	acc.QuotaExhausted = exhausted
	if unlimited || remaining > 0 {
		for _, state := range acc.ModelStates {
			if state == nil {
				continue
			}
			state.QuotaBlocked = false
			state.NextProbeAt = time.Time{}
		}
	}
	return wasExhausted, exhausted, display
}

func formatQuota(value float64) string {
	if value > -0.005 && value < 0.005 {
		value = 0
	}
	return strings.TrimRight(strings.TrimRight(strconv.FormatFloat(value, 'f', 2, 64), "0"), ".")
}

func formatQuotaAmount(unlimited bool, kind string, value float64) string {
	if unlimited && (kind == "total" || kind == "remaining") {
		return "不限"
	}
	return formatQuota(value)
}

func usageCredit(usage map[string]any) (float64, bool) {
	if usage == nil {
		return 0, false
	}
	value, exists := usage["credit"]
	if !exists {
		return 0, false
	}
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case json.Number:
		n, err := v.Float64()
		return n, err == nil
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func usageFromCompletion(data []byte) map[string]any {
	var completion map[string]any
	if json.Unmarshal(data, &completion) != nil {
		return nil
	}
	usage, _ := completion["usage"].(map[string]any)
	return usage
}

func usageNumber(usage map[string]any, key string) (int64, bool) {
	if usage == nil {
		return 0, false
	}
	switch v := usage[key].(type) {
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	case int:
		return int64(v), true
	case int64:
		return v, true
	case json.Number:
		n, err := v.Int64()
		if err == nil {
			return n, true
		}
		f, err := v.Float64()
		return int64(f), err == nil
	case string:
		s := strings.TrimSpace(v)
		n, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return n, true
		}
		f, err := strconv.ParseFloat(s, 64)
		return int64(f), err == nil
	default:
		return 0, false
	}
}

func usageTotalTokens(usage map[string]any) (int64, bool) {
	if n, ok := usageNumber(usage, "total_tokens"); ok {
		return n, true
	}
	prompt, hasPrompt := usageNumber(usage, "prompt_tokens")
	completion, hasCompletion := usageNumber(usage, "completion_tokens")
	if hasPrompt || hasCompletion {
		return prompt + completion, true
	}
	return 0, false
}

func observeModelCredit(acc *Account, model string, usage map[string]any, reqID uint64) {
	credit, ok := usageCredit(usage)
	if !ok || acc == nil {
		return
	}
	// credit=0 只有样本足够大时才判定为免费：极小请求（例如探针）上游也可能记 0，
	// 那不代表该模型真的免费，不能据此让零余额账号去请求收费模型。
	if credit <= 0 {
		total, hasTotal := usageTotalTokens(usage)
		if !hasTotal || total < modelFreeMinTokens {
			log.Printf("[ModelCost] requestId=%d 账号 %s 模型 %s credit=0 但样本过小（total_tokens=%d < %d），忽略本次免费判定",
				reqID, acc.Path, model, total, modelFreeMinTokens)
			return
		}
	}
	accountMu.Lock()
	state := modelStateLocked(acc, model)
	oldClass := state.CostClass
	if credit <= 0 {
		state.CostClass = modelCostFree
		state.QuotaBlocked = false
	} else {
		state.CostClass = modelCostPaid
	}
	state.ObservedAt = time.Now()
	quotaExhausted := acc.QuotaExhausted
	path := acc.Path
	site := acc.Profile().Key
	accountMu.Unlock()
	recordModelCostClass(model, site, credit <= 0)
	markSiteModelServed(site, model)

	if credit <= 0 {
		if oldClass != modelCostFree {
			log.Printf("[ModelCost] requestId=%d 账号 %s 站点=%s 模型 %s 实测 usage.credit=0，已学习为免费模型", reqID, path, site, model)
		}
		if quotaExhausted {
			log.Printf("[FreeModel] requestId=%d 站点=%s 请求的是免费模型 %s，已明确使用付费余额耗尽账号 %s 完成请求，usage.credit=0", reqID, site, model, path)
		}
	} else if oldClass != modelCostPaid {
		log.Printf("[ModelCost] requestId=%d 账号 %s 站点=%s 模型 %s 实测 usage.credit=%s，已学习为收费模型", reqID, path, site, model, formatQuota(credit))
	}
	requestStatusSnapshotRefresh()
}

// refreshAccountQuota 通过用户中心只读计费接口刷新账号额度，不记录或回传 Token。
func refreshAccountQuota(ctx context.Context, acc *Account) error {
	if acc == nil {
		return nil
	}
	if !lockAccountWithContext(ctx, &acc.lock) {
		return ctx.Err()
	}
	defer acc.lock.Unlock()

	accountMu.Lock()
	if acc.Disabled || acc.Auth == nil || acc.Auth.Auth.AccessToken == "" {
		accountMu.Unlock()
		return nil
	}
	auth := *acc.Auth
	path := acc.Path
	prof := acc.Profile()
	accountMu.Unlock()

	traceID := uuid.NewString()
	enterpriseID := strings.TrimSpace(auth.Account.EnterpriseID)
	if enterpriseID != "" {
		return refreshEnterpriseAccountQuota(ctx, acc, auth, path, prof, traceID, enterpriseID)
	}
	log.Printf("[Quota] traceId=%s 账号 %s 开始查询额度，站点=%s，接口=%s", traceID, path, prof.Label, prof.quotaSummaryURL())
	headers := func(r *http.Request) {
		commonHeaders(r, prof)
		r.Header.Set("Authorization", "Bearer "+auth.Auth.AccessToken)
		r.Header.Set("X-Client-Platform", "web")
		r.Header.Set("X-Trace-ID", traceID)
	}
	data, status, err := doJSONContext(ctx, cfg.HttpClient, http.MethodPost, prof.quotaSummaryURL(), headers, strings.NewReader("{}"))
	if err != nil {
		markAccountPlanStale(acc)
		log.Printf("[Quota] traceId=%s 账号 %s 查询额度失败，HTTP=%d，保留上一次额度数据，套餐标记待刷新", traceID, path, status)
		return err
	}
	total, used, remaining, paid, plan, err := parseQuotaSummary(data)
	if err != nil {
		markAccountPlanStale(acc)
		log.Printf("[Quota] traceId=%s 账号 %s 查询额度响应无法解析，保留上一次额度数据，套餐标记待刷新", traceID, path)
		return err
	}
	var summary quotaSummaryData
	_ = json.Unmarshal(data, &summary) // parseQuotaSummary 已校验。
	now := time.Now()
	resources, planErr := fetchPlanResources(ctx, prof, headers, traceID, path, now)
	if planErr == nil {
		plan, planErr = identifyPlan(prof.Key, summary, resources, now)
	}
	wasExhausted, exhausted, plan := applyQuotaObservation(acc, total, used, remaining, false, paid, plan, planErr == nil, now)
	if planErr != nil {
		log.Printf("[套餐判断] traceId=%s 账号=%s 结果=待确认 原因=%v 显示=%s 业务影响=额度仍更新，不把接口失败误报为免费或试用", traceID, path, planErr, plan)
	} else {
		log.Printf("[套餐判断] traceId=%s 账号=%s 有效期校验时间=%s 资源数=%d 显示=%s 结果=完整权益校验成功", traceID, path, now.Format(time.RFC3339), len(resources), plan)
	}
	switch {
	case !wasExhausted && exhausted:
		log.Printf("[Quota] traceId=%s 账号 %s 额度查询成功，总额度=%s，已用=%s，剩余=%s，付费用户=%t，套餐=%s；账号已冻结调度，等待额度恢复", traceID, path, formatQuota(total), formatQuota(used), formatQuota(remaining), paid, plan)
	case wasExhausted && !exhausted:
		log.Printf("[Quota] traceId=%s 账号 %s 额度已恢复，总额度=%s，已用=%s，剩余=%s，付费用户=%t，套餐=%s；账号已自动解除冻结并恢复调度", traceID, path, formatQuota(total), formatQuota(used), formatQuota(remaining), paid, plan)
	default:
		log.Printf("[Quota] traceId=%s 账号 %s 额度查询成功，总额度=%s，已用=%s，剩余=%s，付费用户=%t，套餐=%s", traceID, path, formatQuota(total), formatQuota(used), formatQuota(remaining), paid, plan)
	}
	return nil
}

// refreshEnterpriseAccountQuota 只查询企业成员被分配的额度。
// 外部项目实测个人 get-user-resource* 对企业号为空，不能据此把账号判成剩余 0。
// 调用方已持有 acc.lock。
func refreshEnterpriseAccountQuota(ctx context.Context, acc *Account, auth StoredAuth, path string, prof *upstreamProfile, traceID, enterpriseID string) error {
	endpoint := prof.enterpriseUsageURL()
	log.Printf("[Quota] traceId=%s 账号 %s 开始查询企业分配额度，站点=%s，接口=%s，企业身份=已识别，说明=不请求个人资源包 get-user-resource，避免空包被当成剩余 0", traceID, path, prof.Label, endpoint)
	headers := func(r *http.Request) {
		commonHeaders(r, prof)
		r.Header.Set("Authorization", "Bearer "+auth.Auth.AccessToken)
		r.Header.Set("X-Client-Platform", "web")
		r.Header.Set("X-Trace-ID", traceID)
		r.Header.Set("X-Enterprise-Id", enterpriseID)
		r.Header.Set("X-Tenant-Id", enterpriseID)
		if auth.Account.UID != "" {
			r.Header.Set("X-User-Id", strings.TrimSpace(auth.Account.UID))
		}
		if auth.Auth.Domain != "" {
			r.Header.Set("X-Domain", auth.Auth.Domain)
		}
	}
	data, status, err := doJSONContext(ctx, cfg.HttpClient, http.MethodPost, endpoint, headers, strings.NewReader("{}"))
	if err != nil {
		markAccountPlanStale(acc)
		log.Printf("[Quota] traceId=%s 账号 %s 企业额度查询失败，HTTP=%d，保留上一次额度，业务影响=不把失败当成剩余 0，本轮不解除也不新增强制阻断", traceID, path, status)
		return err
	}
	total, used, remaining, unlimited, err := parseEnterpriseUsage(data)
	if err != nil {
		markAccountPlanStale(acc)
		log.Printf("[Quota] traceId=%s 账号 %s 企业额度响应无法解析，保留上一次额度，原因=%v，业务影响=不把坏数据当成剩余 0", traceID, path, err)
		return err
	}
	now := time.Now()
	wasExhausted, exhausted, plan := applyQuotaObservation(acc, total, used, remaining, unlimited, false, planEnterprise, true, now)
	log.Printf("[套餐判断] traceId=%s 账号=%s 结果=%s 说明=企业号没有个人套餐资源包，不调用 get-user-resource，显示=%s", traceID, path, planEnterprise, plan)
	totalText := formatQuotaAmount(unlimited, "total", total)
	remainText := formatQuotaAmount(unlimited, "remaining", remaining)
	switch {
	case !wasExhausted && exhausted:
		log.Printf("[Quota] traceId=%s 账号 %s 企业额度计算=limitNum %s - credit %s，剩余=%s，不限量=%t，套餐=%s；分配额度已用完，收费模型暂停调度", traceID, path, totalText, formatQuota(used), remainText, unlimited, plan)
	case wasExhausted && !exhausted:
		log.Printf("[Quota] traceId=%s 账号 %s 企业额度已恢复，计算=limitNum %s - credit %s，剩余=%s，不限量=%t，套餐=%s；已解除此前因个人空包或 14018 造成的额度阻断", traceID, path, totalText, formatQuota(used), remainText, unlimited, plan)
	default:
		log.Printf("[Quota] traceId=%s 账号 %s 企业额度查询成功，计算=limitNum %s - credit %s，剩余=%s，不限量=%t，套餐=%s", traceID, path, totalText, formatQuota(used), remainText, unlimited, plan)
	}
	return nil
}

func lockAccountWithContext(ctx context.Context, mu *sync.Mutex) bool {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if mu.TryLock() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

// quotaScanBreaker 额度扫描熔断：连续多轮全部失败（上游宕机/网络故障）时退避跳过，
// 避免每 5 分钟对 N 个账号各发一次注定失败的请求并刷日志；任何非全失败轮次即复位。
var quotaScanBreaker struct {
	sync.Mutex
	consecutiveAllFailed int
	lastRun              time.Time
}

func refreshAllAccountQuotas(ctx context.Context, done chan<- struct{}, schedule ...time.Time) {
	defer close(done)
	next := time.Time{}
	if len(schedule) > 0 {
		next = schedule[0]
	}
	trace, started := startScheduledTask("quota_scan", next)
	quotaScanBreaker.Lock()
	if quotaScanBreaker.consecutiveAllFailed >= 2 && time.Since(quotaScanBreaker.lastRun) < 15*time.Minute {
		quotaScanBreaker.Unlock()
		logScheduledTask("quota_scan", "skipped", "circuit_open", trace, started, next, "连续多轮全部失败，退避跳过本轮，稍后自动恢复")
		return
	}
	quotaScanBreaker.lastRun = time.Now()
	quotaScanBreaker.Unlock()
	accountMu.Lock()
	accs := append([]*Account(nil), accounts...)
	accountMu.Unlock()
	log.Printf("[Quota] 开始批量更新额度，账号数=%d", len(accs))
	processed, failed := 0, 0
	for _, acc := range accs {
		if err := ctx.Err(); err != nil {
			log.Printf("[Quota] 本轮额度更新被取消，尚未扫描的账号将等待下一轮: %v", err)
			break
		}
		accountCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := refreshAccountQuota(accountCtx, acc)
		processed++
		cancel()
		if err != nil {
			failed++
			log.Printf("[Quota] 账号 %s 本轮额度更新未完成: %v", acc.Path, err)
		} else {
			accountMu.Lock()
			if acc != nil && !acc.Disabled && acc.PlanStale {
				failed++
			}
			accountMu.Unlock()
		}
	}
	snapshotErr := writeStatusSnapshot()
	log.Printf("[Quota] 本轮额度更新完成，状态快照已写入")
	status := "success"
	if ctx.Err() != nil {
		status = "cancelled"
	} else if len(accs) == 0 {
		status = "skipped"
	} else if failed == len(accs) {
		status = "failed"
	} else if failed > 0 || snapshotErr != nil {
		status = "partial"
	}
	logScheduledTask("quota_scan", "completed", status, trace, started, next, fmt.Sprintf("账号=%d，已检查=%d，额度/套餐异常=%d，快照失败=%t", len(accs), processed, failed, snapshotErr != nil))
	quotaScanBreaker.Lock()
	if status == "failed" {
		quotaScanBreaker.consecutiveAllFailed++
	} else {
		quotaScanBreaker.consecutiveAllFailed = 0
	}
	quotaScanBreaker.Unlock()
}

func backgroundQuotaRefresher() {
	const interval = 300 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	next := time.Now().Add(interval)
	var cancel context.CancelFunc
	var done chan struct{}
	start := func() {
		if cancel != nil && done != nil {
			select {
			case <-done:
			default:
				log.Printf("[Quota] 新一轮额度扫描到达，正在强制取消上一轮未完成的请求")
				cancel()
			}
		}
		ctx, nextCancel := context.WithCancel(context.Background())
		cancel = nextCancel
		done = make(chan struct{})
		// 快照本轮的 ctx/done/next：下一轮 start() 会重新赋值这些变量，闭包不能捕获变量本身
		scanCtx, scanDone, scanAt := ctx, done, next
		safeGo("quota_scan", func() { refreshAllAccountQuotas(scanCtx, scanDone, scanAt) })
	}
	start()
	for {
		select {
		case tick := <-ticker.C:
			next = tick.Add(interval)
			start()
		case <-quotaScanTrigger:
			log.Printf("[Quota] 凭据池发生变化，立即触发一轮额度扫描")
			start()
		}
	}
}

func requestQuotaScan() {
	select {
	case quotaScanTrigger <- struct{}{}:
	default:
	}
}
