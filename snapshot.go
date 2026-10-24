package main

import (
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// -----------------------------------------------------------------------------
// 状态快照与前台实时监控 (monitor)
// -----------------------------------------------------------------------------

// accountSnapshot 是写入状态快照文件的单个账号状态。
type accountSnapshot struct {
	Alerts            []accountAlertMark `json:"alerts,omitempty"`
	Path              string             `json:"path"`
	SchedulingEnabled *bool              `json:"schedulingEnabled,omitempty"`
	SchedulingReason  string             `json:"schedulingReason,omitempty"`
	Edition           string             `json:"edition,omitempty"` // 站点标识（cn/intl）
	Nickname          string             `json:"nickname"`
	UID               string             `json:"uid"`
	State             string             `json:"state"` // active | cooldown | paid_exhausted | expired | disabled
	CooldownUntil     int64              `json:"cooldownUntil,omitempty"`
	CooldownMsg       string             `json:"cooldownMsg,omitempty"`
	DisabledReason    string             `json:"disabledReason,omitempty"`
	TokenExpiresAt    int64              `json:"tokenExpiresAt,omitempty"`
	QuotaTotal        float64            `json:"quotaTotal,omitempty"`
	QuotaUsed         float64            `json:"quotaUsed,omitempty"`
	QuotaRemaining    float64            `json:"quotaRemaining"`
	IsPaidUser        bool               `json:"isPaidUser"`
	PlanLabel         string             `json:"planLabel,omitempty"` // 官方套餐名称或已知短名称
	PlanCheckedAt     int64              `json:"planCheckedAt,omitempty"`
	PlanStale         bool               `json:"planStale,omitempty"`
	// 登录续期状态：刷新令牌到期时间与最近成功续期时间，便于提前发现需要重新登录的账号。
	RefreshExpiresAt int64                         `json:"refreshExpiresAt,omitempty"`
	LastRefreshTime  int64                         `json:"lastRefreshTime,omitempty"`
	RefreshFailCount int                           `json:"refreshFailCount,omitempty"`
	QuotaKnown       bool                          `json:"quotaKnown,omitempty"`
	QuotaExhausted   bool                          `json:"quotaExhausted,omitempty"`
	QuotaUnlimited   bool                          `json:"quotaUnlimited,omitempty"`
	ModelStates      map[string]modelStateSnapshot `json:"modelStates,omitempty"`
	FreeModels       int                           `json:"freeModels,omitempty"` // 展示值：所属站点模型统计中 0.00x 的模型数，不参与调度
	ModelCooldowns   int                           `json:"modelCooldowns,omitempty"`
}

type modelStateSnapshot struct {
	CostClass     string `json:"costClass,omitempty"`
	CooldownUntil int64  `json:"cooldownUntil,omitempty"`
	QuotaBlocked  bool   `json:"quotaBlocked,omitempty"`
	NextProbeAt   int64  `json:"nextProbeAt,omitempty"`
	LastReason    string `json:"lastReason,omitempty"`
	ObservedAt    int64  `json:"observedAt,omitempty"`
}

// statusSnapshot 是写入 workbuddy-status.json 的完整快照。
type statusSnapshot struct {
	AlertSource  string              `json:"alertSource,omitempty"`
	UpdatedAt    int64               `json:"updatedAt"`
	Accounts     []accountSnapshot   `json:"accounts"`
	Models       []modelStatSnapshot `json:"models,omitempty"`
	ModelCatalog []modelStatSnapshot `json:"modelCatalog,omitempty"` // 管理页完整模型目录，含停用项。
}

// writeStatusSnapshot 将账号池实时状态（含冷却/失效）原子写入状态快照文件。
// serve 后台周期调用；monitor 命令前台读取展示。
func writeStatusSnapshot() error {
	statusSnapshotWriteMu.Lock()
	defer statusSnapshotWriteMu.Unlock()
	accountMu.Lock()
	snap := statusSnapshot{UpdatedAt: time.Now().Unix()}
	now := time.Now()
	for _, acc := range accounts {
		as := accountSnapshot{Path: acc.Path, Edition: acc.Profile().Key}
		as.QuotaTotal = acc.QuotaTotal
		as.QuotaUsed = acc.QuotaUsed
		as.QuotaRemaining = acc.QuotaRemaining
		as.IsPaidUser = acc.IsPaidUser
		as.PlanLabel = acc.PlanLabel
		as.PlanCheckedAt = acc.PlanCheckedAt
		as.PlanStale = acc.PlanStale
		as.RefreshFailCount = acc.RefreshFailCount
		if acc.Auth != nil {
			as.RefreshExpiresAt = acc.Auth.Auth.RefreshExpiresAt
			as.LastRefreshTime = acc.Auth.Auth.LastRefreshTime
			if as.RefreshExpiresAt == 0 {
				// 旧凭据或手工导入的凭据没有该字段，从未验签的令牌声明补一个展示值。
				if exp, ok := jwtExpiry(acc.Auth.Auth.RefreshToken); ok {
					as.RefreshExpiresAt = exp.Unix()
				}
			}
		}
		as.QuotaKnown = acc.QuotaKnown
		as.QuotaExhausted = acc.QuotaExhausted
		as.QuotaUnlimited = acc.QuotaUnlimited
		if len(acc.ModelStates) > 0 {
			as.ModelStates = make(map[string]modelStateSnapshot, len(acc.ModelStates))
			for model, state := range acc.ModelStates {
				if state.CooldownUntil.After(now) {
					as.ModelCooldowns++
				}
				as.ModelStates[model] = modelStateSnapshot{
					CostClass: state.CostClass, CooldownUntil: unixOrZero(state.CooldownUntil),
					QuotaBlocked: state.QuotaBlocked, NextProbeAt: unixOrZero(state.NextProbeAt),
					LastReason: state.LastReason, ObservedAt: unixOrZero(state.ObservedAt),
				}
			}
		}
		switch {
		case acc.Disabled:
			as.State = "disabled"
			as.DisabledReason = acc.DisabledReason
			if acc.Auth != nil {
				as.Nickname = acc.Auth.Account.Nickname
				as.UID = acc.Auth.Account.UID
			} else {
				as.Nickname = acc.Nickname
				as.UID = acc.UID
			}
		case acc.CooldownUntil.After(now):
			as.State = "cooldown"
			as.CooldownUntil = acc.CooldownUntil.Unix()
			as.CooldownMsg = acc.CooldownMsg
			if acc.Auth != nil {
				as.Nickname = acc.Auth.Account.Nickname
				as.UID = acc.Auth.Account.UID
				as.TokenExpiresAt = acc.Auth.Auth.ExpiresAt
			}
		case acc.Auth != nil && acc.Auth.Auth.ExpiresAt > 0 && acc.Auth.Auth.ExpiresAt <= now.Unix():
			as.State = "expired"
			as.Nickname = acc.Auth.Account.Nickname
			as.UID = acc.Auth.Account.UID
			as.TokenExpiresAt = acc.Auth.Auth.ExpiresAt
		case acc.QuotaExhausted:
			as.State = "paid_exhausted"
			if acc.Auth != nil {
				as.Nickname = acc.Auth.Account.Nickname
				as.UID = acc.Auth.Account.UID
				as.TokenExpiresAt = acc.Auth.Auth.ExpiresAt
			}
		default:
			as.State = "active"
			if acc.Auth != nil {
				as.Nickname = acc.Auth.Account.Nickname
				as.UID = acc.Auth.Account.UID
				as.TokenExpiresAt = acc.Auth.Auth.ExpiresAt
			}
		}
		snap.Accounts = append(snap.Accounts, as)
	}
	annotateAccountPolicies(snap.Accounts)
	snap.ModelCatalog = buildAllModelStatSnapshots(now, accounts)
	captureAccountStatsLocked()
	for _, row := range snap.ModelCatalog {
		if disabled, _ := modelDisabled(row.ID); !disabled {
			snap.Models = append(snap.Models, row)
		}
	}
	cnFree, intlFree := freeModelDisplayCounts(snap.Models)
	for i := range snap.Accounts {
		if profileForEdition(snap.Accounts[i].Edition).Key == "intl" {
			snap.Accounts[i].FreeModels = intlFree
		} else {
			snap.Accounts[i].FreeModels = cnFree
		}
	}
	if !lastFreeModelDisplay.initialized || lastFreeModelDisplay.cn != cnFree || lastFreeModelDisplay.intl != intlFree {
		lastFreeModelDisplay.initialized = true
		lastFreeModelDisplay.cn, lastFreeModelDisplay.intl = cnFree, intlFree
		log.Printf("[免费模型展示] traceId=%s 来源=模型统计站点倍率 国内=%d 国际=%d 模型行数=%d 规则=只计0.00x 业务影响=仅更新账号表展示，不改实测账本、调度或计费判断",
			newTraceID(), cnFree, intlFree, len(snap.Models))
	}
	accountMu.Unlock()

	// 告警标记与快照同一次写入，避免出现「快照已异常、日志还没扫到」的时间差。
	marks := recordAccountAlertMarks(snap.Accounts, snap.UpdatedAt)
	for i := range snap.Accounts {
		snap.Accounts[i].Alerts = marks[snap.Accounts[i].Path]
	}
	snap.AlertSource = alertSourceSnapshot

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	// 原子写：临时文件 + fsync + 改名，monitor 读不到半截内容，断电也不损文件
	return atomicWriteFile(statusSnapshotFile, data, 0600)
}

func displayWidth(s string) int {
	width := 0
	for _, r := range s {
		if r >= 0x1100 && (r <= 0x115f || r == 0x2329 || r == 0x232a ||
			(r >= 0x2e80 && r <= 0xa4cf) || (r >= 0xac00 && r <= 0xd7a3) ||
			(r >= 0xf900 && r <= 0xfaff) || (r >= 0xfe10 && r <= 0xfe6f) ||
			(r >= 0xff00 && r <= 0xff60) || (r >= 0xffe0 && r <= 0xffe6)) {
			width += 2
		} else {
			width++
		}
	}
	return width
}

func fitCell(s string, width int) string {
	s = strings.ReplaceAll(strings.ReplaceAll(s, "\r", " "), "\n", " ")
	if displayWidth(s) <= width {
		return s + strings.Repeat(" ", width-displayWidth(s))
	}
	limit := width - 3
	var b strings.Builder
	used := 0
	for len(s) > 0 {
		r, size := utf8.DecodeRuneInString(s)
		rw := displayWidth(string(r))
		if used+rw > limit {
			break
		}
		b.WriteRune(r)
		used += rw
		s = s[size:]
	}
	return b.String() + "..." + strings.Repeat(" ", width-used-3)
}

func renderAccountTable(accs []accountSnapshot) string {
	widths := []int{4, 20, 24, 8, 10, 19, 10, 10, 10, 10, 10, 10}
	for _, a := range accs {
		widths[9] = min(36, max(widths[9], displayWidth(planDisplayLabel(a.PlanLabel, a.PlanStale))))
	}
	headers := []string{"序号", "凭据文件", "账号", "站点", "状态", "Token 有效期", "总额度", "已用", "剩余", "套餐", "免费模型", "模型冷却"}
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
	for i, a := range accs {
		status := "可用"
		expires := "-"
		total, used, remaining, plan := "-", "-", "-", "-"
		if a.QuotaKnown {
			total = formatQuotaAmount(a.QuotaUnlimited, "total", a.QuotaTotal)
			used = formatQuotaAmount(a.QuotaUnlimited, "used", a.QuotaUsed)
			remaining = formatQuotaAmount(a.QuotaUnlimited, "remaining", a.QuotaRemaining)
			plan = planDisplayLabel(a.PlanLabel, a.PlanStale)
		}
		if a.TokenExpiresAt > 0 {
			expires = time.Unix(a.TokenExpiresAt, 0).Format("2006-01-02 15:04:05")
		}
		switch a.State {
		case "cooldown":
			status = "冷却"
		case "expired":
			status = "已过期"
		case "quota_exhausted", "paid_exhausted":
			status = "付费耗尽"
		case "disabled":
			status = "失效"
		}
		b.WriteString(row([]string{
			strconv.Itoa(i + 1), filepath.Base(a.Path), ifEmpty(a.Nickname, "-"),
			profileForEdition(a.Edition).Label, status, expires, total, used, remaining, plan,
			strconv.Itoa(a.FreeModels), strconv.Itoa(a.ModelCooldowns),
		}) + "\n")
	}
	b.WriteString(border())
	for _, a := range accs {
		if a.PlanStale {
			b.WriteString("\n* 套餐权益查询失败，待刷新；名称可能是上次成功结果。")
			break
		}
	}
	now := time.Now()
	for _, a := range accs {
		if a.RefreshExpiresAt <= 0 {
			continue
		}
		exp := time.Unix(a.RefreshExpiresAt, 0)
		switch {
		case !exp.After(now):
			b.WriteString(fmt.Sprintf("\n! %s 刷新令牌已过期，需要重新登录。", filepath.Base(a.Path)))
		case exp.Sub(now) < 7*24*time.Hour:
			b.WriteString(fmt.Sprintf("\n! %s 刷新令牌 %s 到期（剩余 %s），建议尽快重新登录。",
				filepath.Base(a.Path), exp.Format("2006-01-02 15:04:05"), humanDuration(time.Until(exp))))
		}
	}
	for _, a := range accs {
		if a.RefreshFailCount > 0 {
			b.WriteString(fmt.Sprintf("\n! %s 续期连续失败 %d 次，可能需要重新登录。", filepath.Base(a.Path), a.RefreshFailCount))
		}
	}
	return b.String()
}

// statusSnapshotLoop serve 后台每 3 秒刷新一次状态快照。
// statusSnapshotKick 常驻快照协程的唤醒信号；容量 1，多次提醒合并为一次。
var statusSnapshotKick = make(chan struct{}, 1)

// requestStatusSnapshotRefresh 请求/刷新路径的快照刷新入口（异步）。
// 全量快照要持 accountMu 构建并写盘：429 风暴时逐请求同步执行会放大阻塞并拖慢选号，
// 令牌刷新路径还会在 acc.lock 内写盘。这里只发非阻塞信号，由快照协程承担成本。
func requestStatusSnapshotRefresh() {
	select {
	case statusSnapshotKick <- struct{}{}:
	default: // 已有未处理的提醒，合并
	}
}

func statusSnapshotLoop() {
	var lastLogged time.Time
	var previousStatus string
	write := func(next time.Time) {
		started := time.Now()
		err := writeStatusSnapshot()
		status, detail := "success", "账号与模型快照已写入"
		if err != nil {
			status, detail = "failed", safeDebugError(err)
		}
		logFrequentTask("status_snapshot", status, detail, started, next, &lastLogged, &previousStatus, err != nil)
	}
	write(time.Now().Add(3 * time.Second))
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case tick := <-ticker.C:
			write(tick.Add(3 * time.Second))
		case <-statusSnapshotKick:
			// 请求/刷新路径的异步提醒：尽快落盘一次，但不亲自承担快照成本
			write(time.Now().Add(3 * time.Second))
		}
	}
}
