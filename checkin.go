package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

func isAlreadyCheckedIn(status int, message string) bool {
	if status == 0 && message == "" {
		return false
	}
	low := strings.ToLower(message)
	return strings.Contains(message, `"code":10001`) || strings.Contains(message, `"code":14001`) ||
		strings.Contains(message, "已签到") || strings.Contains(low, "already checked in")
}

// checkinAccount 执行国内站每日签到。国际站没有已确认可用的签到体系，明确跳过。
func checkinAccount(ctx context.Context, acc *Account) (string, error) {
	if acc == nil {
		return "skipped", nil
	}
	acc.lock.Lock()
	defer acc.lock.Unlock()

	accountMu.Lock()
	if acc.Disabled || acc.Auth == nil || acc.Auth.Auth.AccessToken == "" {
		accountMu.Unlock()
		return "skipped", nil
	}
	if acc.Profile().Key != profileCN.Key {
		accountMu.Unlock()
		return "global_skipped", nil
	}
	if strings.TrimSpace(acc.Auth.Account.EnterpriseID) != "" {
		accountPath := acc.Path
		accountMu.Unlock()
		log.Printf("[Checkin] traceId=%s 账号 %s 是企业版，不发起个人签到请求；额度仍由企业分配额度接口更新", newTraceID(), accountPath)
		return "enterprise_skipped", nil
	}
	auth := *acc.Auth
	path := acc.Path
	prof := acc.Profile()
	accountMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	log.Printf("[Checkin] 账号 %s 开始每日签到，站点=%s，接口=%s", path, prof.Label, prof.dailyCheckinURL())
	headers := func(r *http.Request) {
		commonHeaders(r, prof)
		r.Header.Set("Authorization", "Bearer "+auth.Auth.AccessToken)
		r.Header.Set("X-Client-Platform", "web")
		if auth.Account.UID != "" {
			r.Header.Set("X-User-Id", auth.Account.UID)
		}
		if auth.Account.EnterpriseID != "" {
			r.Header.Set("X-Enterprise-Id", auth.Account.EnterpriseID)
			r.Header.Set("X-Tenant-Id", auth.Account.EnterpriseID)
		}
		if auth.Auth.Domain != "" {
			r.Header.Set("X-Domain", auth.Auth.Domain)
		}
	}
	_, status, err := doJSONContext(ctx, cfg.HttpClient, http.MethodPost, prof.dailyCheckinURL(), headers, strings.NewReader("{}"))
	if err == nil {
		log.Printf("[Checkin] 账号 %s 每日签到成功", path)
		return "ok", nil
	}
	if isAlreadyCheckedIn(status, err.Error()) {
		log.Printf("[Checkin] 账号 %s 今天已经签到，本次按幂等成功处理", path)
		return "already", nil
	}
	log.Printf("[Checkin] 账号 %s 每日签到失败，HTTP=%d，原因=%v；不改变账号调度状态", path, status, err)
	return "failed", err
}

func checkinAllAccounts(ctx context.Context) string {
	if !dailyCheckinMu.TryLock() {
		log.Printf("[Checkin] 已有一轮签到正在执行，本次重复触发已跳过")
		return "busy"
	}
	defer dailyCheckinMu.Unlock()
	next := nextDailyCheckin(time.Now())
	trace, started := startScheduledTask("daily_checkin", next)

	accountMu.Lock()
	accs := append([]*Account(nil), accounts...)
	accountMu.Unlock()
	log.Printf("[Checkin] 开始每日签到，账号总数=%d，国际站账号将跳过", len(accs))
	ok, already, failed, skipped := 0, 0, 0, 0
	for _, acc := range accs {
		if err := ctx.Err(); err != nil {
			log.Printf("[Checkin] 本轮签到被取消，未处理账号等待下一次触发: %v", err)
			break
		}
		result, err := checkinAccount(ctx, acc)
		switch result {
		case "ok":
			ok++
		case "already":
			already++
		case "failed":
			failed++
			_ = err
		default:
			skipped++
		}
	}
	log.Printf("[Checkin] 本轮签到完成，总数=%d，成功=%d，今日已签=%d，失败=%d，跳过=%d", len(accs), ok, already, failed, skipped)
	status := "success"
	if ctx.Err() != nil {
		status = "cancelled"
	} else if failed > 0 && ok+already == 0 {
		status = "failed"
	} else if failed > 0 {
		status = "partial"
	} else if ok+already == 0 {
		status = "skipped"
	}
	logScheduledTask("daily_checkin", "completed", status, trace, started, next, fmt.Sprintf("成功=%d，今日已签=%d，失败=%d，跳过=%d", ok, already, failed, skipped))
	requestQuotaScan()
	return status
}

// checkinPersistentState 记录北京时间当天的签到尝试次数，服务重启后继续生效。
type checkinPersistentState struct {
	Date          string `json:"date"`
	Attempts      int    `json:"attempts"`
	LastStatus    string `json:"lastStatus,omitempty"`
	LastAttemptAt int64  `json:"lastAttemptAt,omitempty"`
	LastSuccessAt int64  `json:"lastSuccessAt,omitempty"`
}

func beijingLocation() *time.Location {
	return time.FixedZone("UTC+8", 8*60*60)
}

func checkinDay(t time.Time) string {
	return t.In(beijingLocation()).Format("2006-01-02")
}

func todayCheckinTime(now time.Time) time.Time {
	loc := beijingLocation()
	localNow := now.In(loc)
	return time.Date(localNow.Year(), localNow.Month(), localNow.Day(), checkinHour, 0, 0, 0, loc)
}

func nextDailyCheckin(now time.Time) time.Time {
	next := todayCheckinTime(now)
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}
	return next
}

// firstCheckinTime 决定本次服务启动后的首次签到时间：
//   - 开机时间未超过当天 08:00：等到 08:00 定时执行；
//   - 开机时间已超过 08:00：启动后延迟 cfg.CheckinDelay（默认 60 秒）补签。
func firstCheckinTime(now time.Time, delay time.Duration) time.Time {
	today := todayCheckinTime(now)
	if !now.After(today) {
		return today
	}
	if delay < 0 {
		delay = 0
	}
	return now.Add(delay)
}

// shouldRetryCheckin 判断当前这次签到结果是否还允许重试。
// failed 表示整轮失败，partial 表示部分账号失败；两者都可能有账号漏签。
// attempt 是刚完成的尝试序号，达到每日上限后不再重试。
func shouldRetryCheckin(status string, attempt int) bool {
	return attempt < checkinMaxAttempts && (status == "failed" || status == "partial")
}

func loadCheckinState(now time.Time) {
	checkinStateMu.Lock()
	defer checkinStateMu.Unlock()
	var st checkinPersistentState
	if data, err := os.ReadFile(checkinStatePath); err == nil {
		_ = json.Unmarshal(data, &st)
	}
	today := checkinDay(now)
	if st.Date != today {
		st = checkinPersistentState{Date: today}
	}
	checkinState = st
}

// saveCheckinStateLocked 原子写入签到尝试状态；调用方持有 checkinStateMu。
func saveCheckinStateLocked() {
	data, err := json.Marshal(checkinState)
	if err != nil {
		log.Printf("[Checkin] 保存签到尝试状态失败: %v", err)
		return
	}
	if err := atomicWriteFile(checkinStatePath, data, 0600); err != nil {
		log.Printf("[Checkin] 写入签到尝试状态失败: %v", err)
	}
}

func checkinAttemptsToday(now time.Time) int {
	checkinStateMu.Lock()
	defer checkinStateMu.Unlock()
	if checkinState.Date != checkinDay(now) {
		return 0
	}
	return checkinState.Attempts
}

// reserveCheckinAttempt 为当天预留一次尝试名额；达到上限时返回 false。
func reserveCheckinAttempt(now time.Time) (int, bool) {
	checkinStateMu.Lock()
	defer checkinStateMu.Unlock()
	today := checkinDay(now)
	if checkinState.Date != today {
		checkinState = checkinPersistentState{Date: today}
	}
	if checkinState.Attempts >= checkinMaxAttempts {
		return checkinState.Attempts, false
	}
	checkinState.Attempts++
	saveCheckinStateLocked()
	return checkinState.Attempts, true
}

func recordCheckinAttemptResult(status string, now time.Time) {
	checkinStateMu.Lock()
	defer checkinStateMu.Unlock()
	if checkinState.Date != checkinDay(now) {
		return
	}
	checkinState.LastStatus = status
	checkinState.LastAttemptAt = now.Unix()
	if status == "success" {
		checkinState.LastSuccessAt = now.Unix()
	}
	saveCheckinStateLocked()
}

func logNextCheckin(next time.Time, detail string) {
	logScheduledTask("daily_checkin", "scheduled", "", newTraceID(), time.Time{}, next, detail)
	log.Printf("[Checkin] 下一次签到时间=%s；%s", next.Format("2006-01-02 15:04:05 MST"), detail)
}

// backgroundDailyCheckin 统一负责每日签到调度：
//   - 开机早于 08:00：等待当天 08:00 执行首次签到；
//   - 开机已过 08:00：启动后等待 cfg.CheckinDelay（默认 60 秒）补签；
//   - 失败或部分失败：间隔 5 分钟重试，每天最多 3 次；
//   - 尝试次数持久化，服务重启不会突破每日上限。
func backgroundDailyCheckin() {
	ctx := context.Background()
	loadCheckinState(time.Now())
	now := time.Now()
	delay := time.Duration(cfg.CheckinDelay) * time.Second
	if delay < 0 {
		delay = 0
	}
	next := firstCheckinTime(now, delay)
	today := todayCheckinTime(now)
	switch {
	case !now.After(today):
		logNextCheckin(next, "开机早于今日08:00，等待定时签到")
	case checkinAttemptsToday(now) >= checkinMaxAttempts:
		next = nextDailyCheckin(now)
		logNextCheckin(next, fmt.Sprintf("今日已尝试%d次，达到上限，等待下一次08:00", checkinMaxAttempts))
	default:
		logNextCheckin(next, fmt.Sprintf("开机时间已过08:00，%s后执行补签；失败后5分钟重试，每日最多%d次", delay, checkinMaxAttempts))
	}

	for {
		wait := time.Until(next)
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		<-timer.C

		now = time.Now()
		attempt, ok := reserveCheckinAttempt(now)
		if !ok {
			log.Printf("[Checkin] 今日签到尝试次数已达上限 %d，跳过本次触发", checkinMaxAttempts)
			next = nextDailyCheckin(now)
			logNextCheckin(next, fmt.Sprintf("今日已尝试%d次，达到上限，等待下一次08:00", checkinMaxAttempts))
			continue
		}

		log.Printf("[Checkin] 开始今日第 %d/%d 次签到", attempt, checkinMaxAttempts)
		status := checkinAllAccounts(ctx)
		finished := time.Now()
		recordCheckinAttemptResult(status, finished)

		switch {
		case status == "cancelled":
			log.Printf("[Checkin] 第 %d 次签到被取消，停止今日签到调度", attempt)
			return
		case shouldRetryCheckin(status, attempt):
			next = finished.Add(checkinRetryDelay)
			log.Printf("[Checkin] 第 %d 次签到结果=%s，%s后自动重试（第 %d/%d 次）",
				attempt, status, checkinRetryDelay, attempt+1, checkinMaxAttempts)
			logNextCheckin(next, fmt.Sprintf("第%d次签到结果=%s，%s后重试", attempt, status, checkinRetryDelay))
		case status == "failed" || status == "partial":
			next = nextDailyCheckin(finished)
			log.Printf("[Checkin] 第 %d 次签到结果=%s，今日已达 %d 次上限，停止重试", attempt, status, checkinMaxAttempts)
			logNextCheckin(next, fmt.Sprintf("今日已尝试%d次，达到上限，等待下一次08:00", checkinMaxAttempts))
		default:
			next = nextDailyCheckin(finished)
			log.Printf("[Checkin] 第 %d 次签到结果=%s，等待下一次08:00", attempt, status)
			logNextCheckin(next, fmt.Sprintf("第%d次签到结果=%s，等待下一次08:00", attempt, status))
		}
	}
}
