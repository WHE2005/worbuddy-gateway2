package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

// ensureValidTokenFor 对指定账号检查并刷新令牌（多账号版）。
// 失效账号（Disabled 或 Auth 为 nil）直接跳过，不参与调度。
func ensureValidTokenFor(acc *Account) error {
	if acc == nil {
		return nil
	}
	accountMu.Lock()
	disabled := acc.Disabled
	hasAuth := acc.Auth != nil
	expiresAt := int64(0)
	if hasAuth {
		expiresAt = acc.Auth.Auth.ExpiresAt
	}
	accountMu.Unlock()
	if disabled || !hasAuth {
		return nil
	}
	now := time.Now()
	if now.Unix() > expiresAt-900 {
		log.Printf("[Auth] 账号 %s Token 需要续期，当前过期时间=%s，刷新阈值=过期前15分钟，开始调用上游刷新接口", acc.Path, time.Unix(expiresAt, 0).Format("2006-01-02 15:04:05"))
		if err := doRefreshTokenFor(acc); err != nil {
			accountMu.Lock()
			isDisabled := acc.Disabled
			accountMu.Unlock()
			if !isDisabled {
				markCooldown(acc, now.Add(time.Minute), "Token 自动续期失败: "+err.Error())
			}
			return err
		}
	}
	return nil
}

func ensureValidToken() error {
	sa, err := loadAuth()
	if err != nil {
		return err
	}
	// 如果距过期不足 15 分钟，则自动刷新
	if time.Now().Unix() > sa.Auth.ExpiresAt-900 {
		log.Printf("[Auth] 访问令牌即将或已经过期 (ExpiresAt=%s)，正在自动刷新...", time.Unix(sa.Auth.ExpiresAt, 0).Format("2006-01-02 15:04:05"))
		return doRefreshToken(sa)
	}
	return nil
}

func doRefreshToken(sa *StoredAuth) error {
	if sa == nil || sa.Auth.RefreshToken == "" {
		return fmt.Errorf("无法刷新：缺少 RefreshToken")
	}
	if _, err := refreshTokenPayload(sa); err != nil {
		return err
	}
	if err := saveAuth(sa); err != nil {
		return fmt.Errorf("写回凭据失败: %w", err)
	}
	log.Printf("[Auth] Token 刷新成功！新过期时间: %s", time.Unix(sa.Auth.ExpiresAt, 0).Format("2006-01-02 15:04:05"))
	return nil
}

// doRefreshTokenFor 刷新指定账号的令牌并保存回其凭据文件（多账号版）。
func doRefreshTokenFor(acc *Account) error {
	return refreshAccountToken(acc, "按需续期")
}

// refreshAccountToken 刷新指定账号的登录凭据并原子写回。
//
// 失败分两档处理：命中上游「登录态失效」标志（如 12153 / invalid_grant）或连续失败
// 达到 refreshDisableThreshold 次，才禁用账号；普通网络错误、5xx 以及未命中标志的
// 401/403 只累计计数并保留原凭据，避免一次抖动就删掉用户凭据。
func refreshAccountToken(acc *Account, reason string) error {
	if acc == nil {
		return fmt.Errorf("无法刷新：账号缺少 RefreshToken 或已失效")
	}

	// 与该账号的上游请求串行，避免刷新过程中请求读到一半更新的 Token。
	acc.lock.Lock()
	defer acc.lock.Unlock()

	accountMu.Lock()
	if acc.Disabled || acc.Auth == nil || acc.Auth.Auth.RefreshToken == "" {
		accountMu.Unlock()
		return fmt.Errorf("无法刷新：账号缺少 RefreshToken 或已失效")
	}
	refreshed := *acc.Auth
	oldTokens := refreshed.Auth // 覆盖前校验要比对旧凭据，先留存副本。
	path := acc.Path
	oldExpiresAt := refreshed.Auth.ExpiresAt
	accountMu.Unlock()

	log.Printf("[Auth] 账号 %s 开始刷新 Token，触发方式=%s，站点=%s，刷新前访问令牌过期时间=%s",
		path, reason, profileForEdition(refreshed.Edition).Label, time.Unix(oldExpiresAt, 0).Format("2006-01-02 15:04:05"))
	status, err := refreshTokenPayload(&refreshed)
	if err != nil {
		kind := classifyRefreshFailure(status, err.Error())
		summary := refreshFailureSummary(kind, status, err)
		count := recordRefreshFailure(acc, summary)
		if count >= refreshDisableThreshold {
			log.Printf("[Auth] 账号 %s 判定登录态失效，已停止调度并写入失效标记；连续失败=%d 判定依据=%s", path, count, summary)
			disableAccount(acc, fmt.Sprintf("令牌刷新连续失败 %d 次: %s", count, summary))
		} else if kind == refreshFailureSessionDead {
			// 上游明确说登录态没了，但先不删凭据：保留原凭据继续重试，达到阈值才停止调度。
			log.Printf("[Auth] 账号 %s 续期被上游判定登录态失效（连续 %d/%d 次）：%s；暂时保留凭据，达到阈值后才停止调度，建议尽快重新登录",
				path, count, refreshDisableThreshold, summary)
		} else {
			log.Printf("[Auth] 账号 %s Token 刷新失败，旧凭据未覆盖；连续失败=%d/%d 判定=%s 业务影响=本次未续期，账号仍可用到访问令牌过期，稍后自动重试",
				path, count, refreshDisableThreshold, summary)
		}
		return err
	}
	// 覆盖前校验：新凭据必须仍属于同一账号且确实可用，否则保留旧凭据不覆盖。
	if err := validateRefreshedCredential(path, &refreshed, oldTokens); err != nil {
		accountMu.Lock()
		acc.LastRefreshError = "新凭据未通过覆盖前校验: " + err.Error()
		accountMu.Unlock()
		log.Printf("[Auth] 账号 %s 新凭据未通过覆盖前校验，已放弃本次覆盖并保留原凭据；原因=%v 业务影响=账号继续使用原凭据，不销毁、不降级",
			path, err)
		return err
	}
	if err := saveAuthTo(path, &refreshed); err != nil {
		log.Printf("[Auth] 账号 %s Token 已从上游刷新，但写回凭据失败，内存与磁盘均保留旧凭据: %v", path, err)
		return fmt.Errorf("写回凭据失败: %w", err)
	}
	fingerprint := ""
	if st, statErr := os.Stat(path); statErr == nil {
		fingerprint = accountFingerprint(st)
	}
	accountMu.Lock()
	if !acc.Disabled {
		acc.Auth = &refreshed
		acc.Edition = profileForEdition(refreshed.Edition).Key
		acc.fingerprint = fingerprint
		acc.RefreshFailCount = 0
		acc.LastRefreshError = ""
	}
	accountMu.Unlock()
	newExpiresAt := refreshed.Auth.ExpiresAt
	if oldExpiresAt > 0 && newExpiresAt <= oldExpiresAt {
		// 刷新成功但访问令牌期限没有顺延：该账号可能已接近上游的登录态上限。
		log.Printf("[Auth] 账号 %s 刷新成功但访问令牌期限未顺延（%s -> %s），该账号可能已接近登录态上限，建议留意是否需要重新登录",
			path, time.Unix(oldExpiresAt, 0).Format("2006-01-02 15:04:05"), time.Unix(newExpiresAt, 0).Format("2006-01-02 15:04:05"))
	}
	log.Printf("[Auth] 账号 %s Token 刷新成功，访问令牌过期时间由 %s 更新为 %s，触发方式=%s，凭据已安全写回",
		path, time.Unix(oldExpiresAt, 0).Format("2006-01-02 15:04:05"), time.Unix(newExpiresAt, 0).Format("2006-01-02 15:04:05"), reason)
	requestStatusSnapshotRefresh()
	return nil
}

// recordRefreshFailure 累计连续刷新失败次数并返回当前次数。
func recordRefreshFailure(acc *Account, summary string) int {
	accountMu.Lock()
	defer accountMu.Unlock()
	now := time.Now()
	// 时间衰减：距上次失败超过窗口按全新一轮计，避免分散的临时故障累积成永久禁号。
	if !acc.LastRefreshFailAt.IsZero() && now.Sub(acc.LastRefreshFailAt) > refreshFailDecayWindow {
		acc.RefreshFailCount = 0
	}
	acc.LastRefreshFailAt = now
	acc.RefreshFailCount++
	acc.LastRefreshError = summary
	return acc.RefreshFailCount
}

// disabledAccountRecheckInterval 周期复检被禁用账号的间隔。
const disabledAccountRecheckInterval = 30 * time.Minute

// disabledAccountRecheckLoop 周期复检被禁用账号：因「令牌刷新连续失败」被禁用的
// 账号，凭据文件仍在时每 30 分钟尝试一次真实刷新，上游恢复即自动回到账号池，
// 不再依赖用户重新登录。失败保持禁用，失败计数照常累计（受衰减窗口约束）。
func disabledAccountRecheckLoop() {
	ticker := time.NewTicker(disabledAccountRecheckInterval)
	defer ticker.Stop()
	for range ticker.C {
		accountMu.Lock()
		var candidates []*Account
		for _, acc := range accounts {
			if acc.Disabled && acc.Auth != nil {
				candidates = append(candidates, acc)
			}
		}
		accountMu.Unlock()
		for _, acc := range candidates {
			recheckDisabledAccount(acc)
		}
	}
}

// recheckDisabledAccount 对单个被禁用账号做一次真实刷新尝试；成功则恢复调度。
// 与该账号的请求/续期串行（acc.lock），覆盖前校验与磁盘写入复用既有刷新纪律。
func recheckDisabledAccount(acc *Account) bool {
	acc.lock.Lock()
	defer acc.lock.Unlock()

	accountMu.Lock()
	if !acc.Disabled || acc.Auth == nil {
		disabled := acc.Disabled
		accountMu.Unlock()
		return !disabled
	}
	refreshed := *acc.Auth
	path := acc.Path
	accountMu.Unlock()
	oldTokens := refreshed.Auth

	if _, err := refreshTokenPayload(&refreshed); err != nil {
		return false
	}
	if err := validateRefreshedCredential(path, &refreshed, oldTokens); err != nil {
		log.Printf("[Auth] 账号 %s 禁用复检刷新成功但未通过覆盖前校验，保持禁用: %v", path, err)
		return false
	}
	if err := saveAuthTo(path, &refreshed); err != nil {
		log.Printf("[Auth] 账号 %s 禁用复检刷新成功但写回凭据失败，保持禁用: %v", path, err)
		return false
	}
	accountMu.Lock()
	defer accountMu.Unlock()
	if !acc.Disabled {
		return true
	}
	acc.Auth = &refreshed
	acc.Edition = profileForEdition(refreshed.Edition).Key
	if st, statErr := os.Stat(path); statErr == nil {
		acc.fingerprint = accountFingerprint(st)
	}
	acc.Disabled = false
	acc.DisabledReason = ""
	acc.RefreshFailCount = 0
	acc.LastRefreshError = ""
	log.Printf("[Auth] 账号 %s 周期复检成功，已自动恢复调度 说明=历史失效多为上游临时故障或令牌过期，凭据文件与失效标记已同步更新", path)
	requestStatusSnapshotRefresh()
	return true
}

// refreshTokenPayload 调用上游刷新接口并更新内存中的令牌字段（不落盘）。
// 按凭据文件中的 edition 路由到对应站点（国内站/国际站）的刷新接口。
// 返回上游 HTTP 状态码（成功或失败时均为实际状态；网络错误为 0）。
func refreshTokenPayload(sa *StoredAuth) (int, error) {
	registerCredentialSecrets(sa)
	prof := profileForEdition(sa.Edition)
	headers := func(r *http.Request) {
		commonHeaders(r, prof)
		r.Header.Set("X-Refresh-Token", sa.Auth.RefreshToken)
		if sa.Account.EnterpriseID != "" {
			r.Header.Set("X-Enterprise-Id", sa.Account.EnterpriseID)
		}
		r.Header.Set("X-Auth-Refresh-Source", "workbuddy")
	}

	data, status, err := doJSON(cfg.HttpClient, http.MethodPost, prof.tokenRefreshURL(), headers, nil)
	if err != nil {
		return status, fmt.Errorf("上游刷新拒绝 (HTTP %d): %w", status, err)
	}
	var tok tokenData
	if err := json.Unmarshal(data, &tok); err != nil || tok.AccessToken == "" {
		return status, fmt.Errorf("解析新 Token 失败: %w", err)
	}
	registerSecrets(tok.AccessToken, tok.RefreshToken)

	now := time.Now()
	sa.Auth.AccessToken = tok.AccessToken
	if tok.RefreshToken != "" {
		sa.Auth.RefreshToken = tok.RefreshToken
	}
	if tok.Domain != "" {
		sa.Auth.Domain = tok.Domain
	}
	// 接口未返回 expiresIn 时保留旧过期时间（preserveExpiry），避免刷新风暴。
	if tok.ExpiresIn > 0 {
		sa.Auth.ExpiresAt = now.Add(time.Duration(tok.ExpiresIn) * time.Second).Unix()
	}
	// 刷新令牌到期时间：优先用接口返回的 refreshExpiresIn；接口没给时，
	// 从当前刷新令牌未验签的令牌声明读取，避免在界面上显示成未知。
	if tok.RefreshExpiresIn > 0 {
		sa.Auth.RefreshExpiresAt = now.Add(time.Duration(tok.RefreshExpiresIn) * time.Second).Unix()
	} else if exp, ok := jwtExpiry(sa.Auth.RefreshToken); ok {
		sa.Auth.RefreshExpiresAt = exp.Unix()
	}
	sa.Auth.LastRefreshTime = now.Unix()
	return status, nil
}
