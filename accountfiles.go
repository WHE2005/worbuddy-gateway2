package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// -----------------------------------------------------------------------------

func loadAuth() (*StoredAuth, error) {
	authLock.RLock()
	if currAuth != nil {
		defer authLock.RUnlock()
		return currAuth, nil
	}
	authLock.RUnlock()

	data, err := os.ReadFile(cfg.AuthFile)
	if err != nil {
		return nil, fmt.Errorf("读取凭据文件失败 (%s): %w", cfg.AuthFile, err)
	}
	var sa StoredAuth
	if err := json.Unmarshal(data, &sa); err != nil {
		return nil, fmt.Errorf("解析凭据文件失败: %w", err)
	}
	if sa.Auth.AccessToken == "" {
		return nil, fmt.Errorf("凭据文件中缺少 AccessToken")
	}
	registerCredentialSecrets(&sa)

	authLock.Lock()
	currAuth = &sa
	authLock.Unlock()
	return &sa, nil
}

func saveAuth(sa *StoredAuth) error {
	registerCredentialSecrets(sa)
	traceID := newTraceID()
	log.Printf("[凭据保存] traceId=%s 文件=%s 阶段=开始 说明=先写入磁盘，成功后再更新当前内存凭据", traceID, cfg.AuthFile)
	dir := filepath.Dir(cfg.AuthFile)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}
	data, err := json.MarshalIndent(sa, "", "  ")
	if err != nil {
		log.Printf("[凭据保存] traceId=%s 阶段=编码 结果=失败 原因=%v 业务影响=当前内存凭据未切换", traceID, err)
		return err
	}
	if err := atomicWriteFile(cfg.AuthFile, data, 0600); err != nil {
		log.Printf("[凭据保存] traceId=%s 阶段=写入磁盘 结果=失败 原因=%v 业务影响=当前内存凭据未切换", traceID, err)
		return err
	}
	clearDisabledMarker(cfg.AuthFile)
	authLock.Lock()
	currAuth = sa
	authLock.Unlock()
	log.Printf("[凭据保存] traceId=%s 文件=%s 结果=成功 说明=磁盘写入完成，当前内存凭据已更新", traceID, cfg.AuthFile)
	return nil
}

// saveAuthTo 将凭据写入指定路径（多账号模式使用）。
// 写入成功后清除该路径的失效标记（表示账号已重新登录）。
func saveAuthTo(path string, sa *StoredAuth) error {
	registerCredentialSecrets(sa)
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0755)
	}
	data, err := json.MarshalIndent(sa, "", "  ")
	if err != nil {
		return err
	}
	if err := atomicWriteFile(path, data, 0600); err != nil {
		return err
	}
	clearDisabledMarker(path)
	return nil
}

// loadAccountFile 从指定路径读取并解析凭据（不修改全局缓存）。
func loadAccountFile(path string) (*StoredAuth, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取凭据文件失败 (%s): %w", path, err)
	}
	var sa StoredAuth
	if err := json.Unmarshal(data, &sa); err != nil {
		return nil, fmt.Errorf("解析凭据文件失败 (%s): %w", path, err)
	}
	if sa.Auth.AccessToken == "" {
		return nil, fmt.Errorf("凭据文件缺少 AccessToken (%s)", path)
	}
	registerCredentialSecrets(&sa)
	return &sa, nil
}

// isCredentialFile 判断自动发现模式下文件名是否为凭据文件。
// 排除运行时产物：workbuddy-status.json（monitor 状态快照）与签到尝试状态文件。
func isCredentialFile(name string) bool {
	if !strings.HasPrefix(name, "workbuddy") || !strings.HasSuffix(name, ".json") {
		return false
	}
	return name != statusSnapshotFile && name != checkinStateFile
}

// collectConfiguredAuthPaths 解析应加载的凭据路径列表：
//  1. -auth-dir 指定目录 → 目录下所有 workbuddy*.json
//  2. 显式 -auth → 逗号分隔的凭据文件列表（保持用户顺序）
//  3. 均未指定（自动发现模式）→ 扫描当前目录下所有 workbuddy*.json，
//     这样把多个凭据文件放进工作目录即可自动组成多账号池；若一个都没有则回退默认 cfg.AuthFile
func collectConfiguredAuthPaths() []string {
	var paths []string

	if cfg.AuthDir != "" {
		entries, err := os.ReadDir(cfg.AuthDir)
		if err == nil {
			for _, e := range entries {
				if e.IsDir() || !isCredentialFile(e.Name()) {
					continue
				}
				paths = append(paths, filepath.Join(cfg.AuthDir, e.Name()))
			}
			sort.Strings(paths)
		}
		return paths
	}

	if cfg.AuthExplicit {
		for _, p := range strings.Split(cfg.AuthFile, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				paths = append(paths, p)
			}
		}
		return paths
	}

	// 自动发现模式：扫描当前工作目录（systemd WorkingDirectory 即凭据目录）
	entries, err := os.ReadDir(".")
	if err == nil {
		for _, e := range entries {
			if e.IsDir() || !isCredentialFile(e.Name()) {
				continue
			}
			paths = append(paths, e.Name())
		}
		sort.Strings(paths)
	}
	if len(paths) == 0 {
		// 无任何凭据文件时回退默认路径，便于给出"请先 login"的友好提示
		paths = append(paths, cfg.AuthFile)
	}
	return paths
}

// loadAccounts 构建账号池。
// -auth 支持逗号分隔多个凭据文件；-auth-dir 自动加载目录下所有 workbuddy*.json；
// 两者都未指定时自动发现当前目录下所有 workbuddy*.json（多账号免参数）。
func loadAccounts() error {
	accountMu.Lock()
	defer accountMu.Unlock()

	accounts = nil
	rrIndex = 0

	for _, p := range collectConfiguredAuthPaths() {
		sa, err := loadAccountFile(p)
		if err != nil {
			log.Printf("[Auth] 跳过无效凭据文件 %s: %v", p, err)
			continue
		}
		fp := ""
		if st, statErr := os.Stat(p); statErr == nil {
			fp = accountFingerprint(st)
		}
		accounts = append(accounts, &Account{Path: p, Auth: sa, Edition: profileForEdition(sa.Edition).Key, fingerprint: fp})
	}

	// 恢复持久化的失效账号标记（凭据文件已删除，仅供控制台提示重新登录）
	loadDisabledMarkers()
	restoreAccountRuntimeStateLocked()
	restoreStatsAccountStatesLocked()

	if len(accounts) == 0 {
		return fmt.Errorf("未找到有效凭据，请先执行 login 命令扫码登录")
	}
	return nil
}

// restoreAccountRuntimeStateLocked 从 monitor 快照恢复额度与模型级账本；调用方持有 accountMu。
func restoreAccountRuntimeStateLocked() {
	data, err := os.ReadFile(statusSnapshotFile)
	if err != nil {
		return
	}
	var snap statusSnapshot
	if json.Unmarshal(data, &snap) != nil {
		return
	}
	byPath := make(map[string]accountSnapshot, len(snap.Accounts))
	for _, state := range snap.Accounts {
		byPath[state.Path] = state
	}
	for _, acc := range accounts {
		state, ok := byPath[acc.Path]
		if !ok || acc.Disabled {
			continue
		}
		acc.QuotaTotal = state.QuotaTotal
		acc.QuotaUsed = state.QuotaUsed
		acc.QuotaRemaining = state.QuotaRemaining
		acc.IsPaidUser = state.IsPaidUser
		acc.PlanLabel = state.PlanLabel
		acc.PlanCheckedAt = state.PlanCheckedAt
		acc.PlanStale = state.PlanStale || state.PlanCheckedAt == 0 ||
			time.Now().Unix()-state.PlanCheckedAt >= 300
		if acc.PlanCheckedAt == 0 {
			acc.PlanLabel = planUnknown // 不沿用旧版本仅凭试用标记猜出的标签。
		}
		acc.QuotaKnown = state.QuotaKnown
		acc.QuotaUnlimited = state.QuotaUnlimited
		acc.QuotaExhausted = state.QuotaExhausted && !state.QuotaUnlimited
		if len(state.ModelStates) == 0 {
			continue
		}
		acc.ModelStates = make(map[string]*modelRuntimeState, len(state.ModelStates))
		for model, saved := range state.ModelStates {
			restored := &modelRuntimeState{
				CostClass: saved.CostClass, CooldownUntil: timeFromUnix(saved.CooldownUntil),
				QuotaBlocked: saved.QuotaBlocked, NextProbeAt: timeFromUnix(saved.NextProbeAt),
				LastReason: saved.LastReason, ObservedAt: timeFromUnix(saved.ObservedAt),
			}
			sanitizeRestoredModelState(restored)
			acc.ModelStates[normalizeModelName(model)] = restored
		}
	}
}

// sanitizeRestoredModelState 修正历史版本写入快照的脏数据。
//
// 旧版本在收到 14018（账号额度耗尽）时会错误地把 CostClass 覆盖成 paid，
// 但 14018 只说明该账号没额度，不能证明模型收费。快照里若出现
// 「QuotaBlocked=true + CostClass=paid + 原因是 14018」这一旧版本特征组合，
// 就把收费属性降回 unknown，交给后续 usage.credit 证据重新学习。
// 新版本不会再产生这种组合（markModelQuotaBlocked 不再改 CostClass）。
func sanitizeRestoredModelState(state *modelRuntimeState) {
	if state == nil || !state.QuotaBlocked || state.CostClass != modelCostPaid {
		return
	}
	reason := strings.ToLower(state.LastReason)
	if strings.Contains(reason, "14018") || strings.Contains(reason, "credits exhausted") ||
		strings.Contains(state.LastReason, "额度已用尽") {
		state.CostClass = modelCostUnknown
	}
}

func timeFromUnix(value int64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	return time.Unix(value, 0)
}

func unixOrZero(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.Unix()
}
