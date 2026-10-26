package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 凭据热加载：serve 运行期间自动发现凭据文件的新增/更新/删除，免重启收敛账号池
// -----------------------------------------------------------------------------

// accountFingerprint 基于文件 mtime+size 生成凭据文件变更指纹。
func accountFingerprint(st os.FileInfo) string {
	return fmt.Sprintf("%d:%d", st.ModTime().UnixNano(), st.Size())
}

// reloadAccounts 将磁盘上的凭据文件与内存账号池原地收敛（热加载）：
//   - 新增凭据文件 → 自动加入账号池；
//   - 凭据内容变化（重新登录/手动更新）→ 原地替换凭据、清除冷却/失效状态并恢复调度；
//   - 凭据文件被删除 → 移出账号池（已写入失效标记的幻影账号保留，用于提示重新登录）。
//
// 返回本次是否发生变更。调用方需自行处理 accountMu 之外的快照刷新。
func reloadAccounts() bool {
	changed, _ := reloadAccountsResult()
	return changed
}

func reloadAccountsResult() (bool, error) {
	accountMu.Lock()
	defer accountMu.Unlock()
	if cfg.AuthDir != "" || !cfg.AuthExplicit {
		dir := cfg.AuthDir
		if dir == "" {
			dir = "."
		}
		if _, err := os.ReadDir(dir); err != nil {
			return false, fmt.Errorf("凭据目录不可读取，账号池保持不变")
		}
	}

	changed := false
	var failures []error
	seen := make(map[string]bool)

	for _, p := range collectConfiguredAuthPaths() {
		st, err := os.Stat(p)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				failures = append(failures, fmt.Errorf("凭据文件无法读取"))
			}
			continue
		}
		seen[p] = true
		fp := accountFingerprint(st)

		var existing *Account
		for _, acc := range accounts {
			if acc.Path == p {
				existing = acc
				break
			}
		}

		if existing == nil {
			// 新凭据文件：加载并加入账号池（文件可能正在写入，失败则下轮重试）
			sa, err := loadAccountFile(p)
			if err != nil {
				failures = append(failures, fmt.Errorf("新增凭据不可解析，等待下轮重试"))
				log.Printf("[Reload] 凭据文件 %s 暂不可解析，下轮重试: %v", p, err)
				continue
			}
			accounts = append(accounts, &Account{
				Path:        p,
				Auth:        sa,
				Edition:     profileForEdition(sa.Edition).Key,
				fingerprint: fp,
			})
			log.Printf("[Reload] 发现新账号凭据 %s（%s），已自动加入账号池", p, profileForEdition(sa.Edition).Label)
			changed = true
			continue
		}

		if existing.fingerprint == fp {
			continue // 未变化
		}

		// 内容变化：原地替换凭据（重新登录或手动更新）
		sa, err := loadAccountFile(p)
		if err != nil {
			failures = append(failures, fmt.Errorf("已更新凭据不可解析，保留旧内容"))
			// 不更新指纹，下一轮重试（正常写入窗口极短，几乎必在下轮成功）
			log.Printf("[Reload] 凭据文件 %s 变更但暂不可解析，保留旧凭据: %v", p, err)
			continue
		}
		recovered := existing.Disabled
		existing.Auth = sa
		existing.Edition = profileForEdition(sa.Edition).Key
		existing.Disabled = false
		existing.DisabledReason = ""
		existing.CooldownUntil = time.Time{}
		existing.CooldownMsg = ""
		existing.QuotaKnown = false
		existing.QuotaExhausted = false
		existing.QuotaUnlimited = false
		existing.ModelStates = nil
		existing.fingerprint = fp
		clearDisabledMarker(p)
		if recovered {
			log.Printf("[Reload] 账号 %s 重新登录成功，已自动恢复调度", p)
		} else {
			log.Printf("[Reload] 账号 %s 凭据已更新（热加载，无需重启）", p)
		}
		changed = true
	}

	// 收敛：移除已删除的凭据（失效幻影账号保留）
	kept := make([]*Account, 0, len(accounts))
	for _, acc := range accounts {
		if !seen[acc.Path] && !acc.Disabled {
			log.Printf("[Reload] 凭据文件 %s 已删除，已移出账号池", acc.Path)
			changed = true
			continue
		}
		kept = append(kept, acc)
	}
	accounts = kept
	if len(accounts) > 0 {
		rrIndex %= len(accounts)
	} else {
		rrIndex = 0
	}
	return changed, errors.Join(failures...)
}

// accountReloaderLoop serve 后台凭据热加载协程：周期扫描凭据变化并原地收敛账号池。
func accountReloaderLoop() {
	interval := time.Duration(cfg.ReloadInterval) * time.Second
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var lastLogged time.Time
	var previousStatus string
	logScheduledTask("credential_reload", "scheduled", "", newTraceID(), time.Time{}, time.Now().Add(interval), "")
	for tick := range ticker.C {
		next := tick.Add(interval)
		started := time.Now()
		changed, err := reloadAccountsResult()
		if changed {
			requestStatusSnapshotRefresh() // 立即刷新 monitor 状态文件
			requestQuotaScan()
			// 目录刷新成功后会自动触发价格探测；这里再直接触发一次，
			// 保证「原本没有某站点账号、后来加入」时也能立刻对该站点模型探测。
			requestModelsScan()
			requestModelsProbe()
		}
		status, detail := "success", "扫描完成，凭据没有变化"
		if changed {
			detail = "账号池变化已应用，相关刷新已通知"
		}
		if err != nil {
			status, detail = "partial", safeDebugError(err)
			if strings.HasPrefix(err.Error(), "凭据目录不可读取") {
				status = "failed"
			}
		}
		logFrequentTask("credential_reload", status, detail, started, next, &lastLogged, &previousStatus, changed)
	}
}

func normalizeModelName(model string) string {
	return strings.ToLower(strings.TrimSpace(model))
}

func modelStateLocked(acc *Account, model string) *modelRuntimeState {
	if acc.ModelStates == nil {
		acc.ModelStates = make(map[string]*modelRuntimeState)
	}
	model = normalizeModelName(model)
	state := acc.ModelStates[model]
	if state == nil {
		state = &modelRuntimeState{CostClass: modelCostUnknown}
		acc.ModelStates[model] = state
	}
	return state
}

func usableForModelLocked(acc *Account, model string, now time.Time) (accountSelectionKind, bool) {
	if acc.Disabled || acc.CooldownUntil.After(now) {
		return "", false
	}
	// 上游目录明确只在另一站点提供该模型时，不再把请求发到本账号所在站点。
	// 目录尚未覆盖的模型保持原有行为，交给上游判断。
	if modelSiteSupport(accSiteLocked(acc), model) == siteSupportUnsupported {
		return "", false
	}
	model = normalizeModelName(model)
	state := acc.ModelStates[model]
	if state == nil {
		if !acc.QuotaExhausted {
			return selectionNormal, true
		}
		return selectionProbeExhausted, true
	}
	if state.CooldownUntil.After(now) {
		return "", false
	}
	if state.QuotaBlocked {
		return "", false
	}
	if !acc.QuotaExhausted {
		return selectionNormal, true
	}
	if state.CostClass == modelCostFree && !state.QuotaBlocked {
		return selectionFreeExhausted, true
	}
	if state.CostClass == modelCostPaid || now.Before(state.NextProbeAt) {
		return "", false
	}
	return selectionProbeExhausted, true
}

// nextAccountForModel 按「免费耗尽账号优先 → 有余额账号 → 零余额未知模型受控探测」选号。
func nextAccountForModel(model string, attempted map[*Account]bool) (*Account, accountSelectionKind, error) {
	// 站点优先级必须在加锁前计算：preferredFreeSites 会读取账号账本并自行获取 accountMu，
	// 若在持锁期间调用会自锁。语义：该模型「一个站点免费、另一个站点收费」时优先用免费站点，
	// 直到该站点账号全部不可用；其余情况不搞优先，正常轮询。
	preferred := preferredFreeSites(model)

	accountMu.Lock()
	defer accountMu.Unlock()

	// 账号账本里已有成功调用记录时，先把它恢复成站点支持证据，避免目录漏收导致误排除。
	rememberServedSiteModelsLocked()

	if len(accounts) == 0 {
		return nil, "", fmt.Errorf("账号池为空")
	}
	allowed := 0
	for _, acc := range accounts {
		if ok, _ := modelAccountAllowed(model, acc, accounts); ok {
			allowed++
		}
	}
	if allowed == 0 && modelAccountRuleConfigured(model) {
		return nil, "", fmt.Errorf("%w: 模型 %s 没有符合账号黑白名单的凭据文件", errModelAccountPolicy, model)
	}
	now := time.Now()

	pick := func(siteFilter func(string) bool) (*Account, accountSelectionKind, bool) {
		for _, wanted := range []accountSelectionKind{selectionFreeExhausted, selectionNormal, selectionProbeExhausted} {
			for i := 0; i < len(accounts); i++ {
				idx := (rrIndex + i) % len(accounts)
				acc := accounts[idx]
				if attempted != nil && attempted[acc] {
					continue
				}
				if ok, _ := modelAccountAllowed(model, acc, accounts); !ok {
					continue
				}
				if siteFilter != nil && !siteFilter(accSiteLocked(acc)) {
					continue
				}
				kind, ok := usableForModelLocked(acc, model, now)
				if !ok || kind != wanted {
					continue
				}
				if kind == selectionProbeExhausted {
					modelStateLocked(acc, model).NextProbeAt = now.Add(modelProbeDelay)
				}
				rrIndex = (idx + 1) % len(accounts)
				return acc, kind, true
			}
		}
		return nil, "", false
	}

	if len(preferred) > 0 {
		if acc, kind, ok := pick(func(site string) bool { return preferred[site] }); ok {
			return acc, kind, nil
		}
		log.Printf("[FreeSite] 模型 %s 的免费站点账号当前均不可用，回退到其余站点代偿", model)
	}
	if acc, kind, ok := pick(nil); ok {
		return acc, kind, nil
	}

	disabled, accountCooling, modelCooling, quotaBlocked, probeWaiting, policyBlocked, siteUnsupported := 0, 0, 0, 0, 0, 0, 0
	earliest := time.Time{}
	for _, acc := range accounts {
		if ok, _ := modelAccountAllowed(model, acc, accounts); !ok {
			policyBlocked++
			continue
		}
		if modelSiteSupport(accSiteLocked(acc), model) == siteSupportUnsupported {
			siteUnsupported++
			continue
		}
		switch {
		case acc.Disabled:
			disabled++
		case acc.CooldownUntil.After(now):
			accountCooling++
			if earliest.IsZero() || acc.CooldownUntil.Before(earliest) {
				earliest = acc.CooldownUntil
			}
		default:
			state := acc.ModelStates[normalizeModelName(model)]
			if state == nil {
				if acc.QuotaExhausted {
					probeWaiting++
				}
				continue
			}
			if state.CooldownUntil.After(now) {
				modelCooling++
				if earliest.IsZero() || state.CooldownUntil.Before(earliest) {
					earliest = state.CooldownUntil
				}
			} else if state.QuotaBlocked || acc.QuotaExhausted && state.CostClass == modelCostPaid {
				quotaBlocked++
			} else if acc.QuotaExhausted && now.Before(state.NextProbeAt) {
				probeWaiting++
			}
		}
	}
	msg := fmt.Sprintf("当前模型 %s 暂无可用账号：账号名单排除=%d，授权失效=%d，账号冷却=%d，模型冷却=%d，额度阻断=%d，等待探测=%d，站点不支持=%d", model, policyBlocked, disabled, accountCooling, modelCooling, quotaBlocked, probeWaiting, siteUnsupported)
	if !earliest.IsZero() {
		msg += "，最早恢复=" + earliest.Format("2006-01-02 15:04:05")
	}
	return nil, "", fmt.Errorf("%s", msg)
}

func nextAccount() (*Account, error) {
	acc, _, err := nextAccountForModel("", nil)
	return acc, err
}

// markCooldown 将账号屏蔽至指定时间。
func markCooldown(acc *Account, until time.Time, msg string) {
	accountMu.Lock()
	acc.CooldownUntil = until
	acc.CooldownMsg = msg
	accountMu.Unlock()
	log.Printf("[Cooldown] 账号 %s 触发频率限制，自动屏蔽至 %s (提示: %s)",
		acc.Path, until.Format("2006-01-02 15:04:05"), msg)
}

func markModelCooldown(acc *Account, model string, until time.Time, msg string) {
	accountMu.Lock()
	state := modelStateLocked(acc, model)
	state.CooldownUntil = until
	state.LastReason = msg
	accountMu.Unlock()
	log.Printf("[ModelCooldown] 账号 %s 模型 %s 触发模型级频率限制，仅屏蔽该模型至 %s；其他模型仍可调度", acc.Path, model, until.Format("2006-01-02 15:04:05"))
	requestStatusSnapshotRefresh()
}

// markModelQuotaBlocked 记录一次「账号额度耗尽」观测。
//
// 注意：14018 的语义是「该账号的额度用光了」，与模型是否收费无关。
// 上游对额度为 0 的账号会整体拒绝（实测连免费模型也返回 14018），
// 因此这里只标记 QuotaBlocked（该账号暂时无法服务该模型），
// 绝不覆盖 CostClass —— 模型收费属性只能由 usage.credit 证据决定。
func markModelQuotaBlocked(acc *Account, model, msg string) {
	accountMu.Lock()
	state := modelStateLocked(acc, model)
	oldClass := state.CostClass
	state.QuotaBlocked = true
	state.LastReason = msg
	state.ObservedAt = time.Now()
	accountMu.Unlock()
	log.Printf("[ModelQuota] 账号 %s 模型 %s 返回额度耗尽(14018)：仅阻断该账号对该模型的调度，账号额度恢复后自动解除；模型收费属性保持=%s，未被本次错误覆盖",
		acc.Path, model, ifEmpty(oldClass, modelCostUnknown))
	requestStatusSnapshotRefresh()
}

// disableAccount 将账号标记为失效（授权过期/撤销），禁止调度并删除凭据文件，
// 同时写入持久化失效标记，便于控制台提示用户重新登录。
func disableAccount(acc *Account, reason string) {
	accountMu.Lock()
	acc.Disabled = true
	acc.DisabledReason = reason
	acc.CooldownUntil = time.Time{}
	path := acc.Path
	nickname := ""
	uid := ""
	edition := ""
	var authSnapshot *StoredAuth
	if acc.Auth != nil {
		nickname = acc.Auth.Account.Nickname
		uid = acc.Auth.Account.UID
		edition = profileForEdition(acc.Auth.Edition).Key
		acc.Nickname = nickname
		acc.UID = uid
		acc.Edition = edition
		copied := *acc.Auth
		authSnapshot = &copied
	}
	accountMu.Unlock()

	// 写持久化失效标记（凭据删除后仍能在 status 中提示重新登录）
	marker := disabledMarker{
		Path:       path,
		Reason:     reason,
		DisabledAt: time.Now().Unix(),
		Nickname:   nickname,
		UID:        uid,
		Edition:    edition,
	}
	if data, err := json.MarshalIndent(marker, "", "  "); err == nil {
		if werr := os.WriteFile(markerPath(path), data, 0600); werr != nil {
			log.Printf("[Auth] 账号 %s 失效标记写入失败: %v", path, werr)
		}
	}

	// 删除凭据前先做一次只读校验：删除不可逆，只有确认凭据真的不可用才销毁。
	if keep, note := credentialStillUsableForDelete(authSnapshot); keep {
		log.Printf("[Auth] 账号 %s 已停止调度；%s；凭据文件保留在 %s（如需彻底移除请手动删除），重新登录会自动覆盖",
			path, note, path)
		requestStatusSnapshotRefresh()
		return
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Printf("[Auth] 账号 %s 凭据文件删除失败: %v", path, err)
	}
	log.Printf("[Auth] 账号 %s 授权失效，已禁止调度并删除凭据文件（删除前只读校验确认不可用）: %s", path, reason)
	requestStatusSnapshotRefresh()
}

// loadDisabledMarkers 扫描失效标记文件，将其恢复为账号池中的失效账号（Auth 为 nil）。
func loadDisabledMarkers() {
	// 收集标记文件路径：
	// - AuthDir 模式：目录下 *.json.disabled 即为标记文件
	// - 显式 -auth 模式：<凭据路径>.disabled 为标记文件
	// - 自动发现模式：扫描当前目录下所有 *.json.disabled 标记文件
	var markerPaths []string
	if cfg.AuthDir != "" {
		entries, err := os.ReadDir(cfg.AuthDir)
		if err != nil {
			return
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if strings.HasSuffix(name, ".json.disabled") {
				markerPaths = append(markerPaths, filepath.Join(cfg.AuthDir, name))
			}
		}
	} else if cfg.AuthExplicit {
		for _, p := range strings.Split(cfg.AuthFile, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				markerPaths = append(markerPaths, markerPath(p))
			}
		}
	} else {
		// 自动发现模式：扫描当前工作目录
		entries, err := os.ReadDir(".")
		if err == nil {
			for _, e := range entries {
				if e.IsDir() {
					continue
				}
				name := e.Name()
				if strings.HasPrefix(name, "workbuddy") && strings.HasSuffix(name, ".json.disabled") {
					markerPaths = append(markerPaths, name)
				}
			}
			sort.Strings(markerPaths)
		}
	}

	for _, mp := range markerPaths {
		data, err := os.ReadFile(mp)
		if err != nil {
			continue
		}
		var m disabledMarker
		if err := json.Unmarshal(data, &m); err != nil {
			continue
		}
		// 凭据文件比标记更新，说明用户已重新登录或手动续期，旧标记已过期。
		if fi, statErr := os.Stat(m.Path); statErr == nil {
			if mi, mErr := os.Stat(mp); mErr == nil && fi.ModTime().After(mi.ModTime()) {
				log.Printf("[Auth] 账号 %s 凭据文件比失效标记更新，视为已重新登录，清除失效标记", m.Path)
				_ = os.Remove(mp)
				continue
			}
		}
		// 凭据文件仍在时，标记负责把该账号重新置为失效（保留文件是为了可恢复）。
		applied := false
		for _, a := range accounts {
			if a.Path != m.Path {
				continue
			}
			a.Disabled = true
			a.DisabledReason = m.Reason
			if a.Nickname == "" {
				a.Nickname = m.Nickname
			}
			if a.UID == "" {
				a.UID = m.UID
			}
			applied = true
			break
		}
		if applied {
			continue
		}
		accounts = append(accounts, &Account{
			Path:           m.Path,
			Disabled:       true,
			DisabledReason: m.Reason,
			Nickname:       m.Nickname,
			UID:            m.UID,
			Edition:        m.Edition,
		})
	}
}

// clearDisabledMarker 在重新登录成功后清除失效标记。
func clearDisabledMarker(path string) {
	mp := markerPath(path)
	if err := os.Remove(mp); err != nil && !os.IsNotExist(err) {
		log.Printf("[Auth] 失效标记清除失败 (%s): %v", mp, err)
	}
}

// -----------------------------------------------------------------------------
// 以下内容原位于 accountfiles.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// 凭据加载、保存与自动续期
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

// -----------------------------------------------------------------------------
// 以下内容原位于 accounthealth.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

type accountHealth struct {
	Percent *float64 `json:"percent"`
	State   string   `json:"state"` // healthy / degraded / unavailable / empty / unknown
}

// 保持账号表的可用口径；全局主动停用也在分母内，不能被排除后虚报100%。
// 这不是某个指定模型的可调度率，不把单模型冷却等同于整个账号失效。
func summarizeAccountHealth(accs []accountSnapshot, stale bool) accountHealth {
	if stale {
		return accountHealth{State: "unknown"}
	}
	if len(accs) == 0 {
		return accountHealth{State: "empty"}
	}
	healthy := 0
	for _, acc := range accs {
		if acc.State == "active" && (acc.SchedulingEnabled == nil || *acc.SchedulingEnabled) {
			healthy++
		}
	}
	// 向下保留一位，避免999/1000四舍五入成100%。
	percent := math.Floor(float64(healthy)*1000/float64(len(accs))) / 10
	state := "degraded"
	if healthy == len(accs) {
		state = "healthy"
	} else if healthy == 0 {
		state = "unavailable"
	}
	return accountHealth{Percent: &percent, State: state}
}

type accountAlertRule struct {
	Label    string
	Severity string
	Message  string
}

var accountAlertRules = map[string]accountAlertRule{
	"disabled":       {"账号失效", "urgent", "登录授权已失效，账号已停止调度，请重新登录。"},
	"expired":        {"账号过期", "important", "访问令牌已过期，请检查自动续期结果或重新登录。"},
	"paid_exhausted": {"积分用尽", "important", "已确认账号额度耗尽，收费模型暂不可用；等待额度恢复。"},
	"cooldown":       {"账号冷却", "warning", "账号触发频率限制，冷却期间暂不调度，请等待自动恢复。"},
	"paused":         {"账号停用", "notice", "该账号被全局调度规则停用；这不是授权失效，可在账号池恢复。"},
}

func accountAlertKinds(acc accountSnapshot) []string {
	kinds := []string{}
	if acc.SchedulingEnabled != nil && !*acc.SchedulingEnabled {
		kinds = append(kinds, "paused")
	}
	state := acc.State
	if state == "quota_exhausted" {
		state = "paid_exhausted"
	}
	if _, ok := accountAlertRules[state]; ok {
		kinds = append(kinds, state)
	}
	return kinds
}

type accountLogEvent struct {
	At      int64  `json:"at"`
	Account string `json:"account"`
	State   string `json:"state"`
	Trace   string `json:"trace,omitempty"`
	File    string `json:"-"`
}

// 仅用于压低重复日志：不建立新的状态文件。启动及状态转换写一条普通日志，
// 让过期/停用（包括直接编辑配置）也能有真实的可追溯日志来源。

// 兼容已有普通日志。仅匹配已知日志标签/措辞，不将任意包含“失效”的正文当告警。
func parseAccountLogEvent(line, file string) (accountLogEvent, bool) {
	event := accountLogEvent{At: logLineTime(line), File: file}
	if event.At <= 0 || len(line) < 21 {
		return event, false
	}
	body := line[20:]
	if strings.HasPrefix(body, "[账号状态] ") {
		if json.Unmarshal([]byte(strings.TrimPrefix(body, "[账号状态] ")), &event) != nil {
			return accountLogEvent{}, false
		}
		event.File = file
		_, known := accountAlertRules[event.State]
		return event, known && event.At > 0 && event.Account != ""
	}
	accountBetween := func(suffix string) string {
		_, rest, ok := strings.Cut(body, "账号 ")
		if !ok {
			return ""
		}
		name, _, ok := strings.Cut(rest, suffix)
		if !ok {
			return ""
		}
		return strings.TrimSpace(name)
	}
	switch {
	case strings.HasPrefix(body, "[Cooldown] "):
		event.State, event.Account = "cooldown", accountBetween(" 触发频率限制")
	case strings.HasPrefix(body, "[Quota] "):
		if strings.Contains(body, "账号已冻结调度") || strings.Contains(body, "分配额度已用完") {
			event.State = "paid_exhausted"
			event.Account = accountBetween(" 额度查询成功")
			if event.Account == "" {
				event.Account = accountBetween(" 企业额度计算=")
			}
		}
	case strings.HasPrefix(body, "[Auth] "):
		for _, suffix := range []string{" 判定登录态失效", " 授权失效，已禁止调度", " 已停止调度；"} {
			if name := accountBetween(suffix); name != "" {
				event.Account, event.State = name, "disabled"
				break
			}
		}
	case strings.HasPrefix(body, "[管理开关] ") && strings.Contains(body, "结果=成功 类型=account ") && strings.Contains(body, " 启用=false "):
		_, rest, ok := strings.Cut(body, "目标=")
		if ok {
			quoted, _, ok := strings.Cut(rest, " 启用=")
			if ok {
				if name, err := strconv.Unquote(quoted); err == nil {
					event.Account, event.State = name, "paused"
				}
			}
		}
	}
	_, known := accountAlertRules[event.State]
	return event, known && event.Account != ""
}

const accountAlertReadBudget int64 = 4 << 20

type accountAlertScan struct {
	Events    map[string]accountLogEvent
	Scanned   int64
	Truncated bool
}

// 只扫描普通日志，尾部总预算4MiB；读取失败不影响健康度接口或模型请求。
func readAccountAlertLogs() (accountAlertScan, error) {
	scan := accountAlertScan{Events: map[string]accountLogEvent{}}
	info, err := os.Lstat(logDir)
	if errors.Is(err, os.ErrNotExist) {
		return scan, nil
	}
	if err != nil {
		return scan, err
	}
	if !info.IsDir() {
		return scan, errors.New("日志路径不是目录")
	}
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return scan, err
	}
	var names []string
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasPrefix(entry.Name(), "gateway-") && logFilePattern.MatchString(entry.Name()) {
			names = append(names, entry.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	root, err := os.OpenRoot(logDir)
	if err != nil {
		return scan, err
	}
	defer root.Close()
	for _, name := range names {
		budget := accountAlertReadBudget - scan.Scanned
		if budget <= 0 {
			scan.Truncated = true
			break
		}
		f, err := root.Open(name)
		if err != nil {
			return scan, err
		}
		info, err := f.Stat()
		if err != nil || !info.Mode().IsRegular() {
			f.Close()
			return scan, errors.New("账号告警日志不可读")
		}
		size := min(info.Size(), budget)
		start := info.Size() - size
		data := make([]byte, size)
		n, readErr := f.ReadAt(data, start)
		f.Close()
		if (readErr != nil && readErr != io.EOF) || int64(n) != size {
			return scan, errors.New("账号告警日志读取中变化，请重试")
		}
		scan.Scanned += size
		if start > 0 {
			scan.Truncated = true
			if end := bytes.IndexByte(data, '\n'); end >= 0 {
				data = data[end+1:]
			} else {
				data = nil
			}
		}
		// 忽略最后尚未写完整的一行。
		lines := bytes.Split(data, []byte{'\n'})
		for _, line := range lines[:len(lines)-1] {
			if event, ok := parseAccountLogEvent(string(line), name); ok {
				key := event.Account + "\x00" + event.State
				if old, exists := scan.Events[key]; !exists || event.At > old.At {
					scan.Events[key] = event
				}
			}
		}
	}
	return scan, nil
}

var accountAlertCache struct {
	sync.Mutex
	at   time.Time
	scan accountAlertScan
}

type accountAlertView struct {
	Account   string `json:"account"`
	State     string `json:"state"`
	Label     string `json:"label"`
	Severity  string `json:"severity"`
	Message   string `json:"message"`
	At        int64  `json:"at"`
	LogFile   string `json:"logFile"`
	LogOffset int64  `json:"logOffset,omitempty"`
}

func currentAccountAlerts(accs []accountSnapshot, scan accountAlertScan) ([]accountAlertView, int) {
	out := []accountAlertView{}
	missing := 0
	for _, acc := range accs {
		for _, kind := range accountAlertKinds(acc) {
			event, ok := scan.Events[acc.Path+"\x00"+kind]
			if !ok {
				missing++
				continue // 无日志不编造告警“记录”。
			}
			rule := accountAlertRules[kind]
			out = append(out, accountAlertView{Account: acc.Path, State: kind, Label: rule.Label,
				Severity: rule.Severity, Message: rule.Message, At: event.At, LogFile: event.File})
		}
	}
	// 最新日志在上；等级只决定颜色和标签，不改变排序。
	// 秒级时间相同则按账号、类别稳定排序，避免每次轮询跳动。
	sortAccountAlerts(out)
	return out, missing
}

func handleWebUIAccountAlerts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "账号告警只读，仅支持 GET")
		return
	}
	snap, err := readStatusSnapshot()
	if err != nil {
		webSnapshotUnavailable(w, r, err)
		return
	}
	annotateAccountPolicies(snap.Accounts)
	// 新版快照自带告警标记：直接使用，不再读取日志；只有旧快照才回退扫描。
	inline := snap.AlertSource == alertSourceSnapshot
	accountAlertCache.Lock()
	if !inline && (accountAlertCache.at.IsZero() || time.Since(accountAlertCache.at) >= 10*time.Second) {
		scan, readErr := readAccountAlertLogs()
		if readErr != nil {
			accountAlertCache.Unlock()
			log.Printf("[账号告警] traceId=%s 阶段=读取普通日志 结果=失败 说明=不展示伪造空列表，不影响健康度与模型调用", debugTraceID(r))
			writeJSONError(w, http.StatusServiceUnavailable, "账号告警日志暂不可读，保留已有结果")
			return
		}
		accountAlertCache.scan, accountAlertCache.at = scan, time.Now()
	}
	scan := accountAlertCache.scan
	accountAlertCache.Unlock()
	alerts, missing := []accountAlertView{}, 0
	if inline {
		alerts = currentAlertsFromSnapshot(snap.Accounts)
	} else {
		alerts, missing = currentAccountAlerts(snap.Accounts, scan)
	}
	limited := len(alerts) > 100
	if limited {
		alerts = alerts[:100]
	}
	log.Printf("[账号告警] traceId=%s 结果=成功 条目=%d 缺少日志=%d 扫描字节=%d 扫描截断=%t 排序=最新日志优先 说明=先剔除已恢复项，再按时间倒序取前100条；等级只用于颜色和标签，不含令牌或上游错误正文", debugTraceID(r), len(alerts), missing, scan.Scanned, scan.Truncated)
	writeAdminData(w, r, map[string]any{
		"alerts": alerts, "updatedAt": snap.UpdatedAt, "stale": time.Now().Unix()-snap.UpdatedAt > 15,
		"source": "gateway 普通日志", "missingLogCount": missing,
		"scanTruncated": scan.Truncated, "listTruncated": limited,
	})
}

// -----------------------------------------------------------------------------
// 以下内容原位于 accountalerts.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

// accountAlertMark 是写进状态快照的精简告警标记：只记状态、首次时间与日志位置，
// 不复制任何日志正文。页面据此渲染告警，因此不再依赖后台的日志扫描。
type accountAlertMark struct {
	State     string `json:"state"`
	At        int64  `json:"at"`
	LogFile   string `json:"logFile,omitempty"`
	LogOffset int64  `json:"logOffset,omitempty"`
}

// alertSourceSnapshot 写入快照，用来区分「新版内联告警」与「旧版快照需要回退扫日志」。
const alertSourceSnapshot = "snapshot"

var accountAlertMarks = struct {
	sync.Mutex
	current map[string]accountAlertMark
	last    map[string]string
}{current: map[string]accountAlertMark{}, last: map[string]string{}}

// logWriteOffset 返回当天日志文件当前长度，也就是下一条日志的字节偏移。
// 日志是追加写，所以这个偏移正好指向即将写入的那一行。
func logWriteOffset() (string, int64) {
	name := "gateway-" + time.Now().Format("2006-01-02") + ".log"
	info, err := os.Stat(filepath.Join(logDir, name))
	if err != nil {
		return name, -1
	}
	return name, info.Size()
}

// recordAccountAlertMarks 在状态变化时写一条普通日志，并把当前异常记成快照用的精简标记。
// 返回按凭据路径分组的标记，供 writeStatusSnapshot 一并落盘。
func recordAccountAlertMarks(accs []accountSnapshot, now int64) map[string][]accountAlertMark {
	accountAlertMarks.Lock()
	defer accountAlertMarks.Unlock()
	next := make(map[string]string, len(accs))
	current := map[string]accountAlertMark{}
	trace := newTraceID()
	for _, acc := range accs {
		kinds := accountAlertKinds(acc)
		signature := acc.UID + "\x00" + acc.State + "\x00" + strings.Join(kinds, ",")
		next[acc.Path] = signature
		changed := accountAlertMarks.last[acc.Path] != signature
		if len(kinds) == 0 {
			// 恢复正常也要留一条审计日志，但不再生成告警标记。
			if changed {
				if raw, err := json.Marshal(accountLogEvent{At: now, Account: acc.Path, State: acc.State, Trace: trace}); err == nil {
					log.Printf("[账号状态] %s", raw)
				}
			}
			continue
		}
		for _, kind := range kinds {
			key := acc.Path + "\x00" + kind
			mark, seen := accountAlertMarks.current[key]
			if !seen || mark.At <= 0 {
				mark = accountAlertMark{State: kind, At: now}
			}
			// 只有状态真的变化时才写日志，并记下那一行的文件与偏移。
			if changed && !seen {
				file, offset := logWriteOffset()
				if raw, err := json.Marshal(accountLogEvent{At: now, Account: acc.Path, State: kind, Trace: trace}); err == nil {
					log.Printf("[账号状态] %s", raw)
				}
				mark.LogFile, mark.LogOffset = file, offset
			}
			mark.State = kind
			current[key] = mark
		}
	}
	accountAlertMarks.last = next
	// 必须写回：否则标记每轮都会重建，定位符丢失、首次时间也会被刷新。
	accountAlertMarks.current = current
	out := make(map[string][]accountAlertMark, len(accs))
	for key, mark := range current {
		path := key[:strings.Index(key, "\x00")]
		out[path] = append(out[path], mark)
	}
	return out
}

// sortAccountAlerts 统一排序：时间倒序（最新在上），同秒按账号、类别稳定排序。
func sortAccountAlerts(out []accountAlertView) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].At != out[j].At {
			return out[i].At > out[j].At
		}
		if out[i].Account != out[j].Account {
			return out[i].Account < out[j].Account
		}
		return out[i].State < out[j].State
	})
}

// currentAlertsFromSnapshot 直接按快照里的标记生成告警视图，不再扫描日志文件。
// 快照标记与账号当前状态不一致时（例如刚恢复）不展示，保证「恢复即消除」。
func currentAlertsFromSnapshot(accs []accountSnapshot) []accountAlertView {
	out := []accountAlertView{}
	for _, acc := range accs {
		want := map[string]bool{}
		for _, kind := range accountAlertKinds(acc) {
			want[kind] = true
		}
		for _, mark := range acc.Alerts {
			rule, known := accountAlertRules[mark.State]
			if !known || !want[mark.State] {
				continue
			}
			out = append(out, accountAlertView{Account: acc.Path, State: mark.State, Label: rule.Label,
				Severity: rule.Severity, Message: rule.Message, At: mark.At, LogFile: mark.LogFile,
				LogOffset: mark.LogOffset})
		}
	}
	sortAccountAlerts(out)
	return out
}
