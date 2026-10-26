package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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
	"time"

	qrcode "github.com/skip2/go-qrcode"
)

// accountFileNamePattern 限定新增账号只能填一个文件名，不接受目录或通配符。
var accountFileNamePattern = regexp.MustCompile(`^workbuddy[A-Za-z0-9._-]*\.json$`)

// accountCredentialDir 返回凭据落地目录：-auth-dir 优先，否则就是程序运行目录。
func accountCredentialDir() (string, error) {
	accountMu.Lock()
	dir := strings.TrimSpace(cfg.AuthDir)
	accountMu.Unlock()
	if dir == "" {
		dir = "."
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("无法解析凭据目录：%w", err)
	}
	return abs, nil
}

// resolveCredentialPath 把用户填写的文件名解析成运行目录下的绝对路径，绝不接受目录。
func resolveCredentialPath(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("请填写凭据名")
	}
	if name != filepath.Base(name) || strings.ContainsAny(name, `/\*?[]`) {
		return "", errors.New("凭据名只能是文件名，不能包含目录或通配符")
	}
	if !accountFileNamePattern.MatchString(name) {
		return "", errors.New("凭据名需以 workbuddy 开头、以 .json 结尾，只能用字母数字和 . _ -")
	}
	dir, err := accountCredentialDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, name), nil
}

// suggestCredentialFileName 取目录下第一个没被占用的 workbuddyN.json。
func suggestCredentialFileName() string {
	used := map[int]bool{}
	dir, err := accountCredentialDir()
	if err == nil {
		if entries, readErr := os.ReadDir(dir); readErr == nil {
			for _, e := range entries {
				name := strings.TrimSuffix(e.Name(), ".disabled")
				if !strings.HasPrefix(name, "workbuddy") || !strings.HasSuffix(name, ".json") {
					continue
				}
				if n, convErr := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "workbuddy"), ".json")); convErr == nil {
					used[n] = true
				}
			}
		}
	}
	for n := 1; ; n++ {
		if !used[n] {
			return fmt.Sprintf("workbuddy%d.json", n)
		}
	}
}

// loginSession 是一次进行中的扫码登录。CLI 的 login 与网页新增账号共用这条路径，
// 区别只在谁来展示二维码、谁把凭据路径拼出来。
type loginSession struct {
	ID      string
	Site    string
	File    string
	AuthURL string

	state    string
	deadline time.Time
	client   *http.Client
	prof     *upstreamProfile
	cancel   context.CancelFunc

	mu      sync.Mutex
	status  string // pending / done / failed / expired / cancelled
	errText string
	auth    *StoredAuth
	savedTo string
}

var (
	loginSessionsMu sync.Mutex
	loginSessions   = map[string]*loginSession{}
)

// loginSessionRetention 让已结束的登录会话在内存中多保留一段时间。
// CLI 直接持有 *loginSession，不依赖会话表；网页只拿到 id，必须再查一次才能看到
// expired / failed / done。若结束时立刻删除，前端轮询几乎总是拿到 404，表现为
// 「链接失效了但控制台还在等」。
var loginSessionRetention = 2 * time.Minute

func newLoginSessionID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("login-%d", time.Now().UnixNano())
	}
	return "login-" + hex.EncodeToString(buf)
}

// qrDataURI 生成二维码图片；失败只影响展示，不影响登录流程本身。
func (s *loginSession) qrDataURI() string {
	code, err := qrcode.New(s.AuthURL, qrcode.Medium)
	if err != nil {
		return ""
	}
	png, err := code.PNG(240)
	if err != nil {
		return ""
	}
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(png)
}

func (s *loginSession) snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]any{
		"id": s.ID, "site": s.Site, "file": s.File, "authUrl": s.AuthURL,
		"status": s.status, "expiresAt": s.deadline.Unix(),
	}
	if s.status == "pending" {
		remaining := int(time.Until(s.deadline).Seconds())
		if remaining < 0 {
			remaining = 0
		}
		out["remainingSeconds"] = remaining
	} else {
		out["remainingSeconds"] = 0
	}
	if s.errText != "" {
		out["error"] = s.errText
	}
	if s.status == "done" {
		out["savedAs"] = filepath.Base(s.savedTo)
		if s.auth != nil {
			out["account"] = map[string]any{
				"nickname": s.auth.Account.Nickname, "uid": s.auth.Account.UID,
				"enterpriseId": s.auth.Account.EnterpriseID, "tokenExpiresAt": s.auth.Auth.ExpiresAt,
			}
		}
	}
	return out
}

func (s *loginSession) finish(status, errText string, sa *StoredAuth, saved string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status != "pending" {
		return
	}
	s.status, s.errText, s.auth, s.savedTo = status, errText, sa, saved
}

func forgetLoginSession(id string) {
	loginSessionsMu.Lock()
	delete(loginSessions, id)
	loginSessionsMu.Unlock()
}

// scheduleForgetLoginSession 延迟清理已结束的会话，给网页轮询留出读取终态的时间。
func scheduleForgetLoginSession(id string) {
	time.AfterFunc(loginSessionRetention, func() {
		forgetLoginSession(id)
	})
}

// beginAccountLogin 复用 CLI login 的同一条流程：先取 state 与二维码，再轮询换取 token 并落地凭据。
func beginAccountLogin(site, targetPath string) (*loginSession, error) {
	prof := profileForEdition(site)
	client := &http.Client{Timeout: 30 * time.Second, Jar: cfg.HttpClient.Jar}
	pollHeaders := func(r *http.Request) {
		commonHeaders(r, prof)
		r.Header.Set("X-No-Authorization", "1")
	}
	data, _, err := doJSON(client, http.MethodPost, prof.authStateURL(), nil, bytes.NewReader([]byte("{}")))
	if err != nil {
		return nil, fmt.Errorf("获取登录状态失败: %w", err)
	}
	var st authStateData
	_ = json.Unmarshal(data, &st)
	if st.State == "" || st.AuthURL == "" {
		return nil, errors.New("上游返回的登录状态信息异常，请重试")
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &loginSession{
		ID: newLoginSessionID(), Site: prof.Key, File: filepath.Base(targetPath),
		AuthURL: st.AuthURL, state: st.State, deadline: time.Now().Add(prof.LoginTTL),
		client: client, prof: prof, cancel: cancel, status: "pending",
	}
	go s.run(ctx, targetPath, pollHeaders)
	return s, nil
}

func (s *loginSession) run(ctx context.Context, targetPath string, pollHeaders func(*http.Request)) {
	trace := newTraceID()
	log.Printf("[网页登录] traceId=%s 阶段=开始 站点=%s 凭据=%s 说明=已复用 CLI login 同一流程，等待扫码授权",
		trace, s.Site, filepath.Base(targetPath))
	defer scheduleForgetLoginSession(s.ID)
	defer s.cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.finish("cancelled", "", nil, "")
			return
		case <-ticker.C:
			if time.Now().After(s.deadline) {
				log.Printf("[网页登录] traceId=%s 阶段=等待扫码 结果=超时 站点=%s 业务影响=未写入凭据，账号池不变", trace, s.Site)
				s.finish("expired", "登录已超时，请重新获取登录接口", nil, "")
				return
			}
			tokRaw, _, errTok := doJSON(s.client, http.MethodGet, s.prof.authTokenURL(s.state), pollHeaders, nil)
			if errTok != nil {
				continue
			}
			var tok tokenData
			if err := json.Unmarshal(tokRaw, &tok); err != nil || tok.AccessToken == "" {
				continue
			}
			var acct accountData
			acctHeaders := func(r *http.Request) {
				commonHeaders(r, s.prof)
				r.Header.Set("Authorization", "Bearer "+tok.AccessToken)
			}
			if acctRaw, _, errAcct := doJSON(s.client, http.MethodGet, s.prof.loginAcctURL(s.state), acctHeaders, nil); errAcct == nil {
				_ = json.Unmarshal(acctRaw, &acct)
			}
			sa := &StoredAuth{
				Edition: s.prof.Key,
				Auth: StoredTokens{
					AccessToken:  tok.AccessToken,
					RefreshToken: tok.RefreshToken,
					ExpiresAt:    time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second).Unix(),
					Domain:       tok.Domain,
				},
				Account: StoredAccount{UID: acct.UID, EnterpriseID: acct.EnterpriseID, Nickname: acct.Nickname},
			}
			if err := saveAuthTo(targetPath, sa); err != nil {
				log.Printf("[网页登录] traceId=%s 阶段=写入凭据 结果=失败 凭据=%s 原因=%v 业务影响=账号未加入账号池",
					trace, filepath.Base(targetPath), err)
				s.finish("failed", "保存凭据失败："+err.Error(), nil, "")
				return
			}
			if err := ensureAccountLoaded(targetPath); err != nil {
				log.Printf("[网页登录] traceId=%s 阶段=加入账号池 结果=失败 凭据=%s 原因=%v 业务影响=文件已写入，可在下次扫描时加载",
					trace, filepath.Base(targetPath), err)
			}
			requestQuotaScan()
			log.Printf("[网页登录] traceId=%s 阶段=完成 站点=%s 凭据=%s 账号=%s 结果=成功 说明=凭据已自动加载，无需重启",
				trace, s.Site, filepath.Base(targetPath), sa.Account.UID)
			s.finish("done", "", sa, targetPath)
			return
		}
	}
}

// ensureAccountLoaded 让刚写入的凭据立刻进入账号池，不依赖目录扫描是否覆盖该文件。
func ensureAccountLoaded(path string) error {
	if _, err := reloadAccountsResult(); err != nil {
		return err
	}
	accountMu.Lock()
	for _, acc := range accounts {
		if acc.Path == path {
			accountMu.Unlock()
			return nil
		}
	}
	accountMu.Unlock()
	sa, err := loadAccountFile(path)
	if err != nil {
		return err
	}
	accountMu.Lock()
	accounts = append(accounts, &Account{Path: path, Auth: sa, Edition: profileForEdition(sa.Edition).Key})
	accountMu.Unlock()
	log.Printf("[账号新增] 凭据=%s 结果=已加入账号池 说明=目录扫描未覆盖该文件时由网页新增流程直接补入", filepath.Base(path))
	return nil
}

func findLoginSession(id string) *loginSession {
	loginSessionsMu.Lock()
	defer loginSessionsMu.Unlock()
	return loginSessions[id]
}

// deleteAccountCredential 删除指定凭据文件（只认账号池里已有的那个路径）。
func deleteAccountCredential(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || name != filepath.Base(name) {
		return "", errors.New("凭据名无效")
	}
	accountMu.Lock()
	target, duplicates := "", 0
	for _, acc := range accounts {
		if filepath.Base(acc.Path) == name {
			duplicates++
			target = acc.Path
		}
	}
	accountMu.Unlock()
	if duplicates > 1 {
		return "", errors.New("存在同名凭据，无法确定要删除哪一个")
	}
	if target == "" {
		return "", errors.New("账号不存在或已删除")
	}
	if !accountFileNamePattern.MatchString(filepath.Base(target)) {
		return "", errors.New("凭据名不符合 workbuddy*.json，拒绝删除")
	}
	info, err := os.Lstat(target)
	if err != nil {
		return "", fmt.Errorf("凭据不可读取：%w", err)
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("凭据不是普通文件，拒绝删除")
	}
	if err := os.Remove(target); err != nil {
		return "", fmt.Errorf("删除凭据失败：%w", err)
	}
	_ = os.Remove(markerPath(target))
	reloadAccountsResult()
	requestQuotaScan()
	return target, nil
}

// handleWebUIAccountAdd 支持读取默认名（GET）、发起登录（POST）、查询进度（GET ?id=）。
func handleWebUIAccountAdd(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		id := strings.TrimSpace(r.URL.Query().Get("id"))
		if id == "" {
			log.Printf("[账号新增] traceId=%s 阶段=读取默认凭据名 结果=成功 业务影响=只返回文件名，绝对路径不返回前端",
				debugTraceID(r))
			writeAdminData(w, r, map[string]any{"defaultFile": suggestCredentialFileName()})
			return
		}
		s := findLoginSession(id)
		if s == nil {
			log.Printf("[账号新增] traceId=%s 阶段=查询登录进度 id=%s 结果=会话不存在或已结束 业务影响=前端应提示重新获取登录接口",
				debugTraceID(r), id)
			writeJSONError(w, http.StatusNotFound, "登录会话不存在或已结束，请重新获取登录接口")
			return
		}
		writeAdminData(w, r, s.snapshot())
	case http.MethodPost:
		var input struct {
			Site string `json:"site"`
			File string `json:"file"`
		}
		if err := decodeAdminJSON(w, r, &input); err != nil {
			writeJSONError(w, http.StatusBadRequest, "新增账号请求格式无效")
			return
		}
		trace := debugTraceID(r)
		site := strings.ToLower(strings.TrimSpace(input.Site))
		if site != "cn" && site != "intl" {
			log.Printf("[账号新增] traceId=%s 阶段=参数校验 结果=拒绝 站点=%s 原因=站点只能是 cn 或 intl 返回状态码=400 业务影响=未创建登录会话",
				trace, input.Site)
			writeJSONError(w, http.StatusBadRequest, "站点只能选择国内站或国际站")
			return
		}
		target, err := resolveCredentialPath(input.File)
		if err != nil {
			log.Printf("[账号新增] traceId=%s 阶段=参数校验 结果=拒绝 原因=%v 返回状态码=400 业务影响=未创建登录会话", trace, err)
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		if _, err := os.Lstat(target); err == nil {
			log.Printf("[账号新增] traceId=%s 阶段=参数校验 结果=拒绝 凭据=%s 原因=同名文件已存在 返回状态码=409 业务影响=未覆盖已有凭据",
				trace, filepath.Base(target))
			writeJSONError(w, http.StatusConflict, "同名凭据已存在，请换一个名字")
			return
		}
		s, err := beginAccountLogin(site, target)
		if err != nil {
			log.Printf("[账号新增] traceId=%s 阶段=获取登录接口 结果=失败 站点=%s 原因=%v 业务影响=未创建登录会话", trace, site, err)
			writeJSONError(w, http.StatusBadGateway, err.Error())
			return
		}
		loginSessionsMu.Lock()
		loginSessions[s.ID] = s
		loginSessionsMu.Unlock()
		log.Printf("[账号新增] traceId=%s 阶段=获取登录接口 结果=成功 站点=%s 凭据=%s 会话=%s 业务影响=等待扫码后自动写入并加载",
			trace, site, filepath.Base(target), s.ID)
		body := s.snapshot()
		body["qrPng"] = s.qrDataURI()
		writeAdminData(w, r, body)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "仅支持 GET/POST")
	}
}

// handleWebUIAccountCancel 取消进行中的登录会话，避免后台持续轮询。
func handleWebUIAccountCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	var input struct {
		ID string `json:"id"`
	}
	if err := decodeAdminJSON(w, r, &input); err != nil {
		writeJSONError(w, http.StatusBadRequest, "取消登录请求格式无效")
		return
	}
	trace := debugTraceID(r)
	s := findLoginSession(strings.TrimSpace(input.ID))
	if s == nil {
		log.Printf("[账号新增] traceId=%s 阶段=取消登录 结果=会话不存在 业务影响=无需取消", trace)
		writeAdminData(w, r, map[string]any{"cancelled": false})
		return
	}
	s.cancel()
	log.Printf("[账号新增] traceId=%s 阶段=取消登录 结果=成功 站点=%s 凭据=%s 业务影响=停止轮询，未写入任何凭据",
		trace, s.Site, s.File)
	writeAdminData(w, r, map[string]any{"cancelled": true})
}

// handleWebUIAccountDelete 删除指定凭据文件。
func handleWebUIAccountDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "仅支持 POST")
		return
	}
	var input struct {
		File string `json:"file"`
	}
	if err := decodeAdminJSON(w, r, &input); err != nil {
		writeJSONError(w, http.StatusBadRequest, "删除账号请求格式无效")
		return
	}
	trace := debugTraceID(r)
	target, err := deleteAccountCredential(input.File)
	if err != nil {
		log.Printf("[账号删除] traceId=%s 阶段=删除凭据 结果=失败 凭据=%s 原因=%v 返回状态码=400 业务影响=凭据与账号池均未变化",
			trace, input.File, err)
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	log.Printf("[账号删除] traceId=%s 阶段=删除凭据 结果=成功 凭据=%s 业务影响=凭据文件已删除，账号池已同步移除，统计与历史日志保留",
		trace, filepath.Base(target))
	writeAdminData(w, r, map[string]any{"deleted": filepath.Base(target)})
}

// -----------------------------------------------------------------------------
// 以下内容原位于 credentialedit.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

const credentialEditBudget = 256 << 10

var errCredentialChanged = errors.New("凭据文件已被后台续期或其他操作更新，请重新加载后再保存，避免覆盖新令牌")

func credentialRevision(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func openEditableCredential(target string, flag int) (*os.Root, *os.File, error) {
	allowed := false
	for _, configured := range collectConfiguredAuthPaths() {
		if target == configured {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, nil, gatewayValidationError("只能编辑当前配置加载的凭据文件，不能指定任意路径")
	}
	root, err := os.OpenRoot(filepath.Dir(target))
	if err != nil {
		return nil, nil, errors.New("凭据目录不可访问")
	}
	info, err := root.Lstat(filepath.Base(target))
	if err != nil || !info.Mode().IsRegular() {
		root.Close()
		return nil, nil, gatewayValidationError("凭据文件不存在、不是普通文件或为符号链接")
	}
	f, err := root.OpenFile(filepath.Base(target), flag, 0600)
	if err != nil {
		root.Close()
		return nil, nil, errors.New("凭据文件不可读写，请检查文件权限")
	}
	if opened, err := f.Stat(); err != nil || !os.SameFile(info, opened) {
		f.Close()
		root.Close()
		return nil, nil, errCredentialChanged
	}
	return root, f, nil
}

func readEditableCredential(f *os.File) ([]byte, error) {
	if _, err := f.Seek(0, 0); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(f, credentialEditBudget+1))
	if err == nil && len(data) > credentialEditBudget {
		err = gatewayValidationError("凭据文件超过 256 KiB，不能使用网页编辑")
	}
	return data, err
}

func validateCredentialDocument(raw []byte) (StoredAuth, error) {
	var auth StoredAuth
	if len(raw) > credentialEditBudget || len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' {
		return auth, gatewayValidationError("凭据必须是非空 JSON 对象，最大 256 KiB")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&auth); err != nil || ensureJSONEOF(dec) != nil {
		return auth, gatewayValidationError("凭据 JSON 格式或字段类型无效")
	}
	if strings.TrimSpace(auth.Auth.AccessToken) == "" || strings.Contains(auth.Auth.AccessToken, "****") || strings.Contains(auth.Auth.RefreshToken, "****") ||
		strings.Contains(auth.Auth.AccessToken, "[REDACTED]") || strings.Contains(auth.Auth.RefreshToken, "[REDACTED]") {
		return auth, gatewayValidationError("auth.accessToken 不能为空，不能保存脱敏后的令牌占位值")
	}
	if auth.Edition != "" && auth.Edition != "cn" && auth.Edition != "intl" {
		return auth, gatewayValidationError("edition 必须为空、cn 或 intl")
	}
	registerCredentialSecrets(&auth)
	return auth, nil
}

// 获取账号锁后重查池中的对象，避免热加载移出/重新加入账号时操作过期对象。
func lockCredentialAccount(ctx context.Context, target string) (*Account, func(), error) {
	for {
		accountMu.Lock()
		var acc *Account
		for _, item := range accounts {
			if item.Path == target {
				acc = item
				break
			}
		}
		accountMu.Unlock()
		if acc != nil && !lockAccountWithContext(ctx, &acc.lock) {
			return nil, nil, errors.New("等待账号当前请求结束时被取消，凭据未修改")
		}
		accountMu.Lock()
		var current *Account
		for _, item := range accounts {
			if item.Path == target {
				current = item
				break
			}
		}
		if current == acc {
			return acc, func() {
				accountMu.Unlock()
				if acc != nil {
					acc.lock.Unlock()
				}
			}, nil
		}
		accountMu.Unlock()
		if acc != nil {
			acc.lock.Unlock()
		}
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
	}
}

func saveEditedCredential(ctx context.Context, target, revision, content, trace string) error {
	auth, err := validateCredentialDocument([]byte(content))
	if err != nil {
		return err
	}
	acc, release, err := lockCredentialAccount(ctx, target)
	if err != nil {
		return err
	}
	defer release()
	root, f, err := openEditableCredential(target, os.O_RDWR)
	if err != nil {
		return err
	}
	defer root.Close()
	defer f.Close()
	old, err := readEditableCredential(f)
	if err != nil {
		return err
	}
	if revision == "" || revision != credentialRevision(old) {
		return errCredentialChanged
	}
	if ctx.Err() != nil {
		return errors.New("请求已取消，凭据未修改")
	}
	if same, err := root.Stat(filepath.Base(target)); err != nil {
		return errCredentialChanged
	} else if opened, err := f.Stat(); err != nil || !os.SameFile(same, opened) {
		return errCredentialChanged
	}
	log.Printf("[凭据编辑] traceId=%s 阶段=保存 文件=%q 旧字节=%d 新字节=%d 说明=版本检查通过，仅记录文件与长度，绝不记录JSON正文或令牌", trace, target, len(old), len(content))
	// 原子替换前必须先关闭目标句柄：Go 在 Windows 上打开的句柄不含 FILE_SHARE_DELETE，
	// 不关闭时 rename 会因共享冲突失败。写失败时目标文件未被触碰，无需恢复旧内容。
	_ = f.Close()
	if err := atomicWriteFile(target, []byte(content), 0600); err != nil {
		return errors.New("凭据保存失败，原文件未被修改")
	}
	clearDisabledMarker(target)
	info, err := root.Stat(filepath.Base(target))
	if err != nil {
		return err
	}
	added := acc == nil
	if added {
		acc = &Account{Path: target}
		accounts = append(accounts, acc)
	}
	identityChanged := acc.Auth != nil && (acc.Auth.Account.UID != auth.Account.UID ||
		acc.Profile().Key != profileForEdition(auth.Edition).Key ||
		strings.TrimSpace(acc.Auth.Account.EnterpriseID) != strings.TrimSpace(auth.Account.EnterpriseID))
	if identityChanged {
		acc.QuotaKnown, acc.QuotaExhausted, acc.IsPaidUser = false, false, false
		acc.QuotaUnlimited = false
		acc.QuotaTotal, acc.QuotaUsed, acc.QuotaRemaining = 0, 0, 0
		acc.PlanLabel, acc.PlanCheckedAt, acc.PlanStale = "", 0, true
		acc.ModelStates = nil
		acc.CooldownUntil, acc.CooldownMsg = time.Time{}, ""
	}
	acc.Auth, acc.Edition, acc.fingerprint = &auth, profileForEdition(auth.Edition).Key, accountFingerprint(info)
	acc.Disabled, acc.DisabledReason = false, ""
	log.Printf("[凭据编辑] traceId=%s 结果=保存成功 文件=%q 加入账号池=%t 账号身份已变更=%t 说明=令牌未进入日志，未调用上游验证；同一账号保留账本，身份或站点改变则重置对应运行状态", trace, target, added, identityChanged)
	return nil
}

func handleWebUICredentialEdit(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		target := r.URL.Query().Get("file")
		_, release, err := lockCredentialAccount(r.Context(), target)
		if err != nil {
			writeJSONError(w, http.StatusRequestTimeout, "等待账号请求结束时被取消")
			return
		}
		root, f, err := openEditableCredential(target, os.O_RDONLY)
		var data []byte
		if err == nil {
			data, err = readEditableCredential(f)
			f.Close()
			root.Close()
		}
		release()
		if err != nil {
			writeAdminOperationError(w, r, "凭据加载", err)
			return
		}
		var auth StoredAuth
		if json.Unmarshal(data, &auth) == nil {
			registerCredentialSecrets(&auth)
		}
		log.Printf("[凭据编辑] traceId=%s 阶段=加载 文件=%q 字节=%d 说明=管理员主动打开编辑器才返回明文，不记录正文，不在浏览器持久保存", debugTraceID(r), target, len(data))
		// 唯一受管理鉴权保护的明文凭据编辑接口，不使用脱敏视图覆盖可编辑原文。
		writeJSON(w, http.StatusOK, map[string]any{"path": target, "content": string(data), "revision": credentialRevision(data)})
	case http.MethodPost:
		var input struct {
			Path     string `json:"path"`
			Revision string `json:"revision"`
			Content  string `json:"content"`
		}
		if err := decodeAdminJSONLimit(w, r, &input, credentialEditBudget*2+4096); err != nil {
			log.Printf("[凭据编辑] traceId=%s 阶段=参数校验 结果=拒绝 原因=编辑请求格式或长度无效，正文未记录", debugTraceID(r))
			writeJSONError(w, http.StatusBadRequest, "编辑请求格式或长度无效")
			return
		}
		if err := saveEditedCredential(r.Context(), input.Path, input.Revision, input.Content, debugTraceID(r)); err != nil {
			writeAdminOperationError(w, r, "凭据保存", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"path": input.Path, "saved": true, "revision": credentialRevision([]byte(input.Content))})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "仅支持 GET/POST")
	}
}

// -----------------------------------------------------------------------------
// 以下内容原位于 credverify.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

// 凭据校验：在「覆盖」和「删除」这两个不可逆动作之前，先证明新凭据可用、
// 且要删除的凭据确实已经不能用。目的是避免把还能用的凭据销毁掉。
const (
	// credentialVerifyTimeout 只读校验的超时；校验失败不会阻塞太久。
	credentialVerifyTimeout = 8 * time.Second
)

// jwtClaims 解码 JWT 载荷（不做签名校验），仅用于本地一致性比对与到期展示。
func jwtClaims(token string) map[string]any {
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 3 {
		return nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return nil
	}
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		return nil
	}
	return claims
}

// shortID 生成不可逆短标识，用于日志里指代账号而不暴露原始标识。
func shortID(value string) string {
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:12]
}

// credentialIdentity 取出访问令牌声明的账号标识与站点 realm，用于比对新旧凭据是否同一账号。
func credentialIdentity(tokens StoredTokens) (accountID, realm string) {
	claims := jwtClaims(tokens.AccessToken)
	if claims == nil {
		return "", ""
	}
	if sub, ok := claims["sub"].(string); ok {
		accountID = sub
	}
	iss, _ := claims["iss"].(string)
	if iss == "" {
		return accountID, ""
	}
	if i := strings.Index(iss, "/auth/realms/"); i >= 0 {
		return accountID, iss[i+len("/auth/realms/"):]
	}
	return accountID, iss
}

// verifyAccountUsable 用只读接口确认凭据是否还能取到账号数据。
//
// 返回 (true, nil)  凭据可用；
// 返回 (false, nil) 上游明确拒绝（401 或正文明确登录失效），凭据已不可用；
// 返回 (false, err) 无法判定（普通 403、网络、超时、上游 5xx），调用方不得据此销毁凭据。
func verifyAccountUsable(ctx context.Context, auth StoredAuth) (bool, error) {
	registerCredentialSecrets(&auth)
	traceID := newTraceID()
	if state, ok := ctx.Value(debugRequestContextKey{}).(*debugRequestContext); ok {
		state.mu.RLock()
		traceID = state.traceID
		state.mu.RUnlock()
	}
	log.Printf("[凭据校验] traceId=%s 阶段=只读校验开始 站点=%s 说明=确认账号接口是否可用，不写入凭据", traceID, auth.Edition)
	if strings.TrimSpace(auth.Auth.AccessToken) == "" {
		log.Printf("[凭据校验] traceId=%s 结果=无法判定 原因=缺少访问令牌 说明=未调用上游", traceID)
		return false, fmt.Errorf("缺少访问令牌，无法校验")
	}
	if cfg.HttpClient == nil {
		log.Printf("[凭据校验] traceId=%s 结果=无法判定 原因=HTTP客户端未就绪 说明=未调用上游", traceID)
		return false, fmt.Errorf("HTTP 客户端未就绪，无法校验")
	}
	prof := profileForEdition(auth.Edition)
	headers := func(r *http.Request) {
		commonHeaders(r, prof)
		r.Header.Set("Authorization", "Bearer "+auth.Auth.AccessToken)
		r.Header.Set("X-Client-Platform", "web")
		if auth.Account.EnterpriseID != "" {
			r.Header.Set("X-Enterprise-Id", auth.Account.EnterpriseID)
		}
	}
	_, status, err := doJSONContext(ctx, cfg.HttpClient, http.MethodPost, prof.quotaSummaryURL(), headers, strings.NewReader("{}"))
	if err != nil {
		// 普通 403 可能只是权限不足或风控拒绝，不能据此确认 Token 失效。
		// 仅修正只读凭据校验，不改变其他调用方的 403 停用规则。
		if status == http.StatusUnauthorized || isAuthFailure(0, err.Error()) {
			log.Printf("[凭据校验] traceId=%s HTTP=%d 结果=不可用 原因=%v 说明=上游明确返回授权失效", traceID, status, err)
			return false, nil // 上游明确拒绝：凭据确实不可用
		}
		log.Printf("[凭据校验] traceId=%s HTTP=%d 结果=无法判定 原因=%v 业务影响=不得仅凭此次结果销毁凭据", traceID, status, err)
		return false, fmt.Errorf("只读校验未完成 HTTP=%d", status)
	}
	log.Printf("[凭据校验] traceId=%s HTTP=%d 结果=可用 说明=已成功取到账号数据", traceID, status)
	return true, nil
}

// validateRefreshedCredential 在把新凭据写回磁盘之前做两道校验：
//  1. 新旧凭据必须属于同一账号、同一站点，防止上游串号或响应错配导致覆盖错人；
//  2. 新访问令牌必须能取到账号数据，防止用「接口 200 但实际不可用」的凭据覆盖掉还能用的旧凭据。
//
// 校验接口本身不可用时按通过处理，避免因为校验抖动而拒绝掉有效的新凭据。
func validateRefreshedCredential(path string, refreshed *StoredAuth, oldTokens StoredTokens) error {
	oldID, oldRealm := credentialIdentity(oldTokens)
	newID, newRealm := credentialIdentity(refreshed.Auth)
	identityNote := "凭据未携带可解析的账号声明，跳过一致性比对"
	if newID != "" {
		identityNote = fmt.Sprintf("账号标识=%s 站点=%s 与旧凭据一致", shortID(newID), newRealm)
	}
	if oldID != "" && newID != "" && oldID != newID {
		return fmt.Errorf("新凭据账号标识与旧凭据不一致（%s -> %s），拒绝覆盖",
			shortID(oldID), shortID(newID))
	}
	if oldRealm != "" && newRealm != "" && oldRealm != newRealm {
		return fmt.Errorf("新凭据站点与旧凭据不一致（%s -> %s），拒绝覆盖", oldRealm, newRealm)
	}

	ctx, cancel := context.WithTimeout(context.Background(), credentialVerifyTimeout)
	defer cancel()
	usable, err := verifyAccountUsable(ctx, *refreshed)
	if err != nil {
		log.Printf("[凭据校验] 账号=%s 阶段=覆盖前校验 结果=无法判定，按通过处理 一致性=%s 原因=%v 说明=刷新接口已成功，不因校验抖动丢弃新凭据",
			path, identityNote, err)
		return nil
	}
	if !usable {
		log.Printf("[凭据校验] 账号=%s 阶段=覆盖前校验 结果=不通过 一致性=%s 原因=新访问令牌取不到账号数据 业务影响=放弃本次覆盖，保留旧凭据继续使用",
			path, identityNote)
		return fmt.Errorf("新凭据取不到账号数据，拒绝覆盖旧凭据")
	}
	log.Printf("[凭据校验] 账号=%s 阶段=覆盖前校验 结果=通过 一致性=%s 只读校验=新访问令牌可取到账号数据 说明=允许覆盖旧凭据",
		path, identityNote)
	return nil
}

// credentialStillUsableForDelete 判断一个即将被删除的凭据是否其实还能用。
// 返回值 keep=true 表示应保留凭据文件。
func credentialStillUsableForDelete(auth *StoredAuth) (keep bool, note string) {
	if auth == nil || strings.TrimSpace(auth.Auth.AccessToken) == "" {
		return false, "无访问令牌可校验，按已失效处理"
	}
	ctx, cancel := context.WithTimeout(context.Background(), credentialVerifyTimeout)
	defer cancel()
	usable, err := verifyAccountUsable(ctx, *auth)
	switch {
	case err != nil:
		// 无法判定时保守保留：删除不可逆，宁可留一个失效文件。
		return true, fmt.Sprintf("只读校验无法完成（%v），保守保留凭据", err)
	case usable:
		return true, "只读校验显示凭据仍可取到账号数据，保留凭据不删除"
	default:
		return false, "只读校验确认凭据已不可用"
	}
}

// -----------------------------------------------------------------------------
// 以下内容原位于 tokenrefresh.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------
// 以下内容原位于 keepalive.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

// 上游报错里可能带用户标识或令牌片段，写日志前统一隐去。
var (
	uuidLikeRe   = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	bearerLikeRe = regexp.MustCompile(`(?i)(bearer\s+)?eyJ[A-Za-z0-9_\-]{10,}(\.[A-Za-z0-9_\-]+){1,2}`)
)

// 主动续期（保活）参数。参考 ckldy/workbuddy2api 的 keepalive_hours 调度方式：
// 在固定本地时刻主动刷新全部账号，不等访问令牌临近过期。
const (
	// keepaliveAccountGap 账号之间的串行间隔。批量并发刷新登录态是明显的机器特征，
	// 串行加节流更接近真人逐个使用的节奏。
	keepaliveAccountGap = 1 * time.Second
	// keepaliveCatchUpDelay 启动补跑延迟：先让服务稳定，再补做一次漏掉的续期。
	keepaliveCatchUpDelay = 90 * time.Second
	// keepaliveOverdueAfter 距上次成功续期超过该时长即视为需要补跑。
	keepaliveOverdueAfter = 20 * time.Hour
	// refreshDisableThreshold 连续刷新失败多少次才判定登录态失效。
	// 单次失败可能是上游抖动、风控或网络问题，一次就删凭据属于误杀。
	refreshDisableThreshold = 3

	// refreshFailDecayWindow 失败计数衰减窗口：失败分散在不同天（如多个夜间断网
	// 各撞上一次续期）不该累计成永久禁号，只有窗口内的连续失败才逼近阈值。
	refreshFailDecayWindow = 24 * time.Hour
)

// refreshFailureKind 区分「登录态真的没了」和「这次只是没成功」。
type refreshFailureKind int

const (
	refreshFailureTransient refreshFailureKind = iota
	refreshFailureSessionDead
)

func (k refreshFailureKind) String() string {
	if k == refreshFailureSessionDead {
		return "登录态失效"
	}
	return "临时失败"
}

// sessionDeadMarkers 是上游明确表示「刷新令牌已不可用」的标志。
// 只有命中这些标志才按登录态失效处理；普通 401/403 可能是权限或风控，
// 需要连续多次失败才判定，避免一次抖动就删掉用户凭据。
var sessionDeadMarkers = []string{
	"12153",
	"offline user session not found",
	"invalid_grant",
	"token is not active",
	"refresh token failed",
	"session not found",
}

func classifyRefreshFailure(status int, body string) refreshFailureKind {
	low := strings.ToLower(body)
	for _, marker := range sessionDeadMarkers {
		if strings.Contains(low, marker) {
			return refreshFailureSessionDead
		}
	}
	return refreshFailureTransient
}

// hourList 解析 "-keepalive-hours 22" 或 "-keepalive-hours 10,22" 形式的本地小时列表。
// 空值表示关闭主动续期。
type hourList []int

func (h *hourList) String() string {
	if h == nil || len(*h) == 0 {
		return ""
	}
	parts := make([]string, 0, len(*h))
	for _, v := range *h {
		parts = append(parts, strconv.Itoa(v))
	}
	return strings.Join(parts, ",")
}

func (h *hourList) Set(value string) error {
	var out []int
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		n, err := strconv.Atoi(item)
		if err != nil {
			return fmt.Errorf("续期时刻 %q 不是 0-23 的整数", item)
		}
		if n < 0 || n > 23 {
			return fmt.Errorf("续期时刻 %d 超出 0-23 范围", n)
		}
		out = append(out, n)
	}
	sort.Ints(out)
	*h = out
	return nil
}

// nextKeepaliveTime 返回 now 之后最近的一个配置时刻（本地时区）。
func nextKeepaliveTime(now time.Time, hours []int) time.Time {
	var earliest time.Time
	for _, h := range hours {
		t := time.Date(now.Year(), now.Month(), now.Day(), h, 0, 0, 0, now.Location())
		if !t.After(now) {
			t = t.Add(24 * time.Hour)
		}
		if earliest.IsZero() || t.Before(earliest) {
			earliest = t
		}
	}
	return earliest
}

func formatKeepaliveHours(hours []int) string {
	parts := make([]string, 0, len(hours))
	for _, h := range hours {
		parts = append(parts, fmt.Sprintf("%02d:00", h))
	}
	return strings.Join(parts, "、")
}

// jwtExpiry 从 JWT 载荷读取 exp，仅用于展示与到期预警，不做签名校验。
// 手工导入的凭据也能借此看到刷新令牌的到期时间。
func jwtExpiry(token string) (time.Time, bool) {
	claims := jwtClaims(token)
	if claims == nil {
		return time.Time{}, false
	}
	switch v := claims["exp"].(type) {
	case float64:
		if v <= 0 {
			return time.Time{}, false
		}
		return time.Unix(int64(v), 0), true
	case json.Number:
		n, err := v.Int64()
		if err != nil || n <= 0 {
			return time.Time{}, false
		}
		return time.Unix(n, 0), true
	}
	return time.Time{}, false
}

// refreshTokenExpiry 返回刷新令牌到期时间：优先用续期时记录的字段，
// 其次从未验签的令牌声明读取，都拿不到则未知。
func refreshTokenExpiry(tokens StoredTokens) (time.Time, string, bool) {
	if tokens.RefreshExpiresAt > 0 {
		return time.Unix(tokens.RefreshExpiresAt, 0), "续期接口返回", true
	}
	if exp, ok := jwtExpiry(tokens.RefreshToken); ok {
		return exp, "令牌声明", true
	}
	return time.Time{}, "", false
}

// isKeepaliveOverdue 判断账号是否久未成功续期，需要在启动时补跑一次。
func isKeepaliveOverdue(acc *Account, now time.Time) bool {
	if acc == nil || acc.Auth == nil {
		return false
	}
	last := acc.Auth.Auth.LastRefreshTime
	if last <= 0 {
		return true
	}
	return now.Sub(time.Unix(last, 0)) >= keepaliveOverdueAfter
}

// keepaliveLoop 主动续期调度：到点刷新全部账号，另在启动时补做漏掉的一轮。
// 只走令牌刷新接口，不请求模型、不消耗额度。
func keepaliveLoop() {
	if len(cfg.KeepaliveHours) == 0 {
		logScheduledTask("keepalive", "disabled", "disabled", newTraceID(), time.Time{}, time.Time{}, "未设置主动续期时刻")
		logScheduledTask("keepalive_catchup", "disabled", "disabled", newTraceID(), time.Time{}, time.Time{}, "主动续期关闭，启动补跑不执行")
		log.Printf("[续期保活] 阶段=启动 结果=已关闭 原因=未配置主动续期时刻 业务影响=仅在访问令牌临近过期时才刷新，长期空闲账号可能静默失效")
		return
	}
	log.Printf("[续期保活] 阶段=启动 结果=已启用 续期时刻=%s 说明=到点主动刷新全部账号登录凭据，只调刷新接口，不请求模型、不消耗额度",
		formatKeepaliveHours(cfg.KeepaliveHours))

	safeGo("keepalive_catchup", func() {
		next := time.Now().Add(keepaliveCatchUpDelay)
		logScheduledTask("keepalive_catchup", "scheduled", "", newTraceID(), time.Time{}, next, "启动后90秒，只检查久未续期账号")
		time.Sleep(keepaliveCatchUpDelay)
		now := time.Now()
		accountMu.Lock()
		overdue := 0
		for _, acc := range accounts {
			if acc.Disabled || acc.Auth == nil {
				continue
			}
			if isKeepaliveOverdue(acc, now) {
				overdue++
			}
		}
		accountMu.Unlock()
		if overdue == 0 {
			logScheduledTask("keepalive_catchup", "completed", "skipped", newTraceID(), now, time.Time{}, "全部账号近期已续期，无需补跑")
			log.Printf("[续期保活] 阶段=启动补跑 结果=跳过 原因=全部账号均在 %v 内成功续期过", keepaliveOverdueAfter)
			return
		}
		log.Printf("[续期保活] 阶段=启动补跑 待续期账号=%d 原因=距上次成功续期超过 %v 或从未续期 说明=服务已稳定，开始补做一轮", overdue, keepaliveOverdueAfter)
		runKeepaliveOnce("启动补跑", true)
	})

	for {
		next := nextKeepaliveTime(time.Now(), cfg.KeepaliveHours)
		logScheduledTask("keepalive", "scheduled", "", newTraceID(), time.Time{}, next, "服务器本地时间；账号间隔1秒串行续期")
		log.Printf("[续期保活] 阶段=等待下一次续期 时间=%s 距离=%s", next.Format("2006-01-02 15:04:05"), time.Until(next).Truncate(time.Second))
		timer := time.NewTimer(time.Until(next))
		<-timer.C
		runKeepaliveOnce("定时", false)
	}
}

// keepaliveRunMu 串行化保活轮次：启动补跑与定时轮同时到达时后到者直接跳过，
// 避免对同一批账号并发重复刷新（失败计数、调度日志和快照交错）。
var keepaliveRunMu sync.Mutex

// runKeepaliveOnce 串行刷新账号池；onlyOverdue 为真时只处理久未续期的账号。
func runKeepaliveOnce(trigger string, onlyOverdue bool) {
	if !keepaliveRunMu.TryLock() {
		log.Printf("[续期保活] 阶段=跳过 触发方式=%s 原因=上一轮仍在执行 说明=补跑与定时轮不并发", trigger)
		return
	}
	defer keepaliveRunMu.Unlock()
	task := "keepalive"
	next := nextKeepaliveTime(time.Now(), cfg.KeepaliveHours)
	if onlyOverdue {
		task, next = "keepalive_catchup", time.Time{}
	}
	trace, started := startScheduledTask(task, next)
	accountMu.Lock()
	accs := append([]*Account(nil), accounts...)
	accountMu.Unlock()

	now := time.Now()
	log.Printf("[续期保活] 阶段=执行 触发方式=%s 账号总数=%d 只补跑久未续期=%t 结果=开始", trigger, len(accs), onlyOverdue)

	refreshed, skipped, failed, disabled := 0, 0, 0, 0
	first := true
	for _, acc := range accs {
		accountMu.Lock()
		disabledAccount := acc.Disabled
		hasAuth := acc.Auth != nil
		path := acc.Path
		hasRefresh := hasAuth && acc.Auth.Auth.RefreshToken != ""
		overdue := hasAuth && isKeepaliveOverdue(acc, now)
		accountMu.Unlock()

		if disabledAccount {
			skipped++
			log.Printf("[续期保活] 账号=%s 结果=跳过 原因=账号已标记失效，不参与调度", path)
			continue
		}
		if !hasAuth || !hasRefresh {
			skipped++
			log.Printf("[续期保活] 账号=%s 结果=跳过 原因=缺少刷新令牌，无法续期，需要重新登录", path)
			continue
		}
		if onlyOverdue && !overdue {
			skipped++
			continue
		}
		if !first {
			time.Sleep(keepaliveAccountGap) // 串行节流，避免批量并发刷新形成机器特征
		}
		first = false

		if err := refreshAccountToken(acc, "主动续期/"+trigger); err != nil {
			failed++
			accountMu.Lock()
			isDisabled := acc.Disabled
			accountMu.Unlock()
			if isDisabled {
				disabled++
			}
			continue
		}
		refreshed++
	}

	log.Printf("[续期保活] 阶段=执行完成 触发方式=%s 成功=%d 失败=%d 跳过=%d 判定失效=%d 业务影响=成功的账号登录态已顺延，失败的账号保留原凭据等待下次重试",
		trigger, refreshed, failed, skipped, disabled)
	status := "success"
	if failed > 0 && refreshed == 0 {
		status = "failed"
	} else if failed > 0 {
		status = "partial"
	} else if refreshed == 0 {
		status = "skipped"
	}
	logScheduledTask(task, "completed", status, trace, started, next, fmt.Sprintf("触发=%s，成功=%d，失败=%d，跳过=%d，失效=%d", trigger, refreshed, failed, skipped, disabled))
	if refreshed > 0 || disabled > 0 {
		requestStatusSnapshotRefresh()
	}
}

// refreshFailureSummary 生成不含令牌与上游用户标识的失败摘要，用于日志与状态展示。
func refreshFailureSummary(kind refreshFailureKind, status int, err error) string {
	detail := ""
	if err != nil {
		detail = sanitizeUpstreamText(err.Error())
	}
	if len(detail) > 160 {
		detail = detail[:160] + "..."
	}
	return fmt.Sprintf("%s（HTTP %d）%s", kind, status, detail)
}

// sanitizeUpstreamText 去掉上游报错里可能出现的账号标识与令牌片段。
func sanitizeUpstreamText(text string) string {
	text = uuidLikeRe.ReplaceAllString(text, "[已隐去]")
	text = bearerLikeRe.ReplaceAllString(text, "[已隐去]")
	text = strings.ReplaceAll(text, "\n", " ")
	return strings.TrimSpace(text)
}

func humanDuration(d time.Duration) string {
	if d <= 0 {
		return "已过期"
	}
	if days := int(d.Hours()) / 24; days > 0 {
		return fmt.Sprintf("%d 天", days)
	}
	return fmt.Sprintf("%d 小时 %d 分", int(d.Hours()), int(d.Minutes())%60)
}

// formatRenewalStatus 生成 status 命令里的登录续期信息。
// 调用方需持有 accountMu（本函数内部不再加锁）。
func formatRenewalStatus(acc *Account) string {
	if acc == nil || acc.Auth == nil {
		return ""
	}
	tokens := acc.Auth.Auth
	var sb strings.Builder
	if exp, source, ok := refreshTokenExpiry(tokens); ok {
		remain := time.Until(exp)
		state := "有效"
		switch {
		case remain <= 0:
			state = "已过期，需要重新登录"
		case remain < 7*24*time.Hour:
			state = "即将到期，建议尽快重新登录"
		}
		sb.WriteString(fmt.Sprintf("刷新令牌:     至 %s（%s，剩余 %s，%s）\n",
			exp.Local().Format("2006-01-02 15:04:05"), source, humanDuration(remain), state))
	} else {
		sb.WriteString("刷新令牌:     到期时间未知（续期接口未返回，令牌也无法解析）\n")
	}
	if tokens.LastRefreshTime > 0 {
		sb.WriteString(fmt.Sprintf("最近续期:     %s\n", time.Unix(tokens.LastRefreshTime, 0).Format("2006-01-02 15:04:05")))
	} else {
		sb.WriteString("最近续期:     尚无记录（主动续期成功后写入）\n")
	}
	if acc.RefreshFailCount > 0 {
		sb.WriteString(fmt.Sprintf("续期失败:     连续 %d 次，最近原因=%s\n", acc.RefreshFailCount, acc.LastRefreshError))
	} else if acc.LastRefreshError != "" {
		sb.WriteString(fmt.Sprintf("续期告警:     %s\n", acc.LastRefreshError))
	}
	return sb.String()
}
