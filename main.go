package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptrace"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	version = "1.14.20"

	// 状态快照文件名：serve 后台周期写入，monitor 前台命令实时读取展示
	statusSnapshotFile = "workbuddy-status.json"
	// 签到尝试状态：记录当天已尝试次数，保证服务重启后仍遵守“每天最多3次”。
	checkinStateFile = "workbuddy-checkin-state.json"
	logDir           = "logs"

	// defaultSystemPrompt 是当客户端首条消息不是 system 时注入的保底系统提示，
	// 用于满足腾讯上游「首条消息必须是 system prompt」的硬性要求 (code 11128)。
	defaultSystemPrompt = "You are a helpful assistant."

	// upstreamHeaderTimeout 限定「请求发送完成 → 收到上游响应头」的等待上限。
	//
	// 该值必须足够宽松：上游对 3MB+ 大请求的排队与预处理本身就可能接近 1 分钟
	// （实测 2.3MB 请求出现过 50.8s 才回响应头）。早期版本用 60s 会误杀这类
	// 正常但缓慢的请求，表现为 502「timeout awaiting response headers」。
	// 默认 5 分钟，只用于兜底「上游彻底无响应」；长时间无数据的流由
	// upstreamIdleTimeout 的空闲看门狗负责中断。可在 config.json 覆盖。
	upstreamHeaderTimeoutDefault = 5 * time.Minute

	// upstreamIdleTimeoutDefault 是流式响应的「空闲读超时」：只要持续有数据到达
	// 就永不超时，一旦超过该时长没有任何新数据，才判定上游卡死并中断。
	upstreamIdleTimeoutDefault = 120 * time.Second

	// defaultRequestTimeout 用于令牌刷新、额度查询、模型目录等控制类短请求。
	defaultRequestTimeout = 60 * time.Second

	// checkinHour 是北京时间每日定时签到小时。
	checkinHour = 8

	// checkinMaxAttempts 是每天最多签到尝试次数（含首次和重试）。
	checkinMaxAttempts = 3

	// checkinRetryDelay 是签到失败后的重试间隔。
	// 08:00 上游签到接口可能因集中请求而响应缓慢，首次超时并不代表签到未生效；
	// 签到接口幂等，隔几分钟重试一轮可补齐真正漏签的账号。
	checkinRetryDelay = 5 * time.Minute

	// upstreamTransientRetries 是「请求体发送阶段」遇到瞬时网络错误的额外重试次数。
	//
	// 背景：网关与上游 CDN 边缘节点之间的单条 TCP 连接可能被对端重置
	// （connection reset by peer）、被关闭（use of closed network connection）、
	// 或命中已被回收的 keep-alive 连接。这类错误属于瞬时故障，且与请求体大小无关
	// （实测 >5MB 请求 95% 成功，而 0.5MB 请求也会偶发失败）。
	//
	// 只有确认请求头尚未写出时才重试；没有收到响应头不能证明上游没处理 POST。
	// 重试强制使用新连接，默认最多额外尝试 2 次，可在 config.json 覆盖。
	upstreamTransientRetriesDefault = 2

	// upstreamRetryBackoff 是两次重试之间的等待，给上游边缘节点留出恢复时间。
	upstreamRetryBackoffDefault = 300 * time.Millisecond
)

var errModelAccountPolicy = errors.New("model account policy rejected all accounts")

// errStreamClosedWithoutFinish 表示上游连接正常读完（没有读错误），
// 但整段流里从未出现非空 finish_reason。空串不算：上游会在每个中间分片上带 finish_reason:""。
// 这种结束不能当成完整回复，否则下游会把残缺输出当成成功结果。
var errStreamClosedWithoutFinish = errors.New("上游流正常结束，但没有 finish_reason，不能当成完整回复")

// 生效的超时值（默认取上面的 Default，可由 config.json 的 upstream 段覆盖）。
// 定义为变量既便于测试调小阈值，也便于运维按网络状况调整。
var (
	upstreamHeaderTimeout = upstreamHeaderTimeoutDefault
	upstreamIdleTimeout   = upstreamIdleTimeoutDefault
	// upstreamTransientRetries 为生效的瞬时网络错误重试次数。
	upstreamTransientRetries = upstreamTransientRetriesDefault
	// upstreamRetryBackoff 为生效的重试间隔（测试可调小）。
	upstreamRetryBackoff = upstreamRetryBackoffDefault
)

// isTransientNetworkError 判断错误是否为「瞬时网络故障」，这类错误值得重试。
//
// 覆盖：连接被重置/中止（RST）、连接被对端关闭、管道破裂、连接被强制关闭，
// 以及 Go 在复用 keep-alive 连接时常见的 "server closed idle connection"。
// 注意：context.Canceled 与 DeadlineExceeded 不属于此类——前者是客户端主动断开，
// 后者是明确超时，重试都无意义甚至有害。
// errUpstreamNotSent 标记「请求确认未发到上游」（连接被拒、DNS 失败、重试耗尽且
// 请求头/请求体均未写出）。此时换下一个账号重试不会重复计费；包装保留原始错误链，
// errors.Is(err, io.EOF) 等既有判定不受影响。
var errUpstreamNotSent = errors.New("upstream request not sent")

func isTransientNetworkError(err error) bool {
	if err == nil {
		return false
	}
	// 客户端取消或超时：不重试
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		// 网络层超时（如 ResponseHeaderTimeout）说明上游确实没响应，可重试
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"connection reset by peer",
		"connection reset",
		"broken pipe",
		"use of closed network connection",
		"connection refused",
		"server closed idle connection",
		"unexpected eof",
		"http2: server sent goaway",
		"stream error",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

// doUpstreamRequest 发送上游请求，并对瞬时网络错误做有界重试。
//
// 仅在「尚未写出完整请求头」时重试；已发送 POST 后收到 EOF 无法判定上游
// 是否执行过生成，不能安全重放。每次重试都重建请求对象与请求体 reader。
//
// 返回的 cancel 用于在流式读取结束后取消上游 context（调用方负责）。
func doUpstreamRequest(r *http.Request, acc *Account, prof *upstreamProfile, upstreamBytes []byte, reqID uint64, traceID string) (*http.Response, context.CancelFunc, error) {
	// 兼容直接调用与测试；正常入口已经在协议转换前准备完成。只生成一次，不能在 try 循环里重建。
	prepareConversationContext(r, nil)
	var lastErr error
	_, _, retries := upstreamSettings()
	for try := 0; try <= retries; try++ {
		upstreamCtx, upstreamCancel := context.WithCancel(r.Context())
		var headersWritten, bodyWritten atomic.Bool
		trace := &httptrace.ClientTrace{
			WroteHeaders: func() { headersWritten.Store(true) },
			WroteRequest: func(info httptrace.WroteRequestInfo) {
				if info.Err == nil {
					bodyWritten.Store(true)
				}
			},
		}
		req, err := http.NewRequestWithContext(httptrace.WithClientTrace(upstreamCtx, trace), http.MethodPost, prof.chatURL(), bytes.NewReader(upstreamBytes))
		if err != nil {
			upstreamCancel()
			return nil, nil, err
		}
		backendHeaders(req, acc.authSnapshot(), prof)
		if debugLoggingEnabled() {
			req.Header.Set("X-Trace-ID", debugTraceID(r))
			req.Header.Set("X-Parent-Request-ID", strconv.FormatUint(reqID, 10))
		}
		if try > 0 {
			// 重试时不复用已被对端关闭的空闲连接。
			req.Close = true
			cfg.HttpClient.CloseIdleConnections()
			log.Printf("[网络重试] traceId=%s requestId=%d 账号=%s 第 %d/%d 次重试，上一尝试请求头未写出，已重建连接（错误: %s）",
				traceID, reqID, acc.Path, try, retries, safeDebugError(lastErr))
		}

		resp, err := cfg.HttpClient.Do(req)
		if err == nil {
			return resp, upstreamCancel, nil
		}
		upstreamCancel()
		lastErr = err
		if !isTransientNetworkError(err) {
			return nil, nil, err
		}
		if headersWritten.Load() || bodyWritten.Load() {
			log.Printf("[网络重试跳过] traceId=%s requestId=%d 账号=%s 请求头已写出=%t 请求体已写完=%t 原因=上游可能已处理POST，重放可能重复生成 错误=%s",
				traceID, reqID, acc.Path, headersWritten.Load(), bodyWritten.Load(), safeDebugError(err))
			debugEvent(r, "warn", "upstream_retry_skipped", map[string]any{
				"reason": "request_may_have_reached_upstream", "headers_written": headersWritten.Load(),
				"body_written": bodyWritten.Load(), "business_impact": "避免重复执行模型请求",
			})
			return nil, nil, err
		}
		if try < retries {
			debugEvent(r, "warn", "upstream_send_retry", map[string]any{
				"retry": try + 1, "reason": "request_headers_not_written",
				"business_impact": "换新连接重试尚未发出的模型请求",
			})
			select {
			case <-r.Context().Done():
				return nil, nil, err
			case <-time.After(upstreamRetryBackoff):
			}
		}
	}
	if lastErr == nil {
		lastErr = errors.New("upstream unavailable")
	}
	// 重试耗尽仍失败，且历次尝试都未写出请求头/请求体：可安全换号，
	// 包装哨兵供 upstreamChat 在账号池内轮转；%w 保留原始错误链。
	return nil, nil, fmt.Errorf("%w: %w", errUpstreamNotSent, lastErr)
}

// -----------------------------------------------------------------------------
// 上游站点 Profile（国内站 / 国际站）
//
// 国际站 www.workbuddy.ai 与国内站 copilot.tencent.com 走同一套 /v2/plugin/* 协议
// （实测：auth/state、auth/token、login/account、auth/token/refresh、chat/completions
// 的路径与响应包络完全一致），差异仅在：
//   - 上游域名与 Web Origin（国际站位于腾讯 EdgeOne 国际 CDN）
//   - 登录 platform 参数（workbuddy-ai 而非 VSCode），登录在浏览器内完成（邮箱/验证码/SSO）
//   - 等待授权期间 auth/token 轮询返回 code 11217 (login ing...)
// 每个账号凭据文件通过 edition 字段记录所属站点，账号池支持国内/国际混挂轮询。
// -----------------------------------------------------------------------------

type upstreamProfile struct {
	Key       string        // 存储于凭据文件 edition 字段的站点标识
	Label     string        // 控制台展示名
	Base      string        // 上游 API 基础地址
	Origin    string        // Origin/Referer 伪装来源（各站 Web 控制台）
	Platform  string        // auth/state 的 platform 参数
	ClientUA  string        // User-Agent
	ClientID  string        // X-Client-ID
	ClientVer string        // X-Client-Version
	Product   string        // X-Product
	LoginTTL  time.Duration // login 命令等待授权完成的超时
}

var (
	profileCN = upstreamProfile{
		Key: "cn", Label: "国内站",
		Base: "https://copilot.tencent.com", Origin: "https://www.codebuddy.cn",
		Platform: "VSCode", ClientUA: "CLI/2.143.1 CodeBuddy/2.143.1",
		ClientID: "codebuddy-cli", ClientVer: "2.143.1", Product: "SaaS",
		LoginTTL: 5 * time.Minute,
	}
	profileINTL = upstreamProfile{
		Key: "intl", Label: "国际站",
		Base: "https://www.workbuddy.ai", Origin: "https://www.workbuddy.ai",
		Platform: "workbuddy-ai", ClientUA: "CLI/2.143.1 CodeBuddy/2.143.1",
		ClientID: "codebuddy-cli", ClientVer: "2.143.1", Product: "SaaS",
		LoginTTL: 15 * time.Minute, // 浏览器内登录（邮箱/验证码/SSO）比扫码慢，放宽超时
	}
)

// profileForEdition 根据凭据文件中的 edition 标识返回上游站点参数；空值/未知值回退国内站。
func profileForEdition(edition string) *upstreamProfile {
	switch strings.ToLower(strings.TrimSpace(edition)) {
	case "intl", "international", "global", "workbuddy.ai":
		return &profileINTL
	default:
		return &profileCN
	}
}

func (p *upstreamProfile) authStateURL() string {
	return p.Base + "/v2/plugin/auth/state?platform=" + url.QueryEscape(p.Platform)
}

func (p *upstreamProfile) loginAcctURL(state string) string {
	return p.Base + "/v2/plugin/login/account?state=" + url.QueryEscape(state)
}

func (p *upstreamProfile) authTokenURL(state string) string {
	return p.Base + "/v2/plugin/auth/token?state=" + url.QueryEscape(state)
}

func (p *upstreamProfile) tokenRefreshURL() string { return p.Base + "/v2/plugin/auth/token/refresh" }

func (p *upstreamProfile) chatURL() string { return p.Base + "/v2/chat/completions" }

func (p *upstreamProfile) quotaSummaryURL() string {
	return p.Origin + "/billing/meter/get-user-resource-summary"
}

func (p *upstreamProfile) enterpriseUsageURL() string {
	// 外部项目的企业号实测表明，get-user-resource* 对 enterpriseId 返回空包，
	// 必须改问分配额度：credit=本周期已用，limitNum=分配额度。
	return p.Origin + "/v2/billing/meter/get-enterprise-user-usage"
}

func (p *upstreamProfile) dailyCheckinURL() string {
	return p.Origin + "/v2/billing/meter/daily-checkin"
}

// -----------------------------------------------------------------------------
// 数据结构定义
// -----------------------------------------------------------------------------

type StoredAuth struct {
	Auth    StoredTokens  `json:"auth"`
	Account StoredAccount `json:"account"`
	Edition string        `json:"edition,omitempty"` // 站点标识：cn（国内站，默认）| intl（国际站 www.workbuddy.ai）
}

type StoredTokens struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	ExpiresAt    int64  `json:"expiresAt"`
	Domain       string `json:"domain"`
	// 以下为本地续期元数据，官方客户端不读；旧凭据文件缺省为 0，不影响解析。
	RefreshExpiresAt int64 `json:"refreshExpiresAt,omitempty"` // 刷新令牌到期时间（Unix 秒），0=未知
	LastRefreshTime  int64 `json:"lastRefreshTime,omitempty"`  // 最近一次续期成功时间（Unix 秒），0=从未续期
}

type StoredAccount struct {
	UID          string `json:"uid"`
	EnterpriseID string `json:"enterpriseId"`
	Nickname     string `json:"nickname"`
}

type apiEnvelope struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type tokenData struct {
	AccessToken      string `json:"accessToken"`
	RefreshToken     string `json:"refreshToken"`
	ExpiresIn        int64  `json:"expiresIn"`
	RefreshExpiresIn int64  `json:"refreshExpiresIn"`
	Domain           string `json:"domain"`
}

type accountData struct {
	UID          string `json:"uid"`
	EnterpriseID string `json:"enterpriseId"`
	Nickname     string `json:"nickname"`
}

type authStateData struct {
	State   string `json:"state"`
	AuthURL string `json:"authUrl"`
}

type quotaPackage struct {
	CycleTotalCapacity  string `json:"CycleTotalCapacity"`
	CycleUsedCapacity   string `json:"CycleUsedCapacity"`
	CycleRemainCapacity string `json:"CycleRemainCapacity"`
}

type quotaSummaryData struct {
	Packages []quotaPackage `json:"Packages"`
	// IsPaidUser 为上游「正式付费订阅」标记；Pro 试用用户该字段同样为 false。
	IsPaidUser bool `json:"IsPaidUser"`
	// ProTrialStatus 不能单独证明试用仍有效；必须结合当前权益资源。
	ProTrialStatus any `json:"ProTrialStatus"`
	// SubscriptionPackageCode 为当前订阅包编码，非空表示存在订阅包。
	SubscriptionPackageCode string `json:"SubscriptionPackageCode"`
}

// -----------------------------------------------------------------------------
// 全局状态与配置
// -----------------------------------------------------------------------------

type Config struct {
	Addr               string
	Port               int
	PortExplicit       bool
	WebPort            int
	AdminKey           string
	AuthFile           string
	AuthDir            string
	AuthExplicit       bool // 用户是否显式指定了 -auth（未指定时自动扫描目录下所有 workbuddy*.json）
	LoginIntl          bool // login -intl：登录国际站 (www.workbuddy.ai，浏览器内完成登录)
	APIKey             string
	ProxyURL           string
	Verbose            bool
	DebugEnabled       bool   // 仅由工作目录 config.json 的 debug.enabled 控制
	ReloadInterval     int    // 账号池热加载扫描间隔（秒），0 关闭
	MonitorInterval    int    // monitor 状态刷新间隔（秒）
	LogFile            string // monitor 附加展示的日志文件路径
	JournalService     string // monitor 附加展示的 systemd 服务名（journalctl -u）
	LogLines           int    // monitor 展示的最近日志行数
	ModelsRefresh      int    // 官方模型目录刷新间隔（分钟），0 关闭
	AllowPriceProbes   bool   // 仅显式开启后台价格探测；真实生成请求可能消耗积分
	DisablePriceProbes bool   // 旧禁用参数兼容，优先于 AllowPriceProbes；不影响客户端请求及显式 probe 命令
	KeepaliveHours     []int  // 主动续期时刻（本地小时），空表示关闭；到点主动刷新全部账号
	CheckinDelay       int    // serve 启动时若已过 08:00，补签前的等待秒数（等待系统网络就绪），0 关闭缓冲立即补签
	ProbeModels        string // probe 专用：逗号分隔的模型列表
	ProbeLimit         int    // probe 专用：未显式指定模型时的取用数量
	WebUI              bool   // 仅显式 -webui 才启动独立管理端口
	HttpClient         *http.Client
}

// Account 表示一个 CodeBuddy 账号凭据及其运行时状态。
type Account struct {
	Path           string                        // 凭据文件路径
	Auth           *StoredAuth                   // 凭据内容（失效标记恢复时可能为 nil）
	CooldownUntil  time.Time                     // 冷却截止时间（429 频率限制后自动屏蔽到该时间）
	CooldownMsg    string                        // 触发冷却的原因/上游提示
	Disabled       bool                          // 授权失效（token 过期且无法刷新/已撤销）：禁止调度
	DisabledReason string                        // 失效原因，用于控制台展示
	Nickname       string                        // 失效标记持久化字段：昵称（Auth 为 nil 时使用）
	UID            string                        // 失效标记持久化字段：UID
	Edition        string                        // 站点标识（cn/intl）：失效标记恢复时 Auth 为 nil 也可见
	QuotaTotal     float64                       // 最近一次额度查询返回的周期总额度
	QuotaUsed      float64                       // 最近一次额度查询返回的已用额度
	QuotaRemaining float64                       // 最近一次额度查询返回的剩余额度
	IsPaidUser     bool                          // 是否为付费用户（上游 IsPaidUser 原值）
	PlanLabel      string                        // 官方套餐名称或已知套餐短名称
	PlanCheckedAt  int64                         // 最近成功校验权益的时间；旧快照为0
	PlanStale      bool                          // 最近查询失败，展示时标记旧结果
	QuotaKnown     bool                          // 是否已成功获取过额度
	QuotaExhausted bool                          // 已确认额度为 0；额度扫描发现恢复后自动解除
	QuotaUnlimited bool                          // 企业版 limitNum=-1：分配额度不限，禁止按 0 余额阻断
	ModelStates    map[string]*modelRuntimeState // 按模型隔离的成本、限流和额度阻断状态
	fingerprint    string                        // 凭据文件变更指纹（mtime+size，凭据热加载用）
	lock           sync.Mutex                    // 单账号串行锁（防止同账号并发触发 11128）
	// 续期失败计数（仅内存，重启清零）：单次失败可能是上游抖动，连续失败才判定登录态失效。
	RefreshFailCount  int
	LastRefreshError  string
	LastRefreshFailAt time.Time // 最近一次续期失败时刻；间隔超过 refreshFailDecayWindow 时计数清零重计
}

const (
	modelCostUnknown = "unknown"
	modelCostFree    = "free"
	modelCostPaid    = "paid"
	modelProbeDelay  = 5 * time.Minute

	// modelFreeMinTokens 判定“免费模型”所需的最小样本：上游对极小请求也可能记
	// credit=0（例如探针请求），那不是真正的免费，不能据此让零余额账号请求收费模型。
	modelFreeMinTokens = 100
)

type modelRuntimeState struct {
	CostClass     string
	CooldownUntil time.Time
	QuotaBlocked  bool
	NextProbeAt   time.Time
	LastReason    string
	ObservedAt    time.Time
}

type accountSelectionKind string

const (
	selectionNormal         accountSelectionKind = "normal"
	selectionFreeExhausted  accountSelectionKind = "free_exhausted"
	selectionProbeExhausted accountSelectionKind = "probe_exhausted"
)

// Profile 返回该账号对应的上游站点参数（国内站/国际站）。
// 调用方须持有 accountMu（或能证明无并发写）；请求热路径请改用 authSnapshot。
func (acc *Account) Profile() *upstreamProfile {
	if acc.Auth != nil {
		return profileForEdition(acc.Auth.Edition)
	}
	return profileForEdition(acc.Edition)
}

// authSnapshot 在 accountMu 保护下读取当前凭据指针。acc.Auth 的两个写方（凭据热加载、
// 令牌刷新）都在 accountMu 下整体替换指针，且替换后不再修改旧对象，因此快照到的
// 指针可跨锁安全使用；直接裸读该字段则与写方构成数据竞争。
func (acc *Account) authSnapshot() *StoredAuth {
	accountMu.Lock()
	defer accountMu.Unlock()
	return acc.Auth
}

// disabledMarker 是授权失效账号的持久化标记（凭据文件删除后用于控制台提示重新登录）。
type disabledMarker struct {
	Path       string `json:"path"`
	Reason     string `json:"reason"`
	DisabledAt int64  `json:"disabledAt"`
	Nickname   string `json:"nickname,omitempty"`
	UID        string `json:"uid,omitempty"`
	Edition    string `json:"edition,omitempty"` // 站点标识（cn/intl），用于控制台展示
}

// markerPath 返回与凭据文件同目录的失效标记文件路径。
func markerPath(authPath string) string {
	return authPath + ".disabled"
}

var (
	cfg        Config
	authLock   sync.RWMutex
	currAuth   *StoredAuth
	reqCounter uint64

	accountMu sync.Mutex // 账号池保护锁
	accounts  []*Account // 多账号池（单账号时长度为 1，行为与旧版完全一致）
	rrIndex   int        // 轮询游标

	quotaScanTrigger = make(chan struct{}, 1)
	dailyCheckinMu   sync.Mutex

	checkinStateMu   sync.Mutex
	checkinState     checkinPersistentState
	checkinStatePath = checkinStateFile
)

// -----------------------------------------------------------------------------
// 主入口与命令行控制
// -----------------------------------------------------------------------------

func registerAutomaticPriceProbeFlags(fs *flag.FlagSet, target *Config) {
	fs.BoolVar(&target.AllowPriceProbes, "allow-price-probes", false, "允许后台自动价格探测（默认关闭；真实生成请求可能消耗积分）")
	// 兼容旧 systemd / 启动脚本，但不再出现在主要用法说明中。
	// 即使传入 -disable-price-probes=false，也不能绕过显式开启的要求。
	fs.BoolVar(&target.DisablePriceProbes, "disable-price-probes", false, "")
}

func (c Config) automaticPriceProbesAllowed() bool {
	// 两个参数同时出现时，禁用优先，且不依赖命令行参数的排列顺序。
	return c.AllowPriceProbes && !c.DisablePriceProbes
}

func main() {
	if len(os.Args) > 1 {
		first := os.Args[1]
		if first == "help" || first == "-h" || first == "--help" {
			printHelp()
			return
		}
		if first == "version" || first == "-v" || first == "--version" {
			fmt.Printf("WorkBuddy Local Gateway v%s\n", version)
			return
		}
	}

	command := "serve"
	args := os.Args[1:]
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		command = os.Args[1]
		args = os.Args[2:]
	}

	fs := flag.NewFlagSet(command, flag.ExitOnError)
	// legacyAPIKeyFlag 仅用于吞掉旧启动命令里的 -api-key，避免升级后报未知参数而启动失败。
	// 该值不会被任何逻辑读取，也不会产生任何日志或状态变化；用法说明留空，
	// 使其不出现在 --help 与默认参数列表中。
	// TODO(remove): 纯兼容占位，计划在后续版本连同下面这行注册一起删除。
	var legacyAPIKeyFlag string
	fs.StringVar(&cfg.Addr, "addr", "127.0.0.1", "网关监听地址")
	fs.IntVar(&cfg.Port, "port", defaultAPIPort, "API 监听端口（显式传入时覆盖 config.json 的 gateway.apiPort）")
	fs.StringVar(&cfg.AuthFile, "auth", "workbuddy.json", "凭据存储文件路径（支持逗号分隔多个文件实现多账号）")
	fs.StringVar(&cfg.AuthDir, "auth-dir", "", "凭据目录：自动加载目录下所有 workbuddy*.json 作为多账号池")
	fs.StringVar(&cfg.ProxyURL, "proxy", "", "可选：上游请求代理 (如 http://127.0.0.1:7890)")
	fs.StringVar(&legacyAPIKeyFlag, "api-key", "", "")
	// 自定义参数错误提示，避免默认 PrintDefaults 把兼容占位参数列出来。
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "参数有误，请运行 %s --help 查看可用参数\n", os.Args[0])
	}
	fs.BoolVar(&cfg.Verbose, "verbose", false, "输出详细调试日志")
	fs.BoolVar(&cfg.LoginIntl, "intl", false, "login 专用：登录国际站 (www.workbuddy.ai，浏览器内完成登录)；默认登录国内站")
	fs.IntVar(&cfg.MonitorInterval, "interval", 3, "monitor 状态刷新间隔（秒）")
	fs.IntVar(&cfg.ReloadInterval, "reload-interval", 5, "账号池热加载扫描间隔（秒），0 关闭：运行期自动发现新增/更新/删除的凭据文件，免重启")
	fs.StringVar(&cfg.LogFile, "logfile", "", "monitor 附加跟随的日志文件路径（如 -logfile /var/log/workbuddy-gateway.log）")
	fs.StringVar(&cfg.JournalService, "journal", "", "monitor 附加跟随的 systemd 服务名（Linux 下用 journalctl -u <服务> -f 跟随）")
	fs.IntVar(&cfg.LogLines, "lines", 15, "monitor 每次刷新展示的最近日志行数")
	fs.IntVar(&cfg.ModelsRefresh, "models-refresh", 60, "模型目录刷新间隔（分钟），0 关闭（实时接口 + npm 合并）")
	registerAutomaticPriceProbeFlags(fs, &cfg)
	keepaliveHours := hourList{22}
	fs.Var(&keepaliveHours, "keepalive-hours", "主动续期时刻（本地小时，逗号分隔，默认 22）；留空关闭")
	fs.IntVar(&cfg.CheckinDelay, "checkin-delay", 60, "serve 启动时若已过08:00，补签前的等待秒数，等待系统网络就绪避免开机即失败；0 关闭缓冲")
	fs.StringVar(&cfg.ProbeModels, "models", "", "probe 专用：逗号分隔的待探测模型（默认取目录前几个）")
	fs.IntVar(&cfg.ProbeLimit, "limit", 5, "probe 专用：未指定 -models 时探测的模型数量上限")
	fs.BoolVar(&cfg.WebUI, "webui", false, "启用独立网页控制台；管理 Key、API 鉴权与端口由 config.json 配置")
	_ = fs.Parse(args)
	cfg.KeepaliveHours = []int(keepaliveHours)

	// 检测 -auth 是否被显式指定：
	// 若未指定 -auth 且未指定 -auth-dir，则自动扫描当前目录下所有 workbuddy*.json 组成账号池，
	// 这样把多个凭据文件放进工作目录即可自动多账号，无需手写参数。
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "auth" {
			cfg.AuthExplicit = true
		}
		if f.Name == "port" {
			cfg.PortExplicit = true
		}
	})
	serveCommand := command == "serve" || command == "run" || command == "start"
	if serveCommand || command == "probe" {
		if err := loadRuntimeConfig(runtimeConfigFile); err != nil {
			fmt.Fprintf(os.Stderr, "启动失败: %v\n", err)
			os.Exit(1)
		}
		ignoreLegacyAPIKeyFlag(legacyAPIKeyFlag)
	}

	// 初始化 HTTP 客户端
	initHTTPClient()
	closeLog := initFileLogging(command)
	defer closeLog()
	closeDebug := func() {}
	if serveCommand {
		var err error
		closeDebug, err = initDebugLogging()
		if err != nil {
			log.Fatalf("[Debug] 调试日志初始化失败: %v", err)
		}
	}
	defer closeDebug()

	switch command {
	case "serve", "run", "start":
		runServe()
	case "login":
		runLogin()
	case "status":
		runStatus()
	case "refresh":
		runRefresh()
	case "monitor":
		runMonitor()
	case "probe":
		runProbe()
	case "reset":
		runReset()
	case "version", "-v", "--version":
		fmt.Printf("WorkBuddy Local Gateway v%s\n", version)
	default:
		fmt.Printf("未知命令: %s\n\n", command)
		printHelp()
		os.Exit(1)
	}
}

// initFileLogging 将运行日志同时写入控制台和按日期命名的项目日志文件。
func initFileLogging(command string) func() {
	gatewayConfigMu.RLock()
	registerSecrets(cfg.APIKey, cfg.AdminKey, gatewaySettings.APIKey)
	gatewayConfigMu.RUnlock()
	// 文件初始化失败时，控制台仍要隐藏秘密值。
	log.SetOutput(&redactingLogWriter{writer: os.Stderr})
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Printf("[Log] 无法创建日志目录 %s，将仅输出到控制台: %v", logDir, err)
		return func() {}
	}
	path := filepath.Join(logDir, "gateway-"+time.Now().Format("2006-01-02")+".log")
	f, err := newDailyLogWriter("gateway", ".log")
	if err != nil {
		log.Printf("[Log] 无法打开日志文件 %s，将仅输出到控制台: %v", path, err)
		return func() {}
	}
	log.SetOutput(&redactingLogWriter{writer: io.MultiWriter(os.Stderr, f)})
	log.Printf("[Log] 审计日志已启用，文件=%s，命令=%s，版本=%s", path, command, version)
	return func() { _ = f.Close() }
}

func printHelp() {
	fmt.Println(`WorkBuddy Local Gateway - 轻量化跨平台本地 AI 网关
将腾讯 CodeBuddy / 混元反代为标准 OpenAI 协议，支持直接透传任意模型到上游。
支持多账号池：轮询使用多个账号均衡额度；账号触发 429 频率限制后自动冷却屏蔽，
由其余账号代偿，冷却到期自动恢复。

用法:
  workbuddy-gateway [command] [options]

命令:
  serve       启动本地网关 (默认操作)
  login       登录并获取/更新凭据：国内站微信扫码；-intl 登录国际站 (浏览器内完成)
  status      查看账号池状态（含站点、冷却状态与过期时间）
  refresh     手动立即刷新所有账号访问令牌 (Access Token)
  monitor     前台实时监控：周期刷新展示账号状态 + 最近日志 (Ctrl+C 退出)
  probe       主动探测账号对指定模型的免费/收费属性（需 serve 正在运行）
  reset       清空除登录凭据外的全部本地数据（状态/缓存/日志/失效标记），并重新拉取模型与倍率
  version     查看版本信息
  help        查看帮助说明

参数选项:
  -addr <ip>        网关监听地址 (默认: 127.0.0.1)
  -port <port>      网关监听端口 (默认: 8317)
  -auth <path>      凭据文件路径；支持逗号分隔多个文件实现多账号
                    (默认: 自动发现当前目录下所有 workbuddy*.json)
  -auth-dir <dir>   凭据目录：自动加载目录下所有 workbuddy*.json 作为账号池
  -intl             login 专用：登录国际站 www.workbuddy.ai（浏览器内完成登录）
  -reload-interval <sec>
                    账号池热加载扫描间隔（默认 5 秒，0 关闭）：运行期自动发现
                    新增/更新/删除的凭据文件，免重启生效
  -models-refresh <min>
                    模型目录刷新间隔（默认 60 分钟，0 关闭）
                    （目录来源：实时接口 + npm 静态包，合并去重）
  -allow-price-probes
                    开启后台自动价格探测（默认关闭）；
                    会发起真实模型生成请求，可能消耗积分，请确认后开启；
                    不影响客户端请求、目录刷新及显式 probe 命令
  -keepalive-hours <h,h,...>
                    主动续期时刻（本地小时，逗号分隔，默认 22）
                    到点主动刷新全部账号登录凭据，不等访问令牌临近过期；
                    留空关闭。只调刷新接口，不请求模型、不消耗额度
  -webui            启用独立网页控制台 (默认 http://<addr>:8316/ui/)：
                    展示账号池/模型统计/日志/config.json/凭据（令牌脱敏），
                    并可切换模型 API Key 校验；管理 Key 与端口在 config.json
                    的 gateway 段配置，管理 Key 为空时可在服务器本机首次生成

probe 选项:
  -auth <path>      只探测指定凭据文件（文件名或路径均可）；默认探测全部账号
  -models <m1,m2>   指定要探测的模型；默认取模型目录前几个
  -limit <n>        未指定 -models 时探测的模型数量（默认 5，上限 50）
  -addr/-port       需与运行中的 serve 一致；模型 API 开启 Key 校验时 probe 自动携带 gateway.apiKey
  -proxy <url>      设置上游转发代理 (例如 http://127.0.0.1:7890 或 socks5://...)
  -verbose          输出详细调试日志 (请求/响应体)

monitor 选项:
  -interval <sec>   状态刷新间隔秒数 (默认: 3)
  -journal <svc>    同时展示 systemd 服务最近日志 (Linux, 如 -journal workbuddy-gateway)
  -logfile <path>   同时展示指定日志文件最近内容 (如 -logfile /var/log/wb.log)
  -lines <n>        每次刷新展示的最近日志行数 (默认: 15)

多账号说明:
  # 登录第二个账号（保存到不同文件）
  workbuddy-gateway login -auth workbuddy2.json

  # 登录国际站账号（www.workbuddy.ai，浏览器内完成登录）
  workbuddy-gateway login -intl -auth workbuddy-intl.json

  # 自动发现：把多个凭据文件放进工作目录即可自动多账号（无需任何参数）
  workbuddy-gateway serve        # 自动加载 ./workbuddy*.json

  # 启动时指定多个凭据文件（轮询 + 429 自动冷却代偿；国内/国际可混挂）
  workbuddy-gateway serve -auth workbuddy.json,workbuddy2.json

  # 或使用目录模式：目录内所有 workbuddy*.json 自动组成账号池
  workbuddy-gateway serve -auth-dir ./auths

  # 运行期新增/更新/删除凭据文件会自动热加载（默认每 5 秒），无需重启 serve

示例:
  # 首次使用扫码登录（国内站）
  workbuddy-gateway login

  # 登录国际站（www.workbuddy.ai）
  workbuddy-gateway login -intl

  # 启动本地网关 (监听 127.0.0.1:8317)
  workbuddy-gateway serve

  # 启动网关并指定端口和代理
  workbuddy-gateway serve -port 9000 -proxy http://127.0.0.1:7890`)
}

func initHTTPClient() {
	jar, _ := cookiejar.New(nil)
	headerTimeout, _, _ := upstreamSettings()
	transport := &http.Transport{
		// 显式启用 HTTP/2：设置非 nil TLSClientConfig 后标准库默认退回 HTTP/1.1，
		// 高并发下无法多路复用。上游返回 goaway 的瞬时错误本来就按可重试处理。
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        50,
		IdleConnTimeout:     90 * time.Second,
		MaxIdleConnsPerHost: 10,
		// 拨号与 TLS 握手给明确上限：对端黑洞时不再依赖 OS 默认超时。
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: false},
		// 只约束到「收到响应头」为止；流式响应体的长时间传输由 upstreamIdleTimeout 的空闲看门狗保护。
		ResponseHeaderTimeout: headerTimeout,
	}
	if cfg.ProxyURL != "" {
		pURL, err := url.Parse(cfg.ProxyURL)
		if err != nil {
			log.Fatalf("错误: 无效的代理地址 %s: %v", cfg.ProxyURL, err)
		}
		transport.Proxy = http.ProxyURL(pURL)
	}
	// 注意：这里刻意不设置 http.Client.Timeout。
	// 总超时会给整条流设一个硬上限（原为 180s），大请求长时间但持续有输出的流会被中途掐断，
	// 导致上游 usage 丢失、客户端收到不完整响应，并被 CPA 判为 provider 故障而触发 503。
	cfg.HttpClient = &http.Client{
		Transport: &hotConfigTransport{transport: transport},
		Jar:       jar,
	}
}

// idleReadCloser 为上游流式响应体提供「空闲读超时」保护。
// 只要持续有数据到达就不断顺延截止时间；一旦超过 timeout 没有任何新数据，
// 就取消上游请求上下文，使阻塞中的 Read 立即返回错误，从而中断卡死的流。
type idleReadCloser struct {
	rc      io.ReadCloser
	timeout time.Duration
	cancel  context.CancelFunc
	mu      sync.Mutex
	timer   *time.Timer
	last    time.Time // 最近一次读到数据的时刻
	stopped bool
}

func newIdleReadCloser(rc io.ReadCloser, timeout time.Duration, cancel context.CancelFunc) *idleReadCloser {
	b := &idleReadCloser{rc: rc, timeout: timeout, cancel: cancel, last: time.Now()}
	b.mu.Lock()
	b.timer = time.AfterFunc(timeout, b.check)
	b.mu.Unlock()
	return b
}

// check 在计时到期时判断是否真的空闲超时；若期间有新数据到达则按剩余时间续期，
// 避免「数据刚好到达」与「计时器触发」之间的竞态导致误杀活跃流。
func (b *idleReadCloser) check() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stopped {
		return
	}
	idle := time.Since(b.last)
	if idle >= b.timeout {
		b.stopped = true
		b.cancel()
		return
	}
	b.timer.Reset(b.timeout - idle)
}

func (b *idleReadCloser) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if n > 0 {
		b.mu.Lock()
		b.last = time.Now() // 有数据到达即视为活跃
		b.mu.Unlock()
	}
	return n, err
}

func (b *idleReadCloser) Close() error {
	b.mu.Lock()
	b.stopped = true
	if b.timer != nil {
		b.timer.Stop()
	}
	b.mu.Unlock()
	b.cancel()
	return b.rc.Close()
}

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
// CLI 子命令实现: login, status, refresh
// -----------------------------------------------------------------------------
func runServe() {
	// 先绑定全部所需端口；任一失败就释放已经绑定的端口，不启动半套服务。
	servers, listeners, err := prepareGatewayServers()
	if err != nil {
		fmt.Fprintf(os.Stderr, "启动失败: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	configCtx, stopConfigReload := context.WithCancel(context.Background())
	defer stopConfigReload()
	loadModelsCache()
	loadOptionalStats()
	if err := loadAccounts(); err != nil {
		fmt.Printf("警告: 未检测到有效凭据 (%v)。\n请先执行: workbuddy-gateway login 扫码登录，或确保凭据文件存在。\n\n", err)
	} else {
		accountMu.Lock()
		accs := append([]*Account(nil), accounts...)
		accountMu.Unlock()
		for _, acc := range accs {
			if acc.Disabled || acc.Auth == nil {
				continue
			}
			_ = ensureValidTokenFor(acc)
		}
	}

	// 启动后台自动刷新协程（safeGoLoop：单循环 panic 退避重启，不再击穿整个进程）
	safeGoLoop("token_refresher", backgroundTokenRefresher)
	// 主动续期（保活）：到点刷新全部账号登录凭据，不等访问令牌临近过期
	safeGoLoop("keepalive", keepaliveLoop)
	safeGoLoop("quota_refresher", backgroundQuotaRefresher)
	safeGoLoop("daily_checkin", backgroundDailyCheckin)
	// 周期复检被禁用账号：上游恢复即自动回到账号池，不再依赖用户重新登录
	safeGoLoop("disabled_recheck", disabledAccountRecheckLoop)
	if cfg.ModelsRefresh > 0 {
		safeGoLoop("model_catalog_refresh", func() { modelsRefreshLoop(time.Duration(cfg.ModelsRefresh) * time.Minute) })
	} else {
		logScheduledTask("model_catalog", "disabled", "disabled", newTraceID(), time.Time{}, time.Time{}, "模型目录自动刷新已关闭")
	}
	// 价格探测独立于目录刷新：即使关闭目录刷新，也仍可基于缓存探测价格。
	safeGoLoop("price_probe", modelPriceProbeLoop)

	// 启动状态快照协程（monitor 命令实时读取展示）
	safeGoLoop("status_snapshot", statusSnapshotLoop)
	statsCtx, stopStats := context.WithCancel(context.Background())
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		// 不用 safeGoLoop：shutdown 时本循环正常返回，若 panic 重启会违背关闭语义
		runUntilPanic("stats_persistence", func() { statsPersistenceLoop(statsCtx) })
	}()
	defer func() {
		stopStats()
		<-statsDone
	}()

	// 启动凭据热加载协程（新增/更新/删除凭据文件免重启生效）
	if cfg.ReloadInterval > 0 {
		safeGoLoop("credential_reload", accountReloaderLoop)
	} else {
		logScheduledTask("credential_reload", "disabled", "disabled", newTraceID(), time.Time{}, time.Time{}, "凭据自动扫描已关闭")
	}

	listenAddr := servers[0].Addr

	fmt.Println("================================================================")
	fmt.Printf("WorkBuddy 本地网关已启动\n")
	fmt.Printf("   服务监听地址:  http://%s\n", listenAddr)
	fmt.Printf("   Chat 接口地址: http://%s/v1/chat/completions\n", listenAddr)
	fmt.Printf("   Claude Code:   http://%s/v1/messages (ANTHROPIC_BASE_URL 指向本网关)\n", listenAddr)
	fmt.Printf("   Models 接口:   http://%s/v1/models\n", listenAddr)
	fmt.Printf("   模型转发策略:  【完全透传】客户端请求的任意 model 原样中继至上游\n")
	_, modelSource := mergedModelIDs()
	fmt.Printf("   模型列表来源:  %s\n", modelSourceLabel(modelSource))
	if cfg.ReloadInterval > 0 {
		fmt.Printf("   凭据热加载:    每 %ds 自动扫描，新增/更新/删除凭据免重启生效\n", cfg.ReloadInterval)
	} else {
		fmt.Printf("   凭据热加载:    已关闭 (-reload-interval 0)\n")
	}
	if currentAPIKey() != "" {
		printAPIAuthBanner(os.Stdout)
	} else {
		fmt.Printf("   API 鉴权:      未启用 (任何客户端均可直连)\n")
	}
	if cfg.WebUI {
		fmt.Printf("   网页控制台:    已启用 http://%s/ui/ (独立管理 Key)\n", servers[1].Addr)
		if currentAdminKey() == "" {
			fmt.Println("   管理初始化:    请在本机或 SSH 隧道打开网页生成管理 Key，保存成功后仅弹窗展示一次")
		}
	} else {
		fmt.Println("   网页控制台:    未启用 (-webui 可开启)")
	}
	if cfg.ProxyURL != "" {
		fmt.Printf("   上游出口代理:  %s\n", cfg.ProxyURL)
	}
	if cfg.DebugEnabled {
		fmt.Printf("   JSON 调试日志: 已开启 (%s)\n", filepath.Join(logDir, "debug-YYYY-MM-DD.jsonl"))
	} else {
		fmt.Printf("   JSON 调试日志: 已关闭 (%s 中 debug.enabled=false)\n", runtimeConfigFile)
	}
	fmt.Printf("   上游超时:      响应头等待 %v / 流空闲 %v (%s 可覆盖)\n",
		upstreamHeaderTimeout, upstreamIdleTimeout, runtimeConfigFile)
	fmt.Printf("   网络容错:      瞬时错误重试 %d 次 / 间隔 %v（仅未输出前重试，%s 可覆盖）\n",
		upstreamTransientRetries, upstreamRetryBackoff, runtimeConfigFile)
	if modelFilterConfigured() {
		blocked, allowed := modelFilterSummary()
		fmt.Printf("   模型黑白名单:  已启用 (黑名单 %d 个 / 白名单 %d 个，被禁模型已从列表隐藏并拒绝请求)\n", blocked, allowed)
	} else {
		fmt.Printf("   模型黑白名单:  未启用 (%s 中 models 段可配置)\n", runtimeConfigFile)
	}
	fmt.Printf("   模型账号名单:  %d 个模型配置了凭据文件规则（%s）\n", modelAccountRuleCount(), runtimeConfigFile)
	fmt.Println("   配置热生效:    每 1s 扫描 config.json；端口变更需重启")

	// 启动时展示所有账号状态（与 status 命令一致）
	accountMu.Lock()
	accCount := len(accounts)
	activeCount := 0
	disabledCount := 0
	now := time.Now()
	for _, acc := range accounts {
		if acc.Disabled || acc.Auth == nil {
			disabledCount++
		} else {
			activeCount++
		}
	}
	fmt.Printf("   账号池:        %d 个账号 (有效 %d, 失效 %d)\n", accCount, activeCount, disabledCount)
	if accCount > 0 {
		fmt.Println("   ----------------------------------------------------------")
		for i, acc := range accounts {
			fmt.Print(formatAccountStatus(acc, i+1, now))
		}
		fmt.Println("   ----------------------------------------------------------")
	}
	// 末尾汇总：加载到的凭据文件清单（即使上方日志被截断也能确认）
	fileList := make([]string, 0, len(accounts))
	for _, acc := range accounts {
		fileList = append(fileList, acc.Path)
	}
	if len(fileList) > 0 {
		fmt.Printf("   已加载凭据文件 (%d): %s\n", len(fileList), strings.Join(fileList, ", "))
	} else {
		fmt.Println("   未加载到任何凭据文件，请先执行 login 命令扫码登录")
	}
	accountMu.Unlock()

	fmt.Println("================================================================")
	fmt.Println("等待客户端请求中 (按 Ctrl+C 安全停止)...")
	safeGoLoop("runtime_config", func() { runtimeConfigLoop(configCtx) })

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stopChan)
	serveErrors := make(chan error, len(servers))
	for i := range servers {
		go func(i int) {
			if err := servers[i].Serve(listeners[i]); err != nil && err != http.ErrServerClosed {
				serveErrors <- err
			}
		}(i)
	}
	select {
	case <-stopChan:
	case err := <-serveErrors:
		log.Printf("[服务异常] traceId=%s 原因=%v 说明=同时关闭API与管理监听，避免半可用状态", newTraceID(), err)
	}
	fmt.Println("\n正在关闭网关服务...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var shutdown sync.WaitGroup
	for _, server := range servers {
		shutdown.Go(func() {
			if err := server.Shutdown(ctx); err != nil {
				_ = server.Close()
			}
		})
	}
	shutdown.Wait()
	fmt.Println("网关已安全停止。")
}

func printAPIAuthBanner(w io.Writer) {
	fmt.Fprintln(w, "   API 鉴权:      已启用 (密钥已隐藏)")
}

func backgroundTokenRefresher() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	logScheduledTask("token_check", "scheduled", "", newTraceID(), time.Time{}, time.Now().Add(5*time.Minute), "")
	for tick := range ticker.C {
		next := tick.Add(5 * time.Minute)
		trace, started := startScheduledTask("token_check", next)
		accountMu.Lock()
		accs := make([]*Account, len(accounts))
		copy(accs, accounts)
		accountMu.Unlock()
		failed := 0
		for _, acc := range accs {
			if err := ensureValidTokenFor(acc); err != nil {
				failed++
				log.Printf("[BackgroundAuth] 账号 %s 自动检查/续期令牌异常: %v", acc.Path, err)
			}
		}
		status := "success"
		if len(accs) == 0 {
			status = "skipped"
		} else if failed == len(accs) {
			status = "failed"
		} else if failed > 0 {
			status = "partial"
		}
		logScheduledTask("token_check", "completed", status, trace, started, next, fmt.Sprintf("检查账号=%d，异常=%d；仅临近过期时续期", len(accs), failed))
	}
}
