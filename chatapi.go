package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"
)

// -----------------------------------------------------------------------------
// 路由处理: /v1/chat/completions (支持任意 model 透传)
// -----------------------------------------------------------------------------

// upstreamChat 完成「多账号轮询 + 429 冷却代偿 + 授权失效禁用 + 单账号串行」的上游调度。
// 成功时返回 200 响应（调用方负责关闭 Body）与命中的账号/站点；失败时函数内部已写回
// 错误响应并返回 ok=false。Chat Completions 与 Responses 两个入口共用此逻辑。
func upstreamChat(w http.ResponseWriter, r *http.Request, reqID uint64, modelName string, upstreamBytes []byte, startTime time.Time) (*http.Response, *Account, *upstreamProfile, bool) {
	prepareConversationContext(r, nil)
	traceID := w.Header().Get("X-Trace-ID")
	log.Printf("[业务入口] traceId=%s requestId=%d 业务=上游模型调用 请求体字节数=%d", traceID, reqID, len(upstreamBytes))
	recordModelRequest(modelName)
	// API Key 可用范围：被限制的模型对该 Key 表现为「不存在」，未调用上游。
	if scope := apiKeyModelScope(r); len(scope) > 0 && !modelInScope(scope, modelName) {
		log.Printf("[请求被拦截] traceId=%s requestId=%d 拦截层=Key可用范围 模型=%s 结果=拒绝 返回状态码=403 业务影响=未进入上游", traceID, reqID, modelName)
		debugEvent(r, "warn", "model_rejected_by_key_scope", map[string]any{
			"status_code": http.StatusForbidden, "model": modelName, "business_impact": "未调用上游模型",
		})
		recordModelFailure(modelName, "Key范围拒绝")
		writeUpstreamError(w, r, http.StatusForbidden, "model_not_available_for_key", fmt.Sprintf("当前 API Key 无权使用模型 %s", modelName))
		return nil, nil, nil, false
	}
	accountMu.Lock()
	poolSize := len(accounts)
	accountMu.Unlock()
	if poolSize == 0 {
		debugEvent(r, "warn", "request_rejected_no_account", map[string]any{
			"status_code":     http.StatusUnauthorized,
			"reason":          "no_auth",
			"business_impact": "没有可用登录凭据，未调用上游模型",
		})
		recordModelFailure(modelName, "no_auth")
		writeUpstreamError(w, r, http.StatusUnauthorized, "no_auth", "未找到有效登录凭据，请先执行 login 命令扫码登录")
		return nil, nil, nil, false
	}

	var lastRateErr string
	var lastAuthErr string
	var lastNetErr error
	attempted := make(map[*Account]bool, poolSize)
	// 池大小每轮重读：凭据热加载新增的账号也能参与本次请求的换号轮转。
	for attempt := 0; ; attempt++ {
		accountMu.Lock()
		nowPoolSize := len(accounts)
		accountMu.Unlock()
		if attempt >= nowPoolSize {
			break
		}
		acc, selection, err := nextAccountForModel(modelName, attempted)
		if err != nil {
			if errors.Is(err, errModelAccountPolicy) {
				log.Printf("[请求被拒绝] traceId=%s requestId=%d 模型=%s 原因=账号黑白名单没有匹配的凭据 返回状态码=403 业务影响=未进入上游", traceID, reqID, modelName)
				debugEvent(r, "warn", "model_account_policy_rejected", map[string]any{
					"status_code": http.StatusForbidden, "reason": "没有符合模型账号名单的凭据", "business_impact": "未进入上游调用",
				})
				recordModelFailure(modelName, "账号名单拒绝")
				writeUpstreamError(w, r, http.StatusForbidden, "model_account_disabled", fmt.Sprintf("模型 %s 没有可用的凭据文件：已被账号黑白名单禁用", modelName))
				return nil, nil, nil, false
			}
			// 所有账号均不可用（冷却或失效）
			msg := fmt.Sprintf("无可用账号: %v", err)
			if lastAuthErr != "" {
				msg += " | 最近一次授权失效: " + truncate(lastAuthErr, 200)
			}
			if lastRateErr != "" {
				msg += " | 最近一次频率限制: " + truncate(lastRateErr, 200)
			}
			log.Printf("[#%d] %s", reqID, msg)
			debugEvent(r, "warn", "request_rejected_no_available_account", map[string]any{
				"status_code":     http.StatusServiceUnavailable,
				"reason":          "no_available_account",
				"business_impact": "未调用上游模型",
			})
			recordModelFailure(modelName, "无可用账号")
			writeUpstreamError(w, r, http.StatusServiceUnavailable, "no_available_account", msg)
			return nil, nil, nil, false
		}
		attempted[acc] = true
		debugSetAccount(r, acc.Path)
		debugEvent(r, "debug", "account_selected", map[string]any{
			"selection": string(selection),
			"attempt":   attempt + 1,
		})
		if modelAccountRuleConfigured(modelName) {
			// 仅在实际命中账号的日志里附一个短标记，不再逐账号刷名单日志。
			log.Printf("[#%d] 账号名单命中: 模型=%s 凭据=%s（已按 config.json 账号黑白名单筛选）", reqID, modelName, filepath.Base(acc.Path))
			debugEvent(r, "debug", "model_account_policy_hit", map[string]any{
				"model": modelName, "account_file": filepath.Base(acc.Path),
			})
		}
		switch selection {
		case selectionFreeExhausted:
			log.Printf("[FreeModel] requestId=%d 请求的是已知免费模型 %s，选择付费余额耗尽账号 %s 发起请求", reqID, modelName, acc.Path)
		case selectionProbeExhausted:
			log.Printf("[ModelProbe] requestId=%d 账号 %s 付费余额已耗尽，但模型 %s 收费属性未知；执行一次受控探测，5分钟内不重复探测", reqID, acc.Path, modelName)
		}
		if err := ensureValidTokenFor(acc); err != nil {
			lastAuthErr = err.Error()
			continue
		}
		// 令牌刷新时若发现授权失效会禁用账号；若被禁用则跳过换下一个
		accountMu.Lock()
		disabled := acc.Disabled
		accountMu.Unlock()
		if disabled {
			lastAuthErr = acc.DisabledReason
			continue
		}

		// 按账号所属站点（国内站/国际站）路由上游与指纹 Header。
		// acc.Auth 由热加载/令牌刷新在 accountMu 下整体替换，必须在锁内取快照再读 Edition，
		// 避免与写方构成数据竞争（与 doUpstreamRequest 内的 authSnapshot 各自独立快照）。
		prof := profileForEdition(acc.Edition)
		if sa := acc.authSnapshot(); sa != nil {
			prof = profileForEdition(sa.Edition)
		}

		debugEvent(r, "info", "upstream_request_started", map[string]any{
			"upstream_host": prof.Base,
			"attempt":       attempt + 1,
			"payload_bytes": len(upstreamBytes),
		})
		// 注入 CodeBuddy 凭据与指纹 Header
		// 限制同一账号向腾讯上游的请求严格单并发串行排队，防止并发双发触发腾讯风控。
		// 带客户端 ctx 排队：客户端已断开就不再等该账号的串行队列（后台刷新/签到
		// 最长可占锁数分钟），直接结束本次请求。
		if !lockAccountWithContext(r.Context(), &acc.lock) {
			debugEvent(r, "info", "account_queue_cancelled", map[string]any{
				"business_impact": "客户端已取消，未再等待账号串行队列，未调用上游",
			})
			return nil, nil, nil, false
		}
		statsSelectAccount(r, acc, modelName)
		resp, upstreamCancel, err := doUpstreamRequest(r, acc, prof, upstreamBytes, reqID, traceID)
		acc.lock.Unlock()
		if err != nil {
			debugEvent(r, "error", "upstream_request_failed", map[string]any{
				"upstream_host": prof.Base,
				"error_type":    debugErrorType(err),
				"error":         safeDebugError(err),
			})
			log.Printf("[异常] traceId=%s requestId=%d 发生阶段=上游网络调用 账号=%s 异常=%v 业务影响=本次模型请求失败 是否已处理=是", traceID, reqID, acc.Path, err)
			log.Printf("[#%d] 账号 %s [%s] 上游请求失败: %v", reqID, acc.Path, prof.Label, err)
			recordModelFailure(modelName, "网络错误")
			// 请求确认未发出（连接被拒/DNS 失败等）：换下一个账号不会重复计费。
			// 已写出请求头的错误仍直接 502，避免向上游重放 POST。
			if errors.Is(err, errUpstreamNotSent) && r.Context().Err() == nil {
				lastNetErr = err
				log.Printf("[#%d] 账号 %s 网络错误且请求未发出，换下一账号重试", reqID, acc.Path)
				continue
			}
			writeUpstreamError(w, r, http.StatusBadGateway, "upstream_network_error", fmt.Sprintf("网络转发失败: %v", err))
			return nil, nil, nil, false
		}

		// 上游非 200 响应处理
		if resp.StatusCode >= 400 {
			statsFinishAttempt(r, resp.StatusCode)
			errBody, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			upstreamCancel()
			errStr := string(errBody)
			debugEvent(r, "warn", "upstream_response_rejected", map[string]any{
				"upstream_host": prof.Base,
				"status_code":   resp.StatusCode,
				"error_code":    modelStatusFromError(resp.StatusCode, errStr),
			})
			log.Printf("[#%d] 账号 %s [%s] 上游返回 HTTP %d: %s (耗时 %v)", reqID, acc.Path, prof.Label, resp.StatusCode, errStr, time.Since(startTime))
			log.Printf("[外部接口] traceId=%s requestId=%d 上游=%s 状态码=%d 结果=失败 账号=%s", traceID, reqID, prof.Base, resp.StatusCode, acc.Path)

			if isQuotaExhausted(resp.StatusCode, errStr) {
				markModelQuotaBlocked(acc, modelName, errStr)
				if selection == selectionProbeExhausted {
					msg := fmt.Sprintf("当前模型 %s 已在余额耗尽账号 %s 上完成受控探测并确认需要付费额度，本次不再探测其他耗尽账号", modelName, acc.Path)
					log.Printf("[#%d] %s", reqID, msg)
					recordModelFailure(modelName, modelStatusFromError(resp.StatusCode, errStr))
					writeUpstreamError(w, r, http.StatusServiceUnavailable, "model_requires_quota", msg)
					return nil, nil, nil, false
				}
				lastRateErr = errStr
				continue
			}

			if isModelRateLimited(errStr) {
				until, ok := parseResetTime(errStr)
				if !ok {
					until = time.Now().Add(60 * time.Second)
				}
				markModelCooldown(acc, modelName, until, errStr)
				if selection == selectionProbeExhausted {
					msg := fmt.Sprintf("当前模型 %s 在余额耗尽账号 %s 的受控探测中触发模型级限流，本次不再探测其他耗尽账号", modelName, acc.Path)
					recordModelFailure(modelName, modelStatusFromError(resp.StatusCode, errStr))
					writeUpstreamError(w, r, http.StatusServiceUnavailable, "model_rate_limited", msg)
					return nil, nil, nil, false
				}
				lastRateErr = errStr
				continue
			}

			if isRateLimited(resp.StatusCode, errStr) {
				// 429 频率限制：解析重置时间并屏蔽该账号，交由其他账号代偿
				until, ok := parseResetTime(errStr)
				if !ok {
					until = time.Now().Add(60 * time.Second) // 无法解析时默认冷却 60 秒
				}
				markCooldown(acc, until, errStr)
				lastRateErr = errStr
				continue // 尝试下一个账号
			}

			if isAuthFailure(resp.StatusCode, errStr) {
				// 授权失效（401/403 / token 无效 / 登录过期）：禁用该账号并删除凭据文件，
				// 自动改用下一个可用账号，控制台提示用户重新登录
				disableAccount(acc, fmt.Sprintf("上游鉴权失败 (HTTP %d): %s", resp.StatusCode, truncate(errStr, 200)))
				lastAuthErr = errStr
				continue // 尝试下一个账号
			}

			recordModelFailure(modelName, modelStatusFromError(resp.StatusCode, errStr))
			writeUpstreamError(w, r, resp.StatusCode, "upstream_error", fmt.Sprintf("upstream %d: %s", resp.StatusCode, errStr))
			return nil, nil, nil, false
		}

		debugEvent(r, "info", "upstream_response_received", map[string]any{
			"upstream_host": prof.Base,
			"status_code":   resp.StatusCode,
		})
		log.Printf("[外部接口] traceId=%s requestId=%d 上游=%s 状态码=%d 结果=成功 账号=%s", traceID, reqID, prof.Base, resp.StatusCode, acc.Path)
		recordModelSuccess(modelName)
		// 用空闲看门狗包装响应体：持续有数据就永不超时，超过 upstreamIdleTimeout 无数据才中断。
		// 关闭响应体时会一并取消上游 context，完成清理。
		resp.Body = newIdleReadCloser(resp.Body, currentIdleTimeout(), upstreamCancel)
		return resp, acc, prof, true
	}

	// 所有账号的请求都未能发出（连接层失败，如断网/DNS 故障）：如实回 502，
	// 不伪装成 429 冷却——客户端需要区分「服务不可用」与「触发限流」。
	if lastNetErr != nil {
		writeUpstreamError(w, r, http.StatusBadGateway, "upstream_network_error",
			fmt.Sprintf("网络转发失败（已轮询 %d 个账号）: %v", len(attempted), lastNetErr))
		return nil, nil, nil, false
	}

	// 所有账号均因冷却/额度/失效被跳过
	recordModelFailure(modelName, "all_cooldown")
	debugEvent(r, "warn", "request_rejected_all_cooldown", map[string]any{
		"status_code":     http.StatusTooManyRequests,
		"reason":          "all_accounts_cooldown",
		"business_impact": "未调用上游模型",
	})
	writeUpstreamError(w, r, http.StatusTooManyRequests, "all_accounts_cooldown", "所有账号均处于冷却状态")
	return nil, nil, nil, false
}

func handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeOpenAIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "仅支持 POST 请求")
		return
	}

	reqID := requestIDFor(r)
	startTime := requestStartFor(r)

	readStarted := debugBodyReadStarted(r)
	bodyBytes, err := io.ReadAll(r.Body)
	readDuration := debugElapsedSince(readStarted)
	if err != nil {
		debugBodyReadFailed(r, bodyBytes, readStarted, err)
		writeOpenAIError(w, http.StatusBadRequest, "read_error", "读取请求体失败")
		return
	}
	defer r.Body.Close()

	var reqObj map[string]any
	decodeStarted := time.Now()
	decodeErr := json.Unmarshal(bodyBytes, &reqObj)
	decodeDuration := time.Since(decodeStarted)
	if decodeErr != nil {
		debugBodyReadCompleted(r, bodyBytes, readDuration, decodeDuration, false, decodeErr)
		writeOpenAIError(w, http.StatusBadRequest, "invalid_json", "无效的 JSON 请求体")
		return
	}

	// 核心特性：完全透传 model 字段
	// 客户端传什么 model，我们就透传什么 model 给上游，不做任何硬编码限制！
	modelName, _ := reqObj["model"].(string)
	if modelName == "" {
		modelName = "hy4-preview" // 默认保底
		reqObj["model"] = modelName
	}

	isStream, _ := reqObj["stream"].(bool)
	debugSetModelAndStream(r, modelName, isStream)
	debugBodyReadCompleted(r, bodyBytes, readDuration, decodeDuration, true, nil)
	prepareConversationContext(r, reqObj)

	// 模型黑白名单拦截：命中即拒绝，不消耗任何上游账号额度。
	if disabled, reason := modelDisabled(modelName); disabled {
		log.Printf("[请求被拒绝] traceId=%s requestId=%d 拦截层=模型黑白名单 模型=%s 结果=拒绝 原因=%s 返回状态码=403 业务影响=请求未进入上游调用",
			w.Header().Get("X-Trace-ID"), reqID, modelName, reason)
		debugEvent(r, "warn", "model_blocked_by_config", map[string]any{
			"status_code":     http.StatusForbidden,
			"reason":          reason,
			"business_impact": "模型被网关配置禁用，请求未进入上游调用",
		})
		writeOpenAIError(w, http.StatusForbidden, "model_disabled", reason)
		return
	}

	// 腾讯上游强制要求 stream 必须为 true，非流式会被拦截 (code 11101)
	reqObj["stream"] = true

	// 深度思考 (Thinking) 自动适配：混元系列如果未关闭思考，自动赋予 high 档位保证深度思考输出
	applyThinkingRules(reqObj, modelName)

	// 模板净化：改写 Claude Code 等框架被腾讯官方逐字拉黑的固定 prompt 语句
	sanitizeMessages(reqObj)

	// 会话结构归一化：保证首条消息为 system，修复部分非 harness 客户端
	//（以 assistant / tool 续写或回传工具结果）触发的上游 11128 错误
	prepareSystemPromptForUpstream(reqObj, r, reqID, w.Header().Get("X-Trace-ID"))
	// 输出预算归一化：客户端未声明 max_tokens 时补模型输出上限，
	// 避免上游默认预留 384000 导致长会话在远未到窗口上限时被 11133 拒绝
	ensureUpstreamMaxTokens(reqObj, r, reqID, w.Header().Get("X-Trace-ID"), modelName)
	repairReport := repairToolMessageSequence(reqObj)
	logToolSequenceRepair(r, w.Header().Get("X-Trace-ID"), reqID, modelName, repairReport)
	// 11155 防护：与 Responses 入口共用出站推理历史回填，不伪造思维链正文。
	logReasoningHistoryRepair(r, reqID, modelName, repairReasoningHistory(reqObj))

	upstreamBytes, err := json.Marshal(reqObj)
	if err != nil {
		writeOpenAIError(w, http.StatusInternalServerError, "encode_error", "序列化请求失败")
		return
	}

	if cfg.Verbose {
		log.Printf("[#%d][Req] Model: %s | Stream: %v | BodyLen: %d", reqID, modelName, isStream, len(upstreamBytes))
	} else {
		log.Printf("[#%d] POST /v1/chat/completions -> Upstream [Model: %s, Stream: %v]", reqID, modelName, isStream)
	}

	resp, acc, prof, ok := upstreamChat(w, r, reqID, modelName, upstreamBytes, startTime)
	if !ok {
		return
	}
	if isStream {
		streamChatResponse(w, r, resp, modelName, reqID, acc, prof, startTime)
	} else {
		writeChatAggregate(w, r, resp, modelName, reqID, acc, prof, startTime)
	}
}

// streamChatResponse 将上游 SSE 逐行透传为 OpenAI Chat Completions 流式响应。
func streamChatResponse(w http.ResponseWriter, r *http.Request, resp *http.Response, modelName string, reqID uint64, acc *Account, prof *upstreamProfile, startTime time.Time) {
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeOpenAIError(w, http.StatusInternalServerError, "streaming_unsupported", "服务器不支持流式响应 Flush")
		return
	}

	body := newTTFTReader(resp.Body, startTime)
	defer finishFirstToken(r, modelName, reqID, body)
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var usage map[string]any
	finishReason := ""
	sawDone := false
	for scanner.Scan() {
		cleanData := stripDataPrefix(scanner.Text())
		if cleanData == "" {
			continue
		}
		if cleanData == "[DONE]" {
			// 先记住，等确认有真实 finish_reason 再转发。
			// 没有结束原因时转发 [DONE]，等于告诉客户端这次是正常结束。
			sawDone = true
			break
		}
		var chunk map[string]any
		if json.Unmarshal([]byte(cleanData), &chunk) == nil {
			observeFirstToken(body, chunk)
			if u, ok := chunk["usage"].(map[string]any); ok {
				usage = u
			}
			finishReason = noteFinishReason(finishReason, chunk)
		}
		if cleanedChunk := cleanChunkJSON(cleanData); cleanedChunk != "" {
			_, _ = fmt.Fprintf(w, "data: %s\n\n", cleanedChunk)
			flusher.Flush()
		}
	}
	scanErr := scanner.Err()
	completeTokenStream(body, scanErr == nil && finishReason != "")
	if scanErr != nil {
		// 流被中断（上游卡死 / 超时 / 客户端取消）时，绝不补发 [DONE]：
		// 补发等于告诉下游「正常结束」，会让残缺的工具调用被当成完整结果执行。
		// 改为下发一条标准错误事件，让 CPA / 客户端明确知道这次响应不完整。
		debugEvent(r, "error", "stream_response_failed", map[string]any{
			"error_type": debugErrorType(scanErr),
			"error":      safeDebugError(scanErr),
		})
		reason := "上游流式响应中断，本次回复不完整"
		if errors.Is(scanErr, context.DeadlineExceeded) {
			reason = fmt.Sprintf("上游超过 %v 无数据，判定连接卡死并中断，本次回复不完整", currentIdleTimeout())
		}
		errPayload, _ := json.Marshal(map[string]any{
			"error": map[string]any{
				"message": reason,
				"type":    "upstream_stream_interrupted",
				"code":    "stream_interrupted",
			},
		})
		_, _ = fmt.Fprintf(w, "data: %s\n\n", errPayload)
		flusher.Flush()
		log.Printf("[异常] traceId=%s requestId=%d 发生阶段=上游流式读取 账号=%s 异常=%v 业务影响=本次响应不完整，已向客户端下发中断错误而非伪造结束", debugTraceID(r), reqID, acc.Path, scanErr)
	} else if finishReason == "" {
		// 连接是正常关掉的（没有读错误），但整段流没有非空 finish_reason。
		// 这不是成功：不能补 [DONE]，也不能记成 stream_response_completed。
		debugEvent(r, "error", "stream_closed_without_finish", map[string]any{
			"error_type":      "stream_closed_without_finish",
			"finish_reason":   "",
			"saw_done":        sawDone,
			"business_impact": "上游干净结束但没有 finish_reason，已拒绝当成成功，未补发 [DONE]",
		})
		errPayload, _ := json.Marshal(map[string]any{
			"error": map[string]any{
				"message": errStreamClosedWithoutFinish.Error(),
				"type":    "upstream_stream_incomplete",
				"code":    "stream_closed_without_finish",
			},
		})
		_, _ = fmt.Fprintf(w, "data: %s\n\n", errPayload)
		flusher.Flush()
		log.Printf("[异常] traceId=%s requestId=%d 发生阶段=上游流结束 账号=%s 结果=拒绝当成成功 原因=连接正常关闭但没有非空 finish_reason 是否看到DONE=%t 业务影响=不补发 [DONE]，客户端收到 stream_closed_without_finish 是否已处理=是",
			debugTraceID(r), reqID, acc.Path, sawDone)
	} else {
		if sawDone {
			_, _ = fmt.Fprintf(w, "data: [DONE]\n\n")
			flusher.Flush()
		}
		debugEvent(r, "info", "stream_response_completed", map[string]any{
			"status_code":   http.StatusOK,
			"finish_reason": finishReason,
			"saw_done":      sawDone,
		})
	}
	// 只用完整流学习模型收费属性：中断流的 usage 是部分样本，可能把中等长度
	// 输出误标成免费/收费，进而影响零余额账号的调度决策；统计观测不受影响。
	if scanErr == nil && finishReason != "" {
		observeModelCredit(acc, modelName, usage, reqID)
	}
	statsObserveUsage(r, usage, scanErr == nil && finishReason != "")
	recordModelTokens(modelName, usage, reqID)
	recordModelLatency(modelName, time.Since(startTime))
	if scanErr == nil && finishReason != "" {
		log.Printf("[#%d] 流式输出完成 (账号 %s [%s], 结束原因=%s, 耗时 %v, 首Token(v2) %s)", reqID, acc.Path, prof.Label, finishReason, time.Since(startTime), body.text())
	}
}

// writeChatAggregate 聚合上游 SSE 为完整 Chat Completions JSON 响应。
func writeChatAggregate(w http.ResponseWriter, r *http.Request, resp *http.Response, modelName string, reqID uint64, acc *Account, prof *upstreamProfile, startTime time.Time) {
	defer resp.Body.Close()
	body := newTTFTReader(resp.Body, startTime)
	defer finishFirstToken(r, modelName, reqID, body)
	completionJSON, err := aggregateCompletion(body, modelName)
	if err != nil {
		debugEvent(r, "error", "aggregate_response_failed", map[string]any{
			"error_type": debugErrorType(err),
			"error":      safeDebugError(err),
		})
		logAggregateFailure(r, reqID, err)
		writeOpenAIError(w, http.StatusInternalServerError, "aggregate_error", "聚合上游流式响应失败: "+err.Error())
		return
	}
	usage := usageFromCompletion(completionJSON)
	statsObserveUsage(r, usage, true)
	observeModelCredit(acc, modelName, usage, reqID)
	recordModelTokens(modelName, usage, reqID)
	recordModelLatency(modelName, time.Since(startTime))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(completionJSON)
	debugEvent(r, "info", "aggregate_response_completed", map[string]any{
		"status_code":    http.StatusOK,
		"response_bytes": len(completionJSON),
	})
	log.Printf("[#%d] 非流式响应完成 (账号 %s [%s], 耗时 %v, 首Token(v2) %s)", reqID, acc.Path, prof.Label, time.Since(startTime), body.text())
}

// -----------------------------------------------------------------------------
// 路由处理: /v1/models (列出常见模型供客户端自动补全)
// -----------------------------------------------------------------------------

func handleModels(w http.ResponseWriter, r *http.Request) {
	modelIDs, source := mergedModelIDs()
	// Key 可用范围：被限制的模型对该 Key 表现为「不存在」，直接从列表剔除。
	scope := apiKeyModelScope(r)
	modelsList := make([]map[string]any, 0, len(modelIDs))
	for _, id := range modelIDs {
		if len(scope) > 0 && !modelInScope(scope, id) {
			continue
		}
		entry := map[string]any{
			"id": id, "object": "model", "owned_by": "workbuddy", "permission": []any{},
		}
		// 能力扩展字段：只透传目录里真实存在的值，未知一律省略，不做 1M 之类兜底。
		// context_length 是模型能力上限，default_context_length 是客户端默认预算，
		// 两者语义不同，必须分开给，不能合并成一个值。
		// 这里只补充描述信息，不注入上游请求参数、不截断消息，也不改变输出预算逻辑。
		if mc, ok := modelCapabilityFor(id); ok {
			if mc.Ceiling > 0 {
				entry["context_length"] = mc.Ceiling
			}
			if mc.MaxOutput > 0 {
				entry["max_output_tokens"] = mc.MaxOutput
			}
			if mc.Default > 0 {
				entry["default_context_length"] = mc.Default
			}
			if len(mc.Options) >= 2 {
				entry["context_length_options"] = mc.Options
			}
			if len(mc.BySite) > 1 {
				// 站点之间能力不一致：顶层已留空，明细在此，避免下游按较大值预留。
				entry["capabilities_by_site"] = mc.BySite
			}
		}
		modelsList = append(modelsList, entry)
	}
	resp := map[string]any{
		"object": "list",
		"data":   modelsList,
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Model-Source", modelSourceLabel(source))
	_ = json.NewEncoder(w).Encode(resp)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	modelIDs, source := mergedModelIDs()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":       "healthy",
		"timestamp":    time.Now().Unix(),
		"version":      version,
		"model_count":  len(modelIDs),
		"model_source": modelSourceLabel(source),
	})
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = fmt.Fprintf(w, "WorkBuddy Local Gateway v%s is running.\n\nEndpoints:\n- POST /v1/chat/completions\n- POST /v1/responses\n- POST /v1/messages (Anthropic)\n- POST /v1/messages/count_tokens\n- GET  /v1/models\n- GET  /health\n", version)
}

// -----------------------------------------------------------------------------
// 请求转译与净化工具函数
// -----------------------------------------------------------------------------

func applyThinkingRules(obj map[string]any, modelName string) {
	// 遵循 CodeBuddy 规范：仅当客户端显式设置了 reasoning_effort 时才传递与规范化
	// 绝不可强行对普通请求注入 reasoning_effort，否则极易触发腾讯内容与安全策略拦截 (code 11128)
	currEff, exists := obj["reasoning_effort"].(string)
	if !exists || currEff == "" || currEff == "off" || currEff == "none" {
		delete(obj, "reasoning_effort")
		delete(obj, "reasoning_summary")
		return
	}
	// 客户端显式请求思考时，设置 auto
	obj["reasoning_summary"] = "auto"
}

func sanitizeMessages(obj map[string]any) {
	messages, ok := obj["messages"].([]any)
	if !ok {
		return
	}
	for _, m := range messages {
		msg, ok := m.(map[string]any)
		if !ok {
			continue
		}
		// OpenAI 新版 developer 角色（GPT-5 系客户端/Codex）不被腾讯上游接受，
		// 会返回 11128 "Illegal API invocation from an unapproved channel"，
		// 统一归一化为 system（语义等价）。
		if roleOfMessage(msg) == "developer" {
			msg["role"] = "system"
		}
		switch c := msg["content"].(type) {
		case string:
			msg["content"] = sanitizeBlockedTemplates(c)
		case []any:
			for _, partAny := range c {
				part, ok := partAny.(map[string]any)
				if !ok {
					continue
				}
				if t, ok := part["text"].(string); ok {
					part["text"] = sanitizeBlockedTemplates(t)
				}
			}
		}
	}
}

// ensureLeadingSystemMessage 保证 messages 的首条消息符合腾讯上游的会话结构校验。
//
// 上游要求首条消息为 system prompt，否则返回 HTTP 400
// {"code":11128,"msg":"first message is not system prompt"}。实测国内站对 user 开头较宽容，
// 但国际站（workbuddy.ai）严格校验；账号池混挂时表现为约 50% 请求随机失败。部分非 harness
// 客户端在续写或仅回传工具结果时还会以 assistant / tool 作为首条消息。此处统一归一化为：
//  1. 首条已是 system：原样透传；
//  2. 首条是 developer（OpenAI 新版 system 别名）：重命名为 system；
//  3. 后续存在 system/developer：提升到首位（developer 归一化为 system），其余保持原序；
//  4. 其余情况（user / assistant / tool 开头且无 system）：在最前注入一条保底 system。
//
// 返回值只表示本次是否注入了保底 system，用于不记录提示词正文的审计日志。
func ensureLeadingSystemMessage(obj map[string]any) bool {
	messages, ok := obj["messages"].([]any)
	if !ok || len(messages) == 0 {
		obj["messages"] = []any{map[string]any{"role": "system", "content": configuredFallbackSystemPrompt()}}
		return true
	}

	switch roleOfMessage(messages[0]) {
	case "system":
		return false
	case "developer":
		if msg, ok := messages[0].(map[string]any); ok {
			msg["role"] = "system"
		}
		return false
	}

	// 后续存在 system/developer：提升到首位，其余保持原序
	for i := 1; i < len(messages); i++ {
		switch roleOfMessage(messages[i]) {
		case "system", "developer":
			if msg, ok := messages[i].(map[string]any); ok {
				msg["role"] = "system"
			}
			reordered := make([]any, 0, len(messages))
			reordered = append(reordered, messages[i])
			reordered = append(reordered, messages[:i]...)
			reordered = append(reordered, messages[i+1:]...)
			obj["messages"] = reordered
			return false
		}
	}

	// 无任何 system：在最前注入保底 system（兼容国内站/国际站）
	injected := make([]any, 0, len(messages)+1)
	injected = append(injected, map[string]any{"role": "system", "content": configuredFallbackSystemPrompt()})
	injected = append(injected, messages...)
	obj["messages"] = injected
	return true
}

// roleOfMessage 读取消息的 role 字段并归一化为小写去空格；非法结构返回空串。
func roleOfMessage(m any) string {
	msg, ok := m.(map[string]any)
	if !ok {
		return ""
	}
	role, _ := msg["role"].(string)
	return strings.ToLower(strings.TrimSpace(role))
}

func sanitizeBlockedTemplates(s string) string {
	s = strings.ReplaceAll(s,
		"You are Claude Code, Anthropic's official CLI for Claude.",
		"You are Claude Code, Anthropic's official CLI tool for Claude.")
	s = strings.ReplaceAll(s,
		"Main branch (you will usually use this for PRs)",
		"Default branch (you will usually use this for PRs)")
	return s
}

// backendHeaders 设置 CodeBuddy 上游专用指纹与鉴权 Header（按站点 Profile 生成）
func backendHeaders(req *http.Request, sa *StoredAuth, prof *upstreamProfile) {
	commonHeaders(req, prof)
	reqID := uuid.New().String()
	req.Header.Set("X-Request-ID", reqID)
	req.Header.Set("X-Trace-ID", reqID)
	setConversationHeaders(req)
	req.Header.Set("X-Client-ID", prof.ClientID)
	req.Header.Set("X-Client-Version", prof.ClientVer)

	if sa != nil && sa.Auth.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+sa.Auth.AccessToken)
	} else {
		req.Header.Set("X-No-Authorization", "1")
	}
	if sa != nil && sa.Account.UID != "" {
		req.Header.Set("X-User-Id", sa.Account.UID)
	} else {
		req.Header.Set("X-No-User-Id", "1")
	}
	if sa != nil && sa.Account.EnterpriseID != "" {
		req.Header.Set("X-Enterprise-Id", sa.Account.EnterpriseID)
	} else {
		req.Header.Set("X-No-Enterprise-Id", "1")
	}
	if sa != nil && sa.Auth.RefreshToken != "" {
		req.Header.Set("X-Refresh-Token", sa.Auth.RefreshToken)
	}
	if sa != nil && sa.Auth.Domain != "" {
		req.Header.Set("X-Domain", sa.Auth.Domain)
	} else {
		req.Header.Set("X-No-Department-Info", "1")
	}
	req.Header.Set("X-Product", prof.Product)
}

// commonHeaders 按站点 Profile 设置通用伪装 Header（Origin/Referer/UA 因站点而异）。
func commonHeaders(req *http.Request, prof *upstreamProfile) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Origin", prof.Origin)
	req.Header.Set("Referer", prof.Origin+"/")
	req.Header.Set("User-Agent", prof.ClientUA)
}

func doJSON(client *http.Client, method, fullURL string, headers func(*http.Request), body io.Reader) (json.RawMessage, int, error) {
	// 控制类请求（令牌刷新、额度查询、模型目录等）必须有明确上限，避免卡死；
	// 上游对话流不经过这里，因此不会误伤长流。
	ctx, cancel := context.WithTimeout(context.Background(), defaultRequestTimeout)
	defer cancel()
	return doJSONContext(ctx, client, method, fullURL, headers, body)
}

func doJSONContext(ctx context.Context, client *http.Client, method, fullURL string, headers func(*http.Request), body io.Reader) (json.RawMessage, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, fullURL, body)
	if err != nil {
		return nil, 0, err
	}
	if headers != nil {
		headers(req)
	} else {
		commonHeaders(req, &profileCN) // 兜底默认（当前所有调用方均显式传入站点 Profile）
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, resp.StatusCode, fmt.Errorf("http_error: upstream %d: %s", resp.StatusCode, string(raw))
	}
	var env apiEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("解析上游 JSON 失败: %w", err)
	}
	if env.Code != 0 {
		return nil, resp.StatusCode, fmt.Errorf("code=%d msg=%s", env.Code, env.Msg)
	}
	return env.Data, resp.StatusCode, nil
}

// -----------------------------------------------------------------------------
// 流式聚合与格式清理
// -----------------------------------------------------------------------------

// mergedToolCall 累积合并上游按 index 分片下发的工具调用增量。
type mergedToolCall struct {
	ID   string
	Type string
	Name string
	Args strings.Builder
}

// applyToolCallDelta 将上游 tool_calls 增量按 index 归并进 map；order 记录首次出现的
// index 顺序，保证最终输出顺序稳定。Chat Completions 聚合与 Responses 流式共用。
func applyToolCallDelta(toolCalls map[int]*mergedToolCall, order *[]int, tcs []any) {
	for _, tcAny := range tcs {
		tc, ok := tcAny.(map[string]any)
		if !ok {
			continue
		}
		idx := 0
		if v, ok := tc["index"].(float64); ok {
			idx = int(v)
		}
		st, exists := toolCalls[idx]
		if !exists {
			st = &mergedToolCall{Type: "function"}
			toolCalls[idx] = st
			*order = append(*order, idx)
		}
		if id, ok := tc["id"].(string); ok && id != "" {
			st.ID = id
		}
		if t, ok := tc["type"].(string); ok && t != "" {
			st.Type = t
		}
		if fn, ok := tc["function"].(map[string]any); ok {
			if n, ok := fn["name"].(string); ok && n != "" {
				st.Name = n
			}
			if a, ok := fn["arguments"].(string); ok && a != "" {
				st.Args.WriteString(a)
			}
		}
	}
}

// logAggregateFailure 记录非流式聚合失败。没有 finish_reason 的干净结束单独说明，
// 避免和网络中断混在同一句「聚合失败」里。
func logAggregateFailure(r *http.Request, reqID uint64, err error) {
	if errors.Is(err, errStreamClosedWithoutFinish) {
		log.Printf("[异常] traceId=%s requestId=%d 发生阶段=聚合上游流 结果=拒绝当成成功 原因=上游连接正常结束但没有 finish_reason 返回状态码=500 业务影响=不把残缺输出伪装成完整回复 是否已处理=是",
			debugTraceID(r), reqID)
		return
	}
	log.Printf("[#%d] 聚合响应失败: %v", reqID, err)
}

// noteFinishReason 记下分片里的真实结束原因。
// 空串必须忽略：上游会在每一个中间分片上都带 finish_reason:""，那不是结束。
func noteFinishReason(current string, chunk map[string]any) string {
	if chunk == nil {
		return current
	}
	choices, _ := chunk["choices"].([]any)
	for _, c := range choices {
		choice, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if v, ok := choice["finish_reason"].(string); ok && v != "" {
			return v
		}
	}
	return current
}

func aggregateCompletion(r io.Reader, model string) ([]byte, error) {
	var content, reasoning, role, respModel, respID, finish string
	var created int64
	var usage map[string]any
	toolCalls := map[int]*mergedToolCall{}
	var toolOrder []int

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
		observeFirstToken(r, chunk)
		if v, ok := chunk["id"].(string); ok && v != "" {
			respID = v
		}
		if v, ok := chunk["model"].(string); ok && v != "" {
			respModel = v
		}
		if v, ok := chunk["created"].(float64); ok {
			created = int64(v)
		}
		if v, ok := chunk["usage"].(map[string]any); ok {
			usage = v
		}
		choices, _ := chunk["choices"].([]any)
		for _, c := range choices {
			choice, _ := c.(map[string]any)
			if delta, ok := choice["delta"].(map[string]any); ok {
				if v, ok := delta["role"].(string); ok && v != "" {
					role = v
				}
				if v, ok := delta["content"].(string); ok {
					content += v
				}
				if v, ok := delta["reasoning_content"].(string); ok {
					reasoning += v
				}
				if tcs, ok := delta["tool_calls"].([]any); ok {
					applyToolCallDelta(toolCalls, &toolOrder, tcs)
				}
			}
			finish = noteFinishReason(finish, map[string]any{"choices": []any{choice}})
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取上游流失败: %w", err)
	}
	if finish == "" {
		return nil, errStreamClosedWithoutFinish
	}
	completeTokenStream(r, true)

	message := map[string]any{"role": ifEmpty(role, "assistant"), "content": content}
	if reasoning != "" {
		message["reasoning_content"] = reasoning
	}
	if len(toolOrder) > 0 {
		calls := make([]map[string]any, 0, len(toolOrder))
		for _, idx := range toolOrder {
			st := toolCalls[idx]
			id := st.ID
			if id == "" {
				id = fmt.Sprintf("call_%d_%d", time.Now().UnixNano(), idx)
			}
			calls = append(calls, map[string]any{
				"id":    id,
				"type":  ifEmpty(st.Type, "function"),
				"index": idx,
				"function": map[string]any{
					"name":      st.Name,
					"arguments": st.Args.String(),
				},
			})
		}
		message["tool_calls"] = calls
	}
	if created == 0 {
		created = time.Now().Unix()
	}
	result := map[string]any{
		"id":      ifEmpty(respID, fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())),
		"object":  "chat.completion",
		"created": created,
		"model":   ifEmpty(respModel, model),
		"choices": []map[string]any{{
			"index":         0,
			"message":       message,
			"finish_reason": finish,
		}},
	}
	if usage != nil {
		result["usage"] = usage
	}
	return json.Marshal(result)
}

// cleanChunkJSON 清洗单个上游 chat 分片，使其符合 OpenAI 流式分片规范后再透传。
//
// 上游是「类 OpenAI」实现，分片里带有若干偏离规范的噪声。普通 OpenAI 客户端能忍，
// 但 Anthropic 协议翻译层（Claude Code 链路）会据此误判流状态，导致工具不执行：
//
//  1. finish_reason:"" → null（本 bug 的直接原因）。规范要求中间分片为 null、只有终止分片
//     给出真实原因；上游却在整条流的每一个分片上都下发 finish_reason:""。
//     翻译层会取流中「第一个非 null 的 finish_reason」作为最终 stop_reason，
//     于是首个分片就把 stop_reason 锁成 end_turn，真实终止分片的 "tool_calls" 不再被采纳。
//     Claude Code 只在 stop_reason=tool_use 时才执行工具，表现为「网关日志成功但客户端没结果」。
//  2. 旧版 function_call 空壳：上游在含工具调用的终止片追加
//     {"function_call":{"name":"","arguments":""}}，属同类非规范噪声，一并清除。
//  3. delta 中的空值字段（content:""、tool_calls:[] 等）。
func cleanChunkJSON(s string) string {
	var obj map[string]any
	if json.Unmarshal([]byte(s), &obj) != nil {
		return s
	}
	if choices, ok := obj["choices"].([]any); ok {
		for _, c := range choices {
			choice, ok := c.(map[string]any)
			if !ok {
				continue
			}
			// 空 finish_reason 归一化为 null：只有终止分片才应携带真实原因。
			if fr, ok := choice["finish_reason"].(string); ok && fr == "" {
				choice["finish_reason"] = nil
			}
			if delta, ok := choice["delta"].(map[string]any); ok {
				for k, v := range delta {
					if k == "function_call" && isEmptyFunctionCall(v) {
						delete(delta, k)
						continue
					}
					if isEmptyValue(v) {
						delete(delta, k)
					}
				}
			}
		}
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return s
	}
	return string(out)
}

// isEmptyFunctionCall 判断是否为旧版 function_call 空壳（name 与 arguments 均为空）。
// 真正的旧版函数调用会带 name 或 arguments，不应误删。
func isEmptyFunctionCall(v any) bool {
	fc, ok := v.(map[string]any)
	if !ok {
		return false
	}
	name, _ := fc["name"].(string)
	args, _ := fc["arguments"].(string)
	return strings.TrimSpace(name) == "" && strings.TrimSpace(args) == ""
}

func isEmptyValue(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case string:
		return x == ""
	case []any:
		return len(x) == 0
	case map[string]any:
		return len(x) == 0
	}
	return false
}

func stripDataPrefix(s string) string {
	s = strings.TrimSpace(s)
	for strings.HasPrefix(s, "data:") {
		s = strings.TrimSpace(strings.TrimPrefix(s, "data:"))
	}
	return s
}

func writeOpenAIError(w http.ResponseWriter, statusCode int, errType, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message": message,
			"type":    errType,
			"code":    statusCode,
		},
	})
}

func ifEmpty(val, fallback string) string {
	if strings.TrimSpace(val) == "" {
		return fallback
	}
	return val
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// accSiteLocked 返回账号所属站点 key；调用方需持有 accountMu。
func accSiteLocked(acc *Account) string {
	if acc == nil {
		return ""
	}
	if acc.Auth != nil {
		return profileForEdition(acc.Auth.Edition).Key
	}
	return profileForEdition(acc.Edition).Key
}
