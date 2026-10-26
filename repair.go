package main

import (
	"log"
	"net/http"
	"strings"
	"sync"
	"unicode/utf8"
)

type toolSequenceRepairReport struct {
	OriginalMessages   int
	FinalMessages      int
	ParallelBatches    int
	MovedMessages      int
	MergedCallMessages int
	DroppedCalls       int
	DroppedOutputs     int
	TopologyBefore     string
	TopologyAfter      string
}

func (r toolSequenceRepairReport) changed() bool {
	return r.OriginalMessages != r.FinalMessages || r.MovedMessages > 0 || r.MergedCallMessages > 0 ||
		r.DroppedCalls > 0 || r.DroppedOutputs > 0
}

// repairToolMessageSequence 修复发往上游的现代 tool_calls/tool 序列。
//
// 规则：
//   - 先按 tool call ID 做对称裁剪：无结果的调用、无调用的结果、重复项全部删除；
//   - 仅当同一批次至少有两个调用时，才把夹在调用和结果之间的普通消息移到结果之后；
//   - Responses API 转换产生的连续/交错单调用 assistant 消息会合并为一个并行批次；
//   - 结果按原始出现顺序保留，不按调用顺序重排。
func repairToolMessageSequence(obj map[string]any) toolSequenceRepairReport {
	messages, ok := obj["messages"].([]any)
	report := toolSequenceRepairReport{OriginalMessages: len(messages), FinalMessages: len(messages)}
	if !ok || len(messages) == 0 {
		return report
	}
	// 拓扑签名必须在就地修改前抓取（过滤阶段会改写 assistant 消息的 tool_calls）。
	topologyBefore := toolMessageTopology(messages)

	// 每个 ID 只保留第一条调用，以及位于该调用之后的第一条结果。
	callMessageIndex := make(map[string]int)
	callEntryIndex := make(map[string]int)
	for messageIndex, messageAny := range messages {
		message, ok := messageAny.(map[string]any)
		if !ok || roleOfMessage(message) != "assistant" {
			continue
		}
		calls, _ := message["tool_calls"].([]any)
		for entryIndex, callAny := range calls {
			id := toolCallID(callAny)
			if id == "" {
				continue
			}
			if _, exists := callMessageIndex[id]; !exists {
				callMessageIndex[id] = messageIndex
				callEntryIndex[id] = entryIndex
			}
		}
	}

	outputMessageIndex := make(map[string]int)
	for messageIndex, messageAny := range messages {
		message, ok := messageAny.(map[string]any)
		if !ok || roleOfMessage(message) != "tool" {
			continue
		}
		id := toolOutputID(message)
		callIndex, exists := callMessageIndex[id]
		if !exists || messageIndex <= callIndex {
			continue
		}
		if _, exists := outputMessageIndex[id]; !exists {
			outputMessageIndex[id] = messageIndex
		}
	}

	pairedIDs := make(map[string]bool, len(outputMessageIndex))
	for id := range outputMessageIndex {
		pairedIDs[id] = true
	}

	filtered := make([]any, 0, len(messages))
	for messageIndex, messageAny := range messages {
		message, ok := messageAny.(map[string]any)
		if !ok {
			filtered = append(filtered, messageAny)
			continue
		}

		switch roleOfMessage(message) {
		case "assistant":
			calls, hasCalls := message["tool_calls"].([]any)
			if !hasCalls || len(calls) == 0 {
				filtered = append(filtered, messageAny)
				continue
			}
			kept := make([]any, 0, len(calls))
			for entryIndex, callAny := range calls {
				id := toolCallID(callAny)
				if id != "" && pairedIDs[id] && callMessageIndex[id] == messageIndex && callEntryIndex[id] == entryIndex {
					kept = append(kept, callAny)
				} else {
					report.DroppedCalls++
				}
			}
			if len(kept) > 0 {
				message["tool_calls"] = kept
				filtered = append(filtered, messageAny)
				continue
			}
			delete(message, "tool_calls")
			if assistantMessageHasPayload(message) {
				filtered = append(filtered, messageAny)
			}

		case "tool":
			id := toolOutputID(message)
			if id != "" && pairedIDs[id] && outputMessageIndex[id] == messageIndex {
				filtered = append(filtered, messageAny)
			} else {
				report.DroppedOutputs++
			}

		default:
			filtered = append(filtered, messageAny)
		}
	}

	repaired := repairParallelToolBatches(filtered, &report)
	obj["messages"] = repaired
	report.FinalMessages = len(repaired)
	if report.changed() {
		report.TopologyBefore = topologyBefore
		report.TopologyAfter = toolMessageTopology(repaired)
	}
	return report
}

// toolMessageTopology 生成有界的消息拓扑签名：仅角色与工具调用 ID，不含任何正文内容。
// 最多记录前 40 条，用于在真实请求上核对工具序列结构而不泄露提示词或工具结果。
func toolMessageTopology(messages []any) string {
	const maxEntries = 200
	parts := make([]string, 0, maxEntries)
	for i, messageAny := range messages {
		if i >= maxEntries {
			parts = append(parts, "...")
			break
		}
		message, ok := messageAny.(map[string]any)
		if !ok {
			parts = append(parts, "?")
			continue
		}
		role := roleOfMessage(message)
		if role == "assistant" {
			if calls, ok := message["tool_calls"].([]any); ok && len(calls) > 0 {
				ids := make([]string, 0, len(calls))
				for _, callAny := range calls {
					ids = append(ids, toolCallID(callAny))
				}
				parts = append(parts, "assistant["+strings.Join(ids, ",")+"]")
				continue
			}
		}
		if role == "tool" {
			parts = append(parts, "tool["+toolOutputID(message)+"]")
			continue
		}
		parts = append(parts, role)
	}
	return strings.Join(parts, " > ")
}

func repairParallelToolBatches(messages []any, report *toolSequenceRepairReport) []any {
	out := make([]any, 0, len(messages))
	for i := 0; i < len(messages); {
		first, firstCalls, ok := assistantCalls(messages[i])
		if !ok {
			out = append(out, messages[i])
			i++
			continue
		}

		batchCalls := append([]any(nil), firstCalls...)
		deferred := make([]any, 0)
		mergedMessages := 0
		j := i + 1
		for j < len(messages) {
			if next, calls, hasCalls := assistantCalls(messages[j]); hasCalls {
				if !assistantCallOnly(next) {
					break
				}
				batchCalls = append(batchCalls, calls...)
				mergedMessages++
				j++
				continue
			}
			if message, ok := messages[j].(map[string]any); ok && roleOfMessage(message) == "tool" {
				break
			}
			deferred = append(deferred, messages[j])
			j++
		}

		if len(batchCalls) < 2 {
			out = append(out, messages[i])
			i++
			continue
		}

		ids := make(map[string]bool, len(batchCalls))
		for _, callAny := range batchCalls {
			if id := toolCallID(callAny); id != "" {
				ids[id] = true
			}
		}
		outputs := make([]any, 0, len(ids))
		found := make(map[string]bool, len(ids))
		k := j
		for k < len(messages) && len(found) < len(ids) {
			if _, _, nextBatch := assistantCalls(messages[k]); nextBatch {
				break
			}
			if message, ok := messages[k].(map[string]any); ok && roleOfMessage(message) == "tool" {
				id := toolOutputID(message)
				if ids[id] && !found[id] {
					outputs = append(outputs, messages[k])
					found[id] = true
					k++
					continue
				}
			}
			deferred = append(deferred, messages[k])
			k++
		}

		if len(found) != len(ids) {
			out = append(out, messages[i])
			i++
			continue
		}

		first["tool_calls"] = batchCalls
		out = append(out, first)
		out = append(out, outputs...)
		out = append(out, deferred...)
		report.ParallelBatches++
		report.MovedMessages += len(deferred)
		report.MergedCallMessages += mergedMessages
		i = k
	}
	return out
}

func assistantCalls(messageAny any) (map[string]any, []any, bool) {
	message, ok := messageAny.(map[string]any)
	if !ok || roleOfMessage(message) != "assistant" {
		return nil, nil, false
	}
	calls, ok := message["tool_calls"].([]any)
	if !ok || len(calls) == 0 {
		return nil, nil, false
	}
	return message, calls, true
}

func assistantCallOnly(message map[string]any) bool {
	if assistantMessageHasPayload(message) {
		return false
	}
	for key := range message {
		switch key {
		case "role", "content", "tool_calls":
		default:
			return false
		}
	}
	return true
}

func assistantMessageHasPayload(message map[string]any) bool {
	content, exists := message["content"]
	if !exists || content == nil {
		return false
	}
	switch value := content.(type) {
	case string:
		return strings.TrimSpace(value) != ""
	case []any:
		return len(value) > 0
	default:
		return true
	}
}

func toolCallID(callAny any) string {
	call, ok := callAny.(map[string]any)
	if !ok {
		return ""
	}
	id, _ := call["id"].(string)
	return strings.TrimSpace(id)
}

func toolOutputID(message map[string]any) string {
	id, _ := message["tool_call_id"].(string)
	return strings.TrimSpace(id)
}

func logToolSequenceRepair(r *http.Request, traceID string, requestID uint64, model string, report toolSequenceRepairReport) {
	result := "unchanged"
	if report.changed() {
		result = "repaired"
		log.Printf("[ToolSequenceRepair] traceId=%s requestId=%d 模型=%s 结果=已修复 原消息=%d 修复后=%d 并行批次=%d 移出插入消息=%d 合并调用消息=%d 删除无结果调用=%d 删除孤儿或重复结果=%d 业务影响=避免上游11148工具调用序列断裂",
			traceID, requestID, model, report.OriginalMessages, report.FinalMessages, report.ParallelBatches,
			report.MovedMessages, report.MergedCallMessages, report.DroppedCalls, report.DroppedOutputs)
	}
	if !debugLoggingEnabled() {
		return
	}
	fields := map[string]any{
		"result":                       result,
		"original_messages":            report.OriginalMessages,
		"final_messages":               report.FinalMessages,
		"parallel_batches":             report.ParallelBatches,
		"moved_messages":               report.MovedMessages,
		"merged_call_messages":         report.MergedCallMessages,
		"dropped_calls_without_output": report.DroppedCalls,
		"dropped_orphan_outputs":       report.DroppedOutputs,
		"business_impact":              "出站工具调用序列已完成校验，防止上游11148错误",
	}
	if report.changed() {
		fields["topology_before"] = report.TopologyBefore
		fields["topology_after"] = report.TopologyAfter
	}
	debugEvent(r, "debug", "tool_sequence_checked", fields)
}

// -----------------------------------------------------------------------------
// 以下内容原位于 reasoningrepair.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

// reasoningRepairReport 只统计字段形状与数量，不保存或记录思维链正文。
type reasoningRepairReport struct {
	Applied                bool
	ThinkingEnabled        bool
	HasTrace               bool
	AssistantMessages      int
	PreservedContent       int
	CopiedFromReasoning    int
	EmptyContentAdded      int
	InvalidContentReplaced int
	ReasoningMirrored      int
	ReasoningPlaceholders  int
}

// repairReasoningHistory 在发往上游的 Chat 消息形态上做一次共享修复。
// 两条入口都必须在 tool_call 序列修复后、json.Marshal 前调用：
// Chat 原生输入与 Responses 转换后的输入因此遵守同一套 DeepSeek thinking 规则。
// 已有推理字符串不覆盖；未收到推理的助手回合只补空 reasoning_content，并在上游
// 需要非空 reasoning 字段的形态下给 reasoning 一个空白占位，而不是捏造思维链。
func repairReasoningHistory(obj map[string]any) reasoningRepairReport {
	var report reasoningRepairReport
	model, _ := obj["model"].(string)
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(model)), "deepseek") {
		return report
	}
	messages, ok := obj["messages"].([]any)
	if !ok || len(messages) == 0 {
		return report
	}

	report.ThinkingEnabled = thinkingExplicitlyEnabled(obj)
	for _, raw := range messages {
		msg, ok := raw.(map[string]any)
		if !ok || msg["role"] != "assistant" {
			continue
		}
		if _, exists := msg["reasoning_content"]; exists {
			report.HasTrace = true
			break
		}
		if reasoning, ok := msg["reasoning"].(string); ok && reasoning != "" {
			report.HasTrace = true
			break
		}
	}
	// 显式关闭且无既有推理痕迹时不碰历史。关闭后续思考不意味着要抹掉此前
	// 已经产生的推理：有痕迹仍须回放，避免多轮上下文不一致。
	if !report.ThinkingEnabled && !report.HasTrace {
		return report
	}
	report.Applied = true
	for _, raw := range messages {
		msg, ok := raw.(map[string]any)
		if !ok || msg["role"] != "assistant" {
			continue
		}
		report.AssistantMessages++
		rc, isString := msg["reasoning_content"].(string)
		if isString {
			report.PreservedContent++
		} else {
			if _, exists := msg["reasoning_content"]; exists {
				report.InvalidContentReplaced++
			}
			if original, ok := msg["reasoning"].(string); ok {
				rc = original
				report.CopiedFromReasoning++
			} else {
				rc = ""
				report.EmptyContentAdded++
			}
			msg["reasoning_content"] = rc
		}
		if existing, ok := msg["reasoning"].(string); ok && existing != "" {
			continue
		}
		if rc != "" {
			msg["reasoning"] = rc
			report.ReasoningMirrored++
		} else {
			// 对齐上游部分租户对 len(reasoning)>0 的校验；单个空格不是推理内容。
			msg["reasoning"] = " "
			report.ReasoningPlaceholders++
		}
	}
	return report
}

// thinkingExplicitlyEnabled 仅识别客户端已明确开启的思考信号；不替用户开启思考、
// 不更改原有 effort 或 thinking.type。显式 disabled 的优先级高于其他字段。
func thinkingExplicitlyEnabled(obj map[string]any) bool {
	if thinking, ok := obj["thinking"].(map[string]any); ok {
		switch strings.ToLower(strings.TrimSpace(stringField(thinking, "type"))) {
		case "disabled":
			return false
		case "enabled":
			return true
		}
	}
	for _, key := range []string{"reasoning_effort", "reasoningEffort", "reasoning_summary"} {
		if enabledEffort(stringField(obj, key)) {
			return true
		}
	}
	if reasoning, ok := obj["reasoning"].(map[string]any); ok {
		return enabledEffort(stringField(reasoning, "effort"))
	}
	return false
}

func stringField(obj map[string]any, key string) string {
	value, _ := obj[key].(string)
	return value
}

func enabledEffort(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "none", "off", "disabled":
		return false
	default:
		return true
	}
}

// logReasoningHistoryRepair 复用网关已有文件日志/TraceID；敏感正文绝不入日志。
func logReasoningHistoryRepair(r *http.Request, reqID uint64, model string, report reasoningRepairReport) {
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(model)), "deepseek") {
		return
	}
	result := "跳过（思考未开启且无历史痕迹）"
	if report.Applied {
		result = "已核对并补全助手历史"
	}
	log.Printf("[推理历史回填] traceId=%s requestId=%d 模型=%s 结果=%s thinking开启=%t 历史有痕迹=%t 助手消息=%d 原推理保留=%d 从reasoning复制=%d 补空reasoning_content=%d 非字符串归一=%d reasoning镜像=%d reasoning空白占位=%d 业务影响=避免DeepSeek长会话11155且不记录推理正文",
		debugTraceID(r), reqID, model, result, report.ThinkingEnabled, report.HasTrace,
		report.AssistantMessages, report.PreservedContent, report.CopiedFromReasoning,
		report.EmptyContentAdded, report.InvalidContentReplaced,
		report.ReasoningMirrored, report.ReasoningPlaceholders)
	debugEvent(r, "debug", "reasoning_history_repaired", map[string]any{
		"result": result, "thinking_enabled": report.ThinkingEnabled, "has_trace": report.HasTrace,
		"assistant_messages": report.AssistantMessages, "preserved_content": report.PreservedContent,
		"copied_from_reasoning": report.CopiedFromReasoning, "empty_content_added": report.EmptyContentAdded,
		"invalid_content_replaced": report.InvalidContentReplaced,
		"reasoning_mirrored":       report.ReasoningMirrored,
		"reasoning_placeholders":   report.ReasoningPlaceholders,
		"business_impact":          "已校验推理历史字段形状；不保存提示词或推理正文",
	})
}

// -----------------------------------------------------------------------------
// 以下内容原位于 systemprompt.go（合并整理，逻辑未改动）
// -----------------------------------------------------------------------------

// promptSettings 是配置文件内的可选提示词。零值完全保留旧行为。
// 日志仅记录来源和长度，绝不记录配置或客户端的提示词正文。
type promptSettings struct {
	fallback string
	force    string
}

var (
	promptSettingsMu sync.RWMutex
	currentPrompts   promptSettings
)

func setSystemPromptConfig(fallback, force string) {
	promptSettingsMu.Lock()
	currentPrompts = promptSettings{fallback: fallback, force: force}
	promptSettingsMu.Unlock()
}

func configuredSystemPrompts() promptSettings {
	promptSettingsMu.RLock()
	settings := currentPrompts
	promptSettingsMu.RUnlock()
	return settings
}

func configuredFallbackSystemPrompt() string {
	fallback := configuredSystemPrompts().fallback
	if strings.TrimSpace(fallback) == "" {
		return defaultSystemPrompt
	}
	return fallback
}

// prepareSystemPromptForUpstream 在 Chat、Responses、Messages 三个入口统一执行。
// 先保证首条为 system，再把实验性的全局强制文本追加到该 system 内容末尾（后置），
// 使其成为整段提示词中位置最靠后的指令、紧贴其后的用户消息；
// 不删除、不覆盖客户端原有 system。content 数组保留原来的内容块顺序，
// 强制文本作为最后一个内容块追加在数组末尾。
func prepareSystemPromptForUpstream(obj map[string]any, r *http.Request, requestID uint64, traceID string) {
	injected := ensureLeadingSystemMessage(obj)
	settings := configuredSystemPrompts()
	force := settings.force
	forced := strings.TrimSpace(force) != ""
	kind := "未修改"
	messages, _ := obj["messages"].([]any)
	if forced && len(messages) > 0 {
		if first, ok := messages[0].(map[string]any); ok {
			old := first["content"]
			switch content := old.(type) {
			case string:
				kind = "字符串"
				if content == "" {
					first["content"] = force
				} else {
					first["content"] = content + "\n\n" + force
				}
			case []any:
				kind = "内容块数组"
				blocks := make([]any, 0, len(content)+1)
				blocks = append(blocks, content...)
				blocks = append(blocks, map[string]any{"type": "text", "text": force})
				first["content"] = blocks
			case nil:
				kind = "空内容"
				first["content"] = force
			default:
				// 非法 system 内容仍由上游参数校验；不静默丢弃客户端数据。
				forced = false
				kind = "未支持的内容类型，已跳过强制后置"
			}
		}
	}
	fallbackSource := "未使用（客户端提供system）"
	if injected {
		fallbackSource = "内置保底"
		if strings.TrimSpace(settings.fallback) != "" {
			fallbackSource = "配置保底"
		}
	}
	if traceID == "" {
		traceID = debugTraceID(r)
	}
	if traceID == "" {
		traceID = r.Header.Get("X-Trace-ID")
	}
	log.Printf("[系统提示词规则] traceId=%s requestId=%d 阶段=上游请求序列化前 保底来源=%s 强制全局已应用=%t 强制位置=后置(紧贴用户消息) 强制内容类型=%s 强制字符数=%d 结果=保留客户端原有system且未记录提示词正文",
		traceID, requestID, fallbackSource, forced, kind, utf8.RuneCountInString(force))
	debugEvent(r, "debug", "system_prompt_policy_applied", map[string]any{
		"fallback_source": fallbackSource, "forced_applied": forced,
		"forced_position": "append_after_client_system", "forced_content_kind": kind,
		"forced_chars":    utf8.RuneCountInString(force),
		"business_impact": "按配置在请求出站前处理system；强制文本后置于system末尾且不记录提示词正文",
	})
}
