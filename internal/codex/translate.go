package codex

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kylin1993/codex-remote/internal/adapter"
	remotev1 "github.com/kylin1993/codex-remote/protocol/gen/go/codex/remote/v1"
)

func normalizeSource(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return "unknown"
	}
	var value string
	if json.Unmarshal(raw, &value) == nil && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	var object map[string]any
	if json.Unmarshal(raw, &object) == nil {
		for _, key := range []string{"kind", "type", "name", "source"} {
			if v, ok := object[key].(string); ok && strings.TrimSpace(v) != "" {
				return strings.TrimSpace(v)
			}
		}
		if compact, err := json.Marshal(object); err == nil {
			return string(compact)
		}
	}
	return strings.TrimSpace(string(raw))
}

func unixMillis(value int64) int64 {
	if value == 0 {
		return 0
	}
	if value > -1_000_000_000_000 && value < 1_000_000_000_000 {
		return value * 1000
	}
	return value
}

func translateItem(raw json.RawMessage, turnID, fallbackID, method string, semantic adapter.EventSemantic, status remotev1.ItemStatus, budget int, provenance remotev1.ProvenanceKind) *remotev1.Item {
	body := rawObject(raw)
	if item, ok := body["item"].(map[string]any); ok {
		body = item
	}
	id := firstString(body, "id", "itemId")
	if id == "" {
		id = fallbackID
	}
	if turn := firstString(body, "turnId"); turn != "" {
		turnID = turn
	}
	if parsed := itemStatus(firstString(body, "status")); parsed != remotev1.ItemStatus_ITEM_STATUS_UNSPECIFIED {
		status = parsed
	}
	typeName := strings.ToLower(firstString(body, "type", "kind"))
	item := &remotev1.Item{ItemId: id, TurnId: turnID, Status: status, Provenance: provenance}

	switch {
	case typeName == "usermessage" || typeName == "user_message" || strings.Contains(strings.ToLower(method), "usermessage"):
		msg := &remotev1.UserMessageItem{}
		for _, text := range inputTexts(body) {
			bounded, complete := boundString(text, budget)
			msg.Input = append(msg.Input, &remotev1.UserInputPart{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: bounded}}})
			item.Completeness = mergeCompleteness(item.Completeness, complete)
		}
		item.Content = &remotev1.Item_UserMessage{UserMessage: msg}
	case semantic == adapter.SemanticReasoningText || semantic == adapter.SemanticReasoningSummary || strings.Contains(typeName, "reasoning"):
		text := firstText(body, "text", "summary", "content")
		bounded, complete := boundString(text, budget)
		item.Content = &remotev1.Item_ReasoningSummary{ReasoningSummary: &remotev1.ReasoningSummaryItem{Text: bounded}}
		item.Completeness = complete
	case typeName == "agentmessage" || typeName == "agent_message" || semantic == adapter.SemanticAgentText || strings.Contains(strings.ToLower(method), "agentmessage"):
		bounded, complete := boundString(firstText(body, "text", "content", "message"), budget)
		item.Content = &remotev1.Item_AgentMessage{AgentMessage: &remotev1.AgentMessageItem{Text: bounded}}
		item.Completeness = complete
	case semantic == adapter.SemanticPlanDelta || semantic == adapter.SemanticPlanUpdated || typeName == "plan" || typeName == "todolist":
		plan := &remotev1.PlanItem{}
		for _, step := range planSteps(body) {
			text, complete := boundString(step.Text, budget)
			plan.Steps = append(plan.Steps, &remotev1.PlanStep{Text: text, Status: step.Status})
			item.Completeness = mergeCompleteness(item.Completeness, complete)
		}
		item.Content = &remotev1.Item_Plan{Plan: plan}
	case typeName == "commandexecution" || typeName == "command" || semantic == adapter.SemanticCommandOutput || semantic == adapter.SemanticProcessOutput || strings.Contains(strings.ToLower(method), "commandexecution"):
		cmd := &remotev1.CommandItem{Argv: stringSlice(body["argv"]), Cwd: firstString(body, "cwd")}
		if len(cmd.Argv) == 0 {
			cmd.Argv = commandArgv(body["command"])
		}
		output := firstText(body, "aggregatedOutput", "output", "stdout", "text")
		cmd.Output, item.Completeness = boundString(output, budget)
		if exit, ok := int32Value(body["exitCode"]); ok {
			cmd.ExitCode, cmd.HasExitCode = exit, true
		}
		item.Content = &remotev1.Item_Command{Command: cmd}
	case typeName == "filechange" || typeName == "file_change" || semantic == adapter.SemanticFileChangeOutput || semantic == adapter.SemanticDiffUpdated || strings.Contains(strings.ToLower(method), "filechange") || strings.Contains(strings.ToLower(method), "diff"):
		change := &remotev1.FileChangeItem{}
		for _, c := range objectSlice(body["changes"]) {
			change.Changes = append(change.Changes, &remotev1.FileChange{Path: firstString(c, "path"), OldPath: firstString(c, "oldPath", "old_path"), NewPath: firstString(c, "newPath", "new_path"), Kind: fileChangeKind(firstString(c, "kind", "type", "status"))})
		}
		change.UnifiedDiff, item.Completeness = boundString(firstText(body, "unifiedDiff", "diff", "output"), budget)
		item.Content = &remotev1.Item_FileChange{FileChange: change}
	default:
		name := firstString(body, "toolName", "name", "tool", "server")
		if name == "" {
			name = typeName
		}
		if name == "" {
			name = method
		}
		summary := firstText(body, "summary", "input", "arguments", "query", "text")
		result := firstText(body, "resultSummary", "result", "output", "content")
		if summary == "" && result == "" {
			summary = string(raw)
		}
		tool := &remotev1.ToolItem{ToolName: name}
		tool.Summary, item.Completeness = boundString(summary, budget)
		var complete *remotev1.Completeness
		tool.ResultSummary, complete = boundString(result, budget)
		item.Completeness = mergeCompleteness(item.Completeness, complete)
		item.Content = &remotev1.Item_Tool{Tool: tool}
	}
	return item
}

type translatedPlanStep struct{ Text, Status string }

func planSteps(body map[string]any) []translatedPlanStep {
	value := body["steps"]
	if value == nil {
		value = body["plan"]
	}
	var out []translatedPlanStep
	for _, v := range anySlice(value) {
		switch step := v.(type) {
		case string:
			out = append(out, translatedPlanStep{Text: step})
		case map[string]any:
			out = append(out, translatedPlanStep{Text: firstText(step, "step", "text", "description"), Status: firstString(step, "status")})
		}
	}
	return out
}

func rawObject(raw []byte) map[string]any {
	var body map[string]any
	if json.Unmarshal(raw, &body) != nil || body == nil {
		return map[string]any{}
	}
	return body
}

func firstString(body map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := body[key].(string); ok {
			return v
		}
	}
	return ""
}

func firstText(body map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := body[key]; ok {
			if text := textValue(v); text != "" {
				return text
			}
		}
	}
	return ""
}

func textValue(value any) string {
	switch value := value.(type) {
	case string:
		return value
	case []any:
		var parts []string
		for _, part := range value {
			if text := textValue(part); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		for _, key := range []string{"text", "content", "message", "summary", "output"} {
			if text := textValue(value[key]); text != "" {
				return text
			}
		}
		if raw, err := json.Marshal(value); err == nil {
			return string(raw)
		}
	}
	return ""
}

func inputTexts(body map[string]any) []string {
	for _, key := range []string{"input", "content"} {
		if raw, ok := body[key]; ok {
			var out []string
			for _, entry := range anySlice(raw) {
				switch value := entry.(type) {
				case string:
					out = append(out, value)
				case map[string]any:
					if text := firstText(value, "text", "content"); text != "" {
						out = append(out, text)
					}
				}
			}
			if len(out) > 0 {
				return out
			}
		}
	}
	if text := firstText(body, "text", "message"); text != "" {
		return []string{text}
	}
	return nil
}

func anySlice(value any) []any {
	if values, ok := value.([]any); ok {
		return values
	}
	return nil
}

func objectSlice(value any) []map[string]any {
	var out []map[string]any
	for _, value := range anySlice(value) {
		if object, ok := value.(map[string]any); ok {
			out = append(out, object)
		}
	}
	return out
}

func stringSlice(value any) []string {
	var out []string
	for _, value := range anySlice(value) {
		if text, ok := value.(string); ok {
			out = append(out, text)
		}
	}
	return out
}

func commandArgv(value any) []string {
	switch command := value.(type) {
	case string:
		return []string{command}
	case []any:
		return stringSlice(command)
	default:
		return nil
	}
}

func int32Value(value any) (int32, bool) {
	if number, ok := value.(float64); ok {
		return int32(number), true
	}
	return 0, false
}

func fileChangeKind(value string) remotev1.FileChangeKind {
	switch strings.ToLower(value) {
	case "add", "added", "create", "created":
		return remotev1.FileChangeKind_FILE_CHANGE_KIND_ADDED
	case "delete", "deleted", "remove", "removed":
		return remotev1.FileChangeKind_FILE_CHANGE_KIND_DELETED
	case "rename", "renamed", "move", "moved":
		return remotev1.FileChangeKind_FILE_CHANGE_KIND_RENAMED
	case "modify", "modified", "update", "updated":
		return remotev1.FileChangeKind_FILE_CHANGE_KIND_MODIFIED
	default:
		return remotev1.FileChangeKind_FILE_CHANGE_KIND_UNSPECIFIED
	}
}

func itemStatus(value string) remotev1.ItemStatus {
	switch strings.ToLower(value) {
	case "inprogress", "in_progress", "running", "started":
		return remotev1.ItemStatus_ITEM_STATUS_RUNNING
	case "completed", "succeeded", "success":
		return remotev1.ItemStatus_ITEM_STATUS_COMPLETED
	case "failed", "error":
		return remotev1.ItemStatus_ITEM_STATUS_FAILED
	case "cancelled", "canceled", "interrupted":
		return remotev1.ItemStatus_ITEM_STATUS_CANCELLED
	default:
		return remotev1.ItemStatus_ITEM_STATUS_UNSPECIFIED
	}
}

func eventText(e adapter.Event) string {
	text := e.Text
	if text == "" {
		text = deltaText(e.Params)
	}
	if e.Encoding == "base64" {
		if decoded, err := base64.StdEncoding.DecodeString(text); err == nil {
			return string(decoded)
		}
	}
	return text
}

func outputStream(value string) remotev1.OutputStream {
	switch strings.ToLower(value) {
	case "stdout":
		return remotev1.OutputStream_OUTPUT_STREAM_STDOUT
	case "stderr":
		return remotev1.OutputStream_OUTPUT_STREAM_STDERR
	default:
		return remotev1.OutputStream_OUTPUT_STREAM_COMBINED
	}
}

func turnEvent(e adapter.Event) (remotev1.TurnStatus, int64, int64, *remotev1.Error) {
	body := rawObject(e.Params)
	if turn, ok := body["turn"].(map[string]any); ok {
		body = turn
	}
	status := turnStatus(firstString(body, "status"))
	if status == remotev1.TurnStatus_TURN_STATUS_RUNNING && strings.Contains(strings.ToLower(e.Method), "completed") {
		status = remotev1.TurnStatus_TURN_STATUS_COMPLETED
	}
	started := unixMillis(int64Number(body["startedAt"]))
	completed := unixMillis(int64Number(body["completedAt"]))
	failure := turnFailure(body["error"])
	if failure != nil && status == remotev1.TurnStatus_TURN_STATUS_COMPLETED {
		status = remotev1.TurnStatus_TURN_STATUS_FAILED
	}
	return status, started, completed, failure
}

func int64Number(value any) int64 {
	if number, ok := value.(float64); ok {
		return int64(number)
	}
	return 0
}

func turnFailure(value any) *remotev1.Error {
	if value == nil {
		return nil
	}
	message := "turn failed"
	metadata := map[string]string{}
	switch failure := value.(type) {
	case string:
		message = failure
	case map[string]any:
		if text := firstString(failure, "message"); text != "" {
			message = text
		}
		if details := firstString(failure, "additionalDetails"); details != "" {
			metadata["additional_details"] = details
		}
		if info, ok := failure["codexErrorInfo"]; ok {
			if raw, err := json.Marshal(info); err == nil {
				metadata["codex_error_info"] = string(raw)
			}
		}
	default:
		message = fmt.Sprint(value)
	}
	return &remotev1.Error{Code: remotev1.ErrorCode_ERROR_CODE_INTERNAL_ERROR, Message: message, Metadata: metadata}
}
