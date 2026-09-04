package codex

import (
	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
)

func (m *Manager) eventPayloadBudget() int {
	return m.collectionBudget()
}

func (m *Manager) collectionBudget() int {
	budget := m.contentBudget() * 2
	return max(budget-max(min(budget/8, 1024), 128), 1)
}

func (m *Manager) boundCanonicalEvent(event *remotev1.Event) *remotev1.Completeness {
	if event == nil {
		return nil
	}
	if protoJSONSize(event) <= m.eventPayloadBudget() {
		return event.Completeness
	}
	original := protoJSONSize(event)
	target := max(m.eventPayloadBudget()-512, m.eventPayloadBudget()/2)
	var completeness *remotev1.Completeness
	switch payload := event.Event.(type) {
	case *remotev1.Event_ItemStarted:
		completeness = m.boundItem(payload.ItemStarted.Item, target)
	case *remotev1.Event_ItemUpdated:
		completeness = m.boundItem(payload.ItemUpdated.Item, target)
	case *remotev1.Event_ItemCompleted:
		completeness = m.boundItem(payload.ItemCompleted.Item, target)
	case *remotev1.Event_ItemDelta:
		if delta := payload.ItemDelta.GetCommandOutput(); delta != nil {
			delta.Text, completeness = boundString(delta.Text, max(target/2, 1))
		} else if text := payload.ItemDelta.GetText(); text != "" {
			bounded, complete := boundString(text, max(target/2, 1))
			payload.ItemDelta.Delta = &remotev1.ItemDelta_Text{Text: bounded}
			completeness = complete
		}
	case *remotev1.Event_PendingRequestUpdated:
		completeness = m.boundPendingRequest(payload.PendingRequestUpdated.Request, target)
	case *remotev1.Event_WarningRaised:
		completeness = m.boundWarning(payload.WarningRaised.Warning, target)
	case *remotev1.Event_TurnUpdated:
		completeness = boundError(payload.TurnUpdated.Failure, target)
	case *remotev1.Event_CodexUpdated:
		completeness = m.boundCodex(payload.CodexUpdated.Codex, target)
	}
	if protoJSONSize(event) > m.eventPayloadBudget() {
		// A second strict pass handles ProtoJSON expansion and envelope fields.
		strict := max(m.eventPayloadBudget()/4, 1)
		switch payload := event.Event.(type) {
		case *remotev1.Event_ItemStarted:
			completeness = mergeCompleteness(completeness, m.boundItem(payload.ItemStarted.Item, strict))
		case *remotev1.Event_ItemUpdated:
			completeness = mergeCompleteness(completeness, m.boundItem(payload.ItemUpdated.Item, strict))
		case *remotev1.Event_ItemCompleted:
			completeness = mergeCompleteness(completeness, m.boundItem(payload.ItemCompleted.Item, strict))
		case *remotev1.Event_PendingRequestUpdated:
			completeness = mergeCompleteness(completeness, m.boundPendingRequest(payload.PendingRequestUpdated.Request, strict))
		case *remotev1.Event_WarningRaised:
			completeness = mergeCompleteness(completeness, m.boundWarning(payload.WarningRaised.Warning, strict))
		}
	}
	completeness = mergeCompleteness(completeness, budgetCompleteness(original, "canonical event exceeds C/S frame budget"))
	event.Completeness = mergeCompleteness(event.Completeness, completeness)
	if protoJSONSize(event) > m.eventPayloadBudget() {
		event.Completeness.Reason = "frame budget"
	}
	return event.Completeness
}

func (m *Manager) boundItem(item *remotev1.Item, budget int) *remotev1.Completeness {
	if item == nil || protoJSONSize(item) <= budget {
		return nil
	}
	original := protoJSONSize(item)
	complete := budgetCompleteness(original, "item fields or collection exceed C/S frame budget")
	item.Completeness = mergeCompleteness(item.Completeness, complete)
	perField := max(budget/8, 1)
	switch content := item.Content.(type) {
	case *remotev1.Item_UserMessage:
		for _, part := range content.UserMessage.Parts {
			if text := part.GetText(); text != nil {
				text.Text, _ = boundString(text.Text, perField)
			}
		}
		content.UserMessage.Parts = boundedProtoPrefix(content.UserMessage.Parts, 1, func(values []*remotev1.UserMessagePart) bool {
			content.UserMessage.Parts = values
			return protoJSONSize(item) <= budget
		})
	case *remotev1.Item_AgentMessage:
		content.AgentMessage.Text, _ = boundString(content.AgentMessage.Text, perField)
	case *remotev1.Item_ReasoningSummary:
		content.ReasoningSummary.Text, _ = boundString(content.ReasoningSummary.Text, perField)
	case *remotev1.Item_Plan:
		for _, step := range content.Plan.Steps {
			step.Text, _ = boundString(step.Text, perField)
			step.Status, _ = boundString(step.Status, max(perField/2, 1))
		}
		content.Plan.Steps = boundedProtoPrefix(content.Plan.Steps, 1, func(values []*remotev1.PlanStep) bool {
			content.Plan.Steps = values
			return protoJSONSize(item) <= budget
		})
	case *remotev1.Item_Command:
		content.Command.Output, _ = boundString(content.Command.Output, perField)
		content.Command.Cwd, _ = boundString(content.Command.Cwd, perField)
		for i := range content.Command.Argv {
			content.Command.Argv[i], _ = boundString(content.Command.Argv[i], perField)
		}
		content.Command.Argv = boundedProtoPrefix(content.Command.Argv, 1, func(values []string) bool {
			content.Command.Argv = values
			return protoJSONSize(item) <= budget
		})
	case *remotev1.Item_Tool:
		content.Tool.ToolName, _ = boundString(content.Tool.ToolName, perField)
		content.Tool.Summary, _ = boundString(content.Tool.Summary, perField)
		content.Tool.ResultSummary, _ = boundString(content.Tool.ResultSummary, perField)
	case *remotev1.Item_FileChange:
		content.FileChange.UnifiedDiff, _ = boundString(content.FileChange.UnifiedDiff, perField)
		for _, change := range content.FileChange.Changes {
			change.Path, _ = boundString(change.Path, perField)
			change.OldPath, _ = boundString(change.OldPath, perField)
			change.NewPath, _ = boundString(change.NewPath, perField)
		}
		content.FileChange.Changes = boundedProtoPrefix(content.FileChange.Changes, 1, func(values []*remotev1.FileChange) bool {
			content.FileChange.Changes = values
			return protoJSONSize(item) <= budget
		})
	}
	return complete
}

// boundPendingRequest never removes a pending request, question, option, or
// identity needed to answer it. It only removes/truncates presentation fields.
func (m *Manager) boundPendingRequest(pending *remotev1.PendingRequest, budget int) *remotev1.Completeness {
	if pending == nil || protoJSONSize(pending) <= budget {
		return nil
	}
	original := protoJSONSize(pending)
	complete := budgetCompleteness(original, "pending request presentation fields exceed C/S frame budget")
	perField := max(budget/16, 1)
	if approval := pending.GetApproval(); approval != nil {
		approval.Title, _ = boundString(approval.Title, perField)
		approval.Explanation, _ = boundString(approval.Explanation, perField)
		for i := range approval.Command {
			approval.Command[i], _ = boundString(approval.Command[i], perField)
		}
		approval.Command = boundedProtoPrefix(approval.Command, 1, func(values []string) bool {
			approval.Command = values
			return protoJSONSize(pending) <= budget
		})
		if protoJSONSize(pending) > budget {
			approval.Title = validUTF8Prefix(approval.Title, min(len(approval.Title), 1))
			approval.Explanation = validUTF8Prefix(approval.Explanation, min(len(approval.Explanation), 1))
			if len(approval.Command) > 0 {
				approval.Command[0] = validUTF8Prefix(approval.Command[0], min(len(approval.Command[0]), 1))
			}
		}
		return complete
	}
	input := pending.GetUserInput()
	if input == nil {
		return complete
	}
	input.Completeness = mergeCompleteness(input.Completeness, complete)
	for _, question := range input.Questions {
		question.Header, _ = boundString(question.Header, perField)
		question.Prompt, _ = boundString(question.Prompt, perField)
		for _, option := range question.Options {
			option.Label, _ = boundString(option.Label, perField)
			option.Description, _ = boundString(option.Description, perField)
		}
	}
	for _, answer := range input.ResolvedAnswers {
		answer.FreeFormText, _ = boundString(answer.FreeFormText, perField)
	}
	if protoJSONSize(pending) > budget {
		for _, question := range input.Questions {
			question.Header = ""
			question.Prompt = ""
			for _, option := range question.Options {
				option.Description = ""
			}
		}
	}
	if input.Completeness != nil && protoJSONSize(pending) > budget {
		input.Completeness.Reason = "frame budget"
	}
	return complete
}

func (m *Manager) boundWarning(warning *remotev1.Warning, budget int) *remotev1.Completeness {
	if warning == nil || protoJSONSize(warning) <= budget {
		return nil
	}
	original := protoJSONSize(warning)
	warning.Message, _ = boundString(warning.Message, max(budget/3, 1))
	for key := range warning.Metadata {
		warning.Metadata[key], _ = boundString(warning.Metadata[key], max(budget/8, 1))
	}
	if protoJSONSize(warning) > budget {
		warning.Metadata = nil
	}
	return budgetCompleteness(original, "warning exceeds C/S frame budget")
}

func boundError(failure *remotev1.Error, budget int) *remotev1.Completeness {
	if failure == nil || protoJSONSize(failure) <= budget {
		return nil
	}
	original := protoJSONSize(failure)
	failure.Message, _ = boundString(failure.Message, max(budget/3, 1))
	for key, value := range failure.Metadata {
		failure.Metadata[key], _ = boundString(value, max(budget/8, 1))
	}
	return budgetCompleteness(original, "turn failure exceeds C/S frame budget")
}

func (m *Manager) boundCodex(codex *remotev1.Codex, budget int) *remotev1.Completeness {
	if codex == nil || protoJSONSize(codex) <= budget {
		return nil
	}
	original := protoJSONSize(codex)
	for _, warning := range codex.Warnings {
		m.boundWarning(warning, max(budget/4, 1))
	}
	if protoJSONSize(codex) > budget {
		codex.Warnings = nil
	}
	codex.Title, _ = boundString(codex.Title, max(budget/8, 1))
	return budgetCompleteness(original, "codex presentation fields exceed C/S frame budget")
}

// boundedProtoPrefix finds the largest fitting prefix with O(log n) size
// probes. Each probe marshals one prefix, avoiding the O(n²) delete-and-marshal
// pattern that previously held the state commit lock for seconds.
func boundedProtoPrefix[T any](values []T, minimum int, fits func([]T) bool) []T {
	if len(values) == 0 || fits(values) {
		return values
	}
	minimum = min(max(minimum, 0), len(values))
	if !fits(values[:minimum]) {
		return values[:minimum]
	}
	low, high := minimum, len(values)
	for low < high {
		mid := low + (high-low+1)/2
		if fits(values[:mid]) {
			low = mid
		} else {
			high = mid - 1
		}
	}
	return values[:low]
}

func addBudgetWarning(view *remotev1.CurrentView, scope string) {
	if view == nil || view.Codex == nil {
		return
	}
	for _, warning := range view.Codex.Warnings {
		if warning.Metadata["codex_remote_budget_scope"] == scope {
			return
		}
	}
	view.Codex.Warnings = append(view.Codex.Warnings, &remotev1.Warning{Code: remotev1.WarningCode_WARNING_CODE_UNSPECIFIED, Message: "Some fields were truncated to fit the C/S frame budget.", Metadata: map[string]string{"codex_remote_budget_scope": scope}})
}
