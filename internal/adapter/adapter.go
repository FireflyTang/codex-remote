package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

type InitializeResult struct {
	UserAgent      string `json:"userAgent"`
	CodexHome      string `json:"codexHome"`
	PlatformFamily string `json:"platformFamily"`
	PlatformOS     string `json:"platformOs"`
}

type Thread struct {
	ID             string          `json:"id"`
	SessionID      string          `json:"sessionId"`
	CWD            string          `json:"cwd"`
	Name           *string         `json:"name"`
	Preview        string          `json:"preview"`
	CreatedAt      int64           `json:"createdAt"`
	UpdatedAt      int64           `json:"updatedAt"`
	RecencyAt      *int64          `json:"recencyAt"`
	ParentThreadID *string         `json:"parentThreadId"`
	Source         json.RawMessage `json:"source"`
	Status         json.RawMessage `json:"status"`
	Turns          []Turn          `json:"turns"`
}
type Turn struct {
	ID           string            `json:"id"`
	Status       string            `json:"status"`
	StartedAt    *int64            `json:"startedAt"`
	CompletedAt  *int64            `json:"completedAt"`
	DurationMS   *int64            `json:"durationMs"`
	Error        *TurnError        `json:"error"`
	Items        []json.RawMessage `json:"items"`
	ItemsView    string            `json:"itemsView"`
	Completeness TurnCompleteness  `json:"-"`
}
type TurnError struct {
	Message           string          `json:"message"`
	AdditionalDetails *string         `json:"additionalDetails"`
	CodexErrorInfo    json.RawMessage `json:"codexErrorInfo"`
}
type TurnCompleteness string

const (
	TurnCompletenessUnknown TurnCompleteness = "unknown"
	TurnCompletenessPartial TurnCompleteness = "partial"
	TurnCompletenessFull    TurnCompleteness = "full"
)

type ThreadPage struct {
	Data       []Thread `json:"data"`
	NextCursor *string  `json:"nextCursor"`
}
type Event struct {
	Kind      EventKind
	Semantic  EventSemantic
	Method    string
	ThreadID  string
	TurnID    string
	ItemID    string
	PendingID string
	Params    json.RawMessage
	Text      string
	Encoding  string
	Stream    string
	Diff      string
	Plan      []PlanStep
}
type EventSemantic string

const (
	SemanticUnknown          EventSemantic = "unknown"
	SemanticAgentText        EventSemantic = "agent_text"
	SemanticReasoningText    EventSemantic = "reasoning_text"
	SemanticReasoningSummary EventSemantic = "reasoning_summary"
	SemanticCommandOutput    EventSemantic = "command_output"
	SemanticFileChangeOutput EventSemantic = "file_change_output"
	SemanticProcessOutput    EventSemantic = "process_output"
	SemanticPlanDelta        EventSemantic = "plan_delta"
	SemanticPlanUpdated      EventSemantic = "plan_updated"
	SemanticDiffUpdated      EventSemantic = "diff_updated"
)

type PlanStep struct {
	Step   string `json:"step"`
	Status string `json:"status"`
}
type EventKind string

const (
	EventVendor                EventKind = "vendor"
	EventCodexUpdated          EventKind = "codex_updated"
	EventTurnUpdated           EventKind = "turn_updated"
	EventItemStarted           EventKind = "item_started"
	EventItemDelta             EventKind = "item_delta"
	EventItemUpdated           EventKind = "item_updated"
	EventItemCompleted         EventKind = "item_completed"
	EventPendingRequestUpdated EventKind = "pending_request_updated"
	EventWarningRaised         EventKind = "warning_raised"
)

type PendingRequest struct {
	ID        string
	Method    string
	RPCID     json.RawMessage
	ThreadID  string
	TurnID    string
	ItemID    string
	Params    json.RawMessage
	Questions []UserInputQuestion
}
type UserInputQuestion struct {
	ID             string            `json:"id"`
	Header         string            `json:"header"`
	Question       string            `json:"question"`
	AllowsMultiple bool              `json:"allowsMultiple"`
	IsOther        bool              `json:"isOther"`
	IsSecret       bool              `json:"isSecret"`
	Options        []UserInputOption `json:"options"`
}
type UserInputOption struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}
type TextInput struct {
	Type string `json:"type"`
	Text string `json:"text"`
}
type TurnOptions struct {
	Model             string
	ApprovalPolicy    string
	CollaborationMode string
	ReasoningEffort   string
}

type Adapter struct {
	client  *Client
	events  chan Event
	mu      sync.RWMutex
	pending map[string]PendingRequest
	active  map[string]string
	idle    chan struct{}
	closed  chan struct{}
}

func Initialize(ctx context.Context, client *Client) (*Adapter, InitializeResult, error) {
	var out InitializeResult
	params := map[string]any{"clientInfo": map[string]any{"name": "codex-remote-host", "title": "Codex Remote Host", "version": "0.1.0"}, "capabilities": map[string]any{"experimentalApi": true}}
	if err := client.Call(ctx, "initialize", params, &out); err != nil {
		return nil, out, err
	}
	if err := client.Notify("initialized", map[string]any{}); err != nil {
		return nil, out, err
	}
	a := &Adapter{client: client, events: make(chan Event, 128), pending: make(map[string]PendingRequest), active: make(map[string]string), idle: make(chan struct{}, 1), closed: make(chan struct{})}
	go a.consume()
	return a, out, nil
}

func (a *Adapter) Events() <-chan Event  { return a.events }
func (a *Adapter) Done() <-chan struct{} { return a.closed }
func (a *Adapter) Idle() <-chan struct{} { return a.idle }
func (a *Adapter) Close() error          { return a.client.Close() }

func (a *Adapter) ListThreads(ctx context.Context, cwd string, cursor string, limit uint32, sourceKinds []string) (ThreadPage, error) {
	p := map[string]any{"limit": limit, "useStateDbOnly": false}
	if cwd != "" {
		p["cwd"] = cwd
	}
	if cursor != "" {
		p["cursor"] = cursor
	}
	if len(sourceKinds) > 0 {
		p["sourceKinds"] = sourceKinds
	}
	var out ThreadPage
	err := a.client.Call(ctx, "thread/list", p, &out)
	normalizeThreads(out.Data)
	return out, err
}
func (a *Adapter) ReadThread(ctx context.Context, id string, includeTurns bool) (Thread, error) {
	var out struct {
		Thread Thread `json:"thread"`
	}
	err := a.client.Call(ctx, "thread/read", map[string]any{"threadId": id, "includeTurns": includeTurns}, &out)
	normalizeThread(&out.Thread)
	return out.Thread, err
}
func (a *Adapter) StartThread(ctx context.Context, cwd string) (Thread, error) {
	var out struct {
		Thread Thread `json:"thread"`
	}
	err := a.client.Call(ctx, "thread/start", map[string]any{"cwd": cwd, "ephemeral": false}, &out)
	normalizeThread(&out.Thread)
	return out.Thread, err
}
func (a *Adapter) ResumeThread(ctx context.Context, id string) (Thread, error) {
	var out struct {
		Thread Thread `json:"thread"`
	}
	err := a.client.Call(ctx, "thread/resume", map[string]any{"threadId": id}, &out)
	normalizeThread(&out.Thread)
	return out.Thread, err
}
func (a *Adapter) StartTurn(ctx context.Context, threadID string, input []TextInput, opt TurnOptions) (Turn, error) {
	p := map[string]any{"threadId": threadID, "input": input}
	if opt.ApprovalPolicy != "" {
		p["approvalPolicy"] = opt.ApprovalPolicy
	}
	if opt.CollaborationMode != "" {
		if opt.Model == "" {
			return Turn{}, errors.New("collaboration mode requires a model for Codex app-server settings")
		}
		settings := map[string]any{"model": opt.Model}
		if opt.ReasoningEffort != "" {
			settings["reasoning_effort"] = opt.ReasoningEffort
		}
		p["collaborationMode"] = map[string]any{"mode": opt.CollaborationMode, "settings": settings}
	} else {
		if opt.Model != "" {
			p["model"] = opt.Model
		}
		if opt.ReasoningEffort != "" {
			p["effort"] = opt.ReasoningEffort
		}
	}
	var out struct {
		Turn Turn `json:"turn"`
	}
	err := a.client.Call(ctx, "turn/start", p, &out)
	normalizeTurn(&out.Turn)
	if err == nil {
		a.markActive(threadID, out.Turn.ID)
	}
	return out.Turn, err
}
func (a *Adapter) InterruptTurn(ctx context.Context, threadID, turnID string) error {
	return a.client.Call(ctx, "turn/interrupt", map[string]any{"threadId": threadID, "turnId": turnID}, &struct{}{})
}

func (a *Adapter) Pending(id string) (PendingRequest, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	p, ok := a.pending[id]
	return p, ok
}
func (a *Adapter) ActiveTurnCount() int { a.mu.RLock(); defer a.mu.RUnlock(); return len(a.active) }
func (a *Adapter) markActive(threadID, turnID string) {
	a.mu.Lock()
	a.active[threadID] = turnID
	a.mu.Unlock()
	select {
	case <-a.idle:
	default:
	}
}
func (a *Adapter) RespondApproval(id, decision string) error {
	if decision != "accept" && decision != "acceptForSession" && decision != "decline" && decision != "cancel" {
		return fmt.Errorf("unsupported approval decision %q", decision)
	}
	p, ok := a.takePending(id)
	if !ok {
		return errors.New("approval request not pending")
	}
	if p.Method == "item/permissions/requestApproval" {
		var params struct {
			Permissions json.RawMessage `json:"permissions"`
		}
		if err := json.Unmarshal(p.Params, &params); err != nil {
			a.restorePending(p)
			return err
		}
		permissions := any(map[string]any{})
		if decision == "accept" || decision == "acceptForSession" {
			permissions = params.Permissions
		}
		scope := "turn"
		if decision == "acceptForSession" {
			scope = "session"
		}
		if err := a.client.Respond(p.RPCID, map[string]any{"permissions": permissions, "scope": scope}); err != nil {
			a.restorePending(p)
			return err
		}
		return nil
	}
	if p.Method != "item/commandExecution/requestApproval" && p.Method != "item/fileChange/requestApproval" {
		a.restorePending(p)
		return fmt.Errorf("pending request %q is not a supported approval", id)
	}
	if err := a.client.Respond(p.RPCID, map[string]any{"decision": decision}); err != nil {
		a.restorePending(p)
		return err
	}
	return nil
}
func (a *Adapter) RespondUserInput(id string, answers map[string][]string) error {
	p, ok := a.takePending(id)
	if !ok {
		return errors.New("user input request not pending")
	}
	if p.Method != "item/tool/requestUserInput" {
		a.restorePending(p)
		return fmt.Errorf("pending request %q is not user input", id)
	}
	encoded := make(map[string]any, len(answers))
	for q, v := range answers {
		encoded[q] = map[string]any{"answers": v}
	}
	if err := a.client.Respond(p.RPCID, map[string]any{"answers": encoded}); err != nil {
		a.restorePending(p)
		return err
	}
	return nil
}

func (a *Adapter) takePending(id string) (PendingRequest, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	p, ok := a.pending[id]
	if ok {
		delete(a.pending, id)
	}
	return p, ok
}
func (a *Adapter) restorePending(p PendingRequest) { a.mu.Lock(); a.pending[p.ID] = p; a.mu.Unlock() }
func (a *Adapter) consume() {
	defer close(a.closed)
	defer close(a.events)
	for msg := range a.client.Incoming() {
		var ids struct {
			ThreadID   string  `json:"threadId"`
			TurnID     string  `json:"turnId"`
			ItemID     string  `json:"itemId"`
			ApprovalID *string `json:"approvalId"`
			Turn       struct {
				ID string `json:"id"`
			} `json:"turn"`
			Item struct {
				ID string `json:"id"`
			} `json:"item"`
		}
		_ = json.Unmarshal(msg.Params, &ids)
		if ids.TurnID == "" {
			ids.TurnID = ids.Turn.ID
		}
		if ids.ItemID == "" {
			ids.ItemID = ids.Item.ID
		}
		e := translateEvent(msg.Method, len(msg.ID) > 0, ids.ThreadID, ids.TurnID, ids.ItemID, msg.Params)
		if len(msg.ID) > 0 {
			id := requestIDKey(msg.ID)
			if ids.ApprovalID != nil && *ids.ApprovalID != "" {
				id = *ids.ApprovalID
			}
			e.PendingID = id
			a.mu.Lock()
			pending := PendingRequest{ID: id, Method: msg.Method, RPCID: append(json.RawMessage(nil), msg.ID...), ThreadID: ids.ThreadID, TurnID: ids.TurnID, ItemID: ids.ItemID, Params: msg.Params}
			if msg.Method == "item/tool/requestUserInput" {
				var body struct {
					Questions []UserInputQuestion `json:"questions"`
				}
				_ = json.Unmarshal(msg.Params, &body)
				pending.Questions = body.Questions
			}
			a.pending[id] = pending
			a.mu.Unlock()
		}
		if msg.Method == "turn/started" {
			a.markActive(ids.ThreadID, ids.TurnID)
		}
		if msg.Method == "turn/completed" {
			a.mu.Lock()
			delete(a.active, ids.ThreadID)
			idle := len(a.active) == 0
			a.mu.Unlock()
			if idle {
				select {
				case a.idle <- struct{}{}:
				default:
				}
			}
		}
		a.events <- e
	}
}

func normalizeThreads(threads []Thread) {
	for i := range threads {
		normalizeThread(&threads[i])
	}
}

func normalizeThread(thread *Thread) {
	for i := range thread.Turns {
		normalizeTurn(&thread.Turns[i])
	}
}

func normalizeTurn(turn *Turn) {
	switch turn.ItemsView {
	case "full":
		turn.Completeness = TurnCompletenessFull
	case "summary":
		turn.Completeness = TurnCompletenessPartial
	default:
		turn.Completeness = TurnCompletenessUnknown
	}
}

func translateEvent(method string, serverRequest bool, threadID, turnID, itemID string, params json.RawMessage) Event {
	e := Event{Kind: eventKind(method, serverRequest), Semantic: eventSemantic(method), Method: method, ThreadID: threadID, TurnID: turnID, ItemID: itemID, Params: params}
	var body struct {
		Delta       string     `json:"delta"`
		DeltaBase64 string     `json:"deltaBase64"`
		Stream      string     `json:"stream"`
		Diff        string     `json:"diff"`
		Plan        []PlanStep `json:"plan"`
	}
	_ = json.Unmarshal(params, &body)
	e.Text, e.Stream, e.Diff, e.Plan = body.Delta, body.Stream, body.Diff, body.Plan
	if body.DeltaBase64 != "" {
		e.Text = body.DeltaBase64
		e.Encoding = "base64"
	}
	return e
}

func eventKind(method string, serverRequest bool) EventKind {
	if serverRequest {
		return EventPendingRequestUpdated
	}
	switch {
	case method == "thread/started" || method == "thread/status/changed" || method == "thread/name/updated" || method == "thread/settings/updated":
		return EventCodexUpdated
	case method == "turn/started" || method == "turn/completed":
		return EventTurnUpdated
	case method == "item/started":
		return EventItemStarted
	case method == "item/completed":
		return EventItemCompleted
	case method == "turn/plan/updated" || method == "turn/diff/updated":
		return EventItemUpdated
	case method == "error" || method == "warning" || method == "deprecationNotice":
		return EventWarningRaised
	case strings.HasSuffix(method, "/delta") || strings.HasSuffix(method, "Delta"):
		return EventItemDelta
	default:
		return EventVendor
	}
}

func eventSemantic(method string) EventSemantic {
	switch method {
	case "item/agentMessage/delta":
		return SemanticAgentText
	case "item/reasoning/textDelta":
		return SemanticReasoningText
	case "item/reasoning/summaryTextDelta":
		return SemanticReasoningSummary
	case "item/commandExecution/outputDelta", "command/exec/outputDelta":
		return SemanticCommandOutput
	case "item/fileChange/outputDelta":
		return SemanticFileChangeOutput
	case "process/outputDelta":
		return SemanticProcessOutput
	case "item/plan/delta":
		return SemanticPlanDelta
	case "turn/plan/updated":
		return SemanticPlanUpdated
	case "turn/diff/updated":
		return SemanticDiffUpdated
	default:
		return SemanticUnknown
	}
}
