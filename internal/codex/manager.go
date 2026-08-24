package codex

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/kylin1993/codex-remote/internal/activity"
	"github.com/kylin1993/codex-remote/internal/adapter"
	"github.com/kylin1993/codex-remote/internal/capability"
	"github.com/kylin1993/codex-remote/internal/directory"
	"github.com/kylin1993/codex-remote/internal/gateway"
	"github.com/kylin1993/codex-remote/internal/persistence"
	"github.com/kylin1993/codex-remote/internal/runtime"
	"github.com/kylin1993/codex-remote/internal/session"
	remotev1 "github.com/kylin1993/codex-remote/protocol/gen/go/codex/remote/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Runtime interface {
	Adapter() (*adapter.Adapter, error)
	Events() <-chan adapter.Event
	State() runtime.State
}

type Manager struct {
	Runtime             Runtime
	Persistence         *persistence.Store
	Events              *activity.Store
	Directories         directory.Service
	Capabilities        *capability.Service
	HostID, HostVersion string
	StartedAt           time.Time
	MaxPage             uint32
	ContentBudget       int
	Degraded            func() (bool, string)
	commitMu            sync.Mutex
	mu                  sync.RWMutex
	byID                map[string]*remotev1.CurrentView
	byThread            map[string]string
	bySession           map[string]string
	sources             map[string]string
	chunks              map[string]uint64
	asyncError          string
	testBeforeCommit    func(string)
}

func NewManager(rt Runtime, p *persistence.Store, events *activity.Store, dirs directory.Service, caps *capability.Service, hostID, version string) *Manager {
	m := &Manager{Runtime: rt, Persistence: p, Events: events, Directories: dirs, Capabilities: caps, HostID: hostID, HostVersion: version, StartedAt: time.Now(), MaxPage: 100, ContentBudget: 256 << 10, byID: make(map[string]*remotev1.CurrentView), byThread: make(map[string]string), bySession: make(map[string]string), sources: make(map[string]string), chunks: make(map[string]uint64)}
	return m
}

// Restore reopens all managed threads and constructs restart RESET views. It
// must be called whenever Runtime becomes Ready, not only at Host startup.
func (m *Manager) Restore(ctx context.Context) error {
	records, err := m.Persistence.ListCodexes(ctx, 100000, 0)
	if err != nil {
		return err
	}
	ad, err := m.Runtime.Adapter()
	if err != nil {
		return err
	}
	for _, r := range records {
		thread, readErr := ad.ReadThread(ctx, r.ThreadID, true)
		if readErr != nil {
			c := codexFromRecord(r)
			c.Status = remotev1.CodexStatus_CODEX_STATUS_UNAVAILABLE
			c.ActiveTurnId = ""
			view := &remotev1.CurrentView{Codex: c, GeneratedAtUnixMs: time.Now().UnixMilli()}
			m.noteUnrecoverablePending(ctx, r.CodexID, view)
			m.mu.Lock()
			m.ensureMapsLocked()
			m.byID[r.CodexID] = view
			m.byThread[r.ThreadID] = r.CodexID
			m.bySession[sessionKey(r.SessionSource, r.ThreadID)] = r.CodexID
			m.sources[r.CodexID] = normalizeSourceString(r.SessionSource)
			m.mu.Unlock()
			if err := m.persistState(ctx, view); err != nil {
				return err
			}
			continue
		}
		if _, err := ad.ResumeThread(ctx, r.ThreadID); err != nil {
			return fmt.Errorf("resume managed thread %s: %w", r.ThreadID, err)
		}
		c := codexFromRecord(r)
		view := &remotev1.CurrentView{Codex: c, GeneratedAtUnixMs: time.Now().UnixMilli()}
		m.noteUnrecoverablePending(ctx, r.CodexID, view)
		if len(thread.Turns) > 0 {
			last := thread.Turns[len(thread.Turns)-1]
			if turnStatus(last.Status) == remotev1.TurnStatus_TURN_STATUS_RUNNING {
				// Active app-server RPC state cannot be hot-restored.
				view.Codex.Status = remotev1.CodexStatus_CODEX_STATUS_UNAVAILABLE
				view.Codex.ActiveTurnId = ""
			} else {
				view.Codex.Status = remotev1.CodexStatus_CODEX_STATUS_IDLE
				view.Codex.ActiveTurnId = ""
			}
		}
		m.mu.Lock()
		m.ensureMapsLocked()
		m.byID[r.CodexID] = view
		m.byThread[r.ThreadID] = r.CodexID
		m.bySession[sessionKey(r.SessionSource, r.ThreadID)] = r.CodexID
		m.sources[r.CodexID] = normalizeSourceString(r.SessionSource)
		m.mu.Unlock()
		if err := m.persistState(ctx, view); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) RunEvents(ctx context.Context) {
	for {
		select {
		case e, ok := <-m.Runtime.Events():
			if !ok {
				return
			}
			if err := m.applyAdapterEvent(ctx, e); err != nil {
				m.mu.Lock()
				m.asyncError = err.Error()
				m.mu.Unlock()
			}
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) GetHost(_ context.Context, _ *remotev1.GetHostRequest) (*remotev1.GetHostResponse, error) {
	st := m.Runtime.State()
	runtimeStatus := remotev1.RuntimeStatus_RUNTIME_STATUS_UNAVAILABLE
	if st.Status == runtime.StatusReady {
		runtimeStatus = remotev1.RuntimeStatus_RUNTIME_STATUS_READY
	} else if st.Status == runtime.StatusStarting {
		runtimeStatus = remotev1.RuntimeStatus_RUNTIME_STATUS_STARTING
	} else if st.Status == runtime.StatusRestarting {
		runtimeStatus = remotev1.RuntimeStatus_RUNTIME_STATUS_RESTARTING
	} else if st.Status == runtime.StatusStopped {
		runtimeStatus = remotev1.RuntimeStatus_RUNTIME_STATUS_STOPPED
	}
	hostStatus := remotev1.HostStatus_HOST_STATUS_READY
	if runtimeStatus != remotev1.RuntimeStatus_RUNTIME_STATUS_READY {
		hostStatus = remotev1.HostStatus_HOST_STATUS_DEGRADED
	}
	if m.Degraded != nil {
		if degraded, _ := m.Degraded(); degraded {
			hostStatus = remotev1.HostStatus_HOST_STATUS_DEGRADED
		}
	}
	m.mu.RLock()
	asyncError := m.asyncError
	m.mu.RUnlock()
	if asyncError != "" {
		hostStatus = remotev1.HostStatus_HOST_STATUS_DEGRADED
	}
	return &remotev1.GetHostResponse{Host: &remotev1.HostInfo{HostId: m.HostID, Name: m.HostID, Status: hostStatus, HostVersion: m.HostVersion, StartedAtUnixMs: m.StartedAt.UnixMilli(), Runtime: &remotev1.RuntimeInfo{Status: runtimeStatus, AppServerVersion: st.AppServer.UserAgent, StartedAtUnixMs: st.StartedAt.UnixMilli(), RestartCount: st.RestartCount}}, Capabilities: m.Capabilities.Get()}, nil
}

func (m *Manager) ListDirectories(_ context.Context, req *remotev1.ListDirectoriesRequest) (*remotev1.ListDirectoriesResponse, error) {
	parent, err := m.Directories.Normalize(req.ParentPath)
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_DIRECTORY_NOT_ACCESSIBLE, err)
	}
	var dirs []*remotev1.DirectoryEntry
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, &remotev1.DirectoryEntry{Name: e.Name(), Path: filepath.Join(parent, e.Name())})
		}
	}
	sort.Slice(dirs, func(i, j int) bool { return dirs[i].Name < dirs[j].Name })
	start, limit, err := page(req.Page, m.MaxPage, "directories", parent)
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, err)
	}
	end := min(start+limit, len(dirs))
	if start > len(dirs) {
		start = len(dirs)
	}
	out := &remotev1.ListDirectoriesResponse{ParentPath: parent, Directories: dirs[start:end], Page: &remotev1.PageInfo{}}
	if end < len(dirs) {
		out.Page.NextPageToken = encodePageToken(pageToken{Operation: "directories", Query: parent, Offset: end})
	}
	return out, nil
}

func (m *Manager) ListSessionCandidates(ctx context.Context, req *remotev1.ListSessionCandidatesRequest) (*remotev1.ListSessionCandidatesResponse, error) {
	ad, err := m.Runtime.Adapter()
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_RUNTIME_UNAVAILABLE, err)
	}
	normalizedCWD, err := m.Directories.Normalize(req.Cwd)
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, err)
	}
	limit := pageSize(req.Page, m.MaxPage)
	cursor, err := cursorPage(req.Page, "sessions", normalizedCWD)
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, err)
	}
	svc := session.Service{Adapter: ad, Directories: m.Directories}
	cwd, p, err := svc.Discover(ctx, normalizedCWD, cursor, uint32(limit))
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_SESSION_NOT_FOUND, err)
	}
	out := &remotev1.ListSessionCandidatesResponse{NormalizedCwd: cwd, Page: &remotev1.PageInfo{}}
	if p.NextCursor != nil {
		out.Page.NextPageToken = encodePageToken(pageToken{Operation: "sessions", Query: cwd, Cursor: *p.NextCursor})
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var observedSources []string
	for _, t := range p.Data {
		source := normalizeSource(t.Source)
		observedSources = append(observedSources, source)
		c := &remotev1.SessionCandidate{SessionId: t.ID, Cwd: t.CWD, Preview: t.Preview, Source: source, CreatedAtUnixMs: unixMillis(t.CreatedAt), UpdatedAtUnixMs: unixMillis(t.UpdatedAt), Availability: remotev1.SessionAvailability_SESSION_AVAILABILITY_RESUMABLE}
		if t.Name != nil {
			c.Title = *t.Name
		}
		if id := m.bySession[sessionKey(source, t.ID)]; id != "" {
			c.ManagedCodexId = id
			c.Availability = remotev1.SessionAvailability_SESSION_AVAILABILITY_ALREADY_MANAGED
		}
		out.Sessions = append(out.Sessions, c)
	}
	if m.Capabilities != nil {
		m.Capabilities.ObserveSessionSources(observedSources...)
	}
	return out, nil
}

func (m *Manager) ListCodexes(ctx context.Context, req *remotev1.ListCodexesRequest) (*remotev1.ListCodexesResponse, error) {
	start, limit, err := page(req.Page, m.MaxPage, "codexes", "all")
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, err)
	}
	records, err := m.Persistence.ListCodexes(ctx, limit+1, start)
	if err != nil {
		return nil, err
	}
	out := &remotev1.ListCodexesResponse{Page: &remotev1.PageInfo{}}
	if len(records) > limit {
		records = records[:limit]
		out.Page.NextPageToken = encodePageToken(pageToken{Operation: "codexes", Query: "all", Offset: start + limit})
	}
	for _, r := range records {
		out.Codexes = append(out.Codexes, codexFromRecord(r))
	}
	return out, nil
}

func (m *Manager) CreateCodex(ctx context.Context, req *remotev1.CreateCodexRequest) (*remotev1.CreateCodexResponse, error) {
	ad, err := m.Runtime.Adapter()
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_RUNTIME_UNAVAILABLE, err)
	}
	t, created, err := (session.Service{Adapter: ad, Directories: m.Directories}).Create(ctx, req.Cwd, req.CreateDirectoryIfMissing)
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_DIRECTORY_CREATE_FAILED, err)
	}
	title := req.Title
	if title == "" && t.Name != nil {
		title = *t.Name
	}
	c := &remotev1.Codex{CodexId: newID("cdx"), ThreadId: t.ID, Cwd: t.CWD, Title: title, Origin: remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED, Status: remotev1.CodexStatus_CODEX_STATUS_IDLE, CreatedAtUnixMs: time.Now().UnixMilli(), LastActivityAtUnixMs: time.Now().UnixMilli()}
	source := normalizeSource(t.Source)
	if err = m.saveCodex(ctx, c, source); err != nil {
		return nil, err
	}
	if err = m.publishCodex(ctx, c, gateway.RequestIDFromContext(ctx)); err != nil {
		return nil, err
	}
	return &remotev1.CreateCodexResponse{Codex: c, DirectoryCreated: created}, nil
}

func (m *Manager) ImportSession(ctx context.Context, req *remotev1.ImportSessionRequest) (*remotev1.ImportSessionResponse, error) {
	source := normalizeSourceString(req.Source)
	if req.SessionId == "" || strings.TrimSpace(req.Source) == "" {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("session_id and source are required"))
	}
	if existing, err := m.Persistence.GetCodexBySession(ctx, source, req.SessionId); err == nil {
		return &remotev1.ImportSessionResponse{Codex: codexFromRecord(existing), AlreadyManaged: true, HistoryComplete: true}, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	ad, err := m.Runtime.Adapter()
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_RUNTIME_UNAVAILABLE, err)
	}
	listed, err := ad.ReadThread(ctx, req.SessionId, true)
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_SESSION_IMPORT_FAILED, err)
	}
	actualSource := normalizeSource(listed.Source)
	if actualSource != source {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_SESSION_IMPORT_FAILED, fmt.Errorf("session source mismatch: requested %q, app-server returned %q", source, actualSource))
	}
	t, err := (session.Service{Adapter: ad, Directories: m.Directories}).Import(ctx, req.SessionId)
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_SESSION_IMPORT_FAILED, err)
	}
	if len(t.Source) == 0 {
		t.Source = listed.Source
	}
	if t.ID == "" {
		t.ID = listed.ID
	}
	if t.CWD == "" {
		t.CWD = listed.CWD
	}
	if t.Name == nil {
		t.Name = listed.Name
	}
	if t.CreatedAt == 0 {
		t.CreatedAt = listed.CreatedAt
	}
	title := ""
	if t.Name != nil {
		title = *t.Name
	}
	now := time.Now().UnixMilli()
	c := &remotev1.Codex{CodexId: newID("cdx"), ThreadId: t.ID, Cwd: t.CWD, Title: title, Origin: remotev1.CodexOrigin_CODEX_ORIGIN_LOCAL_EXISTING, Status: remotev1.CodexStatus_CODEX_STATUS_IDLE, CreatedAtUnixMs: unixMillis(t.CreatedAt), ImportedAtUnixMs: now, LastActivityAtUnixMs: now}
	if err = m.saveCodex(ctx, c, actualSource); err != nil {
		return nil, err
	}
	if err = m.publishCodex(ctx, c, gateway.RequestIDFromContext(ctx)); err != nil {
		return nil, err
	}
	return &remotev1.ImportSessionResponse{Codex: c, HistoryComplete: true}, nil
}

func (m *Manager) ListHistory(ctx context.Context, req *remotev1.ListHistoryRequest) (*remotev1.ListHistoryResponse, error) {
	c, err := m.lookup(req.CodexId)
	if err != nil {
		return nil, err
	}
	start, limit, err := page(req.Page, m.MaxPage, "history", req.CodexId)
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, err)
	}
	ad, err := m.Runtime.Adapter()
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_RUNTIME_UNAVAILABLE, err)
	}
	thread, err := ad.ReadThread(ctx, c.ThreadId, true)
	if err != nil {
		if m.normalizeUnmaterializedHistory(c, err) {
			return &remotev1.ListHistoryResponse{History: &remotev1.HistoryPage{CodexId: req.CodexId, Page: &remotev1.PageInfo{}, HistoryComplete: true}}, nil
		}
		return nil, err
	}
	end := min(start+limit, len(thread.Turns))
	if start > len(thread.Turns) {
		start = len(thread.Turns)
	}
	h := &remotev1.HistoryPage{CodexId: req.CodexId, Page: &remotev1.PageInfo{}, HistoryComplete: true}
	for _, t := range thread.Turns[start:end] {
		snapshot := m.turnSnapshot(t)
		h.Turns = append(h.Turns, snapshot)
		if snapshot.Completeness != nil && (snapshot.Completeness.Truncated || snapshot.Completeness.Incomplete) {
			h.HistoryComplete = false
			h.Completeness = mergeCompleteness(h.Completeness, snapshot.Completeness)
		}
	}
	if end < len(thread.Turns) {
		h.Page.NextPageToken = encodePageToken(pageToken{Operation: "history", Query: req.CodexId, Offset: end})
	}
	requestedCount := len(h.Turns)
	m.boundHistoryPage(h)
	if len(h.Turns) < requestedCount {
		h.Page.NextPageToken = encodePageToken(pageToken{Operation: "history", Query: req.CodexId, Offset: start + len(h.Turns)})
	}
	return &remotev1.ListHistoryResponse{History: h}, nil
}

func (m *Manager) StartTurn(ctx context.Context, req *remotev1.StartTurnRequest) (*remotev1.StartTurnResponse, error) {
	c, err := m.lookup(req.CodexId)
	if err != nil {
		return nil, err
	}
	if c.Status != remotev1.CodexStatus_CODEX_STATUS_IDLE {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_TURN_ALREADY_RUNNING, errors.New("turn already active"))
	}
	ad, err := m.Runtime.Adapter()
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_RUNTIME_UNAVAILABLE, err)
	}
	var input []adapter.TextInput
	for _, p := range req.Input {
		if t := p.GetText(); t != nil {
			input = append(input, adapter.TextInput{Type: "text", Text: t.Text})
		}
	}
	opt := adapter.TurnOptions{}
	if req.Options != nil {
		opt.Model = req.Options.Model
		opt.ApprovalPolicy = req.Options.ApprovalPolicy
		opt.CollaborationMode = req.Options.Mode
		opt.ReasoningEffort = req.Options.ReasoningEffort
	}
	turn, err := ad.StartTurn(ctx, c.ThreadId, input, opt)
	if err != nil {
		return nil, err
	}
	if err := m.setRunning(ctx, req.CodexId, turn.ID, gateway.RequestIDFromContext(ctx)); err != nil {
		return nil, err
	}
	return &remotev1.StartTurnResponse{TurnId: turn.ID}, nil
}

func (m *Manager) InterruptTurn(ctx context.Context, req *remotev1.InterruptTurnRequest) (*remotev1.InterruptTurnResponse, error) {
	c, err := m.lookup(req.CodexId)
	if err != nil {
		return nil, err
	}
	if c.ActiveTurnId == "" || c.ActiveTurnId != req.TurnId {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_TURN_NOT_RUNNING, errors.New("turn is not active"))
	}
	ad, err := m.Runtime.Adapter()
	if err != nil {
		return nil, err
	}
	if err = ad.InterruptTurn(ctx, c.ThreadId, req.TurnId); err != nil {
		return nil, err
	}
	return &remotev1.InterruptTurnResponse{TurnId: req.TurnId}, nil
}

func (m *Manager) RespondApproval(ctx context.Context, req *remotev1.RespondApprovalRequest) (*remotev1.RespondApprovalResponse, error) {
	c, err := m.lookup(req.CodexId)
	if err != nil {
		return nil, err
	}
	ad, err := m.Runtime.Adapter()
	if err != nil {
		return nil, err
	}
	decision := map[remotev1.ApprovalDecision]string{remotev1.ApprovalDecision_APPROVAL_DECISION_ALLOW: "accept", remotev1.ApprovalDecision_APPROVAL_DECISION_ALLOW_FOR_SESSION: "acceptForSession", remotev1.ApprovalDecision_APPROVAL_DECISION_DENY: "decline"}[req.Decision]
	if decision == "" {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("invalid approval decision"))
	}
	p, ok := ad.Pending(req.ApprovalId)
	if !ok {
		if kind, _, lookupErr := m.Persistence.ResolvedPending(ctx, req.CodexId, req.ApprovalId); lookupErr == nil && kind == "approval" {
			return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_APPROVAL_ALREADY_RESOLVED, errors.New("approval already resolved"))
		}
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_APPROVAL_NOT_FOUND, errors.New("approval not pending"))
	}
	if !pendingBelongsToCodex(c, p) {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_APPROVAL_NOT_FOUND, errors.New("approval does not belong to codex"))
	}
	if err = ad.RespondApproval(req.ApprovalId, decision); err != nil {
		return nil, err
	}
	resolved := &remotev1.Approval{ApprovalId: req.ApprovalId, TurnId: p.TurnID, ItemId: p.ItemID, Status: map[bool]remotev1.ApprovalStatus{true: remotev1.ApprovalStatus_APPROVAL_STATUS_ALLOWED, false: remotev1.ApprovalStatus_APPROVAL_STATUS_DENIED}[req.Decision != remotev1.ApprovalDecision_APPROVAL_DECISION_DENY], ResolvedDecision: req.Decision, ResolvedAtUnixMs: time.Now().UnixMilli()}
	if err := m.resolvePending(ctx, req.CodexId, req.ApprovalId, &remotev1.PendingRequest{Request: &remotev1.PendingRequest_Approval{Approval: resolved}}, gateway.RequestIDFromContext(ctx)); err != nil {
		return nil, err
	}
	return &remotev1.RespondApprovalResponse{Approval: resolved}, nil
}

func (m *Manager) RespondUserInput(ctx context.Context, req *remotev1.RespondUserInputRequest) (*remotev1.RespondUserInputResponse, error) {
	c, err := m.lookup(req.CodexId)
	if err != nil {
		return nil, err
	}
	ad, err := m.Runtime.Adapter()
	if err != nil {
		return nil, err
	}
	p, ok := ad.Pending(req.UserInputRequestId)
	if !ok {
		if kind, _, lookupErr := m.Persistence.ResolvedPending(ctx, req.CodexId, req.UserInputRequestId); lookupErr == nil && kind == "user_input" {
			return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_USER_INPUT_ALREADY_RESOLVED, errors.New("user input already resolved"))
		}
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_USER_INPUT_REQUEST_NOT_FOUND, errors.New("user input not pending"))
	}
	if !pendingBelongsToCodex(c, p) {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_USER_INPUT_REQUEST_NOT_FOUND, errors.New("user input does not belong to codex"))
	}
	answers := map[string][]string{}
	for _, a := range req.Answers {
		var question *adapter.UserInputQuestion
		for i := range p.Questions {
			if p.Questions[i].ID == a.QuestionId {
				question = &p.Questions[i]
				break
			}
		}
		if question == nil {
			return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, fmt.Errorf("unknown question_id %q", a.QuestionId))
		}
		for _, selected := range a.SelectedOptionIds {
			matched := false
			for i, opt := range question.Options {
				if canonicalOptionID(i, opt.Label) == selected {
					answers[a.QuestionId] = append(answers[a.QuestionId], opt.Label)
					matched = true
					break
				}
			}
			if !matched {
				return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, fmt.Errorf("unknown option_id %q", selected))
			}
		}
		if a.FreeFormText != "" {
			answers[a.QuestionId] = append(answers[a.QuestionId], a.FreeFormText)
		}
	}
	if err = ad.RespondUserInput(req.UserInputRequestId, answers); err != nil {
		return nil, err
	}
	resolved := resolvedUserInputState(p, req.Answers, time.Now().UnixMilli())
	if err := m.resolvePending(ctx, req.CodexId, req.UserInputRequestId, &remotev1.PendingRequest{Request: &remotev1.PendingRequest_UserInput{UserInput: resolved}}, gateway.RequestIDFromContext(ctx)); err != nil {
		return nil, err
	}
	return &remotev1.RespondUserInputResponse{Request: resolved}, nil
}

func (m *Manager) saveCodex(ctx context.Context, c *remotev1.Codex, source string) error {
	source = normalizeSourceString(source)
	r := recordFromCodex(c, source)
	view := &remotev1.CurrentView{Codex: proto.Clone(c).(*remotev1.Codex), GeneratedAtUnixMs: time.Now().UnixMilli()}
	m.boundCurrentView(view)
	raw, err := protojson.Marshal(view)
	if err != nil {
		return err
	}
	r.CurrentViewJSON = raw
	if err := m.Persistence.UpsertCodex(ctx, r); err != nil {
		return err
	}
	m.mu.Lock()
	m.ensureMapsLocked()
	m.byID[c.CodexId] = view
	m.byThread[c.ThreadId] = c.CodexId
	m.bySession[sessionKey(source, c.ThreadId)] = c.CodexId
	m.sources[c.CodexId] = source
	m.mu.Unlock()
	return nil
}
func (m *Manager) lookup(id string) (*remotev1.Codex, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v := m.byID[id]
	if v == nil || v.Codex == nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND, errors.New("codex not found"))
	}
	return proto.Clone(v.Codex).(*remotev1.Codex), nil
}

const unmaterializedIncludeTurnsSuffix = " is not materialized yet; includeTurns is unavailable before first user message"

func (m *Manager) normalizeUnmaterializedHistory(c *remotev1.Codex, err error) bool {
	if c == nil || c.Origin != remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED {
		return false
	}
	var rpc *adapter.RPCError
	if !errors.As(err, &rpc) || rpc.Code != -32600 || rpc.Message != "thread "+c.ThreadId+unmaterializedIncludeTurnsSuffix {
		return false
	}
	return true
}
func (m *Manager) persistView(ctx context.Context, id string, v *remotev1.CurrentView) error {
	raw, err := protojson.Marshal(v)
	if err != nil {
		return err
	}
	return m.Persistence.SetCurrentView(ctx, id, raw)
}
func (m *Manager) persistState(ctx context.Context, v *remotev1.CurrentView) error {
	if v == nil || v.Codex == nil {
		return errors.New("current view missing codex")
	}
	raw, err := protojson.Marshal(v)
	if err != nil {
		return err
	}
	m.mu.RLock()
	source := m.sources[v.Codex.CodexId]
	m.mu.RUnlock()
	r := recordFromCodex(v.Codex, source)
	r.CurrentViewJSON = raw
	return m.Persistence.UpsertCodex(ctx, r)
}
func (m *Manager) setRunning(ctx context.Context, id, turnID, requestID string) error {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	now := time.Now().UnixMilli()
	m.mu.Lock()
	v := m.byID[id]
	if v == nil {
		m.mu.Unlock()
		return rpcErr(remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND, errors.New("codex not found"))
	}
	snapshot := proto.Clone(v).(*remotev1.CurrentView)
	if snapshot.Codex.Status != remotev1.CodexStatus_CODEX_STATUS_WAITING_FOR_APPROVAL && snapshot.Codex.Status != remotev1.CodexStatus_CODEX_STATUS_WAITING_FOR_USER_INPUT {
		snapshot.Codex.Status = remotev1.CodexStatus_CODEX_STATUS_RUNNING
	}
	snapshot.Codex.ActiveTurnId = turnID
	snapshot.Codex.LastActivityAtUnixMs = now
	if snapshot.ActiveTurn == nil || snapshot.ActiveTurn.TurnId != turnID {
		snapshot.ActiveTurn = &remotev1.TurnSnapshot{TurnId: turnID, Status: remotev1.TurnStatus_TURN_STATUS_RUNNING, StartedAtUnixMs: now}
	} else {
		snapshot.ActiveTurn.Status = remotev1.TurnStatus_TURN_STATUS_RUNNING
		if snapshot.ActiveTurn.StartedAtUnixMs == 0 {
			snapshot.ActiveTurn.StartedAtUnixMs = now
		}
	}
	snapshot.GeneratedAtUnixMs = now
	m.boundCurrentView(snapshot)
	m.mu.Unlock()
	if m.testBeforeCommit != nil {
		m.testBeforeCommit("set_running")
	}
	if err := m.persistState(ctx, snapshot); err != nil {
		return err
	}
	m.mu.Lock()
	m.byID[id] = snapshot
	m.mu.Unlock()
	if _, err := m.Events.Publish(ctx, &remotev1.Event{CodexId: id, OccurredAtUnixMs: now, CausedByRequestId: requestID, Event: &remotev1.Event_TurnUpdated{TurnUpdated: &remotev1.TurnUpdated{TurnId: turnID, Status: remotev1.TurnStatus_TURN_STATUS_RUNNING, StartedAtUnixMs: now}}}, snapshot, nil, ""); err != nil {
		return err
	}
	return m.publishCodex(ctx, snapshot.Codex, requestID)
}

func (m *Manager) publishCodex(ctx context.Context, c *remotev1.Codex, requestID string) error {
	m.mu.RLock()
	view := m.byID[c.CodexId]
	var snapshot *remotev1.CurrentView
	if view != nil {
		snapshot = proto.Clone(view).(*remotev1.CurrentView)
	}
	m.mu.RUnlock()
	if snapshot == nil {
		return errors.New("current view missing while publishing codex")
	}
	event := &remotev1.Event{CodexId: c.CodexId, OccurredAtUnixMs: time.Now().UnixMilli(), CausedByRequestId: requestID, Event: &remotev1.Event_CodexUpdated{CodexUpdated: &remotev1.CodexUpdated{Codex: proto.Clone(c).(*remotev1.Codex)}}}
	m.boundCanonicalEvent(event)
	_, err := m.Events.Publish(ctx, event, snapshot, &remotev1.Provenance{Kind: remotev1.ProvenanceKind_PROVENANCE_KIND_LIVE_WIRE, ObservedAtUnixMs: time.Now().UnixMilli()}, "")
	return err
}

func (m *Manager) applyAdapterEvent(ctx context.Context, e adapter.Event) error {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	e.ItemID = canonicalEventItemID(e)
	var pending *remotev1.PendingRequest
	if e.Kind == adapter.EventPendingRequestUpdated {
		ad, err := m.Runtime.Adapter()
		if err != nil {
			return err
		}
		p, ok := ad.Pending(e.PendingID)
		if !ok {
			return fmt.Errorf("pending request %q disappeared before canonicalization", e.PendingID)
		}
		pending = pendingFromAdapter(p)
	}

	now := time.Now().UnixMilli()
	m.mu.Lock()
	m.ensureMapsLocked()
	id := m.byThread[e.ThreadID]
	old := m.byID[id]
	if id == "" || old == nil {
		m.mu.Unlock()
		return nil
	}
	view := proto.Clone(old).(*remotev1.CurrentView)
	ev := &remotev1.Event{CodexId: id, OccurredAtUnixMs: now}
	switch e.Kind {
	case adapter.EventCodexUpdated:
		applyCodexParams(view.Codex, e.Params)
		view.Codex.LastActivityAtUnixMs = now
		ev.Event = &remotev1.Event_CodexUpdated{CodexUpdated: &remotev1.CodexUpdated{Codex: proto.Clone(view.Codex).(*remotev1.Codex)}}
	case adapter.EventTurnUpdated:
		status, started, completed, failure := turnEvent(e)
		if e.TurnID == "" {
			e.TurnID = firstString(rawObject(e.Params), "turnId", "id")
		}
		ev.Event = &remotev1.Event_TurnUpdated{TurnUpdated: &remotev1.TurnUpdated{TurnId: e.TurnID, Status: status, StartedAtUnixMs: started, CompletedAtUnixMs: completed, Failure: failure}}
		if status == remotev1.TurnStatus_TURN_STATUS_RUNNING {
			ensureActiveTurn(view, e.TurnID)
			if started != 0 {
				view.ActiveTurn.StartedAtUnixMs = started
			}
		} else {
			view.Codex.Status = remotev1.CodexStatus_CODEX_STATUS_IDLE
			if status == remotev1.TurnStatus_TURN_STATUS_FAILED {
				view.Codex.Status = remotev1.CodexStatus_CODEX_STATUS_ERROR
			}
			view.Codex.ActiveTurnId = ""
			view.Codex.LastActivityAtUnixMs = now
			view.ActiveTurn = nil
		}
	case adapter.EventItemStarted, adapter.EventItemUpdated, adapter.EventItemCompleted:
		status := remotev1.ItemStatus_ITEM_STATUS_RUNNING
		if e.Kind == adapter.EventItemCompleted {
			status = remotev1.ItemStatus_ITEM_STATUS_COMPLETED
		}
		item := m.canonicalItem(e, status)
		ensureActiveTurn(view, e.TurnID)
		upsertItem(view.ActiveTurn, item)
		view.ActiveTurn.Completeness = mergeCompleteness(view.ActiveTurn.Completeness, item.Completeness)
		view.Completeness = mergeCompleteness(view.Completeness, item.Completeness)
		switch e.Kind {
		case adapter.EventItemStarted:
			ev.Event = &remotev1.Event_ItemStarted{ItemStarted: &remotev1.ItemStarted{Item: item}}
		case adapter.EventItemUpdated:
			ev.Event = &remotev1.Event_ItemUpdated{ItemUpdated: &remotev1.ItemUpdated{Item: item}}
		default:
			ev.Event = &remotev1.Event_ItemCompleted{ItemCompleted: &remotev1.ItemCompleted{Item: item}}
		}
		ev.Completeness = mergeCompleteness(ev.Completeness, item.Completeness)
	case adapter.EventItemDelta:
		key := id + "\x00" + e.TurnID + "\x00" + e.ItemID
		m.chunks[key]++
		chunk := m.chunks[key]
		ensureActiveTurn(view, e.TurnID)
		text := eventText(e)
		_, deltaCompleteness := boundString(text, m.contentBudget())
		bounded := m.applyItemDelta(view.ActiveTurn, e, text)
		if item := findItem(view.ActiveTurn, e.ItemID); item != nil {
			view.ActiveTurn.Completeness = mergeCompleteness(view.ActiveTurn.Completeness, item.Completeness)
			view.Completeness = mergeCompleteness(view.Completeness, item.Completeness)
		}
		delta := &remotev1.ItemDelta{TurnId: e.TurnID, ItemId: e.ItemID, ChunkSeq: chunk}
		if e.Semantic == adapter.SemanticCommandOutput || e.Semantic == adapter.SemanticProcessOutput {
			delta.Delta = &remotev1.ItemDelta_CommandOutput{CommandOutput: &remotev1.CommandOutputDelta{Stream: outputStream(e.Stream), Text: bounded}}
		} else {
			delta.Delta = &remotev1.ItemDelta_Text{Text: bounded}
		}
		ev.Event = &remotev1.Event_ItemDelta{ItemDelta: delta}
		ev.Completeness = mergeCompleteness(ev.Completeness, deltaCompleteness)
	case adapter.EventPendingRequestUpdated:
		upsertPending(view, pending)
		if pending.GetApproval() != nil {
			view.Codex.Status = remotev1.CodexStatus_CODEX_STATUS_WAITING_FOR_APPROVAL
		} else {
			view.Codex.Status = remotev1.CodexStatus_CODEX_STATUS_WAITING_FOR_USER_INPUT
		}
		ev.Event = &remotev1.Event_PendingRequestUpdated{PendingRequestUpdated: &remotev1.PendingRequestUpdated{Request: pending}}
	case adapter.EventWarningRaised, adapter.EventVendor:
		message := warningMessage(e.Params)
		if e.Kind == adapter.EventVendor {
			message = "unmapped app-server event " + e.Method + ": " + message
		}
		warning := &remotev1.Warning{Code: remotev1.WarningCode_WARNING_CODE_UNSPECIFIED, Message: message, Metadata: map[string]string{"vendor_method": e.Method}}
		view.Codex.Warnings = append(view.Codex.Warnings, proto.Clone(warning).(*remotev1.Warning))
		ev.Event = &remotev1.Event_WarningRaised{WarningRaised: &remotev1.WarningRaised{Warning: warning}}
	default:
		m.mu.Unlock()
		return nil
	}
	view.GeneratedAtUnixMs = now
	if complete := m.boundCanonicalEvent(ev); complete != nil {
		view.Completeness = mergeCompleteness(view.Completeness, complete)
		addBudgetWarning(view, "canonical_event")
	}
	m.boundCurrentView(view)
	m.byID[id] = view
	snapshot := proto.Clone(view).(*remotev1.CurrentView)
	m.mu.Unlock()
	if m.testBeforeCommit != nil {
		m.testBeforeCommit("adapter_event")
	}

	if err := m.persistState(ctx, snapshot); err != nil {
		return err
	}
	if _, err := m.Events.Publish(ctx, ev, snapshot, &remotev1.Provenance{Kind: remotev1.ProvenanceKind_PROVENANCE_KIND_LIVE_WIRE, ObservedAtUnixMs: now}, ""); err != nil {
		return err
	}
	if e.Kind == adapter.EventTurnUpdated {
		return m.publishCodex(ctx, snapshot.Codex, "")
	}
	return nil
}

func (m *Manager) resolvePending(ctx context.Context, codexID, pendingID string, resolved *remotev1.PendingRequest, requestID string) error {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	resolvedCompleteness := m.boundPendingRequest(resolved, m.eventPayloadBudget())
	kind := "approval"
	if resolved.GetUserInput() != nil {
		kind = "user_input"
	}
	raw, err := protojson.Marshal(resolved)
	if err != nil {
		return err
	}
	if err := m.Persistence.SaveResolvedPending(ctx, codexID, pendingID, kind, raw); err != nil {
		return err
	}
	m.mu.Lock()
	view := m.byID[codexID]
	if view != nil {
		out := view.PendingRequests[:0]
		for _, p := range view.PendingRequests {
			if pendingIDOf(p) != pendingID {
				out = append(out, p)
			}
		}
		view.PendingRequests = out
		if len(out) == 0 {
			if view.ActiveTurn != nil {
				view.Codex.Status = remotev1.CodexStatus_CODEX_STATUS_RUNNING
			} else {
				view.Codex.Status = remotev1.CodexStatus_CODEX_STATUS_IDLE
			}
		}
		view.GeneratedAtUnixMs = time.Now().UnixMilli()
		m.boundCurrentView(view)
	}
	var snapshot *remotev1.CurrentView
	if view != nil {
		snapshot = proto.Clone(view).(*remotev1.CurrentView)
	}
	m.mu.Unlock()
	if snapshot == nil {
		return errors.New("current view missing while resolving pending request")
	}
	if err := m.persistState(ctx, snapshot); err != nil {
		return err
	}
	event := &remotev1.Event{CodexId: codexID, OccurredAtUnixMs: time.Now().UnixMilli(), CausedByRequestId: requestID, Completeness: resolvedCompleteness, Event: &remotev1.Event_PendingRequestUpdated{PendingRequestUpdated: &remotev1.PendingRequestUpdated{Request: resolved}}}
	m.boundCanonicalEvent(event)
	_, err = m.Events.Publish(ctx, event, snapshot, &remotev1.Provenance{Kind: remotev1.ProvenanceKind_PROVENANCE_KIND_LIVE_WIRE, ObservedAtUnixMs: time.Now().UnixMilli()}, "")
	return err
}
func upsertPending(view *remotev1.CurrentView, pending *remotev1.PendingRequest) {
	id := pendingIDOf(pending)
	for i, p := range view.PendingRequests {
		if pendingIDOf(p) == id {
			view.PendingRequests[i] = pending
			return
		}
	}
	view.PendingRequests = append(view.PendingRequests, pending)
}
func pendingIDOf(p *remotev1.PendingRequest) string {
	if p == nil {
		return ""
	}
	if a := p.GetApproval(); a != nil {
		return a.ApprovalId
	}
	if u := p.GetUserInput(); u != nil {
		return u.UserInputRequestId
	}
	return ""
}

func pendingBelongsToCodex(c *remotev1.Codex, pending adapter.PendingRequest) bool {
	return c != nil && pending.ThreadID != "" && pending.ThreadID == c.ThreadId
}

func resolvedUserInputState(p adapter.PendingRequest, answers []*remotev1.UserInputAnswer, resolvedAt int64) *remotev1.UserInputRequestState {
	pendingState := pendingFromAdapter(p).GetUserInput()
	resolved := &remotev1.UserInputRequestState{UserInputRequestId: p.ID, TurnId: p.TurnID, ItemId: p.ItemID, Resolved: true, ResolvedAtUnixMs: resolvedAt}
	if pendingState != nil {
		resolved.Questions = pendingState.Questions
		resolved.CreatedAtUnixMs = pendingState.CreatedAtUnixMs
	}
	for _, answer := range answers {
		if answer == nil {
			continue
		}
		resolved.ResolvedAnswers = append(resolved.ResolvedAnswers, proto.Clone(answer).(*remotev1.UserInputAnswer))
	}
	return resolved
}
func canonicalOptionID(index int, label string) string {
	return fmt.Sprintf("option-%d-%s", index+1, base64.RawURLEncoding.EncodeToString([]byte(label)))
}
func applyCodexParams(c *remotev1.Codex, raw []byte) {
	body := rawObject(raw)
	if thread, ok := body["thread"].(map[string]any); ok {
		body = thread
	}
	if title := firstString(body, "name", "title"); title != "" {
		c.Title = title
	}
	status := firstString(body, "status")
	if object, ok := body["status"].(map[string]any); ok {
		status = firstString(object, "type", "status", "kind")
	}
	switch strings.ToLower(status) {
	case "idle", "inactive":
		c.Status = remotev1.CodexStatus_CODEX_STATUS_IDLE
	case "active", "running":
		if c.Status == remotev1.CodexStatus_CODEX_STATUS_IDLE {
			c.Status = remotev1.CodexStatus_CODEX_STATUS_RUNNING
		}
	case "error", "failed":
		c.Status = remotev1.CodexStatus_CODEX_STATUS_ERROR
	}
}
func warningMessage(raw []byte) string {
	var p struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(raw, &p) == nil && p.Message != "" {
		return p.Message
	}
	return string(raw)
}

func pendingFromAdapter(p adapter.PendingRequest) *remotev1.PendingRequest {
	createdAt := pendingCreatedAt(p.Params)
	if strings.Contains(p.Method, "requestUserInput") {
		state := &remotev1.UserInputRequestState{UserInputRequestId: p.ID, TurnId: p.TurnID, ItemId: p.ItemID, CreatedAtUnixMs: createdAt}
		for _, q := range p.Questions {
			qq := &remotev1.UserInputQuestion{QuestionId: q.ID, Header: q.Header, Prompt: q.Question, AllowsMultiple: q.AllowsMultiple, AllowsFreeForm: q.IsOther}
			for i, o := range q.Options {
				qq.Options = append(qq.Options, &remotev1.UserInputOption{OptionId: canonicalOptionID(i, o.Label), Label: o.Label, Description: o.Description})
			}
			state.Questions = append(state.Questions, qq)
		}
		return &remotev1.PendingRequest{Request: &remotev1.PendingRequest_UserInput{UserInput: state}}
	}
	body := rawObject(p.Params)
	a := &remotev1.Approval{ApprovalId: p.ID, TurnId: p.TurnID, ItemId: p.ItemID, Kind: p.Method, Status: remotev1.ApprovalStatus_APPROVAL_STATUS_PENDING, Title: firstString(body, "title"), Explanation: firstText(body, "reason", "explanation"), Command: commandArgv(body["command"]), AllowedDecisions: []remotev1.ApprovalDecision{remotev1.ApprovalDecision_APPROVAL_DECISION_ALLOW, remotev1.ApprovalDecision_APPROVAL_DECISION_ALLOW_FOR_SESSION, remotev1.ApprovalDecision_APPROVAL_DECISION_DENY}, CreatedAtUnixMs: createdAt}
	return &remotev1.PendingRequest{Request: &remotev1.PendingRequest_Approval{Approval: a}}
}

func pendingCreatedAt(raw json.RawMessage) int64 {
	body := rawObject(raw)
	if value := int64Number(body["startedAtMs"]); value != 0 {
		return value
	}
	if value := int64Number(body["createdAtMs"]); value != 0 {
		return value
	}
	if value := int64Number(body["createdAt"]); value != 0 {
		return unixMillis(value)
	}
	return time.Now().UnixMilli()
}
func (m *Manager) canonicalItem(e adapter.Event, status remotev1.ItemStatus) *remotev1.Item {
	e.ItemID = canonicalEventItemID(e)
	return translateItem(e.Params, e.TurnID, e.ItemID, e.Method, e.Semantic, status, m.contentBudget(), remotev1.ProvenanceKind_PROVENANCE_KIND_LIVE_WIRE)
}

func canonicalEventItemID(e adapter.Event) string {
	if e.ItemID != "" {
		return e.ItemID
	}
	turnID := e.TurnID
	if turnID == "" {
		turnID = firstString(rawObject(e.Params), "turnId")
	}
	switch e.Semantic {
	case adapter.SemanticPlanDelta, adapter.SemanticPlanUpdated:
		return turnID + ":plan"
	case adapter.SemanticDiffUpdated, adapter.SemanticFileChangeOutput:
		return turnID + ":diff"
	default:
		return ""
	}
}
func deltaText(raw []byte) string {
	var p struct {
		Delta string `json:"delta"`
	}
	if json.Unmarshal(raw, &p) == nil && p.Delta != "" {
		return p.Delta
	}
	return string(raw)
}
func ensureActiveTurn(v *remotev1.CurrentView, turnID string) {
	if v.ActiveTurn == nil || v.ActiveTurn.TurnId != turnID {
		v.ActiveTurn = &remotev1.TurnSnapshot{TurnId: turnID, Status: remotev1.TurnStatus_TURN_STATUS_RUNNING, StartedAtUnixMs: time.Now().UnixMilli()}
	}
}
func upsertItem(turn *remotev1.TurnSnapshot, item *remotev1.Item) {
	for i, old := range turn.Items {
		if old.ItemId == item.ItemId {
			turn.Items[i] = item
			return
		}
	}
	turn.Items = append(turn.Items, item)
}
func (m *Manager) appendItemText(turn *remotev1.TurnSnapshot, itemID, text string) string {
	return m.applyItemDelta(turn, adapter.Event{ItemID: itemID, TurnID: turn.TurnId, Semantic: adapter.SemanticAgentText}, text)
}

func (m *Manager) applyItemDelta(turn *remotev1.TurnSnapshot, e adapter.Event, text string) string {
	eventText, _ := boundString(text, m.contentBudget())
	for _, item := range turn.Items {
		if item.ItemId == e.ItemID {
			var target *string
			switch {
			case item.GetAgentMessage() != nil:
				target = &item.GetAgentMessage().Text
			case item.GetReasoningSummary() != nil:
				target = &item.GetReasoningSummary().Text
			case item.GetCommand() != nil:
				target = &item.GetCommand().Output
			case item.GetFileChange() != nil:
				target = &item.GetFileChange().UnifiedDiff
			}
			if target == nil {
				return ""
			}
			original := len(*target) + len(text)
			remaining := max(m.contentBudget()-len(*target), 0)
			bounded := text
			if len(bounded) > remaining {
				bounded = validUTF8Prefix(bounded, remaining)
			}
			*target += bounded
			if original > m.contentBudget() {
				item.Completeness = &remotev1.Completeness{Truncated: true, Incomplete: true, OriginalSizeBytes: uint64(original), Reason: "content exceeds C/S frame budget"}
			}
			return eventText
		}
	}
	body := map[string]any{"item": map[string]any{"id": e.ItemID, "type": "agentMessage", "text": text}}
	if e.Semantic == adapter.SemanticReasoningText || e.Semantic == adapter.SemanticReasoningSummary {
		body["item"].(map[string]any)["type"] = "reasoning"
	} else if e.Semantic == adapter.SemanticCommandOutput || e.Semantic == adapter.SemanticProcessOutput {
		body["item"].(map[string]any)["type"] = "commandExecution"
		body["item"].(map[string]any)["output"] = text
	} else if e.Semantic == adapter.SemanticFileChangeOutput {
		body["item"].(map[string]any)["type"] = "fileChange"
		body["item"].(map[string]any)["diff"] = text
	}
	raw, _ := json.Marshal(body)
	turn.Items = append(turn.Items, translateItem(raw, turn.TurnId, e.ItemID, e.Method, e.Semantic, remotev1.ItemStatus_ITEM_STATUS_RUNNING, m.contentBudget(), remotev1.ProvenanceKind_PROVENANCE_KIND_LIVE_WIRE))
	return eventText
}
func (m *Manager) turnSnapshot(t adapter.Turn) *remotev1.TurnSnapshot {
	out := &remotev1.TurnSnapshot{TurnId: t.ID, Status: turnStatus(t.Status), Provenance: remotev1.ProvenanceKind_PROVENANCE_KIND_IMPORTED_HISTORY}
	if t.StartedAt != nil {
		out.StartedAtUnixMs = unixMillis(*t.StartedAt)
	}
	if t.CompletedAt != nil {
		out.CompletedAtUnixMs = unixMillis(*t.CompletedAt)
	}
	if t.Error != nil {
		failure := map[string]any{"message": t.Error.Message, "codexErrorInfo": json.RawMessage(t.Error.CodexErrorInfo)}
		if t.Error.AdditionalDetails != nil {
			failure["additionalDetails"] = *t.Error.AdditionalDetails
		}
		out.Failure = turnFailure(failure)
		if out.Status == remotev1.TurnStatus_TURN_STATUS_COMPLETED {
			out.Status = remotev1.TurnStatus_TURN_STATUS_FAILED
		}
	}
	if t.Completeness != adapter.TurnCompletenessFull {
		reason := "app-server did not provide full turn items"
		if t.ItemsView != "" {
			reason += " (itemsView=" + t.ItemsView + ")"
		}
		out.Completeness = &remotev1.Completeness{Incomplete: true, Reason: reason}
	}
	for i, raw := range t.Items {
		body := rawObject(raw)
		id := firstString(body, "id", "itemId")
		if id == "" {
			id = fmt.Sprintf("%s-%d", t.ID, i)
		}
		item := translateItem(raw, t.ID, id, "imported", adapter.SemanticUnknown, remotev1.ItemStatus_ITEM_STATUS_COMPLETED, m.contentBudget(), remotev1.ProvenanceKind_PROVENANCE_KIND_IMPORTED_HISTORY)
		if item.Completeness != nil {
			out.Completeness = mergeCompleteness(out.Completeness, item.Completeness)
		}
		out.Items = append(out.Items, item)
	}
	m.boundTurnSnapshot(out)
	return out
}
func (m *Manager) contentBudget() int {
	if m.ContentBudget <= 0 {
		return 256 << 10
	}
	return m.ContentBudget
}
func boundString(value string, budget int) (string, *remotev1.Completeness) {
	if budget <= 0 {
		budget = 1
	}
	if len(value) <= budget {
		return value, nil
	}
	return validUTF8Prefix(value, budget), &remotev1.Completeness{Truncated: true, Incomplete: true, OriginalSizeBytes: uint64(len(value)), Reason: "content exceeds C/S frame budget"}
}

func validUTF8Prefix(value string, budget int) string {
	if budget >= len(value) {
		return value
	}
	if budget <= 0 {
		return ""
	}
	end := budget
	for end > 0 && !utf8.ValidString(value[:end]) {
		end--
	}
	return value[:end]
}
func mergeCompleteness(dst, src *remotev1.Completeness) *remotev1.Completeness {
	if src == nil {
		return dst
	}
	if dst == nil {
		return proto.Clone(src).(*remotev1.Completeness)
	}
	dst.Truncated = dst.Truncated || src.Truncated
	dst.Incomplete = dst.Incomplete || src.Incomplete
	if src.OriginalSizeBytes > dst.OriginalSizeBytes {
		dst.OriginalSizeBytes = src.OriginalSizeBytes
	}
	if dst.Reason == "" {
		dst.Reason = src.Reason
	}
	return dst
}
func findItem(turn *remotev1.TurnSnapshot, id string) *remotev1.Item {
	if turn == nil {
		return nil
	}
	for _, item := range turn.Items {
		if item.ItemId == id {
			return item
		}
	}
	return nil
}
func turnStatus(v string) remotev1.TurnStatus {
	switch strings.ToLower(v) {
	case "completed":
		return remotev1.TurnStatus_TURN_STATUS_COMPLETED
	case "failed":
		return remotev1.TurnStatus_TURN_STATUS_FAILED
	case "interrupted", "cancelled":
		return remotev1.TurnStatus_TURN_STATUS_INTERRUPTED
	default:
		return remotev1.TurnStatus_TURN_STATUS_RUNNING
	}
}
func recordFromCodex(c *remotev1.Codex, source string) persistence.CodexRecord {
	return persistence.CodexRecord{CodexID: c.CodexId, ThreadID: c.ThreadId, SessionSource: normalizeSourceString(source), CWD: c.Cwd, Title: c.Title, Origin: c.Origin.String(), Status: c.Status.String(), ActiveTurnID: c.ActiveTurnId, CreatedAtUnixMS: c.CreatedAtUnixMs, ImportedAtUnixMS: c.ImportedAtUnixMs, LastActivityAtUnixMS: c.LastActivityAtUnixMs}
}
func codexFromRecord(r persistence.CodexRecord) *remotev1.Codex {
	origin := remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED
	if strings.Contains(r.Origin, "LOCAL_EXISTING") {
		origin = remotev1.CodexOrigin_CODEX_ORIGIN_LOCAL_EXISTING
	}
	status := remotev1.CodexStatus_CODEX_STATUS_IDLE
	if n, ok := remotev1.CodexStatus_value[r.Status]; ok {
		status = remotev1.CodexStatus(n)
	}
	return &remotev1.Codex{CodexId: r.CodexID, ThreadId: r.ThreadID, Cwd: r.CWD, Title: r.Title, Origin: origin, Status: status, ActiveTurnId: r.ActiveTurnID, CreatedAtUnixMs: r.CreatedAtUnixMS, ImportedAtUnixMs: r.ImportedAtUnixMS, LastActivityAtUnixMs: r.LastActivityAtUnixMS}
}
func rpcErr(code remotev1.ErrorCode, err error) error {
	return &gateway.RPCError{Detail: &remotev1.Error{Code: code, Message: err.Error()}}
}
func pageSize(p *remotev1.PageRequest, max uint32) int {
	n := uint32(50)
	if p != nil && p.PageSize > 0 {
		n = p.PageSize
	}
	if max > 0 && n > max {
		n = max
	}
	return int(n)
}

type pageToken struct {
	Operation string `json:"op"`
	Query     string `json:"query"`
	Offset    int    `json:"offset,omitempty"`
	Cursor    string `json:"cursor,omitempty"`
}

func page(p *remotev1.PageRequest, max uint32, operation, query string) (int, int, error) {
	start := 0
	if p != nil && p.PageToken != "" {
		token, err := decodePageToken(p.PageToken, operation, query)
		if err != nil || token.Offset < 0 || token.Cursor != "" {
			return 0, 0, errors.New("invalid page token")
		}
		start = token.Offset
	}
	return start, pageSize(p, max), nil
}

func cursorPage(p *remotev1.PageRequest, operation, query string) (string, error) {
	if p == nil || p.PageToken == "" {
		return "", nil
	}
	token, err := decodePageToken(p.PageToken, operation, query)
	if err != nil || token.Offset != 0 || token.Cursor == "" {
		return "", errors.New("invalid page token")
	}
	return token.Cursor, nil
}

func encodePageToken(token pageToken) string {
	raw, _ := json.Marshal(token)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodePageToken(value, operation, query string) (pageToken, error) {
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return pageToken{}, errors.New("invalid page token")
	}
	var token pageToken
	if json.Unmarshal(raw, &token) != nil || token.Operation != operation || token.Query != query {
		return pageToken{}, errors.New("page token does not match operation and normalized query")
	}
	return token, nil
}

func normalizeSourceString(source string) string {
	source = strings.TrimSpace(source)
	if source == "" {
		return "unknown"
	}
	return source
}

func sessionKey(source, sessionID string) string {
	return normalizeSourceString(source) + "\x00" + sessionID
}

func (m *Manager) ensureMapsLocked() {
	if m.byID == nil {
		m.byID = make(map[string]*remotev1.CurrentView)
	}
	if m.byThread == nil {
		m.byThread = make(map[string]string)
	}
	if m.bySession == nil {
		m.bySession = make(map[string]string)
	}
	if m.sources == nil {
		m.sources = make(map[string]string)
	}
	if m.chunks == nil {
		m.chunks = make(map[string]uint64)
	}
}

func (m *Manager) noteUnrecoverablePending(ctx context.Context, codexID string, view *remotev1.CurrentView) {
	raw, err := m.Persistence.CurrentView(ctx, codexID)
	if err != nil {
		return
	}
	old := new(remotev1.CurrentView)
	if protojson.Unmarshal(raw, old) != nil || view == nil || view.Codex == nil {
		return
	}
	if old.Codex != nil {
		view.Codex.Warnings = old.Codex.Warnings
	}
	view.Completeness = mergeCompleteness(view.Completeness, old.Completeness)
	if len(old.PendingRequests) == 0 {
		return
	}
	view.Codex.Warnings = append(view.Codex.Warnings, &remotev1.Warning{Code: remotev1.WarningCode_WARNING_CODE_HISTORY_IMPORT_INCOMPLETE, Message: "pending app-server requests from the previous runtime were cleared because their response RPC cannot be restored", Metadata: map[string]string{"cleared_pending_count": fmt.Sprint(len(old.PendingRequests))}})
	view.Completeness = mergeCompleteness(view.Completeness, &remotev1.Completeness{Incomplete: true, Reason: "pending response RPC state is not restartable"})
}

func protoJSONSize(message proto.Message) int {
	raw, err := protojson.Marshal(message)
	if err != nil {
		return 0
	}
	return len(raw)
}

func budgetCompleteness(original int, reason string) *remotev1.Completeness {
	return &remotev1.Completeness{Truncated: true, Incomplete: true, OriginalSizeBytes: uint64(original), Reason: reason}
}

func (m *Manager) boundTurnSnapshot(turn *remotev1.TurnSnapshot) {
	budget := m.collectionBudget()
	if turn == nil || protoJSONSize(turn) <= budget {
		return
	}
	original := protoJSONSize(turn)
	turn.Completeness = mergeCompleteness(turn.Completeness, budgetCompleteness(original, "turn item collection exceeds C/S frame budget"))
	turn.Items = boundedProtoPrefix(turn.Items, 0, func(values []*remotev1.Item) bool {
		turn.Items = values
		return protoJSONSize(turn) <= budget
	})
	if protoJSONSize(turn) > budget && turn.Failure != nil {
		turn.Failure.Message, _ = boundString(turn.Failure.Message, max(budget/4, 1))
		for key, value := range turn.Failure.Metadata {
			turn.Failure.Metadata[key], _ = boundString(value, max(budget/8, 1))
		}
	}
}

func (m *Manager) boundHistoryPage(history *remotev1.HistoryPage) {
	budget := m.collectionBudget()
	if history == nil || protoJSONSize(history) <= budget {
		return
	}
	original := protoJSONSize(history)
	history.HistoryComplete = false
	history.Completeness = mergeCompleteness(history.Completeness, budgetCompleteness(original, "history collection exceeds C/S frame budget"))
	history.Turns = boundedProtoPrefix(history.Turns, 0, func(values []*remotev1.TurnSnapshot) bool {
		history.Turns = values
		return protoJSONSize(history) <= budget
	})
}

func (m *Manager) boundCurrentView(view *remotev1.CurrentView) {
	budget := m.collectionBudget()
	if view == nil || protoJSONSize(view) <= budget {
		return
	}
	original := protoJSONSize(view)
	view.Completeness = mergeCompleteness(view.Completeness, budgetCompleteness(original, "current view collection exceeds C/S frame budget"))
	addBudgetWarning(view, "current_view")
	if view.ActiveTurn != nil {
		itemBudget := max(budget/max(len(view.ActiveTurn.Items)+len(view.PendingRequests), 1)-256, 1)
		for _, item := range view.ActiveTurn.Items {
			if complete := m.boundItem(item, itemBudget); complete != nil {
				view.ActiveTurn.Completeness = mergeCompleteness(view.ActiveTurn.Completeness, complete)
			}
		}
	}
	pendingBudget := max(budget/max(len(view.PendingRequests), 1)-256, 1)
	for _, pending := range view.PendingRequests {
		view.Completeness = mergeCompleteness(view.Completeness, m.boundPendingRequest(pending, pendingBudget))
	}
	if view.Codex != nil && protoJSONSize(view) > budget {
		removeNonBudgetWarnings(view.Codex)
	}
	if view.Codex != nil && protoJSONSize(view) > budget {
		view.Codex.Title, _ = boundString(view.Codex.Title, max(budget/8, 1))
	}
	if protoJSONSize(view) > budget {
		for _, pending := range view.PendingRequests {
			view.Completeness = mergeCompleteness(view.Completeness, m.boundPendingRequest(pending, max(pendingBudget/2, 1)))
			minimizePendingPresentation(pending)
		}
		if view.ActiveTurn != nil {
			for _, item := range view.ActiveTurn.Items {
				minimizeItemPresentation(item)
			}
			view.ActiveTurn.Completeness = mergeCompleteness(view.ActiveTurn.Completeness, budgetCompleteness(original, "frame budget"))
		}
	}
	if view.Codex != nil && protoJSONSize(view) > budget {
		// Completeness still carries the diagnostic if the warning itself is the
		// last avoidable presentation field preventing a bounded RESET.
		removeBudgetWarning(view.Codex)
	}
	if view.Completeness != nil && protoJSONSize(view) > budget {
		view.Completeness.Reason = "frame budget"
	}
}

func minimizePendingPresentation(pending *remotev1.PendingRequest) {
	if approval := pending.GetApproval(); approval != nil {
		approval.Title = validUTF8Prefix(approval.Title, min(len(approval.Title), 1))
		approval.Explanation = validUTF8Prefix(approval.Explanation, min(len(approval.Explanation), 1))
		if len(approval.Command) > 0 {
			approval.Command = approval.Command[:1]
			approval.Command[0] = validUTF8Prefix(approval.Command[0], min(len(approval.Command[0]), 1))
		}
		return
	}
	if input := pending.GetUserInput(); input != nil {
		for _, question := range input.Questions {
			question.Header = ""
			question.Prompt = ""
			for _, option := range question.Options {
				option.Description = ""
			}
		}
	}
}

func minimizeItemPresentation(item *remotev1.Item) {
	if item == nil {
		return
	}
	item.Completeness = mergeCompleteness(item.Completeness, &remotev1.Completeness{Truncated: true, Incomplete: true, Reason: "frame budget"})
	switch content := item.Content.(type) {
	case *remotev1.Item_UserMessage:
		if len(content.UserMessage.Input) > 1 {
			content.UserMessage.Input = content.UserMessage.Input[:1]
		}
		if len(content.UserMessage.Input) > 0 && content.UserMessage.Input[0].GetText() != nil {
			content.UserMessage.Input[0].GetText().Text = ""
		}
	case *remotev1.Item_AgentMessage:
		content.AgentMessage.Text = ""
	case *remotev1.Item_ReasoningSummary:
		content.ReasoningSummary.Text = ""
	case *remotev1.Item_Plan:
		if len(content.Plan.Steps) > 1 {
			content.Plan.Steps = content.Plan.Steps[:1]
		}
		if len(content.Plan.Steps) > 0 {
			content.Plan.Steps[0].Text = ""
		}
	case *remotev1.Item_Command:
		content.Command.Output = ""
		content.Command.Cwd = ""
		if len(content.Command.Argv) > 1 {
			content.Command.Argv = content.Command.Argv[:1]
		}
		if len(content.Command.Argv) > 0 {
			content.Command.Argv[0] = ""
		}
	case *remotev1.Item_Tool:
		content.Tool.Summary = ""
		content.Tool.ResultSummary = ""
	case *remotev1.Item_FileChange:
		content.FileChange.UnifiedDiff = ""
		if len(content.FileChange.Changes) > 1 {
			content.FileChange.Changes = content.FileChange.Changes[:1]
		}
		if len(content.FileChange.Changes) > 0 {
			content.FileChange.Changes[0].Path = ""
			content.FileChange.Changes[0].OldPath = ""
			content.FileChange.Changes[0].NewPath = ""
		}
	}
}

func removeNonBudgetWarnings(codex *remotev1.Codex) {
	out := codex.Warnings[:0]
	for _, warning := range codex.Warnings {
		if warning.Metadata["codex_remote_budget_scope"] != "" {
			out = append(out, warning)
		}
	}
	codex.Warnings = out
}

func removeBudgetWarning(codex *remotev1.Codex) {
	for i := len(codex.Warnings) - 1; i >= 0; i-- {
		if codex.Warnings[i].Metadata["codex_remote_budget_scope"] != "" {
			codex.Warnings = append(codex.Warnings[:i], codex.Warnings[i+1:]...)
			return
		}
	}
}
func newID(prefix string) string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}
