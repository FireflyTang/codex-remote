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

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"github.com/kylin1993/codex-remote/internal/activity"
	"github.com/kylin1993/codex-remote/internal/adapter"
	"github.com/kylin1993/codex-remote/internal/attachment"
	"github.com/kylin1993/codex-remote/internal/capability"
	"github.com/kylin1993/codex-remote/internal/directory"
	"github.com/kylin1993/codex-remote/internal/gateway"
	"github.com/kylin1993/codex-remote/internal/persistence"
	"github.com/kylin1993/codex-remote/internal/runtime"
	"github.com/kylin1993/codex-remote/internal/session"
	workspacecore "github.com/kylin1993/codex-remote/internal/workspace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Runtime interface {
	Adapter() (*adapter.Adapter, error)
	Events() <-chan adapter.Event
	State() runtime.State
}

type eventPublisher func(context.Context, *remotev1.Event, *remotev1.CurrentView, *remotev1.Provenance, string) (*remotev1.Event, error)

type AttachmentResolver interface {
	Upload(context.Context, string, string, string, string, []byte) (attachment.Descriptor, error)
	Download(context.Context, string, string) (attachment.Descriptor, []byte, error)
	Resolve(context.Context, string, string) (attachment.Descriptor, string, error)
	DescribePath(context.Context, string, string) (attachment.Descriptor, error)
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
	Clock               func() time.Time
	LeaseDuration       time.Duration
	LeaseWarningBefore  time.Duration
	LeaseSweepInterval  time.Duration
	Workspaces          *workspacecore.Service
	Attachments         AttachmentResolver
	commitMu            sync.Mutex
	mu                  sync.RWMutex
	byID                map[string]*remotev1.CurrentView
	byThread            map[string]string
	bySession           map[string]string
	sources             map[string]string
	logicalOwners       map[string]string
	manualTitle         map[string]bool
	warningDeadline     map[string]int64
	chunks              map[string]uint64
	workspaceChildCodex map[string]string
	workspaceChildState map[string]workspaceChildObservation
	asyncError          string
	testBeforeCommit    func(string)
	testPublishEvent    eventPublisher
}

func NewManager(rt Runtime, p *persistence.Store, events *activity.Store, dirs directory.Service, caps *capability.Service, hostID, version string) *Manager {
	m := &Manager{Runtime: rt, Persistence: p, Events: events, Directories: dirs, Capabilities: caps, HostID: hostID, HostVersion: version, StartedAt: time.Now(), MaxPage: 100, ContentBudget: 256 << 10, byID: make(map[string]*remotev1.CurrentView), byThread: make(map[string]string), bySession: make(map[string]string), sources: make(map[string]string), logicalOwners: make(map[string]string), manualTitle: make(map[string]bool), warningDeadline: make(map[string]int64), chunks: make(map[string]uint64)}
	ws, _ := workspacecore.New(workspacecore.Config{})
	m.SetWorkspaceService(ws)
	return m
}

func (m *Manager) now() time.Time {
	if m.Clock != nil {
		return m.Clock()
	}
	return time.Now()
}

func (m *Manager) leaseDuration() time.Duration {
	if m.LeaseDuration > 0 {
		return m.LeaseDuration
	}
	return 2 * time.Hour
}

func (m *Manager) leaseWarningBefore() time.Duration {
	if m.LeaseWarningBefore > 0 {
		return m.LeaseWarningBefore
	}
	return 30 * time.Minute
}

func (m *Manager) leaseSweepInterval() time.Duration {
	if m.LeaseSweepInterval > 0 {
		return m.LeaseSweepInterval
	}
	return time.Minute
}

// Restore reopens all managed threads and constructs restart RESET views. It
// must be called whenever Runtime becomes Ready, not only at Host startup.
func (m *Manager) Restore(ctx context.Context) error {
	m.commitMu.Lock()
	err := m.restoreLocked(ctx)
	m.commitMu.Unlock()
	if err != nil {
		return err
	}
	// sweepLease takes commitMu per Codex, so it must run after the restore
	// transaction releases the global state-commit boundary.
	return m.sweepLeases(ctx)
}

func (m *Manager) restoreLocked(ctx context.Context) error {
	records, err := m.Persistence.ListCodexes(ctx, 100000, 0)
	if err != nil {
		return err
	}
	if m.testBeforeCommit != nil {
		m.testBeforeCommit("restore")
	}
	ad, err := m.Runtime.Adapter()
	if err != nil {
		return err
	}
	for _, r := range records {
		if r.LogicalOwnerID == "" {
			r.LogicalOwnerID, err = m.Persistence.EnsureLogicalOwner(ctx, r.SessionSource, r.ThreadID)
			if err != nil {
				return err
			}
		} else if err = m.Persistence.BindLogicalOwner(ctx, r.SessionSource, r.ThreadID, r.LogicalOwnerID); err != nil {
			return err
		}
		c := codexFromRecord(r)
		if c.ManagementState == remotev1.ManagementState_MANAGEMENT_STATE_UNSPECIFIED {
			c.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_MANAGED
			c.ManagedUntilUnixMs = m.now().Add(m.leaseDuration()).UnixMilli()
			r.ManagementState = c.ManagementState.String()
			r.ManagedUntilUnixMS = c.ManagedUntilUnixMs
			r.WarningDeadlineUnixMS = 0
		}
		if c.ManagementState == remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
			view := &remotev1.CurrentView{Codex: c, GeneratedAtUnixMs: m.now().UnixMilli()}
			if len(r.CurrentViewJSON) > 0 {
				persisted := new(remotev1.CurrentView)
				if unmarshalCurrentView(r.CurrentViewJSON, persisted) == nil {
					view = persisted
					if view.Codex == nil {
						view.Codex = c
					} else {
						view.Codex.ManagementState = c.ManagementState
						view.Codex.ManagedUntilUnixMs = c.ManagedUntilUnixMs
					}
				}
			}
			if err := m.ensureWorkspace(r.CodexID, c.Cwd, view); err != nil {
				return err
			}
			m.registerRestored(r, view)
			if err := m.persistState(ctx, view); err != nil {
				return err
			}
			continue
		}
		thread, readErr := ad.ReadThread(ctx, r.ThreadID, true)
		if readErr != nil {
			if m.canRestoreExistingUnmaterializedRemoteCodex(r, c, readErr) {
				if err := m.restoreExistingUnmaterializedRemoteCodex(ctx, r, c); err != nil {
					return err
				}
				continue
			}
			if m.canReplaceUnmaterializedRemoteCodex(r, c, readErr) {
				if err := m.replaceUnmaterializedRemoteCodex(ctx, ad, &r, c); err != nil {
					return err
				}
				continue
			}
			persistedAgents := uint32(0)
			if len(r.CurrentViewJSON) > 0 {
				persisted := new(remotev1.CurrentView)
				if unmarshalCurrentView(r.CurrentViewJSON, persisted) == nil && persisted.WorkspaceAccessState != nil {
					persistedAgents = persisted.WorkspaceAccessState.ActiveAgentCount
				}
			}
			c.Status = remotev1.CodexStatus_CODEX_STATUS_UNAVAILABLE
			c.ActiveTurnId = ""
			view := &remotev1.CurrentView{Codex: c, GeneratedAtUnixMs: m.now().UnixMilli()}
			m.noteUnrecoverablePending(ctx, r.CodexID, view)
			if err := m.ensureWorkspace(r.CodexID, c.Cwd, view); err != nil {
				return err
			}
			if r.ActiveTurnID != "" || persistedAgents > 0 {
				state, err := m.Workspaces.RestoreAgent(r.CodexID, r.ActiveTurnID)
				if err != nil {
					return err
				}
				view.WorkspaceAccessState = state
			}
			m.registerRestored(r, view)
			if err := m.persistState(ctx, view); err != nil {
				return err
			}
			continue
		}
		if _, err := ad.ResumeThread(ctx, r.ThreadID); err != nil {
			return fmt.Errorf("resume managed thread %s: %w", r.ThreadID, err)
		}
		if !r.ManualTitleOverride {
			reconcileRestoredThreadTitle(c, thread)
		}
		// Active RPC correlation cannot survive a Host restart. Start from a
		// conservative idle snapshot and only mark an upstream running turn as
		// unavailable below.
		c.Status = remotev1.CodexStatus_CODEX_STATUS_IDLE
		c.ActiveTurnId = ""
		view := &remotev1.CurrentView{Codex: c, GeneratedAtUnixMs: m.now().UnixMilli()}
		m.noteUnrecoverablePending(ctx, r.CodexID, view)
		if err := m.ensureWorkspace(r.CodexID, c.Cwd, view); err != nil {
			return err
		}
		if state, err := m.restoreCollabAgents(r.CodexID, thread); err != nil {
			return err
		} else if state != nil {
			view.WorkspaceAccessState = state
		}
		if len(thread.Turns) > 0 {
			last := thread.Turns[len(thread.Turns)-1]
			if turnStatus(last.Status) == remotev1.TurnStatus_TURN_STATUS_RUNNING {
				// Active app-server RPC state cannot be hot-restored.
				view.Codex.Status = remotev1.CodexStatus_CODEX_STATUS_UNAVAILABLE
				view.Codex.ActiveTurnId = ""
				state, err := m.Workspaces.RestoreAgent(r.CodexID, last.ID)
				if err != nil {
					return err
				}
				view.WorkspaceAccessState = state
			}
		}
		m.registerRestored(r, view)
		if err := m.persistState(ctx, view); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) canReplaceUnmaterializedRemoteCodex(r persistence.CodexRecord, c *remotev1.Codex, readErr error) bool {
	return forgottenSessionNotFound(readErr) && m.safeUnmaterializedRemoteCodex(r, c)
}

func (m *Manager) canRestoreExistingUnmaterializedRemoteCodex(r persistence.CodexRecord, c *remotev1.Codex, readErr error) bool {
	return m.normalizeUnmaterializedHistory(c, readErr) && m.safeUnmaterializedRemoteCodex(r, c)
}

func (m *Manager) safeUnmaterializedRemoteCodex(r persistence.CodexRecord, c *remotev1.Codex) bool {
	if c == nil || c.Origin != remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED || r.ActiveTurnID != "" {
		return false
	}
	if len(r.CurrentViewJSON) == 0 {
		return true
	}
	view := new(remotev1.CurrentView)
	if unmarshalCurrentView(r.CurrentViewJSON, view) != nil {
		return false
	}
	return view.ActiveTurn == nil && len(view.PendingRequests) == 0 && (view.Codex == nil || view.Codex.ActiveTurnId == "")
}

func (m *Manager) restoreExistingUnmaterializedRemoteCodex(ctx context.Context, r persistence.CodexRecord, c *remotev1.Codex) error {
	c.Status = remotev1.CodexStatus_CODEX_STATUS_IDLE
	c.ActiveTurnId = ""
	r.Status = c.Status.String()
	r.ActiveTurnID = ""
	view := &remotev1.CurrentView{Codex: c, GeneratedAtUnixMs: m.now().UnixMilli()}
	if err := m.ensureWorkspace(r.CodexID, c.Cwd, view); err != nil {
		return err
	}
	m.registerRestored(r, view)
	return m.persistState(ctx, view)
}

func (m *Manager) replaceUnmaterializedRemoteCodex(ctx context.Context, ad *adapter.Adapter, r *persistence.CodexRecord, c *remotev1.Codex) error {
	oldThreadID := r.ThreadID
	oldSource := normalizeSourceString(r.SessionSource)
	replacement, err := ad.StartThread(ctx, c.Cwd)
	if err != nil {
		return fmt.Errorf("replace unmaterialized thread %s: %w", oldThreadID, err)
	}
	if replacement.ID == "" {
		return fmt.Errorf("replace unmaterialized thread %s: app-server returned an empty thread id", oldThreadID)
	}
	newSource := oldSource
	if len(replacement.Source) != 0 {
		newSource = normalizeSource(replacement.Source)
	}
	if err := m.Persistence.BindLogicalOwner(ctx, newSource, replacement.ID, r.LogicalOwnerID); err != nil {
		return err
	}
	c.ThreadId = replacement.ID
	if replacement.CWD != "" {
		c.Cwd = replacement.CWD
	}
	c.Status = remotev1.CodexStatus_CODEX_STATUS_IDLE
	c.ActiveTurnId = ""
	r.ThreadID = replacement.ID
	r.SessionSource = newSource
	r.CWD = c.Cwd
	r.Status = c.Status.String()
	r.ActiveTurnID = ""
	view := &remotev1.CurrentView{Codex: c, GeneratedAtUnixMs: m.now().UnixMilli()}
	if err := m.ensureWorkspace(r.CodexID, c.Cwd, view); err != nil {
		return err
	}
	m.mu.Lock()
	m.ensureMapsLocked()
	if m.byThread[oldThreadID] == r.CodexID {
		delete(m.byThread, oldThreadID)
	}
	if key := sessionKey(oldSource, oldThreadID); m.bySession[key] == r.CodexID {
		delete(m.bySession, key)
	}
	m.mu.Unlock()
	m.registerRestored(*r, view)
	return m.persistState(ctx, view)
}

func (m *Manager) registerRestored(r persistence.CodexRecord, view *remotev1.CurrentView) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureMapsLocked()
	m.byID[r.CodexID] = view
	m.byThread[r.ThreadID] = r.CodexID
	m.bySession[sessionKey(r.SessionSource, r.ThreadID)] = r.CodexID
	m.sources[r.CodexID] = normalizeSourceString(r.SessionSource)
	m.logicalOwners[r.CodexID] = r.LogicalOwnerID
	m.manualTitle[r.CodexID] = r.ManualTitleOverride
	m.warningDeadline[r.CodexID] = r.WarningDeadlineUnixMS
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
	} else {
		forgotten, listErr := m.Persistence.ListForgottenSessions(ctx, cwd)
		if listErr != nil {
			return nil, listErr
		}
		seen := make(map[string]struct{}, len(p.Data)+len(forgotten))
		for _, thread := range p.Data {
			seen[sessionKey(normalizeSource(thread.Source), thread.ID)] = struct{}{}
		}
		for _, candidate := range forgotten {
			key := sessionKey(candidate.Source, candidate.SessionID)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			rawSource, _ := json.Marshal(candidate.Source)
			thread := adapter.Thread{ID: candidate.SessionID, SessionID: candidate.SessionID, CWD: candidate.CWD, Preview: candidate.Preview, CreatedAt: candidate.CreatedAtUnixMS, UpdatedAt: candidate.UpdatedAtUnixMS, Source: rawSource}
			if candidate.Title != "" {
				title := candidate.Title
				thread.Name = &title
			}
			p.Data = append(p.Data, thread)
		}
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
	now := m.now()
	c := &remotev1.Codex{CodexId: newID("cdx"), ThreadId: t.ID, Cwd: t.CWD, Title: title, Origin: remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED, Status: remotev1.CodexStatus_CODEX_STATUS_IDLE, ManagementState: remotev1.ManagementState_MANAGEMENT_STATE_MANAGED, ManagedUntilUnixMs: now.Add(m.leaseDuration()).UnixMilli(), CreatedAtUnixMs: now.UnixMilli(), LastActivityAtUnixMs: now.UnixMilli()}
	source := normalizeSource(t.Source)
	ownerID, err := m.Persistence.EnsureLogicalOwner(ctx, source, t.ID)
	if err != nil {
		return nil, err
	}
	if err = m.saveCodex(ctx, c, source, ownerID); err != nil {
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
	forgotten, forgottenErr := m.Persistence.GetForgottenSession(ctx, source, req.SessionId)
	hasForgotten := forgottenErr == nil
	if forgottenErr != nil && !errors.Is(forgottenErr, sql.ErrNoRows) {
		return nil, forgottenErr
	}
	listed, readErr := ad.ReadThread(ctx, req.SessionId, true)
	t := adapter.Thread{}
	actualSource := source
	remoteCandidate := &remotev1.Codex{ThreadId: req.SessionId, Origin: remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED}
	switch {
	case readErr == nil:
		actualSource = normalizeSource(listed.Source)
		if actualSource != source {
			return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_SESSION_IMPORT_FAILED, fmt.Errorf("session source mismatch: requested %q, app-server returned %q", source, actualSource))
		}
		t, err = (session.Service{Adapter: ad, Directories: m.Directories}).Import(ctx, req.SessionId)
	case hasForgotten && !forgotten.Materialized && strings.Contains(forgotten.Origin, "REMOTE_CREATED") && (m.normalizeUnmaterializedHistory(remoteCandidate, readErr) || m.normalizeUnmaterializedResume(remoteCandidate, readErr)):
		t = threadFromForgotten(forgotten)
		if _, resumeErr := ad.ResumeThread(ctx, req.SessionId); resumeErr != nil && !m.normalizeUnmaterializedResume(remoteCandidate, resumeErr) {
			return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_SESSION_IMPORT_FAILED, resumeErr)
		}
	case hasForgotten && !forgotten.Materialized && strings.Contains(forgotten.Origin, "REMOTE_CREATED") && forgottenSessionNotFound(readErr):
		t, _, err = (session.Service{Adapter: ad, Directories: m.Directories}).Create(ctx, forgotten.CWD, false)
		if err == nil && len(t.Source) == 0 {
			t.Source, _ = json.Marshal(source)
		}
		actualSource = normalizeSource(t.Source)
	default:
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_SESSION_IMPORT_FAILED, readErr)
	}
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
	now := m.now()
	origin := remotev1.CodexOrigin_CODEX_ORIGIN_LOCAL_EXISTING
	if hasForgotten && strings.Contains(forgotten.Origin, "REMOTE_CREATED") {
		origin = remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED
	}
	c := &remotev1.Codex{CodexId: newID("cdx"), ThreadId: t.ID, Cwd: t.CWD, Title: title, Origin: origin, Status: remotev1.CodexStatus_CODEX_STATUS_IDLE, ManagementState: remotev1.ManagementState_MANAGEMENT_STATE_MANAGED, ManagedUntilUnixMs: now.Add(m.leaseDuration()).UnixMilli(), CreatedAtUnixMs: unixMillis(t.CreatedAt), ImportedAtUnixMs: now.UnixMilli(), LastActivityAtUnixMs: now.UnixMilli()}
	ownerID := ""
	if hasForgotten {
		ownerID = forgotten.LogicalOwnerID
	}
	if ownerID == "" {
		ownerID, err = m.Persistence.EnsureLogicalOwner(ctx, actualSource, t.ID)
	} else {
		err = m.Persistence.BindLogicalOwner(ctx, actualSource, t.ID, ownerID)
	}
	if err != nil {
		return nil, err
	}
	if err = m.saveCodex(ctx, c, actualSource, ownerID); err != nil {
		return nil, err
	}
	if err = m.publishCodex(ctx, c, gateway.RequestIDFromContext(ctx)); err != nil {
		return nil, err
	}
	if hasForgotten {
		if err = m.Persistence.DeleteForgottenSession(ctx, source, req.SessionId); err != nil {
			return nil, err
		}
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
		snapshot := m.turnSnapshot(t, m.imageResolver(ctx, req.CodexId))
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
	if c.ManagementState == remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
		if _, err = ad.ResumeThread(ctx, c.ThreadId); err != nil {
			if !m.normalizeUnmaterializedResume(c, err) {
				return nil, err
			}
		}
	}
	m.mu.RLock()
	ownerID := m.logicalOwners[req.CodexId]
	m.mu.RUnlock()
	var input []adapter.TurnInput
	acceptedParts := make([]*remotev1.UserMessagePart, 0, len(req.Input))
	for _, p := range req.Input {
		if t := p.GetText(); t != nil {
			input = append(input, adapter.TurnInput{Type: "text", Text: t.Text})
			acceptedParts = append(acceptedParts, &remotev1.UserMessagePart{Content: &remotev1.UserMessagePart_Text{Text: proto.Clone(t).(*remotev1.TextInput)}})
			continue
		}
		if image := p.GetImage(); image != nil {
			if m.Attachments == nil {
				return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_CAPABILITY_NOT_SUPPORTED, errors.New("image attachments are not enabled"))
			}
			descriptor, path, resolveErr := m.Attachments.Resolve(ctx, ownerID, image.AttachmentId)
			if resolveErr != nil {
				return nil, attachmentRPCError(resolveErr)
			}
			input = append(input, adapter.TurnInput{Type: "localImage", Path: path})
			acceptedParts = append(acceptedParts, &remotev1.UserMessagePart{Content: &remotev1.UserMessagePart_Image{Image: attachmentDescriptorProto(descriptor)}})
			continue
		}
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("turn input part is empty"))
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
	workspaceErr := m.WorkspaceAgentStarted(ctx, req.CodexId, turn.ID)
	if workspaceErr != nil {
		m.noteAsyncError(workspaceErr)
	}
	if err := m.setRunning(ctx, req.CodexId, turn.ID, gateway.RequestIDFromContext(ctx)); err != nil {
		return nil, errors.Join(workspaceErr, err)
	}
	if err := m.publishAcceptedUserMessage(ctx, req.CodexId, turn.ID, acceptedParts, gateway.RequestIDFromContext(ctx)); err != nil {
		return nil, errors.Join(workspaceErr, err)
	}
	if workspaceErr != nil {
		return nil, workspaceErr
	}
	return &remotev1.StartTurnResponse{TurnId: turn.ID}, nil
}

func (m *Manager) publishAcceptedUserMessage(ctx context.Context, codexID, turnID string, parts []*remotev1.UserMessagePart, requestID string) error {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	now := m.now().UnixMilli()
	item := &remotev1.Item{ItemId: turnID + ":user-message", TurnId: turnID, Status: remotev1.ItemStatus_ITEM_STATUS_COMPLETED, Content: &remotev1.Item_UserMessage{UserMessage: &remotev1.UserMessageItem{Parts: parts}}, Provenance: remotev1.ProvenanceKind_PROVENANCE_KIND_LIVE_WIRE}
	m.boundItem(item, m.contentBudget())
	m.mu.Lock()
	view := m.byID[codexID]
	if view == nil || view.Codex == nil {
		m.mu.Unlock()
		return rpcErr(remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND, errors.New("codex not found"))
	}
	snapshot := proto.Clone(view).(*remotev1.CurrentView)
	ensureActiveTurn(snapshot, turnID)
	upsertItem(snapshot.ActiveTurn, item)
	snapshot.ActiveTurn.Completeness = mergeCompleteness(snapshot.ActiveTurn.Completeness, item.Completeness)
	snapshot.Completeness = mergeCompleteness(snapshot.Completeness, item.Completeness)
	snapshot.GeneratedAtUnixMs = now
	m.mu.Unlock()
	if err := m.persistState(ctx, snapshot); err != nil {
		m.noteAsyncError(err)
		return err
	}
	m.mu.Lock()
	m.byID[codexID] = snapshot
	m.mu.Unlock()
	event := &remotev1.Event{CodexId: codexID, OccurredAtUnixMs: now, CausedByRequestId: requestID, Event: &remotev1.Event_ItemCompleted{ItemCompleted: &remotev1.ItemCompleted{Item: proto.Clone(item).(*remotev1.Item)}}}
	m.boundCanonicalEvent(event)
	if _, err := m.publishEvent(ctx, event, snapshot, &remotev1.Provenance{Kind: remotev1.ProvenanceKind_PROVENANCE_KIND_LIVE_WIRE, ObservedAtUnixMs: now}, ""); err != nil {
		m.noteAsyncError(err)
		return err
	}
	return nil
}

func (m *Manager) SetAttachmentService(service AttachmentResolver) {
	m.Attachments = service
}

func (m *Manager) UploadImageAttachment(ctx context.Context, req *remotev1.UploadImageAttachmentRequest) (*remotev1.UploadImageAttachmentResponse, error) {
	if req == nil || req.CodexId == "" {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("codex id is required"))
	}
	if _, err := m.lookup(req.CodexId); err != nil {
		return nil, err
	}
	if m.Attachments == nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_CAPABILITY_NOT_SUPPORTED, errors.New("image attachments are not enabled"))
	}
	m.mu.RLock()
	ownerID := m.logicalOwners[req.CodexId]
	m.mu.RUnlock()
	descriptor, err := m.Attachments.Upload(ctx, ownerID, req.Filename, req.MimeType, req.Sha256, req.Content)
	if err != nil {
		return nil, attachmentRPCError(err)
	}
	return &remotev1.UploadImageAttachmentResponse{Attachment: attachmentDescriptorProto(descriptor)}, nil
}

func (m *Manager) DownloadImageAttachment(ctx context.Context, req *remotev1.DownloadImageAttachmentRequest) (*remotev1.DownloadImageAttachmentResponse, error) {
	if req == nil || req.CodexId == "" || req.AttachmentId == "" {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("codex id and attachment id are required"))
	}
	if _, err := m.lookup(req.CodexId); err != nil {
		return nil, err
	}
	if m.Attachments == nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_CAPABILITY_NOT_SUPPORTED, errors.New("image attachments are not enabled"))
	}
	m.mu.RLock()
	ownerID := m.logicalOwners[req.CodexId]
	m.mu.RUnlock()
	descriptor, content, err := m.Attachments.Download(ctx, ownerID, req.AttachmentId)
	if err != nil {
		return nil, attachmentRPCError(err)
	}
	return &remotev1.DownloadImageAttachmentResponse{Attachment: attachmentDescriptorProto(descriptor), Content: content}, nil
}

func (m *Manager) UnmanageCodex(ctx context.Context, req *remotev1.UnmanageCodexRequest) (*remotev1.UnmanageCodexResponse, error) {
	if req == nil || req.CodexId == "" {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("codex id is required"))
	}
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	m.mu.Lock()
	m.ensureMapsLocked()
	view := m.byID[req.CodexId]
	if view == nil || view.Codex == nil {
		m.mu.Unlock()
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND, errors.New("codex not found"))
	}
	snapshot := proto.Clone(view).(*remotev1.CurrentView)
	original := proto.Clone(view).(*remotev1.CurrentView)
	if snapshot.Codex.ManagementState == remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
		m.mu.Unlock()
		return &remotev1.UnmanageCodexResponse{Codex: proto.Clone(snapshot.Codex).(*remotev1.Codex)}, nil
	}
	if manualUnmanageBusy(snapshot) {
		m.mu.Unlock()
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_CODEX_BUSY, errors.New("codex is busy"))
	}
	oldWarning := m.warningDeadline[req.CodexId]
	m.warningDeadline[req.CodexId] = 0
	snapshot.Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED
	snapshot.Codex.ManagedUntilUnixMs = 0
	snapshot.GeneratedAtUnixMs = m.now().UnixMilli()
	m.mu.Unlock()
	if err := m.persistState(ctx, snapshot); err != nil {
		m.mu.Lock()
		m.warningDeadline[req.CodexId] = oldWarning
		m.mu.Unlock()
		return nil, err
	}
	m.mu.Lock()
	m.byID[req.CodexId] = snapshot
	m.mu.Unlock()
	if err := m.publishCodex(ctx, snapshot.Codex, gateway.RequestIDFromContext(ctx)); err != nil {
		if errors.Is(err, persistence.ErrEventCommitOutcomeUnknown) {
			return nil, m.preserveLifecycleState(ctx, req.CodexId, snapshot, 0, false, err)
		}
		return nil, m.rollbackLifecycleState(ctx, req.CodexId, original, oldWarning, err)
	}
	return &remotev1.UnmanageCodexResponse{Codex: proto.Clone(snapshot.Codex).(*remotev1.Codex)}, nil
}

func (m *Manager) RenameCodex(ctx context.Context, req *remotev1.RenameCodexRequest) (*remotev1.RenameCodexResponse, error) {
	if req == nil || req.CodexId == "" {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("codex id is required"))
	}
	title := strings.TrimSpace(req.Title)
	if title == "" {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("title is required"))
	}
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	m.mu.Lock()
	m.ensureMapsLocked()
	view := m.byID[req.CodexId]
	if view == nil || view.Codex == nil {
		m.mu.Unlock()
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND, errors.New("codex not found"))
	}
	snapshot := proto.Clone(view).(*remotev1.CurrentView)
	snapshot.Codex.Title = title
	snapshot.GeneratedAtUnixMs = m.now().UnixMilli()
	oldManualTitle := m.manualTitle[req.CodexId]
	m.manualTitle[req.CodexId] = true
	m.mu.Unlock()
	if err := m.persistState(ctx, snapshot); err != nil {
		m.mu.Lock()
		m.manualTitle[req.CodexId] = oldManualTitle
		m.mu.Unlock()
		return nil, err
	}
	m.mu.Lock()
	m.byID[req.CodexId] = snapshot
	m.mu.Unlock()
	if err := m.publishCodex(ctx, snapshot.Codex, gateway.RequestIDFromContext(ctx)); err != nil {
		return nil, err
	}
	return &remotev1.RenameCodexResponse{Codex: proto.Clone(snapshot.Codex).(*remotev1.Codex)}, nil
}

func (m *Manager) ForgetCodex(ctx context.Context, req *remotev1.ForgetCodexRequest) (*remotev1.ForgetCodexResponse, error) {
	if req == nil || req.CodexId == "" {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("codex id is required"))
	}
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	m.mu.RLock()
	view := m.byID[req.CodexId]
	if view == nil || view.Codex == nil {
		m.mu.RUnlock()
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND, errors.New("codex not found"))
	}
	codex := proto.Clone(view.Codex).(*remotev1.Codex)
	source := m.sources[req.CodexId]
	ownerID := m.logicalOwners[req.CodexId]
	m.mu.RUnlock()
	if codex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_CONFLICT, errors.New("codex must be unmanaged before it can be forgotten"))
	}
	materialized := false
	if m.Runtime != nil {
		if ad, adapterErr := m.Runtime.Adapter(); adapterErr == nil {
			_, readErr := ad.ReadThread(ctx, codex.ThreadId, true)
			materialized = readErr == nil
		}
	}
	if err := m.Persistence.UpsertForgottenSession(ctx, persistence.ForgottenSessionRecord{
		Source: source, SessionID: codex.ThreadId, LogicalOwnerID: ownerID, CWD: codex.Cwd, Title: codex.Title, Origin: codex.Origin.String(),
		CreatedAtUnixMS: codex.CreatedAtUnixMs, UpdatedAtUnixMS: codex.LastActivityAtUnixMs, Materialized: materialized,
	}); err != nil {
		return nil, err
	}
	if _, err := m.Events.Forget(ctx, req.CodexId, gateway.RequestIDFromContext(ctx), m.Persistence.DeleteCodex); err != nil {
		return nil, err
	}
	m.mu.Lock()
	delete(m.byID, req.CodexId)
	if m.byThread[codex.ThreadId] == req.CodexId {
		delete(m.byThread, codex.ThreadId)
	}
	if key := sessionKey(source, codex.ThreadId); m.bySession[key] == req.CodexId {
		delete(m.bySession, key)
	}
	delete(m.sources, req.CodexId)
	delete(m.logicalOwners, req.CodexId)
	delete(m.manualTitle, req.CodexId)
	delete(m.warningDeadline, req.CodexId)
	for key := range m.chunks {
		if strings.HasPrefix(key, req.CodexId+"\x00") {
			delete(m.chunks, key)
		}
	}
	for child, codexID := range m.workspaceChildCodex {
		if codexID == req.CodexId {
			delete(m.workspaceChildCodex, child)
			delete(m.workspaceChildState, child)
		}
	}
	m.mu.Unlock()
	if m.Workspaces != nil {
		m.Workspaces.Unregister(req.CodexId)
	}
	return &remotev1.ForgetCodexResponse{CodexId: req.CodexId}, nil
}

func manualUnmanageBusy(view *remotev1.CurrentView) bool {
	if view == nil || view.Codex == nil {
		return false
	}
	switch view.Codex.Status {
	case remotev1.CodexStatus_CODEX_STATUS_RUNNING,
		remotev1.CodexStatus_CODEX_STATUS_WAITING_FOR_APPROVAL,
		remotev1.CodexStatus_CODEX_STATUS_WAITING_FOR_USER_INPUT,
		remotev1.CodexStatus_CODEX_STATUS_INTERRUPTING:
		return true
	}
	return view.Codex.ActiveTurnId != "" || view.ActiveTurn != nil || len(view.PendingRequests) != 0
}

func automaticUnmanageSafe(view *remotev1.CurrentView) bool {
	return view != nil && view.Codex != nil && view.Codex.Status == remotev1.CodexStatus_CODEX_STATUS_IDLE && !manualUnmanageBusy(view)
}

func hasManagementWarning(warnings []*remotev1.Warning, deadline int64) bool {
	for _, warning := range warnings {
		if warning != nil && warning.Code == remotev1.WarningCode_WARNING_CODE_MANAGEMENT_EXPIRING_SOON && warning.ManagedUntilUnixMs == deadline {
			return true
		}
	}
	return false
}

func (m *Manager) renewLease(ctx context.Context, codexID string) error {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	now := m.now()
	m.mu.Lock()
	m.ensureMapsLocked()
	view := m.byID[codexID]
	if view == nil || view.Codex == nil {
		m.mu.Unlock()
		return rpcErr(remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND, errors.New("codex not found"))
	}
	if view.Codex.ManagementState == remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED {
		m.mu.Unlock()
		return nil
	}
	snapshot := proto.Clone(view).(*remotev1.CurrentView)
	original := proto.Clone(view).(*remotev1.CurrentView)
	oldWarning := m.warningDeadline[codexID]
	m.warningDeadline[codexID] = 0
	snapshot.Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_MANAGED
	snapshot.Codex.ManagedUntilUnixMs = now.Add(m.leaseDuration()).UnixMilli()
	snapshot.Codex.LastActivityAtUnixMs = now.UnixMilli()
	snapshot.GeneratedAtUnixMs = now.UnixMilli()
	m.mu.Unlock()
	if err := m.persistState(ctx, snapshot); err != nil {
		m.mu.Lock()
		m.warningDeadline[codexID] = oldWarning
		m.mu.Unlock()
		return err
	}
	m.mu.Lock()
	m.byID[codexID] = snapshot
	m.mu.Unlock()
	if err := m.publishCodex(ctx, snapshot.Codex, ""); err != nil {
		if errors.Is(err, persistence.ErrEventCommitOutcomeUnknown) {
			return m.preserveLifecycleState(ctx, codexID, snapshot, 0, false, err)
		}
		return m.rollbackLifecycleState(ctx, codexID, original, oldWarning, err)
	}
	return nil
}

func (m *Manager) RenewForegroundCodexes(ctx context.Context, ids []string) error {
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		m.mu.RLock()
		view := m.byID[id]
		eligible := view != nil && view.Codex != nil && (view.Codex.ManagementState == remotev1.ManagementState_MANAGEMENT_STATE_MANAGED || view.Codex.ManagementState == remotev1.ManagementState_MANAGEMENT_STATE_EXPIRING_SOON)
		m.mu.RUnlock()
		if eligible {
			if err := m.renewLease(ctx, id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Manager) RunLeaseSweeper(ctx context.Context) {
	if err := m.sweepLeases(ctx); err != nil {
		m.noteAsyncError(err)
	}
	ticker := time.NewTicker(m.leaseSweepInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.sweepLeases(ctx); err != nil {
				m.noteAsyncError(err)
			}
		}
	}
}

func (m *Manager) sweepLeases(ctx context.Context) error {
	m.mu.RLock()
	ids := make([]string, 0, len(m.byID))
	for id := range m.byID {
		ids = append(ids, id)
	}
	m.mu.RUnlock()
	for _, id := range ids {
		if err := m.sweepLease(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) sweepLease(ctx context.Context, codexID string) error {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	now := m.now()
	m.mu.Lock()
	m.ensureMapsLocked()
	view := m.byID[codexID]
	if view == nil || view.Codex == nil {
		m.mu.Unlock()
		return nil
	}
	state := view.Codex.ManagementState
	deadline := view.Codex.ManagedUntilUnixMs
	if (state != remotev1.ManagementState_MANAGEMENT_STATE_MANAGED && state != remotev1.ManagementState_MANAGEMENT_STATE_EXPIRING_SOON) || deadline <= 0 {
		m.mu.Unlock()
		return nil
	}
	snapshot := proto.Clone(view).(*remotev1.CurrentView)
	original := proto.Clone(view).(*remotev1.CurrentView)
	oldWarning := m.warningDeadline[codexID]
	newWarning := oldWarning
	var warning *remotev1.Warning
	changed := false
	if now.UnixMilli() >= deadline && automaticUnmanageSafe(snapshot) {
		snapshot.Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_UNMANAGED
		snapshot.Codex.ManagedUntilUnixMs = 0
		newWarning = 0
		changed = true
	} else if now.UnixMilli() >= deadline-m.leaseWarningBefore().Milliseconds() {
		if snapshot.Codex.ManagementState != remotev1.ManagementState_MANAGEMENT_STATE_EXPIRING_SOON {
			snapshot.Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_EXPIRING_SOON
			changed = true
		}
		if oldWarning != deadline {
			warning = &remotev1.Warning{Code: remotev1.WarningCode_WARNING_CODE_MANAGEMENT_EXPIRING_SOON, Message: "Codex management lease is expiring soon", ManagedUntilUnixMs: deadline, Metadata: map[string]string{"managed_until_unix_ms": fmt.Sprintf("%d", deadline)}}
			if !hasManagementWarning(snapshot.Codex.Warnings, deadline) {
				snapshot.Codex.Warnings = append(snapshot.Codex.Warnings, proto.Clone(warning).(*remotev1.Warning))
			}
			newWarning = deadline
			changed = true
		}
	}
	if !changed {
		m.mu.Unlock()
		return nil
	}
	snapshot.GeneratedAtUnixMs = now.UnixMilli()
	m.warningDeadline[codexID] = newWarning
	m.mu.Unlock()
	if err := m.persistState(ctx, snapshot); err != nil {
		m.mu.Lock()
		m.warningDeadline[codexID] = oldWarning
		m.mu.Unlock()
		return err
	}
	m.mu.Lock()
	m.byID[codexID] = snapshot
	m.mu.Unlock()
	if err := m.publishCodex(ctx, snapshot.Codex, ""); err != nil {
		if errors.Is(err, persistence.ErrEventCommitOutcomeUnknown) {
			// The transition event may have committed. Keep EXPIRING/new state,
			// but clear the warning marker so the missing WarningRaised can retry.
			marker := newWarning
			persist := false
			if warning != nil {
				marker = 0
				persist = true
			}
			return m.preserveLifecycleState(ctx, codexID, snapshot, marker, persist, err)
		}
		return m.rollbackLifecycleState(ctx, codexID, original, oldWarning, err)
	}
	if warning != nil {
		if _, err := m.publishEvent(ctx, &remotev1.Event{CodexId: codexID, OccurredAtUnixMs: now.UnixMilli(), Event: &remotev1.Event_WarningRaised{WarningRaised: &remotev1.WarningRaised{Warning: warning}}}, snapshot, nil, ""); err != nil {
			if errors.Is(err, persistence.ErrEventCommitOutcomeUnknown) {
				// It may already be canonical. Keep the deadline marker to suppress
				// duplicates and expose the uncertainty as degradation.
				return m.preserveLifecycleState(ctx, codexID, snapshot, newWarning, false, err)
			}
			// CodexUpdated is already canonical. Preserve EXPIRING/current view,
			// clear only the marker, and retry WarningRaised on the next sweep.
			return m.preserveLifecycleState(ctx, codexID, snapshot, 0, true, err)
		}
	}
	return nil
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
	if err = m.renewLease(ctx, req.CodexId); err != nil {
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
	if err := m.renewLease(ctx, req.CodexId); err != nil {
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
	if err := m.renewLease(ctx, req.CodexId); err != nil {
		return nil, err
	}
	return &remotev1.RespondUserInputResponse{Request: resolved}, nil
}

func (m *Manager) saveCodex(ctx context.Context, c *remotev1.Codex, source string, ownerIDs ...string) error {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	source = normalizeSourceString(source)
	ownerID := ""
	if len(ownerIDs) != 0 {
		ownerID = ownerIDs[0]
	}
	if ownerID == "" {
		var err error
		ownerID, err = m.Persistence.EnsureLogicalOwner(ctx, source, c.ThreadId)
		if err != nil {
			return err
		}
	}
	r := recordFromCodex(c, source)
	r.LogicalOwnerID = ownerID
	view := &remotev1.CurrentView{Codex: proto.Clone(c).(*remotev1.Codex), GeneratedAtUnixMs: time.Now().UnixMilli()}
	if err := m.ensureWorkspace(c.CodexId, c.Cwd, view); err != nil {
		return err
	}
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
	m.logicalOwners[c.CodexId] = ownerID
	m.manualTitle[c.CodexId] = false
	m.warningDeadline[c.CodexId] = 0
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
const unmaterializedResumePrefix = "no rollout found for thread id "

func (m *Manager) normalizeUnmaterializedResume(c *remotev1.Codex, err error) bool {
	if c == nil || c.Origin != remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED {
		return false
	}
	var rpc *adapter.RPCError
	return errors.As(err, &rpc) && rpc.Code == -32600 && rpc.Message == unmaterializedResumePrefix+c.ThreadId
}

func forgottenSessionNotFound(err error) bool {
	var rpc *adapter.RPCError
	return errors.As(err, &rpc) && rpc.Code == -32004 && strings.EqualFold(strings.TrimSpace(rpc.Message), "thread not found")
}

func (m *Manager) normalizeUnmaterializedHistory(c *remotev1.Codex, err error) bool {
	if c == nil || c.Origin != remotev1.CodexOrigin_CODEX_ORIGIN_REMOTE_CREATED {
		return false
	}
	var rpc *adapter.RPCError
	if !errors.As(err, &rpc) || rpc.Code != -32600 {
		return false
	}
	return rpc.Message == "thread "+c.ThreadId+unmaterializedIncludeTurnsSuffix || rpc.Message == unmaterializedResumePrefix+c.ThreadId
}
func (m *Manager) persistView(ctx context.Context, id string, v *remotev1.CurrentView) error {
	raw, err := protojson.Marshal(v)
	if err != nil {
		return err
	}
	return m.Persistence.SetCurrentView(ctx, id, raw)
}

// unmarshalCurrentView accepts the v1.1 text-only UserMessageItem JSON shape.
// The wire field changed from input to parts in v1.2 without changing the
// nested text representation, so this focused rewrite preserves old snapshots
// without discarding unrelated unknown fields.
func unmarshalCurrentView(raw []byte, view *remotev1.CurrentView) error {
	if err := protojson.Unmarshal(raw, view); err == nil {
		return nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	migrateUserMessageInput(value)
	migrated, err := json.Marshal(value)
	if err != nil {
		return err
	}
	proto.Reset(view)
	return protojson.Unmarshal(migrated, view)
}

func migrateUserMessageInput(value any) {
	switch value := value.(type) {
	case []any:
		for _, entry := range value {
			migrateUserMessageInput(entry)
		}
	case map[string]any:
		if userMessage, ok := value["userMessage"].(map[string]any); ok {
			if _, exists := userMessage["parts"]; !exists {
				if input, legacy := userMessage["input"]; legacy {
					userMessage["parts"] = input
					delete(userMessage, "input")
				}
			}
		}
		for _, child := range value {
			migrateUserMessageInput(child)
		}
	}
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
	ownerID := m.logicalOwners[v.Codex.CodexId]
	warningDeadline := m.warningDeadline[v.Codex.CodexId]
	manualTitleOverride := m.manualTitle[v.Codex.CodexId]
	m.mu.RUnlock()
	r := recordFromCodex(v.Codex, source)
	r.LogicalOwnerID = ownerID
	r.ManualTitleOverride = manualTitleOverride
	r.WarningDeadlineUnixMS = warningDeadline
	r.CurrentViewJSON = raw
	return m.Persistence.UpsertCodex(ctx, r)
}

func (m *Manager) rollbackLifecycleState(ctx context.Context, codexID string, original *remotev1.CurrentView, warningDeadline int64, publishErr error) error {
	m.mu.Lock()
	m.ensureMapsLocked()
	m.warningDeadline[codexID] = warningDeadline
	m.byID[codexID] = proto.Clone(original).(*remotev1.CurrentView)
	m.mu.Unlock()
	if err := m.persistState(ctx, original); err != nil {
		rollbackErr := fmt.Errorf("rollback lifecycle state: %w", err)
		combined := errors.Join(publishErr, rollbackErr)
		m.noteAsyncError(combined)
		return combined
	}
	if m.Events != nil {
		if err := m.Events.ReloadDurable(ctx, codexID); err != nil {
			rollbackErr := fmt.Errorf("reload rolled back lifecycle state: %w", err)
			combined := errors.Join(publishErr, rollbackErr)
			m.noteAsyncError(combined)
			return combined
		}
	}
	return publishErr
}

func (m *Manager) preserveLifecycleState(ctx context.Context, codexID string, snapshot *remotev1.CurrentView, warningDeadline int64, persist bool, outcomeErr error) error {
	m.mu.Lock()
	m.ensureMapsLocked()
	m.warningDeadline[codexID] = warningDeadline
	m.byID[codexID] = proto.Clone(snapshot).(*remotev1.CurrentView)
	m.mu.Unlock()
	if persist {
		if err := m.persistState(ctx, snapshot); err != nil {
			combined := errors.Join(outcomeErr, fmt.Errorf("persist uncertain lifecycle state: %w", err))
			m.noteAsyncError(combined)
			return combined
		}
	}
	if m.Events != nil {
		if err := m.Events.ReloadDurable(ctx, codexID); err != nil {
			combined := errors.Join(outcomeErr, fmt.Errorf("reload uncertain lifecycle state: %w", err))
			m.noteAsyncError(combined)
			return combined
		}
	}
	m.noteAsyncError(outcomeErr)
	return outcomeErr
}

func (m *Manager) noteAsyncError(err error) {
	if err == nil {
		return
	}
	m.mu.Lock()
	m.asyncError = err.Error()
	m.mu.Unlock()
}

func (m *Manager) setRunning(ctx context.Context, id, turnID, requestID string) error {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	nowTime := m.now()
	now := nowTime.UnixMilli()
	m.mu.Lock()
	m.ensureMapsLocked()
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
	snapshot.Codex.ManagementState = remotev1.ManagementState_MANAGEMENT_STATE_MANAGED
	snapshot.Codex.ManagedUntilUnixMs = nowTime.Add(m.leaseDuration()).UnixMilli()
	m.warningDeadline[id] = 0
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
		m.mu.Lock()
		// StartTurn already succeeded upstream. Even when durability fails,
		// retain the honest running/managed snapshot in memory so a retry cannot
		// start a duplicate turn against an apparently unmanaged idle Codex.
		m.warningDeadline[id] = 0
		m.byID[id] = snapshot
		m.mu.Unlock()
		m.noteAsyncError(err)
		return err
	}
	m.mu.Lock()
	m.byID[id] = snapshot
	m.mu.Unlock()
	if _, err := m.publishEvent(ctx, &remotev1.Event{CodexId: id, OccurredAtUnixMs: now, CausedByRequestId: requestID, Event: &remotev1.Event_TurnUpdated{TurnUpdated: &remotev1.TurnUpdated{TurnId: turnID, Status: remotev1.TurnStatus_TURN_STATUS_RUNNING, StartedAtUnixMs: now}}}, snapshot, nil, ""); err != nil {
		// StartTurn already succeeded upstream. Keep the honest running snapshot
		// (including its renewed lease) and return an unknown outcome rather than
		// rolling back into a state that permits a duplicate upstream turn.
		m.noteAsyncError(err)
		return err
	}
	if err := m.publishCodex(ctx, snapshot.Codex, requestID); err != nil {
		m.noteAsyncError(err)
		return err
	}
	return nil
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
	_, err := m.publishEvent(ctx, event, snapshot, &remotev1.Provenance{Kind: remotev1.ProvenanceKind_PROVENANCE_KIND_LIVE_WIRE, ObservedAtUnixMs: time.Now().UnixMilli()}, "")
	return err
}

func (m *Manager) publishEvent(ctx context.Context, event *remotev1.Event, view *remotev1.CurrentView, provenance *remotev1.Provenance, parentRecordID string) (*remotev1.Event, error) {
	if m.testPublishEvent != nil {
		return m.testPublishEvent(ctx, event, view, provenance, parentRecordID)
	}
	return m.Events.Publish(ctx, event, view, provenance, parentRecordID)
}

func (m *Manager) applyAdapterEvent(ctx context.Context, e adapter.Event) error {
	if err := m.applyCollabAgentItem(ctx, e); err != nil {
		return err
	}
	turnID := e.TurnID
	if turnID == "" {
		turnID = firstString(rawObject(e.Params), "turnId", "id")
	}
	status, _, _, _ := turnEvent(e)
	terminal := e.Kind == adapter.EventTurnUpdated && status != remotev1.TurnStatus_TURN_STATUS_RUNNING
	err := m.applyAdapterEventLocked(ctx, e)
	if err == nil && terminal && turnID != "" {
		m.mu.RLock()
		codexID := m.byThread[e.ThreadID]
		childCodexID := m.workspaceChildCodex[e.ThreadID]
		m.mu.RUnlock()
		if childCodexID != "" {
			m.mu.Lock()
			observation := m.workspaceChildState[e.ThreadID]
			observation.Terminal = true
			m.workspaceChildState[e.ThreadID] = observation
			m.mu.Unlock()
			return m.WorkspaceAgentStopped(ctx, childCodexID, "subagent:"+e.ThreadID)
		}
		if codexID != "" {
			return m.WorkspaceAgentStopped(ctx, codexID, turnID)
		}
	}
	return err
}

func (m *Manager) applyAdapterEventLocked(ctx context.Context, e adapter.Event) error {
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
		preservedTitle := view.Codex.Title
		applyCodexParams(view.Codex, e.Method, e.Params)
		if m.manualTitle[id] {
			view.Codex.Title = preservedTitle
		}
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
		item := m.canonicalItem(e, status, m.imageResolverForOwner(ctx, m.logicalOwners[id]))
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
func applyCodexParams(c *remotev1.Codex, method string, raw []byte) {
	body := rawObject(raw)
	if method == "thread/name/updated" {
		name, present := body["threadName"]
		if !present {
			return
		}
		if name == nil {
			c.Title = ""
		} else if value, ok := name.(string); ok {
			c.Title = value
		}
		return
	}
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

func reconcileRestoredThreadTitle(c *remotev1.Codex, thread adapter.Thread) {
	if c != nil && thread.Name != nil {
		c.Title = *thread.Name
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
func (m *Manager) canonicalItem(e adapter.Event, status remotev1.ItemStatus, imageResolvers ...imageDescriptorResolver) *remotev1.Item {
	e.ItemID = canonicalEventItemID(e)
	return translateItem(e.Params, e.TurnID, e.ItemID, e.Method, e.Semantic, status, m.contentBudget(), remotev1.ProvenanceKind_PROVENANCE_KIND_LIVE_WIRE, imageResolvers...)
}

func (m *Manager) imageResolver(ctx context.Context, codexID string) imageDescriptorResolver {
	m.mu.RLock()
	ownerID := m.logicalOwners[codexID]
	m.mu.RUnlock()
	return m.imageResolverForOwner(ctx, ownerID)
}

func (m *Manager) imageResolverForOwner(ctx context.Context, ownerID string) imageDescriptorResolver {
	return func(path string) (*remotev1.ImageAttachment, error) {
		if m.Attachments == nil || ownerID == "" {
			return nil, errors.New("image attachment resolver is unavailable")
		}
		descriptor, err := m.Attachments.DescribePath(ctx, ownerID, path)
		if err != nil {
			return nil, err
		}
		return attachmentDescriptorProto(descriptor), nil
	}
}

func attachmentDescriptorProto(d attachment.Descriptor) *remotev1.ImageAttachment {
	out := &remotev1.ImageAttachment{AttachmentId: d.AttachmentID, Filename: d.Filename, MimeType: d.MIMEType, SizeBytes: d.SizeBytes, Sha256: d.SHA256}
	if d.WidthPixels != 0 {
		out.WidthPixels = proto.Uint32(d.WidthPixels)
	}
	if d.HeightPixels != 0 {
		out.HeightPixels = proto.Uint32(d.HeightPixels)
	}
	return out
}

func attachmentRPCError(err error) error {
	switch {
	case errors.Is(err, attachment.ErrNotFound):
		return rpcErr(remotev1.ErrorCode_ERROR_CODE_IMAGE_ATTACHMENT_NOT_FOUND, err)
	case errors.Is(err, attachment.ErrTooLarge):
		return rpcErr(remotev1.ErrorCode_ERROR_CODE_IMAGE_ATTACHMENT_TOO_LARGE, err)
	case errors.Is(err, attachment.ErrMIMEUnsupported):
		return rpcErr(remotev1.ErrorCode_ERROR_CODE_IMAGE_ATTACHMENT_MIME_TYPE_UNSUPPORTED, err)
	case errors.Is(err, attachment.ErrContentInvalid):
		return rpcErr(remotev1.ErrorCode_ERROR_CODE_IMAGE_ATTACHMENT_CONTENT_INVALID, err)
	case errors.Is(err, attachment.ErrHashMismatch):
		return rpcErr(remotev1.ErrorCode_ERROR_CODE_IMAGE_ATTACHMENT_HASH_MISMATCH, err)
	default:
		return err
	}
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
func (m *Manager) turnSnapshot(t adapter.Turn, imageResolvers ...imageDescriptorResolver) *remotev1.TurnSnapshot {
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
		item := translateItem(raw, t.ID, id, "imported", adapter.SemanticUnknown, remotev1.ItemStatus_ITEM_STATUS_COMPLETED, m.contentBudget(), remotev1.ProvenanceKind_PROVENANCE_KIND_IMPORTED_HISTORY, imageResolvers...)
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
	return persistence.CodexRecord{CodexID: c.CodexId, ThreadID: c.ThreadId, SessionSource: normalizeSourceString(source), CWD: c.Cwd, Title: c.Title, Origin: c.Origin.String(), Status: c.Status.String(), ActiveTurnID: c.ActiveTurnId, ManagementState: c.ManagementState.String(), ManagedUntilUnixMS: c.ManagedUntilUnixMs, CreatedAtUnixMS: c.CreatedAtUnixMs, ImportedAtUnixMS: c.ImportedAtUnixMs, LastActivityAtUnixMS: c.LastActivityAtUnixMs}
}
func threadFromForgotten(r persistence.ForgottenSessionRecord) adapter.Thread {
	rawSource, _ := json.Marshal(normalizeSourceString(r.Source))
	t := adapter.Thread{ID: r.SessionID, SessionID: r.SessionID, CWD: r.CWD, Preview: r.Preview, CreatedAt: r.CreatedAtUnixMS, UpdatedAt: r.UpdatedAtUnixMS, Source: rawSource}
	if r.Title != "" {
		title := r.Title
		t.Name = &title
	}
	return t
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
	management := remotev1.ManagementState_MANAGEMENT_STATE_UNSPECIFIED
	if n, ok := remotev1.ManagementState_value[r.ManagementState]; ok {
		management = remotev1.ManagementState(n)
	}
	return &remotev1.Codex{CodexId: r.CodexID, ThreadId: r.ThreadID, Cwd: r.CWD, Title: r.Title, Origin: origin, Status: status, ActiveTurnId: r.ActiveTurnID, ManagementState: management, ManagedUntilUnixMs: r.ManagedUntilUnixMS, CreatedAtUnixMs: r.CreatedAtUnixMS, ImportedAtUnixMs: r.ImportedAtUnixMS, LastActivityAtUnixMs: r.LastActivityAtUnixMS}
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
	if m.logicalOwners == nil {
		m.logicalOwners = make(map[string]string)
	}
	if m.manualTitle == nil {
		m.manualTitle = make(map[string]bool)
	}
	if m.warningDeadline == nil {
		m.warningDeadline = make(map[string]int64)
	}
	if m.chunks == nil {
		m.chunks = make(map[string]uint64)
	}
	if m.workspaceChildCodex == nil {
		m.workspaceChildCodex = make(map[string]string)
	}
	if m.workspaceChildState == nil {
		m.workspaceChildState = make(map[string]workspaceChildObservation)
	}
}

func (m *Manager) noteUnrecoverablePending(ctx context.Context, codexID string, view *remotev1.CurrentView) {
	raw, err := m.Persistence.CurrentView(ctx, codexID)
	if err != nil {
		return
	}
	old := new(remotev1.CurrentView)
	if unmarshalCurrentView(raw, old) != nil || view == nil || view.Codex == nil {
		return
	}
	if old.Codex != nil {
		view.Codex.Warnings = make([]*remotev1.Warning, 0, len(old.Codex.Warnings))
		for _, warning := range old.Codex.Warnings {
			if warning != nil {
				view.Codex.Warnings = append(view.Codex.Warnings, proto.Clone(warning).(*remotev1.Warning))
			}
		}
	}
	if old.WorkspaceAccessState != nil {
		view.WorkspaceAccessState = proto.Clone(old.WorkspaceAccessState).(*remotev1.WorkspaceAccessState)
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
		if len(content.UserMessage.Parts) > 1 {
			content.UserMessage.Parts = content.UserMessage.Parts[:1]
		}
		if len(content.UserMessage.Parts) > 0 && content.UserMessage.Parts[0].GetText() != nil {
			content.UserMessage.Parts[0].GetText().Text = ""
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
