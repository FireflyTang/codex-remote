package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"github.com/kylin1993/codex-remote/internal/adapter"
	"github.com/kylin1993/codex-remote/internal/persistence"
	workspacecore "github.com/kylin1993/codex-remote/internal/workspace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type workspaceChildObservation struct {
	ItemID   string
	Terminal bool
}

type collabAgentItem struct {
	Type              string                      `json:"type"`
	ID                string                      `json:"id"`
	Tool              string                      `json:"tool"`
	SenderThreadID    string                      `json:"senderThreadId"`
	ReceiverThreadIDs []string                    `json:"receiverThreadIds"`
	AgentsStates      map[string]collabAgentState `json:"agentsStates"`
}

type collabAgentState struct {
	Status string `json:"status"`
}

func parseCollabAgentItem(raw []byte) (collabAgentItem, bool) {
	var envelope struct {
		Item json.RawMessage `json:"item"`
	}
	_ = json.Unmarshal(raw, &envelope)
	if len(envelope.Item) != 0 {
		raw = envelope.Item
	}
	var item collabAgentItem
	if json.Unmarshal(raw, &item) != nil || item.Type != "collabAgentToolCall" {
		return collabAgentItem{}, false
	}
	return item, true
}

func collabAgentActive(status string) bool { return status == "pendingInit" || status == "running" }
func collabAgentTerminal(status string) bool {
	switch status {
	case "interrupted", "completed", "errored", "shutdown", "notFound":
		return true
	}
	return false
}

func (m *Manager) applyCollabAgentItem(ctx context.Context, e adapter.Event) error {
	if e.Kind != adapter.EventItemStarted && e.Kind != adapter.EventItemUpdated && e.Kind != adapter.EventItemCompleted {
		return nil
	}
	item, ok := parseCollabAgentItem(e.Params)
	if !ok {
		return nil
	}
	sender := item.SenderThreadID
	if sender == "" {
		sender = e.ThreadID
	}
	m.mu.Lock()
	m.ensureMapsLocked()
	codexID := m.byThread[sender]
	if codexID == "" {
		codexID = m.workspaceChildCodex[sender]
	}
	if codexID == "" {
		m.mu.Unlock()
		return nil
	}
	children := append([]string(nil), item.ReceiverThreadIDs...)
	for child := range item.AgentsStates {
		children = append(children, child)
	}
	sort.Strings(children)
	unique := children[:0]
	for _, child := range children {
		if child != "" && (len(unique) == 0 || unique[len(unique)-1] != child) {
			unique = append(unique, child)
		}
	}
	type transition struct {
		child  string
		active bool
	}
	var transitions []transition
	for _, child := range unique {
		m.workspaceChildCodex[child] = codexID
		state, present := item.AgentsStates[child]
		if !present {
			continue
		}
		active, terminal := collabAgentActive(state.Status), collabAgentTerminal(state.Status)
		if !active && !terminal {
			continue
		}
		key := child
		observation := m.workspaceChildState[key]
		// A child terminal notification is sticky across replayed/late collab
		// snapshots. Only an explicit resumeAgent operation may reactivate the
		// same child thread; a different spawn/send/wait item is not evidence
		// that the already terminal child started running again.
		if observation.Terminal && active && (item.Tool != "resumeAgent" || observation.ItemID == item.ID) {
			continue
		}
		if terminal {
			observation = workspaceChildObservation{ItemID: item.ID, Terminal: true}
		} else {
			observation = workspaceChildObservation{ItemID: item.ID}
		}
		m.workspaceChildState[key] = observation
		transitions = append(transitions, transition{child: child, active: active})
	}
	m.mu.Unlock()
	for _, transition := range transitions {
		agentID := "subagent:" + transition.child
		var err error
		if transition.active {
			err = m.WorkspaceAgentStarted(ctx, codexID, agentID)
		} else {
			err = m.WorkspaceAgentStopped(ctx, codexID, agentID)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) restoreCollabAgents(codexID string, thread adapter.Thread) (*remotev1.WorkspaceAccessState, error) {
	type restored struct{ itemID, status string }
	latest := make(map[string]restored)
	for _, turn := range thread.Turns {
		for _, raw := range turn.Items {
			item, ok := parseCollabAgentItem(raw)
			if !ok {
				continue
			}
			children := append([]string(nil), item.ReceiverThreadIDs...)
			for child := range item.AgentsStates {
				children = append(children, child)
			}
			for _, child := range children {
				if child != "" {
					if state, ok := item.AgentsStates[child]; ok {
						latest[child] = restored{item.ID, state.Status}
					}
				}
			}
		}
	}
	m.mu.Lock()
	m.ensureMapsLocked()
	for child, state := range latest {
		m.workspaceChildCodex[child] = codexID
		m.workspaceChildState[child] = workspaceChildObservation{ItemID: state.itemID, Terminal: collabAgentTerminal(state.status)}
	}
	m.mu.Unlock()
	var access *remotev1.WorkspaceAccessState
	children := make([]string, 0, len(latest))
	for child := range latest {
		children = append(children, child)
	}
	sort.Strings(children)
	for _, child := range children {
		if !collabAgentActive(latest[child].status) {
			continue
		}
		state, err := m.Workspaces.RestoreAgent(codexID, "subagent:"+child)
		if err != nil {
			return nil, err
		}
		access = state
	}
	return access, nil
}

func (m *Manager) SetWorkspaceService(service *workspacecore.Service) {
	m.Workspaces = service
	if service != nil {
		service.SetStateSink(m.commitWorkspaceState)
	}
}

func (m *Manager) WorkspaceCapabilities() *remotev1.WorkspaceCapabilities {
	if m.Workspaces == nil {
		return nil
	}
	return m.Workspaces.Capabilities()
}

func (m *Manager) ensureWorkspace(codexID, root string, view *remotev1.CurrentView) error {
	if m.Workspaces == nil {
		service, _ := workspacecore.New(workspacecore.Config{})
		m.SetWorkspaceService(service)
	}
	state, err := m.Workspaces.Register(codexID, root, view.GetWorkspaceAccessState())
	if err != nil {
		return err
	}
	view.WorkspaceAccessState = state
	return nil
}

func (m *Manager) workspaceError(err error) error {
	var workspaceErr *workspacecore.Error
	if errors.As(err, &workspaceErr) {
		return rpcErr(workspaceErr.Code, workspaceErr.Err)
	}
	return err
}

func (m *Manager) commitWorkspaceState(ctx context.Context, codexID string, state *remotev1.WorkspaceAccessState) error {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()
	m.mu.Lock()
	view := m.byID[codexID]
	if view == nil || view.Codex == nil {
		m.mu.Unlock()
		return rpcErr(remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND, errors.New("codex not found"))
	}
	snapshot := proto.Clone(view).(*remotev1.CurrentView)
	snapshot.WorkspaceAccessState = proto.Clone(state).(*remotev1.WorkspaceAccessState)
	snapshot.GeneratedAtUnixMs = m.now().UnixMilli()
	m.mu.Unlock()
	event := &remotev1.Event{CodexId: codexID, OccurredAtUnixMs: m.now().UnixMilli(), Event: &remotev1.Event_WorkspaceAccessStateUpdated{WorkspaceAccessStateUpdated: &remotev1.WorkspaceAccessStateUpdated{AccessState: proto.Clone(state).(*remotev1.WorkspaceAccessState)}}}
	published, err := m.publishEvent(ctx, event, snapshot, nil, "")
	if err != nil {
		if errors.Is(err, persistence.ErrEventCommitOutcomeUnknown) {
			raw, readErr := m.Persistence.CurrentView(ctx, codexID)
			durable := new(remotev1.CurrentView)
			if readErr == nil {
				readErr = protojson.Unmarshal(raw, durable)
			}
			if readErr == nil && durable.WorkspaceAccessState != nil && proto.Equal(durable.WorkspaceAccessState, state) {
				m.mu.Lock()
				m.byID[codexID] = durable
				m.mu.Unlock()
				_ = m.Events.ReloadDurable(ctx, codexID)
				return nil
			}
			if readErr == nil {
				m.mu.Lock()
				m.byID[codexID] = durable
				m.mu.Unlock()
				_ = m.Events.ReloadDurable(ctx, codexID)
				return fmt.Errorf("workspace event commit did not advance durable state: %v", err)
			}
			// The filesystem mutation may match an event that committed. Keep the
			// matching in-memory state and surface an honest unknown outcome.
			m.mu.Lock()
			m.byID[codexID] = snapshot
			m.mu.Unlock()
			m.noteAsyncError(errors.Join(err, readErr))
			return err
		}
		m.noteAsyncError(err)
		return err
	}
	if published != nil {
		snapshot.HeadEventSeq = published.EventSeq
	}
	m.mu.Lock()
	m.byID[codexID] = snapshot
	m.mu.Unlock()
	return nil
}

func (m *Manager) WorkspaceAgentStarted(ctx context.Context, codexID, agentID string) error {
	if m.Workspaces == nil {
		m.mu.RLock()
		current := m.byID[codexID]
		var view *remotev1.CurrentView
		if current != nil {
			view = proto.Clone(current).(*remotev1.CurrentView)
		}
		m.mu.RUnlock()
		if view == nil || view.Codex == nil {
			return rpcErr(remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND, errors.New("codex not found"))
		}
		if err := m.ensureWorkspace(codexID, view.Codex.Cwd, view); err != nil {
			return m.workspaceError(err)
		}
		m.mu.Lock()
		m.byID[codexID].WorkspaceAccessState = view.WorkspaceAccessState
		m.mu.Unlock()
	}
	return m.workspaceError(m.Workspaces.AgentStarted(ctx, codexID, agentID))
}

func (m *Manager) WorkspaceAgentStopped(ctx context.Context, codexID, agentID string) error {
	if m.Workspaces == nil {
		return errors.New("workspace service is unavailable")
	}
	return m.workspaceError(m.Workspaces.AgentStopped(ctx, codexID, agentID))
}

func (m *Manager) GetWorkspace(_ context.Context, req *remotev1.GetWorkspaceRequest) (*remotev1.GetWorkspaceResponse, error) {
	if req == nil || req.CodexId == "" {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("codex id is required"))
	}
	if _, err := m.lookup(req.CodexId); err != nil {
		return nil, err
	}
	state, root, err := m.Workspaces.State(req.CodexId)
	if err != nil {
		return nil, m.workspaceError(err)
	}
	return &remotev1.GetWorkspaceResponse{CodexId: req.CodexId, WorkspaceRoot: root, AccessState: state}, nil
}

func (m *Manager) ListWorkspaceEntries(_ context.Context, req *remotev1.ListWorkspaceEntriesRequest) (*remotev1.ListWorkspaceEntriesResponse, error) {
	if req == nil || req.CodexId == "" {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("codex id is required"))
	}
	start, size, err := page(req.Page, m.MaxPage, "workspace_list", req.CodexId+"\x00"+req.RelativeDirectory)
	if err != nil {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, err)
	}
	entries, err := m.Workspaces.List(req.CodexId, req.RelativeDirectory)
	if err != nil {
		return nil, m.workspaceError(err)
	}
	if start > len(entries) {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("page token offset is out of range"))
	}
	end := min(start+size, len(entries))
	pageInfo := &remotev1.PageInfo{}
	if end < len(entries) {
		pageInfo.NextPageToken = encodePageToken(pageToken{Operation: "workspace_list", Query: req.CodexId + "\x00" + req.RelativeDirectory, Offset: end})
	}
	return &remotev1.ListWorkspaceEntriesResponse{CodexId: req.CodexId, RelativeDirectory: req.RelativeDirectory, Entries: entries[start:end], Page: pageInfo}, nil
}

func (m *Manager) ReadWorkspaceTextFile(_ context.Context, req *remotev1.ReadWorkspaceTextFileRequest) (*remotev1.ReadWorkspaceTextFileResponse, error) {
	if req == nil || req.CodexId == "" {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("codex id is required"))
	}
	entry, text, err := m.Workspaces.ReadText(req.CodexId, req.RelativePath)
	if err != nil {
		return nil, m.workspaceError(err)
	}
	return &remotev1.ReadWorkspaceTextFileResponse{Entry: entry, Utf8Text: text}, nil
}

func (m *Manager) WriteWorkspaceTextFile(ctx context.Context, req *remotev1.WriteWorkspaceTextFileRequest) (*remotev1.WriteWorkspaceTextFileResponse, error) {
	if req == nil || req.CodexId == "" {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("codex id is required"))
	}
	entry, err := m.Workspaces.WriteText(ctx, req.CodexId, req.RelativePath, req.Utf8Text, req.ExpectedRevision, req.ExpectedQuiescenceToken, req.Condition)
	if err != nil {
		return nil, m.workspaceError(err)
	}
	return &remotev1.WriteWorkspaceTextFileResponse{Entry: entry}, nil
}

func (m *Manager) UploadWorkspaceEntry(ctx context.Context, req *remotev1.UploadWorkspaceEntryRequest) (*remotev1.UploadWorkspaceEntryResponse, error) {
	if req == nil || req.CodexId == "" {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("codex id is required"))
	}
	entry, err := m.Workspaces.Upload(ctx, req.CodexId, req.DestinationPath, req.Kind, req.Content, req.Condition, req.ExpectedRevision, req.ExpectedQuiescenceToken)
	if err != nil {
		return nil, m.workspaceError(err)
	}
	return &remotev1.UploadWorkspaceEntryResponse{Entry: entry}, nil
}

func (m *Manager) DownloadWorkspaceEntry(_ context.Context, req *remotev1.DownloadWorkspaceEntryRequest) (*remotev1.DownloadWorkspaceEntryResponse, error) {
	if req == nil || req.CodexId == "" {
		return nil, rpcErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, errors.New("codex id is required"))
	}
	entry, kind, filename, content, err := m.Workspaces.Download(req.CodexId, req.RelativePath)
	if err != nil {
		return nil, m.workspaceError(err)
	}
	if filename == ".zip" {
		filename = path.Base(req.RelativePath) + ".zip"
	}
	return &remotev1.DownloadWorkspaceEntryResponse{Entry: entry, Kind: kind, Filename: filename, Content: content}, nil
}
