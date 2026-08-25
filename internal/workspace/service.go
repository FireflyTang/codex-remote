package workspace

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"github.com/kylin1993/codex-remote/internal/persistence"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	MaxTextFileBytes, MaxInlineUploadBytes, MaxInlineDownloadBytes, MaxArchiveExpandedBytes uint64
	MaxArchiveEntryCount                                                                    uint32
	Clock                                                                                   func() time.Time
}

type Error struct {
	Code remotev1.ErrorCode
	Err  error
}

func (e *Error) Error() string { return e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

type StateSink func(context.Context, string, *remotev1.WorkspaceAccessState) error

type workspaceState struct {
	// mu protects this Codex's root and access state. Filesystem reads and state
	// sink calls must happen after releasing it.
	mu          sync.Mutex
	coordinator sync.RWMutex
	root        string
	displayRoot string
	state       *remotev1.WorkspaceAccessState
	agentIDs    map[string]struct{}
	pending     chan struct{}
	removed     bool
}

type Service struct {
	mu                  sync.RWMutex
	config              Config
	states              map[string]*workspaceState
	sink                StateSink
	beforeListEntryTest func(string)
}

func New(config Config) (*Service, error) {
	if config.MaxTextFileBytes == 0 {
		config.MaxTextFileBytes = 1 << 20
	}
	if config.MaxInlineUploadBytes == 0 {
		config.MaxInlineUploadBytes = 2 << 20
	}
	if config.MaxInlineDownloadBytes == 0 {
		config.MaxInlineDownloadBytes = 2 << 20
	}
	if config.MaxArchiveExpandedBytes == 0 {
		config.MaxArchiveExpandedBytes = 32 << 20
	}
	if config.MaxArchiveEntryCount == 0 {
		config.MaxArchiveEntryCount = 1000
	}
	return &Service{config: config, states: make(map[string]*workspaceState)}, nil
}

func (s *Service) SetStateSink(sink StateSink) { s.mu.Lock(); s.sink = sink; s.mu.Unlock() }

func (s *Service) Capabilities() *remotev1.WorkspaceCapabilities {
	return &remotev1.WorkspaceCapabilities{MaxTextFileBytes: s.config.MaxTextFileBytes, MaxInlineUploadBytes: s.config.MaxInlineUploadBytes, MaxInlineDownloadBytes: s.config.MaxInlineDownloadBytes, MaxArchiveExpandedBytes: s.config.MaxArchiveExpandedBytes, MaxArchiveEntryCount: s.config.MaxArchiveEntryCount}
}

func (s *Service) now() time.Time {
	if s.config.Clock != nil {
		return s.config.Clock()
	}
	return time.Now()
}

func (s *Service) Register(codexID, root string, persisted *remotev1.WorkspaceAccessState) (*remotev1.WorkspaceAccessState, error) {
	resolved, err := registerRoot(root)
	if err != nil {
		return nil, err
	}
	state := cloneState(persisted)
	if state == nil || state.Generation == 0 {
		state = &remotev1.WorkspaceAccessState{Generation: 1, MutationStatus: remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_ALLOWED, QuiescenceToken: newToken(), ObservedAtUnixMs: s.now().UnixMilli()}
	} else if state.ActiveAgentCount != 0 || state.MutationStatus != remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_ALLOWED || state.QuiescenceToken == "" {
		state.Generation++
		state.ActiveAgentCount = 0
		state.MutationStatus = remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_ALLOWED
		state.QuiescenceToken = newToken()
		state.ObservedAtUnixMs = s.now().UnixMilli()
	}
	s.mu.Lock()
	ws := s.states[codexID]
	if ws == nil {
		ws = &workspaceState{root: resolved, displayRoot: root, state: state, agentIDs: make(map[string]struct{})}
		s.states[codexID] = ws
		s.mu.Unlock()
		return cloneState(state), nil
	}
	s.mu.Unlock()

	// A runtime Ready restore can overlap a live workspace transition. Preserve
	// a newer in-memory generation instead of replacing it with an older
	// persisted snapshot. No state sink is called while ws.mu is held.
	ws.coordinator.Lock()
	defer ws.coordinator.Unlock()
	ws.mu.Lock()
	ws.root = resolved
	ws.displayRoot = root
	ws.removed = false
	if ws.state == nil || ws.state.Generation < state.Generation {
		ws.state = state
		ws.agentIDs = make(map[string]struct{})
	}
	result := cloneState(ws.state)
	ws.mu.Unlock()
	return result, nil
}

// Unregister removes the Host-owned workspace mapping for a forgotten Codex.
// The coordinator drains admitted operations before the mapping disappears.
func (s *Service) Unregister(codexID string) {
	s.mu.RLock()
	ws := s.states[codexID]
	s.mu.RUnlock()
	if ws == nil {
		return
	}
	ws.coordinator.Lock()
	s.mu.Lock()
	if s.states[codexID] == ws {
		delete(s.states, codexID)
	}
	s.mu.Unlock()
	ws.mu.Lock()
	ws.removed = true
	ws.mu.Unlock()
	ws.coordinator.Unlock()
}

func (s *Service) State(codexID string) (*remotev1.WorkspaceAccessState, string, error) {
	ws, err := s.known(codexID)
	if err != nil {
		return nil, "", err
	}
	ws.coordinator.RLock()
	defer ws.coordinator.RUnlock()
	if _, err := rootForAccess(ws); err != nil {
		return nil, "", err
	}
	ws.mu.Lock()
	state, displayRoot := cloneState(ws.state), ws.displayRoot
	ws.mu.Unlock()
	return state, displayRoot, nil
}

func rootForAccess(ws *workspaceState) (string, error) {
	ws.mu.Lock()
	if ws.removed {
		ws.mu.Unlock()
		return "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND, "codex not found")
	}
	root := ws.root
	ws.mu.Unlock()
	resolved, err := canonicalRoot(root)
	if err != nil {
		return "", err
	}
	ws.mu.Lock()
	if ws.root == root {
		ws.root = resolved
	}
	ws.mu.Unlock()
	return resolved, nil
}

func lockMutationCoordinator(ctx context.Context, ws *workspaceState) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ws.coordinator.Lock()
		ws.mu.Lock()
		if ws.removed {
			ws.mu.Unlock()
			ws.coordinator.Unlock()
			return workspaceErr(remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND, "codex not found")
		}
		pending := ws.pending
		ws.mu.Unlock()
		if pending == nil {
			return nil
		}
		ws.coordinator.Unlock()
		select {
		case <-pending:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func beginPendingLocked(ws *workspaceState) chan struct{} {
	pending := make(chan struct{})
	ws.pending = pending
	return pending
}

func finishPending(ws *workspaceState, pending chan struct{}) {
	ws.mu.Lock()
	if ws.pending == pending {
		ws.pending = nil
		close(pending)
	}
	ws.mu.Unlock()
}

func (s *Service) AgentStarted(ctx context.Context, codexID, agentID string) error {
	if agentID == "" {
		return workspaceErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, "agent id is required")
	}
	ws, err := s.known(codexID)
	if err != nil {
		return err
	}
	if err := lockMutationCoordinator(ctx, ws); err != nil {
		return err
	}
	ws.mu.Lock()
	if _, exists := ws.agentIDs[agentID]; exists {
		ws.mu.Unlock()
		ws.coordinator.Unlock()
		return nil
	}
	previous := cloneState(ws.state)
	ws.agentIDs[agentID] = struct{}{}
	ws.state.Generation++
	ws.state.ActiveAgentCount = uint32(len(ws.agentIDs))
	ws.state.MutationStatus = remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_BUSY
	ws.state.QuiescenceToken = ""
	ws.state.ObservedAtUnixMs = s.now().UnixMilli()
	state := cloneState(ws.state)
	pending := beginPendingLocked(ws)
	sink := s.stateSink()
	ws.mu.Unlock()
	ws.coordinator.Unlock()
	if err := emit(ctx, sink, codexID, state); err != nil {
		if !errors.Is(err, persistence.ErrEventCommitOutcomeUnknown) {
			ws.mu.Lock()
			if proto.Equal(ws.state, state) {
				delete(ws.agentIDs, agentID)
				ws.state = previous
			}
			ws.mu.Unlock()
		}
		finishPending(ws, pending)
		return err
	}
	finishPending(ws, pending)
	return nil
}

// RestoreAgent reconstructs a known active agent without publishing during the
// enclosing Host RESET rebuild. The returned state must be persisted in that
// RESET CurrentView.
func (s *Service) RestoreAgent(codexID, agentID string) (*remotev1.WorkspaceAccessState, error) {
	if agentID == "" {
		agentID = "restore-unknown"
	}
	ws, err := s.known(codexID)
	if err != nil {
		return nil, err
	}
	// Restore runs under Manager.commitMu and deliberately bypasses a pending
	// live sink, which may itself be waiting for that commit boundary.
	ws.coordinator.Lock()
	defer ws.coordinator.Unlock()
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.removed {
		return nil, workspaceErr(remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND, "codex not found")
	}
	if _, exists := ws.agentIDs[agentID]; exists {
		return cloneState(ws.state), nil
	}
	ws.agentIDs[agentID] = struct{}{}
	ws.state.Generation++
	ws.state.ActiveAgentCount = uint32(len(ws.agentIDs))
	ws.state.MutationStatus = remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_BUSY
	ws.state.QuiescenceToken = ""
	ws.state.ObservedAtUnixMs = s.now().UnixMilli()
	return cloneState(ws.state), nil
}

func (s *Service) AgentStopped(ctx context.Context, codexID, agentID string) error {
	ws, err := s.known(codexID)
	if err != nil {
		return err
	}
	if err := lockMutationCoordinator(ctx, ws); err != nil {
		return err
	}
	ws.mu.Lock()
	if _, exists := ws.agentIDs[agentID]; !exists {
		ws.mu.Unlock()
		ws.coordinator.Unlock()
		return nil
	}
	previous := cloneState(ws.state)
	delete(ws.agentIDs, agentID)
	ws.state.Generation++
	ws.state.ActiveAgentCount = uint32(len(ws.agentIDs))
	if len(ws.agentIDs) == 0 {
		ws.state.MutationStatus = remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_ALLOWED
		ws.state.QuiescenceToken = newToken()
	} else {
		ws.state.MutationStatus = remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_BUSY
		ws.state.QuiescenceToken = ""
	}
	ws.state.ObservedAtUnixMs = s.now().UnixMilli()
	state := cloneState(ws.state)
	pending := beginPendingLocked(ws)
	sink := s.stateSink()
	ws.mu.Unlock()
	ws.coordinator.Unlock()
	if err := emit(ctx, sink, codexID, state); err != nil {
		if !errors.Is(err, persistence.ErrEventCommitOutcomeUnknown) {
			ws.mu.Lock()
			if proto.Equal(ws.state, state) {
				ws.agentIDs[agentID] = struct{}{}
				ws.state = previous
			}
			ws.mu.Unlock()
		}
		finishPending(ws, pending)
		return err
	}
	finishPending(ws, pending)
	return nil
}

func (s *Service) List(ctx context.Context, codexID, relative string, start, size int) ([]*remotev1.WorkspaceEntry, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	ws, err := s.known(codexID)
	if err != nil {
		return nil, 0, err
	}
	ws.coordinator.RLock()
	defer ws.coordinator.RUnlock()
	root, err := rootForAccess(ws)
	if err != nil {
		return nil, 0, err
	}
	abs, err := resolve(root, relative, true, true)
	if err != nil {
		return nil, 0, err
	}
	info, err := os.Stat(abs)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND, "directory not found")
	}
	if err != nil {
		return nil, 0, err
	}
	if !info.IsDir() {
		return nil, 0, workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_TYPE_UNSUPPORTED, "entry is not a directory")
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	dir, err := os.ReadDir(abs)
	if err != nil {
		return nil, 0, err
	}
	total := len(dir)
	if start < 0 || start > total || size < 0 {
		return nil, total, workspaceErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, "page offset is out of range")
	}
	end := min(start+size, total)
	out := make([]*remotev1.WorkspaceEntry, 0, end-start)
	for _, item := range dir[start:end] {
		if err := ctx.Err(); err != nil {
			return nil, total, err
		}
		child := item.Name()
		if relative != "" {
			child = relative + "/" + child
		}
		if s.beforeListEntryTest != nil {
			s.beforeListEntryTest(child)
		}
		entry, err := entryMetadata(root, child, s.config.MaxTextFileBytes, false)
		if err != nil {
			return nil, total, err
		}
		out = append(out, entry)
	}
	return out, total, nil
}

func (s *Service) ReadText(ctx context.Context, codexID, relative string) (*remotev1.WorkspaceEntry, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	ws, err := s.known(codexID)
	if err != nil {
		return nil, "", err
	}
	ws.coordinator.RLock()
	defer ws.coordinator.RUnlock()
	root, err := rootForAccess(ws)
	if err != nil {
		return nil, "", err
	}
	abs, err := resolve(root, relative, false, true)
	if err != nil {
		return nil, "", err
	}
	for attempts := 0; attempts < 2; attempts++ {
		before, err := os.Stat(abs)
		if errors.Is(err, os.ErrNotExist) {
			return nil, "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND, "file not found")
		}
		if err != nil {
			return nil, "", err
		}
		if !before.Mode().IsRegular() {
			return nil, "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_TYPE_UNSUPPORTED, "entry is not a regular file")
		}
		if uint64(before.Size()) > s.config.MaxTextFileBytes {
			return nil, "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_TEXT_TOO_LARGE, "text file exceeds hard limit")
		}
		content, err := readFileContext(ctx, abs)
		if err != nil {
			return nil, "", err
		}
		after, err := os.Stat(abs)
		if err != nil {
			continue
		}
		if os.SameFile(before, after) && before.Size() == after.Size() && before.ModTime() == after.ModTime() {
			if !utf8.Valid(content) {
				return nil, "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_TEXT_NOT_UTF8, "file is not UTF-8")
			}
			return stableTextEntry(relative, before, content), string(content), nil
		}
	}
	return nil, "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_CONFLICT, "file changed during read")
}

func (s *Service) WriteText(ctx context.Context, codexID, relative, text, expectedRevision, token string, condition remotev1.WorkspaceWriteCondition) (*remotev1.WorkspaceEntry, error) {
	if !utf8.ValidString(text) {
		return nil, workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_TEXT_NOT_UTF8, "text is not UTF-8")
	}
	if uint64(len(text)) > s.config.MaxTextFileBytes {
		return nil, workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_TEXT_TOO_LARGE, "text exceeds hard limit")
	}
	ws, err := s.known(codexID)
	if err != nil {
		return nil, err
	}
	if err := lockMutationCoordinator(ctx, ws); err != nil {
		return nil, err
	}
	root, err := rootForAccess(ws)
	if err != nil {
		ws.coordinator.Unlock()
		return nil, err
	}
	abs, err := resolve(root, relative, false, false)
	if err != nil {
		ws.coordinator.Unlock()
		return nil, err
	}
	if err := validateMutationState(ws, token); err != nil {
		ws.coordinator.Unlock()
		return nil, err
	}
	if err := validateMutationTarget(abs, condition, expectedRevision, remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_REGULAR_FILE); err != nil {
		ws.coordinator.Unlock()
		return nil, err
	}
	backup, err := stageTarget(abs)
	if err != nil {
		ws.coordinator.Unlock()
		return nil, err
	}
	if err := atomicFile(abs, []byte(text)); err != nil {
		ws.coordinator.Unlock()
		return nil, rollbackMutation(backup, err)
	}
	entry, err := entryMetadata(root, relative, s.config.MaxTextFileBytes, true)
	if err != nil {
		ws.coordinator.Unlock()
		return nil, rollbackMutation(backup, err)
	}
	ws.mu.Lock()
	previous, state := s.advanceMutationLocked(ws)
	pending := beginPendingLocked(ws)
	sink := s.stateSink()
	ws.mu.Unlock()
	ws.coordinator.Unlock()
	if err := emit(ctx, sink, codexID, state); err != nil {
		if errors.Is(err, persistence.ErrEventCommitOutcomeUnknown) {
			backup.discard()
			finishPending(ws, pending)
			return nil, err
		}
		s.rollbackState(ws, state, previous)
		finishPending(ws, pending)
		return nil, rollbackMutation(backup, err)
	}
	finishPending(ws, pending)
	backup.discard()
	return entry, nil
}

func (s *Service) Upload(ctx context.Context, codexID, destination string, kind remotev1.WorkspaceUploadKind, content []byte, token string) (*remotev1.WorkspaceEntry, error) {
	if uint64(len(content)) > s.config.MaxInlineUploadBytes {
		return nil, workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_UPLOAD_TOO_LARGE, "upload exceeds hard limit")
	}
	ws, err := s.known(codexID)
	if err != nil {
		return nil, err
	}
	if err := lockMutationCoordinator(ctx, ws); err != nil {
		return nil, err
	}
	root, err := rootForAccess(ws)
	if err != nil {
		ws.coordinator.Unlock()
		return nil, err
	}
	abs, err := resolve(root, destination, false, false)
	if err != nil {
		ws.coordinator.Unlock()
		return nil, err
	}
	if kind != remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_REGULAR_FILE && kind != remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_ZIP_DIRECTORY {
		ws.coordinator.Unlock()
		return nil, workspaceErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, "upload kind is required")
	}
	if err := validateMutationState(ws, token); err != nil {
		ws.coordinator.Unlock()
		return nil, err
	}
	if err := validateUploadTarget(abs); err != nil {
		ws.coordinator.Unlock()
		return nil, err
	}
	backup, err := stageTarget(abs)
	if err != nil {
		ws.coordinator.Unlock()
		return nil, err
	}
	if kind == remotev1.WorkspaceUploadKind_WORKSPACE_UPLOAD_KIND_REGULAR_FILE {
		if err := atomicFile(abs, content); err != nil {
			ws.coordinator.Unlock()
			return nil, rollbackMutation(backup, err)
		}
	} else if err := s.extractZIPAtomic(ctx, abs, content); err != nil {
		ws.coordinator.Unlock()
		return nil, rollbackMutation(backup, err)
	}
	entry, err := entryMetadata(root, destination, s.config.MaxTextFileBytes, true)
	if err != nil {
		ws.coordinator.Unlock()
		return nil, rollbackMutation(backup, err)
	}
	ws.mu.Lock()
	previous, state := s.advanceMutationLocked(ws)
	pending := beginPendingLocked(ws)
	sink := s.stateSink()
	ws.mu.Unlock()
	ws.coordinator.Unlock()
	if err := emit(ctx, sink, codexID, state); err != nil {
		if errors.Is(err, persistence.ErrEventCommitOutcomeUnknown) {
			backup.discard()
			finishPending(ws, pending)
			return nil, err
		}
		s.rollbackState(ws, state, previous)
		finishPending(ws, pending)
		return nil, rollbackMutation(backup, err)
	}
	finishPending(ws, pending)
	backup.discard()
	return entry, nil
}

func (s *Service) Download(ctx context.Context, codexID, relative string) (*remotev1.WorkspaceEntry, remotev1.WorkspaceDownloadKind, string, []byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, "", nil, err
	}
	ws, err := s.known(codexID)
	if err != nil {
		return nil, 0, "", nil, err
	}
	ws.coordinator.RLock()
	defer ws.coordinator.RUnlock()
	root, err := rootForAccess(ws)
	if err != nil {
		return nil, 0, "", nil, err
	}
	abs, err := resolve(root, relative, false, true)
	if err != nil {
		return nil, 0, "", nil, err
	}
	info, err := os.Stat(abs)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, "", nil, workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND, "entry not found")
	}
	if err != nil {
		return nil, 0, "", nil, err
	}
	entry, err := entryMetadata(root, relative, s.config.MaxTextFileBytes, false)
	if err != nil {
		return nil, 0, "", nil, err
	}
	if info.Mode().IsRegular() {
		if uint64(info.Size()) > s.config.MaxInlineDownloadBytes {
			return nil, 0, "", nil, workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_DOWNLOAD_TOO_LARGE, "download exceeds hard limit")
		}
		content, err := readFileContext(ctx, abs)
		return entry, remotev1.WorkspaceDownloadKind_WORKSPACE_DOWNLOAD_KIND_REGULAR_FILE, filepath.Base(abs), content, err
	}
	if !info.IsDir() {
		return nil, 0, "", nil, workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_TYPE_UNSUPPORTED, "entry type cannot be downloaded")
	}
	content, err := s.zipDirectory(ctx, abs)
	if err != nil {
		return nil, 0, "", nil, err
	}
	return entry, remotev1.WorkspaceDownloadKind_WORKSPACE_DOWNLOAD_KIND_ZIP_DIRECTORY, filepath.Base(abs) + ".zip", content, nil
}

func (s *Service) known(id string) (*workspaceState, error) {
	s.mu.RLock()
	ws := s.states[id]
	s.mu.RUnlock()
	if ws == nil {
		return nil, workspaceErr(remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND, "codex not found")
	}
	return ws, nil
}
func (s *Service) stateSink() StateSink {
	s.mu.RLock()
	sink := s.sink
	s.mu.RUnlock()
	return sink
}
func emit(ctx context.Context, sink StateSink, id string, state *remotev1.WorkspaceAccessState) error {
	if sink == nil {
		return nil
	}
	return sink(ctx, id, cloneState(state))
}
func (s *Service) advanceMutationLocked(ws *workspaceState) (*remotev1.WorkspaceAccessState, *remotev1.WorkspaceAccessState) {
	previous := cloneState(ws.state)
	ws.state.Generation++
	ws.state.ObservedAtUnixMs = s.now().UnixMilli()
	ws.state.QuiescenceToken = newToken()
	return previous, cloneState(ws.state)
}
func (s *Service) rollbackState(ws *workspaceState, attempted, previous *remotev1.WorkspaceAccessState) {
	ws.mu.Lock()
	if proto.Equal(ws.state, attempted) {
		ws.state = previous
	}
	ws.mu.Unlock()
}
func cloneState(v *remotev1.WorkspaceAccessState) *remotev1.WorkspaceAccessState {
	if v == nil {
		return nil
	}
	return proto.Clone(v).(*remotev1.WorkspaceAccessState)
}
func newToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("q-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}
func workspaceErr(code remotev1.ErrorCode, message string) error {
	return &Error{Code: code, Err: errors.New(message)}
}

func canonicalRoot(root string) (string, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND, "workspace root does not exist")
		}
		return "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND, "workspace root is not a directory")
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND, "workspace root is not a directory")
	}
	return filepath.Clean(resolved), nil
}

// registerRoot permits a session identity to be imported while its historical
// cwd is temporarily absent. Existing roots must already resolve to a
// directory. Missing suffixes are anchored beneath the nearest canonical,
// existing ancestor so later access retains the usual symlink boundary.
func registerRoot(root string) (string, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if resolved, err := canonicalRoot(abs); err == nil {
		return resolved, nil
	}
	if _, err := os.Lstat(abs); err == nil || !errors.Is(err, os.ErrNotExist) {
		return "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND, "workspace root is not a directory")
	}

	ancestor := filepath.Dir(abs)
	missing := []string{filepath.Base(abs)}
	for {
		resolved, err := filepath.EvalSymlinks(ancestor)
		if err == nil {
			info, statErr := os.Stat(resolved)
			if statErr != nil || !info.IsDir() {
				return "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND, "workspace root is not a directory")
			}
			parts := append([]string{resolved}, missing...)
			return filepath.Clean(filepath.Join(parts...)), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND, "workspace root is not a directory")
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND, "workspace root does not exist")
		}
		missing = append([]string{filepath.Base(ancestor)}, missing...)
		ancestor = parent
	}
}

func canonicalRelative(relative string, allowRoot bool) error {
	if relative == "" {
		if allowRoot {
			return nil
		}
		return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_PATH_INVALID, "empty path is not allowed")
	}
	if !utf8.ValidString(relative) {
		return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_PATH_INVALID, "workspace path is not UTF-8")
	}
	if strings.Contains(relative, "\\") || strings.HasPrefix(relative, "/") || path.Clean(relative) != relative || relative == "." {
		return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_PATH_INVALID, "workspace path is not canonical")
	}
	for _, part := range strings.Split(relative, "/") {
		if part == "" || part == "." || part == ".." {
			return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_PATH_INVALID, "workspace path is not canonical")
		}
	}
	return nil
}
func within(root, candidate string) bool {
	rel, err := filepath.Rel(root, candidate)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}
func resolve(root, relative string, allowRoot, mustExist bool) (string, error) {
	if err := canonicalRelative(relative, allowRoot); err != nil {
		return "", err
	}
	abs := filepath.Join(root, filepath.FromSlash(relative))
	check := abs
	if !mustExist {
		check = filepath.Dir(abs)
	} else if info, err := os.Lstat(abs); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_TYPE_UNSUPPORTED, "symbolic links cannot be accessed directly")
	}
	resolved, err := filepath.EvalSymlinks(check)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND, "workspace parent does not exist")
		}
		return "", err
	}
	if !within(root, resolved) {
		return "", workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_PATH_OUTSIDE_ROOT, "workspace path resolves outside root")
	}
	if mustExist {
		abs = resolved
	}
	return abs, nil
}

func entryMetadata(root, relative string, textLimit uint64, inspectText bool) (*remotev1.WorkspaceEntry, error) {
	if err := canonicalRelative(relative, true); err != nil {
		return nil, err
	}
	abs := filepath.Join(root, filepath.FromSlash(relative))
	info, err := os.Lstat(abs)
	if err != nil {
		return nil, err
	}
	kind := remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_OTHER
	if info.Mode().IsRegular() {
		kind = remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_REGULAR_FILE
	} else if info.IsDir() {
		kind = remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_DIRECTORY
	} else if info.Mode()&os.ModeSymlink != 0 {
		kind = remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_SYMBOLIC_LINK
	}
	revision := ""
	if kind == remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_REGULAR_FILE {
		revision = metadataRevision(info)
	}
	viewable := kind == remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_REGULAR_FILE && uint64(info.Size()) <= textLimit
	if inspectText && viewable {
		if content, err := os.ReadFile(abs); err == nil {
			viewable = utf8.Valid(content)
		} else {
			viewable = false
		}
	}
	return &remotev1.WorkspaceEntry{RelativePath: relative, Name: path.Base(relative), Kind: kind, SizeBytes: uint64(max(info.Size(), 0)), ModifiedAtUnixMs: info.ModTime().UnixMilli(), Revision: revision, TextViewable: viewable, TextEditable: viewable}, nil
}

func stableTextEntry(relative string, info os.FileInfo, content []byte) *remotev1.WorkspaceEntry {
	return &remotev1.WorkspaceEntry{RelativePath: relative, Name: path.Base(relative), Kind: remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_REGULAR_FILE, SizeBytes: uint64(len(content)), ModifiedAtUnixMs: info.ModTime().UnixMilli(), Revision: metadataRevision(info), TextViewable: true, TextEditable: true}
}

func metadataRevision(info os.FileInfo) string {
	h := sha256.New()
	if info.Mode().IsRegular() {
		h.Write([]byte("file\x00"))
	} else if info.Mode()&os.ModeSymlink != 0 {
		h.Write([]byte("symlink\x00"))
	} else if info.IsDir() {
		h.Write([]byte("dir\x00"))
	} else {
		h.Write([]byte("other\x00"))
	}
	hashIdentity(h, info)
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

func hashIdentity(writer io.Writer, info os.FileInfo) {
	fmt.Fprintf(writer, "%d\x00%d\x00%d\x00%d\x00", info.Mode(), info.Size(), info.ModTime().UnixNano(), info.ModTime().UnixNano())
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		fmt.Fprintf(writer, "%d\x00%d\x00%d\x00%d\x00", stat.Dev, stat.Ino, stat.Ctim.Sec, stat.Ctim.Nsec)
	}
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.r.Read(p)
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return n, ctxErr
	}
	return n, err
}

func readFileContext(ctx context.Context, name string) ([]byte, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(contextReader{ctx: ctx, r: f})
}

func validateMutationState(ws *workspaceState, token string) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.state.ActiveAgentCount != 0 || ws.state.MutationStatus != remotev1.WorkspaceMutationStatus_WORKSPACE_MUTATION_STATUS_ALLOWED || token == "" || token != ws.state.QuiescenceToken {
		return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_BUSY, "workspace is busy or quiescence token is stale")
	}
	return nil
}

func validateMutationTarget(target string, condition remotev1.WorkspaceWriteCondition, expected string, want remotev1.WorkspaceEntryKind) error {
	info, err := os.Lstat(target)
	exists := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if condition != remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY && condition != remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_REPLACE_ONLY && condition != remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_UPSERT {
		return workspaceErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, "write condition is required")
	}
	if condition == remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_CREATE_ONLY {
		if expected != "" {
			return workspaceErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, "CREATE_ONLY revision must be empty")
		}
		if exists {
			return workspaceErr(remotev1.ErrorCode_ERROR_CODE_CONFLICT, "target already exists")
		}
	}
	if condition == remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_REPLACE_ONLY && expected == "" {
		return workspaceErr(remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST, "REPLACE_ONLY revision is required")
	}
	if condition == remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_REPLACE_ONLY && !exists {
		return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_NOT_FOUND, "target does not exist")
	}
	if condition == remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_UPSERT && expected == "" && exists {
		return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_REVISION_CONFLICT, "target unexpectedly exists")
	}
	if condition == remotev1.WorkspaceWriteCondition_WORKSPACE_WRITE_CONDITION_UPSERT && expected != "" && !exists {
		return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_REVISION_CONFLICT, "target unexpectedly does not exist")
	}
	if exists {
		kind := remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_OTHER
		if info.Mode().IsRegular() {
			kind = remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_REGULAR_FILE
		}
		if info.IsDir() {
			kind = remotev1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_DIRECTORY
		}
		if kind != want {
			return workspaceErr(remotev1.ErrorCode_ERROR_CODE_CONFLICT, "target kind conflicts with request")
		}
		if expected != "" {
			revision := metadataRevision(info)
			if revision != expected {
				return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_REVISION_CONFLICT, "workspace revision changed")
			}
		}
	}
	return nil
}

func validateUploadTarget(target string) error {
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() && !info.IsDir() {
		return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_TYPE_UNSUPPORTED, "upload target is a symbolic link or unsupported entry")
	}
	return nil
}

func atomicFile(target string, content []byte) error {
	parent := filepath.Dir(target)
	temp, err := os.CreateTemp(parent, ".codex-remote-write-*")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err = temp.Chmod(0o644); err == nil {
		_, err = temp.Write(content)
	}
	if err == nil {
		err = temp.Sync()
	}
	if closeErr := temp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(name, target)
}

type targetBackup struct {
	target  string
	backup  string
	existed bool
}

func stageTarget(target string) (*targetBackup, error) {
	b := &targetBackup{target: target}
	if _, err := os.Lstat(target); errors.Is(err, os.ErrNotExist) {
		return b, nil
	} else if err != nil {
		return nil, err
	}
	b.backup = filepath.Join(filepath.Dir(target), ".codex-remote-rollback-"+newToken())
	if err := os.Rename(target, b.backup); err != nil {
		return nil, err
	}
	b.existed = true
	return b, nil
}

func (b *targetBackup) rollback() error {
	if b == nil {
		return nil
	}
	if err := os.RemoveAll(b.target); err != nil {
		return err
	}
	if b.existed {
		return os.Rename(b.backup, b.target)
	}
	return nil
}

func (b *targetBackup) discard() {
	if b != nil && b.existed {
		_ = os.RemoveAll(b.backup)
	}
}

func rollbackMutation(backup *targetBackup, cause error) error {
	if err := backup.rollback(); err != nil {
		return errors.Join(cause, fmt.Errorf("workspace filesystem rollback failed: %w", err))
	}
	return cause
}

func (s *Service) extractZIPAtomic(ctx context.Context, target string, content []byte) error {
	reader, err := zip.NewReader(bytes.NewReader(content), int64(len(content)))
	if err != nil {
		return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_INVALID, "invalid ZIP archive")
	}
	if uint32(len(reader.File)) > s.config.MaxArchiveEntryCount {
		return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_TOO_MANY_ENTRIES, "archive has too many entries")
	}
	parent := filepath.Dir(target)
	staging, err := os.MkdirTemp(parent, ".codex-remote-upload-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	seen := make(map[string]struct{})
	var expanded uint64
	for _, item := range reader.File {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := strings.TrimSuffix(item.Name, "/")
		if err := canonicalRelative(name, false); err != nil {
			return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_INVALID, "archive path is invalid")
		}
		if _, ok := seen[name]; ok {
			return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_INVALID, "archive has duplicate paths")
		}
		seen[name] = struct{}{}
		mode := item.Mode()
		if mode&os.ModeSymlink != 0 || (!mode.IsRegular() && !mode.IsDir()) {
			return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_INVALID, "archive contains unsupported entry")
		}
		expanded += item.UncompressedSize64
		if expanded > s.config.MaxArchiveExpandedBytes {
			return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_EXPANDED_TOO_LARGE, "archive expansion exceeds hard limit")
		}
		dest := filepath.Join(staging, filepath.FromSlash(name))
		if !within(staging, dest) {
			return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_INVALID, "archive path escapes staging")
		}
		if mode.IsDir() {
			if err := os.MkdirAll(dest, 0o755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return err
		}
		src, err := item.Open()
		if err != nil {
			return err
		}
		out, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err == nil {
			_, err = io.Copy(out, contextReader{ctx: ctx, r: io.LimitReader(src, int64(s.config.MaxArchiveExpandedBytes)+1)})
		}
		src.Close()
		if out != nil {
			out.Close()
		}
		if err != nil {
			return err
		}
	}
	return replaceTree(target, staging)
}

func replaceTree(target, staging string) error {
	backup := target + ".codex-remote-backup-" + newToken()
	exists := false
	if _, err := os.Lstat(target); err == nil {
		exists = true
		if err := os.Rename(target, backup); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Rename(staging, target); err != nil {
		if exists {
			_ = os.Rename(backup, target)
		}
		return err
	}
	if exists {
		_ = os.RemoveAll(backup)
	}
	return nil
}

func (s *Service) zipDirectory(ctx context.Context, root string) ([]byte, error) {
	type node struct {
		abs, rel string
		info     os.FileInfo
	}
	var nodes []node
	var expanded uint64
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil {
			return err
		}
		if p == root {
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 || (!info.Mode().IsRegular() && !info.IsDir()) {
			return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ENTRY_TYPE_UNSUPPORTED, "directory contains unsupported entry")
		}
		rel, _ := filepath.Rel(root, p)
		nodes = append(nodes, node{p, filepath.ToSlash(rel), info})
		if uint32(len(nodes)) > s.config.MaxArchiveEntryCount {
			return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_TOO_MANY_ENTRIES, "directory has too many entries")
		}
		if info.Mode().IsRegular() {
			expanded += uint64(info.Size())
			if expanded > s.config.MaxArchiveExpandedBytes {
				return workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_ARCHIVE_EXPANDED_TOO_LARGE, "directory expansion exceeds hard limit")
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].rel < nodes[j].rel })
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name := node.rel
		if node.info.IsDir() {
			name += "/"
		}
		header, err := zip.FileInfoHeader(node.info)
		if err != nil {
			return nil, err
		}
		header.Name = name
		header.Method = zip.Deflate
		w, err := zw.CreateHeader(header)
		if err != nil {
			return nil, err
		}
		if node.info.Mode().IsRegular() {
			f, err := os.Open(node.abs)
			if err != nil {
				return nil, err
			}
			_, err = io.Copy(w, contextReader{ctx: ctx, r: f})
			f.Close()
			if err != nil {
				return nil, err
			}
		}
		if uint64(buf.Len()) > s.config.MaxInlineDownloadBytes {
			return nil, workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_DOWNLOAD_TOO_LARGE, "ZIP download exceeds hard limit")
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	if uint64(buf.Len()) > s.config.MaxInlineDownloadBytes {
		return nil, workspaceErr(remotev1.ErrorCode_ERROR_CODE_WORKSPACE_DOWNLOAD_TOO_LARGE, "ZIP download exceeds hard limit")
	}
	return buf.Bytes(), nil
}
