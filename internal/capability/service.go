package capability

import (
	"errors"
	"sort"
	"strings"
	"sync"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"google.golang.org/protobuf/proto"
)

type Service struct {
	mu    sync.RWMutex
	value *remotev1.Capabilities
}

const (
	DefaultMaxTextFileBytes        = uint64(512 << 10)
	DefaultMaxInlineUploadBytes    = uint64(2 << 20)
	DefaultMaxInlineDownloadBytes  = uint64(2 << 20)
	DefaultMaxArchiveExpandedBytes = uint64(32 << 20)
	DefaultMaxArchiveEntryCount    = uint32(1000)
)

func DefaultWorkspaceCapabilities() *remotev1.WorkspaceCapabilities {
	return &remotev1.WorkspaceCapabilities{
		MaxTextFileBytes:        DefaultMaxTextFileBytes,
		MaxInlineUploadBytes:    DefaultMaxInlineUploadBytes,
		MaxInlineDownloadBytes:  DefaultMaxInlineDownloadBytes,
		MaxArchiveExpandedBytes: DefaultMaxArchiveExpandedBytes,
		MaxArchiveEntryCount:    DefaultMaxArchiveEntryCount,
	}
}

// WorkspaceCapabilitiesForFrame keeps binary payloads at most half of a frame
// for ProtoJSON base64 expansion. Text is limited to one eighth because a
// valid UTF-8 control byte can occupy six JSON bytes (for example, \u0000),
// while the surrounding Frame/Response/WorkspaceEntry still needs headroom.
// The remaining archive limits do not travel inline in their expanded form.
func WorkspaceCapabilitiesForFrame(maxFrameBytes uint64) (*remotev1.WorkspaceCapabilities, error) {
	if maxFrameBytes < 8 {
		return nil, errors.New("max frame bytes must allow a positive workspace payload")
	}
	caps := DefaultWorkspaceCapabilities()
	textLimit := maxFrameBytes / 8
	binaryLimit := maxFrameBytes / 2
	caps.MaxTextFileBytes = min(caps.MaxTextFileBytes, textLimit)
	caps.MaxInlineUploadBytes = min(caps.MaxInlineUploadBytes, binaryLimit)
	caps.MaxInlineDownloadBytes = min(caps.MaxInlineDownloadBytes, binaryLimit)
	return caps, nil
}

func (s *Service) SetWorkspaceCapabilities(v *remotev1.WorkspaceCapabilities) error {
	if v == nil || v.GetMaxTextFileBytes() == 0 || v.GetMaxInlineUploadBytes() == 0 || v.GetMaxInlineDownloadBytes() == 0 || v.GetMaxArchiveExpandedBytes() == 0 || v.GetMaxArchiveEntryCount() == 0 {
		return errors.New("all workspace capability limits must be positive")
	}
	s.mu.Lock()
	s.value.Workspace = proto.Clone(v).(*remotev1.WorkspaceCapabilities)
	s.mu.Unlock()
	return nil
}

// ObserveSessionSources incorporates source kinds actually reported by the
// connected app-server without replacing the protocol's known baseline.
func (s *Service) ObserveSessionSources(sources ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[string]struct{}, len(s.value.SessionSourceKinds)+len(sources))
	for _, source := range s.value.SessionSourceKinds {
		seen[source] = struct{}{}
	}
	for _, source := range sources {
		source = strings.TrimSpace(source)
		if source != "" {
			seen[source] = struct{}{}
		}
	}
	s.value.SessionSourceKinds = s.value.SessionSourceKinds[:0]
	for source := range seen {
		s.value.SessionSourceKinds = append(s.value.SessionSourceKinds, source)
	}
	sort.Strings(s.value.SessionSourceKinds)
}

func New(maxWatches, maxPage uint32) *Service {
	return &Service{value: &remotev1.Capabilities{FeatureIds: []string{"directories", "sessions", "history", "watch_replay", "approvals", "user_input", "interrupt", "diagnostic_audit", "management_lease", "unmanage_codex", "rename_codex", "forget_codex", "workspace"}, SessionSourceKinds: []string{"cli", "vscode", "exec", "appServer", "unknown"}, MaxWatchesPerConnection: maxWatches, MaxPageSize: maxPage, Workspace: DefaultWorkspaceCapabilities()}}
}
func (s *Service) Get() *remotev1.Capabilities {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return proto.Clone(s.value).(*remotev1.Capabilities)
}
func (s *Service) Update(v *remotev1.Capabilities) {
	if v == nil {
		return
	}
	s.mu.Lock()
	s.value = proto.Clone(v).(*remotev1.Capabilities)
	s.mu.Unlock()
}
