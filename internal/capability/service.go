package capability

import (
	"sort"
	"strings"
	"sync"

	remotev1 "github.com/kylin1993/codex-remote/protocol/gen/go/codex/remote/v1"
	"google.golang.org/protobuf/proto"
)

type Service struct {
	mu    sync.RWMutex
	value *remotev1.Capabilities
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
	return &Service{value: &remotev1.Capabilities{FeatureIds: []string{"directories", "sessions", "history", "watch_replay", "approvals", "user_input", "interrupt", "diagnostic_audit"}, SessionSourceKinds: []string{"cli", "vscode", "exec", "appServer", "unknown"}, MaxWatchesPerConnection: maxWatches, MaxPageSize: maxPage}}
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
