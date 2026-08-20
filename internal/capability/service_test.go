package capability

import "testing"

func TestObserveSessionSourcesFromRuntime(t *testing.T) {
	s := New(4, 20)
	s.ObserveSessionSources("jetbrains", " cli ", "")
	got := s.Get()
	found := false
	for _, source := range got.SessionSourceKinds {
		found = found || source == "jetbrains"
	}
	if !found || got.MaxWatchesPerConnection != 4 || got.MaxPageSize != 20 {
		t.Fatalf("capabilities %+v", got)
	}
}
