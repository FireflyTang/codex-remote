package blackbox_test

import (
	"path/filepath"
	"testing"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
)

func TestRuntimeRecoversAfterAppServerSocketDisconnect(t *testing.T) {
	requireScenario(t, "runtime-disconnect")
	c := dial(t)
	c.hello(t)
	codexID := createWatchedCodex(t, c)
	started := c.request(t, request("disconnect-start", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{CodexId: codexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "disconnect runtime transport"}}}}}})).GetStartTurn()
	if started == nil || started.TurnId == "" {
		t.Fatalf("StartTurn before disconnect=%+v", started)
	}

	// Give the runtime supervisor time to observe Adapter.Done; otherwise an
	// immediate READY read could race ahead of the forced socket close.
	time.Sleep(100 * time.Millisecond)
	deadline := time.Now().Add(8 * time.Second)
	for attempt := 0; ; attempt++ {
		response := c.request(t, request("runtime-health-"+time.Now().Format("150405.000000000"), &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}})).GetGetHost()
		if response != nil && response.Host != nil && response.Host.Runtime != nil && response.Host.Runtime.Status == remotev1.RuntimeStatus_RUNTIME_STATUS_READY {
			cwd := filepath.Join(testWorkspace(t), "after-runtime-reconnect")
			created := c.request(t, request("after-runtime-reconnect", &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{Cwd: cwd, CreateDirectoryIfMissing: true}})).GetCreateCodex()
			if created == nil || created.Codex == nil {
				t.Fatalf("runtime reported READY but app-server RPC did not recover: %+v", created)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime did not return READY after app-server socket loss; last=%+v", response)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
