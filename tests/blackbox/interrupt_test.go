package blackbox_test

import (
	"testing"
	"time"

	remotev1 "github.com/kylin1993/codex-remote/protocol/gen/go/codex/remote/v1"
)

func TestInterruptLifecycleAndDedup(t *testing.T) {
	requireScenario(t, "interrupt")
	c := dial(t)
	c.hello(t)
	codexID := createWatchedCodex(t, c)
	started := c.request(t, request("interrupt-start", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{CodexId: codexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "wait"}}}}}})).GetStartTurn()
	if started == nil || started.TurnId == "" {
		t.Fatalf("StartTurn=%+v", started)
	}
	time.Sleep(20 * time.Millisecond)
	id := "interrupt-once"
	req := request(id, &remotev1.Request_InterruptTurn{InterruptTurn: &remotev1.InterruptTurnRequest{CodexId: codexID, TurnId: started.TurnId}})
	interrupted := c.request(t, req).GetInterruptTurn()
	if interrupted == nil || interrupted.TurnId != started.TurnId {
		t.Fatalf("InterruptTurn=%+v", interrupted)
	}
	replay := c.request(t, req).GetInterruptTurn()
	if replay == nil || !replay.Deduplicated || replay.TurnId != started.TurnId {
		t.Fatalf("dedup InterruptTurn=%+v", replay)
	}
	conflict := c.request(t, request(id, &remotev1.Request_InterruptTurn{InterruptTurn: &remotev1.InterruptTurnRequest{CodexId: codexID, TurnId: "changed"}}))
	if conflict.GetError() == nil || conflict.GetError().Code != remotev1.ErrorCode_ERROR_CODE_CONFLICT {
		t.Fatalf("changed-payload InterruptTurn=%+v", conflict)
	}
	c.readUntil(t, func(f *remotev1.Frame) bool {
		return f.GetEvent() != nil && f.GetEvent().GetTurnUpdated() != nil && f.GetEvent().GetTurnUpdated().Status == remotev1.TurnStatus_TURN_STATUS_INTERRUPTED
	})
}
