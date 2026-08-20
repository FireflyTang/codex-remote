package blackbox_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/websocket"
	remotev1 "github.com/kylin1993/codex-remote/protocol/gen/go/codex/remote/v1"
)

func TestLargeVendorOutputIsExplicitlyBounded(t *testing.T) {
	requireScenario(t, "large")
	c := dial(t)
	hello := c.hello(t)
	if hello.MaxFrameBytes != 64<<10 {
		t.Fatalf("ServerHello.max_frame_bytes=%d, want %d", hello.MaxFrameBytes, 64<<10)
	}
	codexID := createWatchedCodex(t, c)
	started := c.request(t, request("large-start", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{
		CodexId: codexID,
		Input:   []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "produce oversized output"}}}},
	}})).GetStartTurn()
	if started == nil || started.TurnId == "" {
		t.Fatalf("StartTurn=%+v", started)
	}

	var completed *remotev1.Item
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		frame, err := nextFrameBefore(t, c, deadline)
		if err != nil {
			t.Fatalf("large output did not produce a bounded completion while keeping the connection usable: %v", err)
		}
		event := frame.GetEvent()
		if event == nil {
			continue
		}
		if delta := event.GetItemDelta(); delta != nil && len(delta.GetText()) >= int(hello.MaxFrameBytes) {
			t.Fatalf("unbounded delta bytes=%d exceeds advertised frame budget=%d", len(delta.GetText()), hello.MaxFrameBytes)
		}
		if item := event.GetItemCompleted(); item != nil {
			completed = item.Item
		}
		if turn := event.GetTurnUpdated(); turn != nil && turn.Status == remotev1.TurnStatus_TURN_STATUS_COMPLETED {
			break
		}
	}
	if completed == nil {
		t.Fatal("large output did not produce ItemCompleted within 5s")
	}
	if completed == nil || !explicitlyIncomplete(completed.Completeness) {
		t.Fatalf("oversized completed item lacks truncation metadata: %+v", completed)
	}

	history := c.request(t, request("large-history", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: codexID}})).GetListHistory()
	if history == nil || history.History == nil || !explicitlyIncomplete(history.History.Completeness) || history.History.HistoryComplete {
		t.Fatalf("oversized history is not explicitly incomplete: %+v", history)
	}
	if len(history.History.Turns) == 0 {
		t.Fatalf("oversized history lacks bounded turn metadata: %+v", history.History)
	}
	boundedTurn := history.History.Turns[0]
	if len(boundedTurn.Items) == 0 {
		if !explicitlyIncomplete(boundedTurn.Completeness) {
			t.Fatalf("dropped oversized history item is not disclosed on its turn: %+v", boundedTurn)
		}
	} else if !explicitlyIncomplete(boundedTurn.Items[0].Completeness) {
		t.Fatalf("retained oversized history item lacks truncation metadata: %+v", boundedTurn.Items[0])
	}
	if got := c.request(t, request("large-still-usable", &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}})).GetGetHost(); got == nil {
		t.Fatal("connection became unusable after bounded large output")
	}
}

func TestSlowConsumerGetsExplicitProtocolClose(t *testing.T) {
	requireScenario(t, "burst")
	c := dial(t)
	hello := c.hello(t)
	cwd := testWorkspace(t)
	created := c.request(t, request("burst-create", &remotev1.Request_CreateCodex{CreateCodex: &remotev1.CreateCodexRequest{Cwd: cwd, CreateDirectoryIfMissing: true}})).GetCreateCodex()
	if created == nil || created.Codex == nil {
		t.Fatalf("CreateCodex=%+v", created)
	}
	codexID := created.Codex.CodexId
	initial := c.request(t, request("burst-initial-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID}})).GetWatchCodex()
	if initial == nil || initial.Mode != remotev1.WatchMode_WATCH_MODE_RESET {
		t.Fatalf("initial Watch=%+v", initial)
	}
	c.request(t, request("burst-unwatch", &remotev1.Request_UnwatchCodex{UnwatchCodex: &remotev1.UnwatchCodexRequest{CodexId: codexID}}))
	started := c.request(t, request("burst-start", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{
		CodexId: codexID,
		Input:   []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "burst"}}}},
	}})).GetStartTurn()
	if started == nil || started.TurnId == "" {
		t.Fatalf("StartTurn=%+v", started)
	}
	completedBy := time.Now().Add(10 * time.Second)
	for attempt := 0; ; attempt++ {
		history := c.request(t, request("burst-history-"+time.Now().Format("150405.000000000"), &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: codexID}})).GetListHistory()
		if history != nil && history.History != nil && len(history.History.Turns) > 0 && history.History.Turns[0].Status == remotev1.TurnStatus_TURN_STATUS_COMPLETED {
			break
		}
		if time.Now().After(completedBy) {
			t.Fatalf("burst fixture did not complete before replay test; last history=%+v", history)
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Replaying hundreds of retained events synchronously makes queue pressure
	// deterministic without depending on app-server/SQLite production speed.
	c.writeFrame(t, &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: request("burst-replay-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{
		CodexId: codexID, AfterEventSeq: &initial.HeadEventSeq, AfterHostRunId: hello.HostRunId,
	}})}})

	// Intentionally stop consuming while Watch synchronously queues its replay.
	time.Sleep(750 * time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		typ, raw, err := c.conn.Read(ctx)
		cancel()
		if err != nil {
			var closeErr websocket.CloseError
			if errors.As(err, &closeErr) {
				t.Fatalf("WebSocket closed before application Close=SLOW_CONSUMER: %v", closeErr)
			}
			t.Fatalf("slow consumer read failed before application Close: %v", err)
		}
		if typ != websocket.MessageText {
			continue
		}
		frame := new(remotev1.Frame)
		if err := strictJSON.Unmarshal(raw, frame); err != nil {
			t.Fatalf("decode slow-consumer frame: %v", err)
		}
		if closeFrame := frame.GetClose(); closeFrame != nil {
			if closeFrame.Code != remotev1.CloseCode_CLOSE_CODE_SLOW_CONSUMER || !closeFrame.ReconnectAllowed {
				t.Fatalf("slow-consumer Close=%+v", closeFrame)
			}
			c2 := dial(t)
			c2.hello(t)
			if got := c2.request(t, request("burst-reconnect", &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}})).GetGetHost(); got == nil {
				t.Fatal("Host unusable after reconnect from SLOW_CONSUMER")
			}
			return
		}
	}
	t.Fatal("slow consumer did not receive application Close=SLOW_CONSUMER")
}

func TestMultipleLargeItemsBoundCollectionsAndKeepConnectionUsable(t *testing.T) {
	requireScenario(t, "multi-large")
	c := dial(t)
	c.hello(t)
	codexID := createWatchedCodex(t, c)
	c.request(t, request("multi-large-start", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{CodexId: codexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "many large items"}}}}}}))
	for {
		event := c.readUntil(t, func(frame *remotev1.Frame) bool { return frame.GetEvent() != nil }).GetEvent()
		if turn := event.GetTurnUpdated(); turn != nil && turn.Status == remotev1.TurnStatus_TURN_STATUS_COMPLETED {
			break
		}
	}
	history := c.request(t, request("multi-large-history", &remotev1.Request_ListHistory{ListHistory: &remotev1.ListHistoryRequest{CodexId: codexID}})).GetListHistory()
	if history == nil || history.History == nil || !explicitlyIncomplete(history.History.Completeness) || history.History.HistoryComplete {
		t.Fatalf("multi-item history lacks collection completeness: %+v", history)
	}
	reset := c.request(t, request("multi-large-reset", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID}})).GetWatchCodex()
	if reset == nil || reset.ResetView == nil || !explicitlyIncomplete(reset.ResetView.Completeness) {
		t.Fatalf("multi-item CurrentView lacks collection completeness: %+v", reset)
	}
	if got := c.request(t, request("multi-large-still-usable", &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}})).GetGetHost(); got == nil {
		t.Fatal("connection unusable after multi-item collection bounding")
	}
}

func explicitlyIncomplete(value *remotev1.Completeness) bool {
	return value != nil && (value.Truncated || value.Incomplete) && value.Reason != ""
}

func nextFrameBefore(t *testing.T, c *wireClient, deadline time.Time) (*remotev1.Frame, error) {
	t.Helper()
	if len(c.inbox) > 0 {
		frame := c.inbox[0]
		c.inbox = c.inbox[1:]
		return frame, nil
	}
	for {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		typ, raw, err := c.conn.Read(ctx)
		cancel()
		if err != nil {
			return nil, err
		}
		if typ != websocket.MessageText {
			continue
		}
		frame := new(remotev1.Frame)
		if err := strictJSON.Unmarshal(raw, frame); err != nil {
			return nil, err
		}
		if ping := frame.GetPing(); ping != nil {
			c.writeFrame(t, &remotev1.Frame{Payload: &remotev1.Frame_Pong{Pong: &remotev1.Pong{Nonce: ping.Nonce, PingSentAtUnixMs: ping.SentAtUnixMs, PongSentAtUnixMs: time.Now().UnixMilli()}}})
			continue
		}
		return frame, nil
	}
}
