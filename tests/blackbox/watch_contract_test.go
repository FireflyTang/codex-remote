package blackbox_test

import (
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	remotev1 "github.com/kylin1993/codex-remote/protocol/gen/go/codex/remote/v1"
)

func TestWatchValidatesCodexRequestIDAndDeadline(t *testing.T) {
	requireScenario(t, "normal")
	c := dial(t)
	c.hello(t)
	unknown := c.request(t, request("watch-unknown", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: "missing-codex"}}))
	if unknown.GetError() == nil || unknown.GetError().Code != remotev1.ErrorCode_ERROR_CODE_CODEX_NOT_FOUND {
		t.Fatalf("unknown Watch=%+v", unknown)
	}

	empty := &remotev1.Request{Request: &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: "missing-codex"}}}
	c.writeFrame(t, &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: empty}})
	emptyResponse := c.readUntil(t, func(frame *remotev1.Frame) bool {
		return frame.GetResponse() != nil && frame.GetResponse().RequestId == ""
	}).GetResponse()
	if emptyResponse.GetError() == nil || emptyResponse.GetError().Code != remotev1.ErrorCode_ERROR_CODE_INVALID_REQUEST {
		t.Fatalf("empty request_id Watch=%+v", emptyResponse)
	}

	codexID := createWatchedCodex(t, c)
	expired := request("expired-watch", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID}})
	expired.DeadlineUnixMs = time.Now().Add(-time.Second).UnixMilli()
	response := c.request(t, expired)
	if response.GetError() == nil || response.GetError().Code != remotev1.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED {
		t.Fatalf("expired Watch=%+v", response)
	}
	expiredUnwatch := request("expired-unwatch", &remotev1.Request_UnwatchCodex{UnwatchCodex: &remotev1.UnwatchCodexRequest{CodexId: codexID}})
	expiredUnwatch.DeadlineUnixMs = time.Now().Add(-time.Second).UnixMilli()
	response = c.request(t, expiredUnwatch)
	if response.GetError() == nil || response.GetError().Code != remotev1.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED {
		t.Fatalf("expired Unwatch=%+v", response)
	}
}

func TestRewatchResponsePrecedesReplacementStream(t *testing.T) {
	requireScenario(t, "rewatch")
	c := dial(t)
	hello := c.hello(t)
	codexID := createWatchedCodex(t, c)
	c.request(t, request("rewatch-start", &remotev1.Request_StartTurn{StartTurn: &remotev1.StartTurnRequest{CodexId: codexID, Input: []*remotev1.UserInputPart{{Content: &remotev1.UserInputPart_Text{Text: &remotev1.TextInput{Text: "rewatch"}}}}}}))
	before := c.readUntil(t, func(frame *remotev1.Frame) bool {
		delta := frame.GetEvent().GetItemDelta()
		return delta != nil && delta.GetText() == "rewatch-before"
	}).GetEvent()
	c.inbox = nil
	after := before.EventSeq
	req := request("rewatch-replace", &remotev1.Request_WatchCodex{WatchCodex: &remotev1.WatchCodexRequest{CodexId: codexID, AfterEventSeq: &after, AfterHostRunId: hello.HostRunId}})
	c.writeFrame(t, &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: req}})
	for {
		frame := c.readNetworkFrame(t)
		if ping := frame.GetPing(); ping != nil {
			c.writeFrame(t, &remotev1.Frame{Payload: &remotev1.Frame_Pong{Pong: &remotev1.Pong{Nonce: ping.Nonce, PingSentAtUnixMs: ping.SentAtUnixMs, PongSentAtUnixMs: time.Now().UnixMilli()}}})
			continue
		}
		if event := frame.GetEvent(); event != nil {
			t.Fatalf("replacement Watch emitted event seq=%d before its Response: %+v", event.EventSeq, event)
		}
		if response := frame.GetResponse(); response != nil && response.RequestId == req.RequestId {
			watch := response.GetWatchCodex()
			if watch == nil || watch.Mode != remotev1.WatchMode_WATCH_MODE_RESUMED {
				t.Fatalf("replacement Watch=%+v", response)
			}
			break
		}
	}
	next := c.readUntil(t, func(frame *remotev1.Frame) bool { return frame.GetEvent() != nil }).GetEvent()
	if next.EventSeq <= after {
		t.Fatalf("old watch event leaked after replacement: seq=%d checkpoint=%d", next.EventSeq, after)
	}
}

func TestInboundOversizeGetsFormalFrameTooLargeClose(t *testing.T) {
	requireScenario(t, "normal")
	c := dial(t)
	hello := c.hello(t)
	raw := []byte(`{"request":{"requestId":"` + strings.Repeat("x", int(hello.MaxFrameBytes)+1024) + `","getHost":{}}}`)
	c.writeRaw(t, websocket.MessageText, raw)
	frame := c.readUntil(t, func(frame *remotev1.Frame) bool { return frame.GetClose() != nil })
	if frame.GetClose().Code != remotev1.CloseCode_CLOSE_CODE_FRAME_TOO_LARGE {
		t.Fatalf("oversize Close=%+v", frame.GetClose())
	}
}
