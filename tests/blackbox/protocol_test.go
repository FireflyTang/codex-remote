package blackbox_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/coder/websocket"
	remotev1 "github.com/kylin1993/codex-remote/protocol/gen/go/codex/remote/v1"
)

func TestUpgradeRejectsMissingAndWrongSubprotocol(t *testing.T) {
	u := targetURL(t)
	for _, tc := range []struct {
		name string
		subs []string
	}{
		{"missing", nil},
		{"wrong", []string{"not-codex-remote"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, resp, err := websocket.Dial(ctx, u, &websocket.DialOptions{Subprotocols: tc.subs})
			if conn != nil {
				conn.CloseNow()
				t.Fatal("upgrade unexpectedly succeeded")
			}
			if err == nil || resp == nil || resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("err=%v status=%v, want HTTP 400", err, status(resp))
			}
		})
	}
}

func TestWrongPathDoesNotUpgrade(t *testing.T) {
	u := targetURL(t)
	if suffix := os.Getenv("CODEX_REMOTE_BLACKBOX_BAD_PATH"); suffix != "" {
		u = suffix
	} else {
		u += "/not-found"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, u, &websocket.DialOptions{Subprotocols: []string{subprotocol}})
	if conn != nil {
		conn.CloseNow()
		t.Fatal("wrong path unexpectedly upgraded")
	}
	if err == nil || resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("err=%v status=%v, want HTTP 404", err, status(resp))
	}
}

func status(resp *http.Response) any {
	if resp == nil {
		return nil
	}
	return resp.StatusCode
}

func TestHandshakeV10AndGetHost(t *testing.T) {
	c := dial(t)
	hello := c.hello(t)
	resp := c.request(t, &remotev1.Request{RequestId: "get-host", Request: &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}}})
	if resp.GetError() != nil {
		t.Fatalf("GetHost error: %v", resp.GetError())
	}
	got := resp.GetGetHost()
	if got == nil || got.Host == nil || got.Host.HostId != hello.HostId {
		t.Fatalf("GetHost host=%v, hello host_id=%q", got, hello.HostId)
	}
}

func TestHandshakeRejectsUnsupportedMinor(t *testing.T) {
	c := dial(t)
	c.writeFrame(t, &remotev1.Frame{Payload: &remotev1.Frame_ClientHello{ClientHello: &remotev1.ClientHello{ClientId: "bad-minor", ClientRunId: "run", ProtocolVersion: &remotev1.ProtocolVersion{Major: 1, Minor: 1}}}})
	f := c.readUntil(t, func(f *remotev1.Frame) bool { return f.GetClose() != nil })
	if f.GetClose().Code != remotev1.CloseCode_CLOSE_CODE_PROTOCOL_VERSION_UNSUPPORTED {
		t.Fatalf("close code=%v", f.GetClose().Code)
	}
}

func TestHandshakeRequiredBeforeRequest(t *testing.T) {
	c := dial(t)
	c.writeFrame(t, &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: &remotev1.Request{RequestId: "too-early", Request: &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}}}}})
	f := c.readUntil(t, func(f *remotev1.Frame) bool { return f.GetClose() != nil })
	if f.GetClose().Code != remotev1.CloseCode_CLOSE_CODE_HELLO_REQUIRED {
		t.Fatalf("close code=%v, want HELLO_REQUIRED", f.GetClose().Code)
	}
}

func TestInvalidAndBinaryFramesCloseConnection(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  websocket.MessageType
		raw  []byte
	}{
		{"invalid-json", websocket.MessageText, []byte("{")},
		{"empty-frame", websocket.MessageText, []byte("{}")},
		{"unknown-field", websocket.MessageText, []byte(`{"mystery":{}}`)},
		{"binary", websocket.MessageBinary, []byte{0x00, 0x01}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := dial(t)
			c.writeRaw(t, tc.typ, tc.raw)
			_ = expectClose(t, c)
		})
	}
}

func TestConcurrentRequestsCorrelateByRequestID(t *testing.T) {
	c := dial(t)
	c.hello(t)
	const count = 32
	responses := make(chan string, count)
	errCh := make(chan error, count)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("parallel-%02d", i)
		c.writeFrame(t, &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: &remotev1.Request{RequestId: id, Request: &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}}}}})
	}
	go func() {
		seen := map[string]bool{}
		for len(seen) < count {
			f := c.readFrame(t)
			if r := f.GetResponse(); r != nil {
				if seen[r.RequestId] {
					errCh <- fmt.Errorf("duplicate response %s", r.RequestId)
					return
				}
				seen[r.RequestId] = true
				responses <- r.RequestId
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := 0; i < count; i++ {
		select {
		case err := <-errCh:
			t.Fatal(err)
		case <-responses:
		case <-ctx.Done():
			t.Fatalf("received %d/%d responses", i, count)
		}
	}
}

func TestExpiredDeadlineDoesNotDispatch(t *testing.T) {
	c := dial(t)
	c.hello(t)
	r := c.request(t, &remotev1.Request{RequestId: "expired", DeadlineUnixMs: time.Now().Add(-time.Second).UnixMilli(), Request: &remotev1.Request_GetHost{GetHost: &remotev1.GetHostRequest{}}})
	if r.GetError() == nil || r.GetError().Code != remotev1.ErrorCode_ERROR_CODE_DEADLINE_EXCEEDED {
		t.Fatalf("response error=%v, want DEADLINE_EXCEEDED", r.GetError())
	}
}
