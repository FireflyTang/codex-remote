package blackbox_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	remotev1 "github.com/FireflyTang/codex-remote-protocol/gen/go/codex/remote/v1"
	"github.com/coder/websocket"
	"google.golang.org/protobuf/encoding/protojson"
)

const subprotocol = "codex-remote.v1.protojson"

var strictJSON = protojson.UnmarshalOptions{DiscardUnknown: false}

type wireClient struct {
	conn  *websocket.Conn
	mu    sync.Mutex
	inbox []*remotev1.Frame
}

func targetURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("CODEX_REMOTE_BLACKBOX_URL")
	if u == "" {
		t.Skip("CODEX_REMOTE_BLACKBOX_URL is unset; a runnable Host /connect listener is not available yet")
	}
	return u
}

func dial(t *testing.T) *wireClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, resp, err := websocket.Dial(ctx, targetURL(t), &websocket.DialOptions{Subprotocols: []string{subprotocol}})
	if err != nil {
		if resp != nil {
			t.Fatalf("dial Host: %v (HTTP %s)", err, resp.Status)
		}
		t.Fatalf("dial Host: %v", err)
	}
	if got := c.Subprotocol(); got != subprotocol {
		c.CloseNow()
		t.Fatalf("negotiated subprotocol %q, want %q", got, subprotocol)
	}
	// The wire driver must be able to inspect a frame before asserting it fits
	// the Host-advertised budget. coder/websocket otherwise defaults to 32 KiB,
	// below both the normal and limit-test ServerHello values.
	c.SetReadLimit(8 << 20)
	client := &wireClient{conn: c}
	t.Cleanup(func() { _ = c.CloseNow() })
	return client
}

func (c *wireClient) writeFrame(t *testing.T, frame *remotev1.Frame) {
	t.Helper()
	raw, err := protojson.MarshalOptions{UseProtoNames: false}.Marshal(frame)
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	c.writeRaw(t, websocket.MessageText, raw)
}

func (c *wireClient) writeRaw(t *testing.T, typ websocket.MessageType, raw []byte) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.conn.Write(ctx, typ, raw); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func (c *wireClient) readFrame(t *testing.T) *remotev1.Frame {
	t.Helper()
	if len(c.inbox) > 0 {
		frame := c.inbox[0]
		c.inbox = c.inbox[1:]
		return frame
	}
	return c.readNetworkFrame(t)
}

func (c *wireClient) readNetworkFrame(t *testing.T) *remotev1.Frame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	typ, raw, err := c.conn.Read(ctx)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("message type=%v, want text", typ)
	}
	frame := new(remotev1.Frame)
	if err := strictJSON.Unmarshal(raw, frame); err != nil {
		t.Fatalf("decode ProtoJSON frame %q: %v", raw, err)
	}
	return frame
}

func (c *wireClient) readUntil(t *testing.T, match func(*remotev1.Frame) bool) *remotev1.Frame {
	t.Helper()
	for i, frame := range c.inbox {
		if match(frame) {
			c.inbox = append(c.inbox[:i], c.inbox[i+1:]...)
			return frame
		}
	}
	for i := 0; i < 128; i++ {
		frame := c.readNetworkFrame(t)
		if ping := frame.GetPing(); ping != nil {
			c.writeFrame(t, &remotev1.Frame{Payload: &remotev1.Frame_Pong{Pong: &remotev1.Pong{Nonce: ping.Nonce, PingSentAtUnixMs: ping.SentAtUnixMs, PongSentAtUnixMs: time.Now().UnixMilli()}}})
			continue
		}
		if match(frame) {
			return frame
		}
		c.inbox = append(c.inbox, frame)
	}
	t.Fatal("matching frame not received after 128 frames")
	return nil
}

func (c *wireClient) hello(t *testing.T) *remotev1.ServerHello {
	t.Helper()
	c.writeFrame(t, &remotev1.Frame{Payload: &remotev1.Frame_ClientHello{ClientHello: &remotev1.ClientHello{
		ClientId: "blackbox-client", ClientRunId: fmt.Sprintf("run-%d", time.Now().UnixNano()),
		ProtocolVersion: &remotev1.ProtocolVersion{Major: 1, Minor: 1}, ClientName: "blackbox", ClientVersion: "test",
	}}})
	hello := c.readUntil(t, func(f *remotev1.Frame) bool { return f.GetServerHello() != nil }).GetServerHello()
	if hello.ProtocolVersion == nil || hello.ProtocolVersion.Major != 1 || hello.ProtocolVersion.Minor != 1 {
		t.Fatalf("server protocol=%v, want 1.1", hello.ProtocolVersion)
	}
	if hello.ConnectionId == "" || hello.HostId == "" || hello.HostRunId == "" {
		t.Fatalf("server hello missing identities: %+v", hello)
	}
	return hello
}

func (c *wireClient) request(t *testing.T, request *remotev1.Request) *remotev1.Response {
	t.Helper()
	c.writeFrame(t, &remotev1.Frame{Payload: &remotev1.Frame_Request{Request: request}})
	return c.readUntil(t, func(f *remotev1.Frame) bool {
		return f.GetResponse() != nil && f.GetResponse().RequestId == request.RequestId
	}).GetResponse()
}

func expectClose(t *testing.T, c *wireClient) websocket.CloseError {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		_, _, err := c.conn.Read(ctx)
		if err == nil {
			continue
		}
		var closeErr websocket.CloseError
		if errors.As(err, &closeErr) {
			return closeErr
		}
		t.Fatalf("expected WebSocket close, got %v", err)
	}
}
