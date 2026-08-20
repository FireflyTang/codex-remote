package adapter

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func listenFake(t *testing.T, serve func(*websocket.Conn)) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "app.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err == nil {
			serve(c)
		}
	})}
	t.Cleanup(func() { _ = server.Close() })
	go server.Serve(ln)
	return path
}

func TestClientReceivesLargeVendorMessage(t *testing.T) {
	payload := strings.Repeat("v", 260<<10)
	path := listenFake(t, func(c *websocket.Conn) {
		defer c.CloseNow()
		_ = encodeWS(c, map[string]any{"method": "vendor/large", "params": map[string]any{"payload": payload}})
		<-time.After(100 * time.Millisecond)
	})
	c, err := Dial(context.Background(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	select {
	case msg := <-c.Incoming():
		if msg.Method != "vendor/large" {
			t.Fatalf("method=%q", msg.Method)
		}
		var params struct {
			Payload string `json:"payload"`
		}
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			t.Fatal(err)
		}
		if len(params.Payload) != len(payload) {
			t.Fatalf("payload bytes=%d want=%d", len(params.Payload), len(payload))
		}
	case <-c.Done():
		t.Fatal("large message closed adapter connection")
	case <-time.After(2 * time.Second):
		t.Fatal("large message was not delivered")
	}
}
func decodeMap(t *testing.T, c *websocket.Conn) map[string]json.RawMessage {
	t.Helper()
	var m map[string]json.RawMessage
	_, raw, err := c.Read(context.Background())
	if err == nil {
		err = json.Unmarshal(raw, &m)
	}
	if err != nil {
		t.Error(err)
	}
	return m
}
func encodeWS(c *websocket.Conn, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.Write(context.Background(), websocket.MessageText, raw)
}

func TestClientMultiplexesOutOfOrderResponses(t *testing.T) {
	path := listenFake(t, func(c *websocket.Conn) {
		defer c.CloseNow()
		a := decodeMap(t, c)
		b := decodeMap(t, c)
		_ = encodeWS(c, map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(b["id"]), "result": map[string]any{"value": "second"}})
		_ = encodeWS(c, map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(a["id"]), "result": map[string]any{"value": "first"}})
	})
	c, err := Dial(context.Background(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	type result struct {
		Value string `json:"value"`
	}
	var one, two result
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := c.Call(context.Background(), "one", map[string]any{}, &one); err != nil {
			t.Error(err)
		}
	}()
	time.Sleep(10 * time.Millisecond)
	go func() {
		defer wg.Done()
		if err := c.Call(context.Background(), "two", map[string]any{}, &two); err != nil {
			t.Error(err)
		}
	}()
	wg.Wait()
	if one.Value != "first" || two.Value != "second" {
		t.Fatalf("one=%+v two=%+v", one, two)
	}
}

func TestAdapterInitializeEventsApprovalAndUserInput(t *testing.T) {
	responses := make(chan map[string]json.RawMessage, 2)
	path := listenFake(t, func(c *websocket.Conn) {
		defer c.CloseNow()
		init := decodeMap(t, c)
		_ = encodeWS(c, map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(init["id"]), "result": map[string]any{"userAgent": "codex-cli/0.147.0", "codexHome": "/tmp/codex", "platformFamily": "unix", "platformOs": "linux"}})
		_ = decodeMap(t, c)
		_ = encodeWS(c, map[string]any{"jsonrpc": "2.0", "id": 7, "method": "item/commandExecution/requestApproval", "params": map[string]any{"threadId": "th", "turnId": "tu", "itemId": "it", "startedAtMs": 1}})
		responses <- decodeMap(t, c)
		_ = encodeWS(c, map[string]any{"jsonrpc": "2.0", "id": "ask", "method": "item/tool/requestUserInput", "params": map[string]any{"threadId": "th", "turnId": "tu", "itemId": "it", "isBlocking": true, "questions": []any{map[string]any{"id": "q1", "header": "Choice", "question": "Continue?", "allowsMultiple": true, "isOther": true, "options": []any{map[string]any{"label": "Yes", "description": "continue"}, map[string]any{"label": "Later", "description": "defer"}}}}}})
		responses <- decodeMap(t, c)
	})
	c, err := Dial(context.Background(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, info, err := Initialize(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if info.UserAgent != "codex-cli/0.147.0" {
		t.Fatalf("info=%+v", info)
	}
	e := <-a.Events()
	if e.PendingID != "7" || e.ThreadID != "th" {
		t.Fatalf("approval event=%+v", e)
	}
	if err := a.RespondApproval("7", "accept"); err != nil {
		t.Fatal(err)
	}
	approval := <-responses
	if string(approval["result"]) != "{\"decision\":\"accept\"}" {
		t.Fatalf("approval response=%s", approval["result"])
	}
	e = <-a.Events()
	if e.PendingID != "ask" {
		t.Fatalf("input event=%+v", e)
	}
	pending, ok := a.Pending("ask")
	if !ok || len(pending.Questions) != 1 || !pending.Questions[0].AllowsMultiple || !pending.Questions[0].IsOther || len(pending.Questions[0].Options) != 2 || pending.Questions[0].Options[0].Label != "Yes" {
		t.Fatalf("pending=%+v ok=%v", pending, ok)
	}
	if err := a.RespondUserInput("ask", map[string][]string{"q1": {"yes"}}); err != nil {
		t.Fatal(err)
	}
	input := <-responses
	var body struct {
		Answers map[string]struct {
			Answers []string `json:"answers"`
		} `json:"answers"`
	}
	if err := json.Unmarshal(input["result"], &body); err != nil || len(body.Answers["q1"].Answers) != 1 {
		t.Fatalf("input=%s err=%v", input["result"], err)
	}
}

func TestPermissionApprovalEchoesRequestedProfile(t *testing.T) {
	responses := make(chan map[string]json.RawMessage, 1)
	path := listenFake(t, func(c *websocket.Conn) {
		defer c.CloseNow()
		init := decodeMap(t, c)
		_ = encodeWS(c, map[string]any{"id": json.RawMessage(init["id"]), "result": map[string]any{"userAgent": "fake", "codexHome": "/tmp", "platformFamily": "unix", "platformOs": "linux"}})
		_ = decodeMap(t, c)
		_ = encodeWS(c, map[string]any{"id": "perm", "method": "item/permissions/requestApproval", "params": map[string]any{"threadId": "th", "turnId": "tu", "itemId": "it", "cwd": "/tmp", "startedAtMs": 1, "permissions": map[string]any{"network": map[string]any{"enabled": true}}}})
		responses <- decodeMap(t, c)
	})
	c, err := Dial(context.Background(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, _, err := Initialize(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	e := <-a.Events()
	if e.PendingID != "perm" {
		t.Fatalf("event=%+v", e)
	}
	if err := a.RespondApproval("perm", "acceptForSession"); err != nil {
		t.Fatal(err)
	}
	response := <-responses
	var result struct {
		Scope       string `json:"scope"`
		Permissions struct {
			Network struct {
				Enabled bool `json:"enabled"`
			} `json:"network"`
		} `json:"permissions"`
	}
	if err := json.Unmarshal(response["result"], &result); err != nil || result.Scope != "session" || !result.Permissions.Network.Enabled {
		t.Fatalf("result=%s err=%v", response["result"], err)
	}
}
