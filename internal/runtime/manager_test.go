package runtime

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestFakeAppServerProcess(t *testing.T) {
	if os.Getenv("CODEX_REMOTE_FAKE_APP_SERVER") != "1" {
		return
	}
	var socket string
	for i, arg := range os.Args {
		if arg == "--listen" && i+1 < len(os.Args) {
			socket = strings.TrimPrefix(os.Args[i+1], "unix://")
		}
	}
	if socket == "" {
		os.Exit(3)
	}
	_ = os.Remove(socket)
	ln, err := net.Listen("unix", socket)
	if err != nil {
		os.Exit(4)
	}
	done := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			os.Exit(5)
		}
		var req map[string]json.RawMessage
		read := func() error {
			_, raw, err := c.Read(context.Background())
			if err == nil {
				err = json.Unmarshal(raw, &req)
			}
			return err
		}
		write := func(v any) error {
			raw, err := json.Marshal(v)
			if err != nil {
				return err
			}
			return c.Write(context.Background(), websocket.MessageText, raw)
		}
		if read() != nil {
			os.Exit(6)
		}
		_ = write(map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(req["id"]), "result": map[string]any{"userAgent": "fake-codex/0.147.0", "codexHome": "/tmp/fake-" + strconv.Itoa(os.Getpid()), "platformFamily": "unix", "platformOs": "linux"}})
		if read() != nil {
			os.Exit(7)
		}
		countPath := os.Getenv("CODEX_REMOTE_FAKE_COUNT")
		count := 0
		if raw, err := os.ReadFile(countPath); err == nil {
			count, _ = strconv.Atoi(string(raw))
		}
		count++
		_ = os.WriteFile(countPath, []byte(strconv.Itoa(count)), 0o600)
		mode := os.Getenv("CODEX_REMOTE_FAKE_MODE")
		if mode == "crash-once" && count == 1 {
			os.Exit(9)
		}
		if (mode == "disconnect-once" && count == 1) || (mode == "disconnect-twice" && count <= 2) {
			c.CloseNow()
			return
		}
		if mode == "active-once" && count == 1 {
			_ = write(map[string]any{"jsonrpc": "2.0", "method": "turn/started", "params": map[string]any{"threadId": "th", "turn": map[string]any{"id": "tu", "status": "inProgress", "items": []any{}}}})
			time.Sleep(150 * time.Millisecond)
			_ = write(map[string]any{"jsonrpc": "2.0", "method": "turn/completed", "params": map[string]any{"threadId": "th", "turn": map[string]any{"id": "tu", "status": "completed", "items": []any{}}}})
		}
		for {
			if read() != nil {
				close(done)
				return
			}
		}
	})}
	go server.Serve(ln)
	<-done
}

func fakeConfig(t *testing.T, mode string) Config {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("CODEX_REMOTE_FAKE_APP_SERVER", "1")
	t.Setenv("CODEX_REMOTE_FAKE_MODE", mode)
	t.Setenv("CODEX_REMOTE_FAKE_COUNT", filepath.Join(dir, "count"))
	return Config{Executable: os.Args[0], BaseArgs: []string{"-test.run=TestFakeAppServerProcess", "--"}, SocketPath: filepath.Join(dir, "app.sock"), StartTimeout: 3 * time.Second, ConnectInterval: 10 * time.Millisecond, MaxRestarts: 3, Backoff: []time.Duration{10 * time.Millisecond}, StopTimeout: time.Second}
}
func waitReadyRestart(t *testing.T, m *Manager, min uint64) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case s := <-m.States():
			if s.Status == StatusReady && s.RestartCount >= min {
				return
			}
		case <-deadline:
			t.Fatalf("timeout state=%+v", m.State())
		}
	}
}
func closeManager(t *testing.T, m *Manager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestManagerStartsAndPerformsPlannedRestart(t *testing.T) {
	m := New(fakeConfig(t, ""))
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.State().AppServer.UserAgent != "fake-codex/0.147.0" {
		t.Fatalf("state=%+v", m.State())
	}
	if err := m.RequestRestart(); err != nil {
		t.Fatal(err)
	}
	waitReadyRestart(t, m, 1)
	closeManager(t, m)
}
func TestManagerRestartsAfterUnexpectedExit(t *testing.T) {
	m := New(fakeConfig(t, "crash-once"))
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitReadyRestart(t, m, 1)
	closeManager(t, m)
}

func TestManagerRestartsWhenWebSocketDropsButProcessLives(t *testing.T) {
	cfg := fakeConfig(t, "disconnect-twice")
	cfg.MaxRestarts = 1
	m := New(cfg)
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	processIdentity := m.State().AppServer.CodexHome
	deadline := time.Now().Add(5 * time.Second)
	for {
		raw, _ := os.ReadFile(os.Getenv("CODEX_REMOTE_FAKE_COUNT"))
		count, _ := strconv.Atoi(string(raw))
		state := m.State()
		if count >= 3 && state.Status == StatusReady {
			if state.RestartCount != 0 {
				t.Fatalf("process restarted instead of reconnecting: %+v", state)
			}
			if state.AppServer.CodexHome != processIdentity {
				t.Fatalf("process identity changed: %q -> %q", processIdentity, state.AppServer.CodexHome)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reconnect timeout count=%d state=%+v", count, state)
		}
		time.Sleep(10 * time.Millisecond)
	}
	restarting, ready := 0, 0
	drain := true
	for drain {
		select {
		case state := <-m.States():
			if state.Status == StatusRestarting {
				restarting++
			}
			if state.Status == StatusReady {
				ready++
			}
		default:
			drain = false
		}
	}
	if restarting < 2 || ready < 3 {
		t.Fatalf("missing disconnect state transitions: restarting=%d ready=%d", restarting, ready)
	}
	closeManager(t, m)
}
func TestManagerDefersRestartUntilTurnCompletes(t *testing.T) {
	m := New(fakeConfig(t, "active-once"))
	if err := m.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		ad, err := m.Adapter()
		if err == nil && ad.ActiveTurnCount() == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("active turn was not observed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := m.RequestRestart(); err != ErrRestartDeferred {
		t.Fatalf("got %v", err)
	}
	waitReadyRestart(t, m, 1)
	closeManager(t, m)
}

func TestManagerRefusesSocketOwnedByAnotherListener(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "occupied.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	cfg := fakeConfig(t, "")
	cfg.SocketPath = path
	m := New(cfg)
	if err := m.Start(context.Background()); err == nil {
		t.Fatal("expected live socket rejection")
	}
	select {
	case <-m.Done():
	case <-time.After(time.Second):
		t.Fatal("failed start did not finish runtime")
	}
	if _, err := net.Dial("unix", path); err != nil {
		t.Fatalf("existing listener was disturbed: %v", err)
	}
}
