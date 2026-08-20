package adapter

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestRealCodexAppServerReadOnlySmoke(t *testing.T) {
	if os.Getenv("CODEX_REMOTE_REAL_APPSERVER") != "1" {
		t.Skip("set CODEX_REMOTE_REAL_APPSERVER=1 to run against installed codex")
	}
	codex, err := exec.LookPath("codex")
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(t.TempDir(), "app.sock")
	cmd := exec.Command(codex, "app-server", "--listen", "unix://"+socket)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			done := make(chan struct{})
			go func() { _ = cmd.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				_ = cmd.Process.Kill()
				<-done
			}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var client *Client
	for {
		client, err = Dial(ctx, socket, nil)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("dial real app-server: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
	}
	a, info, err := Initialize(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	if info.PlatformOS != "linux" || info.UserAgent == "" {
		t.Fatalf("initialize=%+v", info)
	}
	page, err := a.ListThreads(ctx, "", "", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Data) > 1 {
		t.Fatalf("limit ignored: %d", len(page.Data))
	}
}
