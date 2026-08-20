// fake_app_server is an external deterministic Codex app-server fixture.
// It intentionally shares no internal Host packages.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type message struct {
	JSONRPC string          `json:"jsonrpc,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
}

type fixture struct {
	mu        sync.Mutex
	threads   map[string]map[string]any
	pending   map[string]func()
	scenario  string
	stateFile string
	next      int
	disrupted bool
}

func main() {
	socket := flag.String("socket", "", "Unix socket path")
	listen := flag.String("listen", "", "Codex-compatible unix:// socket URL")
	scenario := flag.String("scenario", "normal", "black-box fixture scenario")
	stateFile := flag.String("state-file", "", "optional JSON persistence for restart tests")
	flag.Parse()
	if *socket == "" && len(*listen) > len("unix://") && (*listen)[:len("unix://")] == "unix://" {
		*socket = (*listen)[len("unix://"):]
	}
	if *socket == "" {
		log.Fatal("-socket is required")
	}
	_ = os.Remove(*socket)
	ln, err := net.Listen("unix", *socket)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	f := &fixture{threads: map[string]map[string]any{}, pending: map[string]func(){}, scenario: *scenario, stateFile: *stateFile}
	if err := f.load(); err != nil {
		log.Fatal(err)
	}
	f.seedScenario()
	srv := &http.Server{Handler: http.HandlerFunc(f.serve)}
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func (f *fixture) serve(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer c.CloseNow()
	var writeMu sync.Mutex
	write := func(v any) {
		raw, _ := json.Marshal(v)
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = c.Write(context.Background(), websocket.MessageText, raw)
	}
	for {
		typ, raw, err := c.Read(context.Background())
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var req message
		if json.Unmarshal(raw, &req) != nil {
			continue
		}
		if req.Method == "" && len(req.ID) > 0 {
			f.resolve(string(req.ID))
			continue
		}
		if req.Method == "" || len(req.ID) == 0 {
			continue
		}
		f.handle(req, write)
		if f.scenario == "runtime-disconnect" && req.Method == "turn/start" && f.markDisrupted() {
			_ = c.Close(websocket.StatusInternalError, "deterministic app-server transport loss")
			return
		}
	}
}

func (f *fixture) handle(req message, write func(any)) {
	respond := func(result any) { write(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}) }
	switch req.Method {
	case "initialize":
		respond(map[string]any{"userAgent": "codex-remote-blackbox-fake/1.0", "codexHome": "/tmp/fake-codex", "platformFamily": "unix", "platformOs": "linux"})
	case "thread/list":
		var p struct {
			CWD    string `json:"cwd"`
			Cursor string `json:"cursor"`
			Limit  int    `json:"limit"`
		}
		_ = json.Unmarshal(req.Params, &p)
		f.mu.Lock()
		data := make([]map[string]any, 0, len(f.threads))
		for _, thread := range f.threads {
			if p.CWD == "" || thread["cwd"] == p.CWD {
				data = append(data, thread)
			}
		}
		f.mu.Unlock()
		sort.Slice(data, func(i, j int) bool { return fmt.Sprint(data[i]["id"]) < fmt.Sprint(data[j]["id"]) })
		start, _ := strconv.Atoi(p.Cursor)
		if start < 0 || start > len(data) {
			start = len(data)
		}
		limit := p.Limit
		if limit <= 0 || limit > len(data)-start {
			limit = len(data) - start
		}
		end := start + limit
		var next any
		if end < len(data) {
			value := strconv.Itoa(end)
			next = value
		}
		respond(map[string]any{"data": data[start:end], "nextCursor": next})
	case "thread/start":
		var p struct {
			CWD string `json:"cwd"`
		}
		_ = json.Unmarshal(req.Params, &p)
		f.mu.Lock()
		f.next++
		id := fmt.Sprintf("fake-thread-%03d", f.next)
		thread := map[string]any{"id": id, "sessionId": id, "cwd": p.CWD, "preview": "deterministic fake session", "createdAt": time.Now().Unix(), "updatedAt": time.Now().Unix(), "source": "appServer", "status": "idle", "turns": []any{}}
		f.threads[id] = thread
		_ = f.persistLocked()
		f.mu.Unlock()
		respond(map[string]any{"thread": thread})
		write(map[string]any{"jsonrpc": "2.0", "method": "thread/started", "params": map[string]any{"threadId": id, "thread": thread}})
	case "thread/read", "thread/resume":
		var p struct {
			ThreadID string `json:"threadId"`
		}
		_ = json.Unmarshal(req.Params, &p)
		f.mu.Lock()
		thread := f.threads[p.ThreadID]
		f.mu.Unlock()
		if thread == nil {
			write(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": -32004, "message": "thread not found"}})
			return
		}
		respond(map[string]any{"thread": thread})
	case "turn/start":
		var p struct {
			ThreadID string `json:"threadId"`
		}
		_ = json.Unmarshal(req.Params, &p)
		turnID := fmt.Sprintf("turn-%d", time.Now().UnixNano())
		f.updateTurn(p.ThreadID, turnID, "inProgress")
		if f.scenario == "early-large" {
			f.emitEarlyLarge(write, p.ThreadID, turnID)
			respond(map[string]any{"turn": map[string]any{"id": turnID, "status": "inProgress", "items": []any{}}})
			return
		}
		respond(map[string]any{"turn": map[string]any{"id": turnID, "status": "inProgress", "items": []any{}}})
		go f.emitTurn(write, p.ThreadID, turnID)
	case "turn/interrupt":
		var p struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
		}
		_ = json.Unmarshal(req.Params, &p)
		f.updateTurn(p.ThreadID, p.TurnID, "interrupted")
		respond(map[string]any{})
		write(map[string]any{"jsonrpc": "2.0", "method": "turn/completed", "params": map[string]any{"threadId": p.ThreadID, "turn": map[string]any{"id": p.TurnID, "status": "interrupted"}}})
	default:
		write(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": -32601, "message": "method not found"}})
	}
}

func (f *fixture) emitTurn(write func(any), threadID, turnID string) {
	output := "deterministic output"
	if f.scenario == "large" {
		output = strings.Repeat("large-output-", 20000)
	}
	write(map[string]any{"jsonrpc": "2.0", "method": "thread/status/changed", "params": map[string]any{"threadId": threadID, "status": "active"}})
	write(map[string]any{"jsonrpc": "2.0", "method": "turn/started", "params": map[string]any{"threadId": threadID, "turn": map[string]any{"id": turnID, "status": "inProgress"}}})
	if f.scenario == "synthetic-upsert" {
		f.emitSyntheticPlanDiff(write, threadID, turnID, false)
		return
	}
	write(map[string]any{"jsonrpc": "2.0", "method": "item/started", "params": map[string]any{"threadId": threadID, "turnId": turnID, "item": map[string]any{"id": "item-agent", "type": "agentMessage", "text": ""}}})
	write(map[string]any{"jsonrpc": "2.0", "method": "turn/plan/updated", "params": map[string]any{"threadId": threadID, "turnId": turnID, "plan": []any{map[string]any{"step": "deterministic", "status": "inProgress"}}}})
	write(map[string]any{"jsonrpc": "2.0", "method": "warning", "params": map[string]any{"threadId": threadID, "turnId": turnID, "message": "deterministic warning"}})
	finishStatus := func(status string, failure map[string]any) {
		f.updateTurn(threadID, turnID, status)
		write(map[string]any{"jsonrpc": "2.0", "method": "item/completed", "params": map[string]any{"threadId": threadID, "turnId": turnID, "item": map[string]any{"id": "item-agent", "type": "agentMessage", "text": output}}})
		turn := map[string]any{"id": turnID, "status": status, "startedAt": time.Now().Add(-time.Second).Unix(), "completedAt": time.Now().Unix()}
		if failure != nil {
			turn["error"] = failure
		}
		write(map[string]any{"jsonrpc": "2.0", "method": "turn/completed", "params": map[string]any{"threadId": threadID, "turn": turn}})
	}
	finish := func() { finishStatus("completed", nil) }
	switch f.scenario {
	case "interrupt":
		return
	case "approval":
		f.setPending(`"approval-rpc-1"`, finish)
		write(map[string]any{"jsonrpc": "2.0", "id": "approval-rpc-1", "method": "item/commandExecution/requestApproval", "params": map[string]any{"approvalId": "approval-1", "threadId": threadID, "turnId": turnID, "itemId": "item-command", "command": []string{"printf", "approved"}, "reason": "deterministic approval"}})
		return
	case "user-input", "pending-restart":
		f.setPending(`"user-input-rpc-1"`, finish)
		write(map[string]any{
			"jsonrpc": "2.0", "id": "user-input-rpc-1", "method": "item/tool/requestUserInput",
			"params": map[string]any{
				"threadId": threadID, "turnId": turnID, "itemId": "item-tool",
				"questions": []any{map[string]any{
					"id": "choice", "header": "Choice", "question": "Choose one",
					"options": []any{
						map[string]any{"label": "A", "description": "first"},
						map[string]any{"label": "B", "description": "second"},
					},
				}},
			},
		})
		return
	case "large":
		write(map[string]any{"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": map[string]any{"threadId": threadID, "turnId": turnID, "itemId": "item-agent", "delta": output}})
	case "multi-large":
		for i, item := range f.historyItems() {
			body := item.(map[string]any)
			write(map[string]any{"jsonrpc": "2.0", "method": "item/completed", "params": map[string]any{"threadId": threadID, "turnId": turnID, "item": body}})
			write(map[string]any{"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": map[string]any{"threadId": threadID, "turnId": turnID, "itemId": fmt.Sprintf("large-item-%d", i), "delta": "tail"}})
		}
	case "structured":
		f.emitStructured(write, threadID, turnID)
	case "failed":
		finishStatus("failed", map[string]any{"message": "deterministic turn failure", "additionalDetails": "fixture failure detail"})
		return
	case "runtime-disconnect":
		return
	case "rewatch":
		write(map[string]any{"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": map[string]any{"threadId": threadID, "turnId": turnID, "itemId": "item-agent", "delta": "rewatch-before"}})
		time.Sleep(150 * time.Millisecond)
		write(map[string]any{"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": map[string]any{"threadId": threadID, "turnId": turnID, "itemId": "item-agent", "delta": "rewatch-after"}})
	case "burst":
		chunk := strings.Repeat("burst-output-", 5000)
		for range 500 {
			write(map[string]any{"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": map[string]any{"threadId": threadID, "turnId": turnID, "itemId": "item-agent", "delta": chunk}})
		}
	default:
		write(map[string]any{"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": map[string]any{"threadId": threadID, "turnId": turnID, "itemId": "item-agent", "delta": "deterministic output"}})
	}
	finish()
}

func (f *fixture) updateTurn(threadID, turnID, status string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	thread := f.threads[threadID]
	if thread == nil {
		return
	}
	turns, _ := thread["turns"].([]any)
	for _, raw := range turns {
		if turn, ok := raw.(map[string]any); ok && turn["id"] == turnID {
			turn["status"] = status
			if status != "inProgress" {
				turn["completedAt"] = time.Now().Unix()
			}
			if status == "failed" {
				turn["error"] = map[string]any{"message": "deterministic turn failure", "additionalDetails": "fixture failure detail"}
			}
			_ = f.persistLocked()
			return
		}
	}
	thread["turns"] = append(turns, map[string]any{"id": turnID, "status": status, "startedAt": time.Now().Unix(), "itemsView": "full", "items": f.historyItems()})
	_ = f.persistLocked()
}

func (f *fixture) load() error {
	if f.stateFile == "" {
		return nil
	}
	raw, err := os.ReadFile(f.stateFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, &struct {
		Threads *map[string]map[string]any `json:"threads"`
		Next    *int                       `json:"next"`
	}{Threads: &f.threads, Next: &f.next})
}

func (f *fixture) persistLocked() error {
	if f.stateFile == "" {
		return nil
	}
	raw, err := json.Marshal(map[string]any{"threads": f.threads, "next": f.next})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(f.stateFile), 0o700); err != nil {
		return err
	}
	tmp := f.stateFile + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, f.stateFile)
}

func (f *fixture) setPending(id string, done func()) {
	f.mu.Lock()
	f.pending[id] = done
	f.mu.Unlock()
}

func (f *fixture) resolve(id string) {
	f.mu.Lock()
	done := f.pending[id]
	delete(f.pending, id)
	f.mu.Unlock()
	if done != nil {
		done()
	}
}

func (f *fixture) markDisrupted() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.disrupted {
		return false
	}
	f.disrupted = true
	return true
}

func (f *fixture) seedScenario() {
	if f.scenario != "sessions" || len(f.threads) != 0 {
		return
	}
	workspace := filepath.Join(filepath.Dir(f.stateFile), "workspace")
	now := time.Now().Unix()
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, cwd := range []string{
		filepath.Join(workspace, "unmanaged"),
		filepath.Join(workspace, "unmanaged"),
		filepath.Join(workspace, "other"),
	} {
		id := fmt.Sprintf("existing-exec-%03d", i+1)
		f.threads[id] = map[string]any{
			"id": id, "sessionId": id, "cwd": cwd, "name": "existing session " + id,
			"preview": "unmanaged deterministic session", "createdAt": now - int64(100+i),
			"updatedAt": now - int64(i), "source": "exec", "status": "idle", "turns": []any{},
		}
	}
	f.next = 3
	_ = f.persistLocked()
}

func (f *fixture) historyItems() []any {
	switch f.scenario {
	case "structured":
		return []any{
			map[string]any{"id": "item-user", "type": "userMessage", "status": "completed", "content": []any{map[string]any{"type": "text", "text": "structured user input"}}},
			map[string]any{"id": "item-reasoning", "type": "reasoning", "status": "completed", "summary": "structured reasoning"},
			map[string]any{"id": "item-plan", "type": "plan", "status": "completed", "steps": []any{map[string]any{"step": "structured plan", "status": "completed"}}},
			map[string]any{"id": "item-command", "type": "commandExecution", "status": "completed", "command": []string{"printf", "structured"}, "cwd": "/tmp", "aggregatedOutput": "stdout line\nstderr line", "exitCode": 7},
			map[string]any{"id": "item-file", "type": "fileChange", "status": "completed", "changes": []any{map[string]any{"path": "new.txt", "oldPath": "old.txt", "newPath": "new.txt", "kind": "renamed"}}, "diff": "--- old.txt\n+++ new.txt"},
			map[string]any{"id": "item-tool", "type": "mcpToolCall", "status": "completed", "toolName": "fixture.tool", "summary": "tool input", "resultSummary": "tool result"},
			map[string]any{"id": "item-agent", "type": "agentMessage", "status": "completed", "text": "structured agent output"},
		}
	case "multi-large":
		items := make([]any, 0, 8)
		for i := range 8 {
			items = append(items, map[string]any{"id": fmt.Sprintf("large-item-%d", i), "type": "agentMessage", "status": "completed", "text": strings.Repeat(fmt.Sprintf("item-%d-", i), 9000)})
		}
		return items
	default:
		output := "deterministic output"
		if f.scenario == "large" {
			output = strings.Repeat("large-output-", 20000)
		}
		return []any{map[string]any{"id": "item-agent", "type": "agentMessage", "status": "completed", "text": output}}
	}
}

func (f *fixture) emitStructured(write func(any), threadID, turnID string) {
	items := f.historyItems()
	for _, raw := range items {
		item := raw.(map[string]any)
		write(map[string]any{"jsonrpc": "2.0", "method": "item/started", "params": map[string]any{"threadId": threadID, "turnId": turnID, "item": item}})
	}
	write(map[string]any{"jsonrpc": "2.0", "method": "item/reasoning/summaryTextDelta", "params": map[string]any{"threadId": threadID, "turnId": turnID, "itemId": "item-reasoning", "delta": " plus delta"}})
	write(map[string]any{"jsonrpc": "2.0", "method": "item/commandExecution/outputDelta", "params": map[string]any{"threadId": threadID, "turnId": turnID, "itemId": "item-command", "stream": "stderr", "delta": "command delta"}})
	write(map[string]any{"jsonrpc": "2.0", "method": "item/fileChange/outputDelta", "params": map[string]any{"threadId": threadID, "turnId": turnID, "itemId": "item-file", "delta": "diff delta"}})
	write(map[string]any{"jsonrpc": "2.0", "method": "turn/plan/updated", "params": map[string]any{"threadId": threadID, "turnId": turnID, "plan": []any{map[string]any{"step": "structured plan", "status": "completed"}}}})
	for _, item := range items {
		write(map[string]any{"jsonrpc": "2.0", "method": "item/completed", "params": map[string]any{"threadId": threadID, "turnId": turnID, "item": item}})
	}
}

func (f *fixture) emitSyntheticPlanDiff(write func(any), threadID, turnID string, large bool) {
	steps := []any{
		map[string]any{"step": "first synthetic step", "status": "inProgress"},
		map[string]any{"step": "second synthetic step", "status": "pending"},
	}
	diff := "--- old.txt\n+++ new.txt\n@@ synthetic diff"
	if large {
		steps = make([]any, 0, 800)
		for i := range 800 {
			steps = append(steps, map[string]any{"step": fmt.Sprintf("large plan step %04d %s", i, strings.Repeat("p", 300)), "status": "inProgress"})
		}
		diff = strings.Repeat("@@ large deterministic diff line\n", 10000)
	}
	write(map[string]any{"jsonrpc": "2.0", "method": "turn/plan/updated", "params": map[string]any{"threadId": threadID, "turnId": turnID, "plan": steps}})
	steps[0].(map[string]any)["status"] = "completed"
	write(map[string]any{"jsonrpc": "2.0", "method": "turn/plan/updated", "params": map[string]any{"threadId": threadID, "turnId": turnID, "plan": steps}})
	write(map[string]any{"jsonrpc": "2.0", "method": "turn/diff/updated", "params": map[string]any{"threadId": threadID, "turnId": turnID, "diff": diff}})
}

func (f *fixture) emitEarlyLarge(write func(any), threadID, turnID string) {
	f.emitSyntheticPlanDiff(write, threadID, turnID, true)
	changes := make([]any, 0, 1200)
	for i := range 1200 {
		changes = append(changes, map[string]any{
			"path": fmt.Sprintf("generated/path/%04d-%s.txt", i, strings.Repeat("f", 120)),
			"kind": "modified",
		})
	}
	write(map[string]any{"jsonrpc": "2.0", "method": "item/completed", "params": map[string]any{
		"threadId": threadID, "turnId": turnID,
		"item": map[string]any{"id": "early-large-file", "type": "fileChange", "status": "completed", "changes": changes, "diff": strings.Repeat("diff payload\n", 15000)},
	}})
	write(map[string]any{"jsonrpc": "2.0", "method": "warning", "params": map[string]any{
		"threadId": threadID, "turnId": turnID, "message": "early vendor warning " + strings.Repeat("w", 180000),
	}})

	approvalCommand := make([]string, 0, 6000)
	for i := range 6000 {
		approvalCommand = append(approvalCommand, fmt.Sprintf("approval-arg-%04d-%s", i, strings.Repeat("a", 30)))
	}
	f.setPending(`"early-approval-rpc"`, func() {})
	write(map[string]any{"jsonrpc": "2.0", "id": "early-approval-rpc", "method": "item/commandExecution/requestApproval", "params": map[string]any{
		"approvalId": "early-large-approval", "threadId": threadID, "turnId": turnID, "itemId": "early-command",
		"title": "large approval", "reason": strings.Repeat("approval explanation ", 12000), "command": approvalCommand,
	}})

	questions := make([]any, 0, 20)
	for q := range 20 {
		options := make([]any, 0, 40)
		for o := range 40 {
			options = append(options, map[string]any{
				"label":       fmt.Sprintf("q%02d-option-%02d", q, o),
				"description": strings.Repeat(fmt.Sprintf("description-%02d-%02d ", q, o), 80),
			})
		}
		questions = append(questions, map[string]any{
			"id": fmt.Sprintf("large-question-%02d", q), "header": fmt.Sprintf("Question %02d", q),
			"question": strings.Repeat(fmt.Sprintf("large prompt %02d ", q), 120), "options": options,
			"allowsMultiple": q == 19,
		})
	}
	f.setPending(`"early-input-rpc"`, func() {})
	write(map[string]any{"jsonrpc": "2.0", "id": "early-input-rpc", "method": "item/tool/requestUserInput", "params": map[string]any{
		"threadId": threadID, "turnId": turnID, "itemId": "early-tool", "questions": questions,
	}})
}
