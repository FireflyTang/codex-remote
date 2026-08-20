package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/coder/websocket"
)

var ErrDisconnected = errors.New("app-server disconnected")

// DefaultReadLimit allows large command output, diffs, and vendor payloads while
// still keeping a finite per-message bound for the personal Demo.
const DefaultReadLimit int64 = 64 << 20

type DialOptions struct {
	ReadLimit int64
}

type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("app-server RPC error %d: %s", e.Code, e.Message)
}

type Message struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *RPCError       `json:"error,omitempty"`
}

type WireDirection string

const (
	WireIn  WireDirection = "in"
	WireOut WireDirection = "out"
)

type WireObserver func(WireDirection, []byte)

type Client struct {
	conn *websocket.Conn

	nextID    atomic.Uint64
	writeMu   sync.Mutex
	mu        sync.Mutex
	pending   map[string]chan Message
	incoming  chan Message
	done      chan struct{}
	closeOnce sync.Once
	observer  WireObserver
}

func Dial(ctx context.Context, socketPath string, observer WireObserver) (*Client, error) {
	return DialWithOptions(ctx, socketPath, observer, DialOptions{})
}

func DialWithOptions(ctx context.Context, socketPath string, observer WireObserver, options DialOptions) (*Client, error) {
	d := net.Dialer{}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return d.DialContext(ctx, "unix", socketPath)
	}}
	conn, _, err := websocket.Dial(ctx, "ws://localhost/rpc", &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}})
	if err != nil {
		return nil, err
	}
	readLimit := options.ReadLimit
	if readLimit <= 0 {
		readLimit = DefaultReadLimit
	}
	conn.SetReadLimit(readLimit)
	c := &Client{conn: conn, pending: make(map[string]chan Message), incoming: make(chan Message, 128), done: make(chan struct{}), observer: observer}
	go c.readLoop()
	return c, nil
}

func (c *Client) Incoming() <-chan Message { return c.incoming }
func (c *Client) Done() <-chan struct{}    { return c.done }

func (c *Client) Call(ctx context.Context, method string, params any, result any) error {
	id := fmt.Sprintf("remote-%d", c.nextID.Add(1))
	ch := make(chan Message, 1)
	c.mu.Lock()
	select {
	case <-c.done:
		c.mu.Unlock()
		return ErrDisconnected
	default:
	}
	c.pending[id] = ch
	c.mu.Unlock()

	req := struct {
		JSONRPC string `json:"jsonrpc"`
		ID      string `json:"id"`
		Method  string `json:"method"`
		Params  any    `json:"params"`
	}{"2.0", id, method, params}
	if err := c.write(req); err != nil {
		c.removePending(id)
		return err
	}
	select {
	case msg := <-ch:
		if msg.Error != nil {
			return msg.Error
		}
		if result == nil || len(msg.Result) == 0 {
			return nil
		}
		if err := json.Unmarshal(msg.Result, result); err != nil {
			return fmt.Errorf("decode %s result: %w", method, err)
		}
		return nil
	case <-ctx.Done():
		c.removePending(id)
		return ctx.Err()
	case <-c.done:
		c.removePending(id)
		return ErrDisconnected
	}
}

func (c *Client) Notify(method string, params any) error {
	return c.write(struct {
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
		Params  any    `json:"params,omitempty"`
	}{"2.0", method, params})
}

func (c *Client) Respond(id json.RawMessage, result any) error {
	if len(id) == 0 {
		return errors.New("missing server request id")
	}
	return c.write(struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  any             `json:"result"`
	}{"2.0", id, result})
}

func (c *Client) write(v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.done:
		return ErrDisconnected
	default:
	}
	if c.observer != nil {
		c.observer(WireOut, append([]byte(nil), raw...))
	}
	if err := c.conn.Write(context.Background(), websocket.MessageText, raw); err != nil {
		c.closeWithError()
		return err
	}
	return nil
}

func (c *Client) readLoop() {
	defer close(c.incoming)
	for {
		typ, payload, err := c.conn.Read(context.Background())
		if err != nil {
			break
		}
		if typ != websocket.MessageText {
			continue
		}
		raw := append([]byte(nil), payload...)
		if c.observer != nil {
			c.observer(WireIn, raw)
		}
		var msg Message
		if json.Unmarshal(raw, &msg) != nil {
			continue
		}
		if len(msg.ID) != 0 && msg.Method == "" {
			key := requestIDKey(msg.ID)
			c.mu.Lock()
			ch := c.pending[key]
			delete(c.pending, key)
			c.mu.Unlock()
			if ch != nil {
				ch <- msg
			}
			continue
		}
		select {
		case c.incoming <- msg:
		case <-c.done:
			return
		}
	}
	c.closeWithError()
}

func requestIDKey(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return string(raw)
}

func (c *Client) removePending(id string) { c.mu.Lock(); delete(c.pending, id); c.mu.Unlock() }
func (c *Client) closeWithError() {
	c.closeOnce.Do(func() {
		c.conn.CloseNow()
		close(c.done)
		c.mu.Lock()
		c.pending = make(map[string]chan Message)
		c.mu.Unlock()
	})
}
func (c *Client) Close() error { c.closeWithError(); return nil }
