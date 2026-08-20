package adapter

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/coder/websocket"
)

func TestClientPropagatesRPCErrorAndDisconnect(t *testing.T) {
	path := listenFake(t, func(c *websocket.Conn) {
		req := decodeMap(t, c)
		_ = encodeWS(c, map[string]any{"id": json.RawMessage(req["id"]), "error": map[string]any{"code": -32602, "message": "bad params"}})
		c.CloseNow()
	})
	c, err := Dial(context.Background(), path, nil)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	err = c.Call(context.Background(), "bad", map[string]any{}, &out)
	rpcErr, ok := err.(*RPCError)
	if !ok || rpcErr.Code != -32602 {
		t.Fatalf("err=%T %v", err, err)
	}
	<-c.Done()
	if err := c.Call(context.Background(), "after-close", nil, nil); err != ErrDisconnected {
		t.Fatalf("disconnect err=%v", err)
	}
}
