package wsclient

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/sobbk/nano-wsclient/codec"
	"github.com/sobbk/nano-wsclient/message"
	"github.com/sobbk/nano-wsclient/packet"
)

// Client is a reusable nano websocket debug client.
type Client struct {
	cfg     Config
	conn    *websocket.Conn
	decoder *codec.Decoder

	mu       sync.Mutex
	nextID   uint64
	pending  map[uint64]chan *message.Message
	pushFunc func(route string, data []byte)
	done     chan struct{}

	writeMu sync.Mutex
}

// Dial connects to a nano websocket server, sends the handshake packet,
// and starts the read and heartbeat loops.
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	cfg = cfg.withDefaults()
	if cfg.Addr == "" {
		return nil, fmt.Errorf("Addr 不能为空")
	}
	if len(cfg.Routes) > 0 {
		message.SetDictionary(cfg.Routes)
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, cfg.Addr, nil)
	if err != nil {
		return nil, fmt.Errorf("websocket 连接失败: %w", err)
	}

	c := &Client{
		cfg:     cfg,
		conn:    conn,
		decoder: codec.NewDecoder(),
		nextID:  1,
		pending: make(map[uint64]chan *message.Message),
		done:    make(chan struct{}),
	}

	go c.readLoop()
	go c.heartbeatLoop()

	if err := c.sendHandshake(); err != nil {
		_ = c.Close()
		return nil, err
	}

	return c, nil
}

// Close closes the underlying websocket connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// SetPushHandler registers the callback invoked for every decoded push
// message. route is the nano route string, data is the decrypted and
// ComResponse-unwrapped business payload.
func (c *Client) SetPushHandler(h func(route string, data []byte)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pushFunc = h
}

// Call sends a request and blocks until the matching response arrives. It
// unwraps the ComResponse{code,data} envelope and unmarshals data into resp
// when code == 200.
func (c *Client) Call(ctx context.Context, route string, req, resp proto.Message) (int32, error) {
	rawReq, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("请求序列化失败: %w", err)
	}

	id := c.nextMessageID()
	respCh := make(chan *message.Message, 1)
	c.mu.Lock()
	c.pending[id] = respCh
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	if err := c.sendMessage(&message.Message{Type: message.Request, ID: id, Route: route, Data: rawReq}); err != nil {
		return 0, err
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-c.done:
		return 0, fmt.Errorf("连接已关闭")
	case msg := <-respCh:
		code, body, err := DecodeComResponse(msg.Data)
		if err != nil {
			return 0, err
		}
		if code == 200 && resp != nil && len(body) > 0 {
			if err := proto.Unmarshal(body, resp); err != nil {
				return code, fmt.Errorf("响应反序列化失败: %w", err)
			}
		}
		return code, nil
	}
}

func (c *Client) readLoop() {
	defer close(c.done)
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		packets, err := c.decoder.Decode(data)
		if err != nil {
			c.reportError(fmt.Errorf("packet 解码失败: %w", err))
			continue
		}
		for _, p := range packets {
			c.handlePacket(p)
		}
	}
}

func (c *Client) handlePacket(p *packet.Packet) {
	switch p.Type {
	case packet.Handshake:
		_ = c.sendPacket(packet.HandshakeAck, nil)
	case packet.Data:
		c.handleDataPacket(p.Data)
	}
}

func (c *Client) handleDataPacket(data []byte) {
	msg, err := message.Decode(data)
	if err != nil {
		c.reportError(fmt.Errorf("message 解码失败: %w", err))
		return
	}
	msg.Data = c.cfg.Decrypt(msg.Data)

	switch msg.Type {
	case message.Response:
		c.mu.Lock()
		ch := c.pending[msg.ID]
		c.mu.Unlock()
		if ch != nil {
			ch <- msg
		}
	case message.Push:
		c.mu.Lock()
		handler := c.pushFunc
		c.mu.Unlock()
		if handler == nil {
			return
		}
		code, body, err := DecodeComResponse(msg.Data)
		if err != nil {
			c.reportError(fmt.Errorf("push ComResponse 解包失败: %w", err))
			return
		}
		if code != 200 {
			c.reportError(fmt.Errorf("push code=%d", code))
			return
		}
		handler(msg.Route, body)
	}
}

func (c *Client) heartbeatLoop() {
	ticker := time.NewTicker(c.cfg.Heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := c.sendPacket(packet.Heartbeat, nil); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

type handshakePayload struct {
	Platform  int32  `json:"platform"`
	Version   string `json:"version"`
	Language  string `json:"language"`
	Timestamp int64  `json:"timestamp"`
	Nonce     string `json:"nonce"`
}

func (c *Client) sendHandshake() error {
	p := handshakePayload{
		Platform:  c.cfg.Platform,
		Version:   c.cfg.Version,
		Language:  c.cfg.Language,
		Timestamp: time.Now().Unix(),
		Nonce:     randNonce(10),
	}
	bz, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("构建握手包失败: %w", err)
	}
	return c.sendPacket(packet.Handshake, bz)
}

func (c *Client) sendMessage(msg *message.Message) error {
	msg.Data = c.cfg.Encrypt(msg.Data)
	encoded, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("编码 message 失败: %w", err)
	}
	return c.sendPacket(packet.Data, encoded)
}

func (c *Client) sendPacket(typ packet.Type, data []byte) error {
	encoded, err := codec.Encode(typ, data)
	if err != nil {
		return fmt.Errorf("编码 packet 失败: %w", err)
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteMessage(websocket.BinaryMessage, encoded)
}

func (c *Client) reportError(err error) {
	if c.cfg.OnError != nil {
		c.cfg.OnError(err)
	}
}

func (c *Client) nextMessageID() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextID
	c.nextID++
	return id
}

func randNonce(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
