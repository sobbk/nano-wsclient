package wsclient

import "time"

// Config describes how to connect to a nano websocket server.
type Config struct {
	Addr      string
	Platform  int32
	Version   string
	Language  string
	Heartbeat time.Duration

	// Routes is an optional compressed-route dictionary (route string -> code), applied via message.SetDictionary before dialing. Leave nil if the server does not use route compression.
	Routes map[string]uint16

	// Encrypt/Decrypt 默认透传，对应 castle-ws 现状（协议未加密）。
	// 注意：若线上开启加密，服务端握手校验还会要求 HMAC sign 字段（见 castle-ws internal/game/handshake.go），当前 handshakePayload 没有对应字段，届时仍需同步修改本模块的握手逻辑，不只是填 Encrypt/Decrypt。
	Encrypt func([]byte) []byte
	Decrypt func([]byte) []byte

	// OnError, if set, is called with protocol-level errors that would otherwise be silently dropped (decode failures, non-200 push envelopes). Useful for debugging why an expected response or push never arrived.
	OnError func(error)
}

func (c Config) withDefaults() Config {
	if c.Heartbeat <= 0 {
		c.Heartbeat = 10 * time.Second
	}
	if c.Encrypt == nil {
		c.Encrypt = func(b []byte) []byte { return b }
	}
	if c.Decrypt == nil {
		c.Decrypt = func(b []byte) []byte { return b }
	}
	return c
}
