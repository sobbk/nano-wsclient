package wsclient

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

func TestDecodeComResponse(t *testing.T) {
	var raw []byte
	raw = protowire.AppendTag(raw, 1, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 200)
	raw = protowire.AppendTag(raw, 2, protowire.BytesType)
	raw = protowire.AppendBytes(raw, []byte("body"))

	code, body, err := DecodeComResponse(raw)
	if err != nil {
		t.Fatalf("DecodeComResponse failed: %v", err)
	}
	if code != 200 {
		t.Fatalf("code=%d want 200", code)
	}
	if string(body) != "body" {
		t.Fatalf("body=%q want body", body)
	}
}
