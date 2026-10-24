package protocol

import (
	"reflect"
	"testing"
)

func TestCommandEncodeDecode(t *testing.T) {
	cmd := Command{
		ClientID:      "client-1",
		RequestID:     42,
		Op:            OpCAS,
		Key:           "foo",
		Value:         "bar",
		ExpectedValue: "baz",
	}

	bytes, err := cmd.Encode()
	if err != nil {
		t.Fatalf("failed to encode command: %v", err)
	}

	decoded, err := DecodeCommand(bytes)
	if err != nil {
		t.Fatalf("failed to decode command: %v", err)
	}

	if !reflect.DeepEqual(cmd, decoded) {
		t.Fatalf("mismatch after decode: got %+v, want %+v", decoded, cmd)
	}

	hash1 := cmd.PayloadHash()
	hash2 := decoded.PayloadHash()
	if hash1 != hash2 {
		t.Fatalf("hash mismatch: %s != %s", hash1, hash2)
	}
}

func TestCommandPayloadHashSensitivity(t *testing.T) {
	cmd1 := Command{Op: OpPut, Key: "a", Value: "1"}
	cmd2 := Command{Op: OpPut, Key: "a", Value: "2"}

	if cmd1.PayloadHash() == cmd2.PayloadHash() {
		t.Fatal("expected different hashes for different payloads")
	}
}
