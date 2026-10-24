package raftkvv1

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestProtobufRoundTrip(t *testing.T) {
	req := &PutRequest{
		ClientId:  "client-1",
		RequestId: 101,
		Key:       "user:name",
		Value:     "Alice",
	}

	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal PutRequest: %v", err)
	}

	var decoded PutRequest
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal PutRequest: %v", err)
	}

	if decoded.ClientId != req.ClientId || decoded.RequestId != req.RequestId || decoded.Key != req.Key || decoded.Value != req.Value {
		t.Fatalf("decoded request does not match: got %+v, want %+v", decoded, req)
	}
}

func TestAppendEntriesProto(t *testing.T) {
	entry := &LogEntryProto{
		Index: 5,
		Term:  2,
		Data:  []byte("test-command"),
	}
	req := &AppendEntriesArgs{
		Term:         2,
		LeaderId:     "node-1",
		PrevLogIndex: 4,
		PrevLogTerm:  2,
		Entries:      []*LogEntryProto{entry},
		LeaderCommit: 4,
	}

	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal AppendEntriesArgs: %v", err)
	}

	var decoded AppendEntriesArgs
	if err := proto.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal AppendEntriesArgs: %v", err)
	}

	if decoded.Term != 2 || decoded.LeaderId != "node-1" || len(decoded.Entries) != 1 {
		t.Fatalf("decoded AppendEntriesArgs does not match original")
	}
}
