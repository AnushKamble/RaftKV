package statemachine

import (
	"testing"

	"raftkv/internal/protocol"
)

func TestKV_SequentialSemantics(t *testing.T) {
	sm := NewKVStateMachine()

	// 1. Put k1 = v1
	r1 := sm.Apply(protocol.LogEntry{
		Index: 1,
		Term:  1,
		Command: protocol.Command{
			Op:    protocol.OpPut,
			Key:   "k1",
			Value: "v1",
		},
	})
	if !r1.Success || r1.CommitIndex != 1 {
		t.Fatalf("unexpected put result: %+v", r1)
	}

	// 2. Get k1
	r2 := sm.Apply(protocol.LogEntry{
		Index: 2,
		Term:  1,
		Command: protocol.Command{
			Op:  protocol.OpGet,
			Key: "k1",
		},
	})
	if !r2.Success || !r2.Found || r2.Value != "v1" {
		t.Fatalf("unexpected get result: %+v", r2)
	}

	// 3. CAS k1 from "wrong" to "v2" -> should fail swap
	r3 := sm.Apply(protocol.LogEntry{
		Index: 3,
		Term:  1,
		Command: protocol.Command{
			Op:            protocol.OpCAS,
			Key:           "k1",
			ExpectedValue: "wrong",
			Value:         "v2",
		},
	})
	if !r3.Success || r3.Swapped || r3.Value != "v1" {
		t.Fatalf("expected CAS mismatch: %+v", r3)
	}

	// 4. CAS k1 from "v1" to "v2" -> should succeed
	r4 := sm.Apply(protocol.LogEntry{
		Index: 4,
		Term:  1,
		Command: protocol.Command{
			Op:            protocol.OpCAS,
			Key:           "k1",
			ExpectedValue: "v1",
			Value:         "v2",
		},
	})
	if !r4.Success || !r4.Swapped || r4.Value != "v2" {
		t.Fatalf("expected CAS success: %+v", r4)
	}

	// 5. Delete k1 -> found=true
	r5 := sm.Apply(protocol.LogEntry{
		Index: 5,
		Term:  1,
		Command: protocol.Command{
			Op:  protocol.OpDelete,
			Key: "k1",
		},
	})
	if !r5.Success || !r5.Found {
		t.Fatalf("expected delete found=true: %+v", r5)
	}

	// 6. Get k1 after delete -> found=false
	r6 := sm.Apply(protocol.LogEntry{
		Index: 6,
		Term:  1,
		Command: protocol.Command{
			Op:  protocol.OpGet,
			Key: "k1",
		},
	})
	if !r6.Success || r6.Found {
		t.Fatalf("expected get found=false: %+v", r6)
	}

	// 7. CAS non-existent key with expected="" -> creates key
	r7 := sm.Apply(protocol.LogEntry{
		Index: 7,
		Term:  1,
		Command: protocol.Command{
			Op:            protocol.OpCAS,
			Key:           "k2",
			ExpectedValue: "",
			Value:         "created",
		},
	})
	if !r7.Success || !r7.Swapped || r7.Value != "created" {
		t.Fatalf("expected CAS create: %+v", r7)
	}

	if sm.LastApplied() != 7 {
		t.Fatalf("expected lastApplied=7, got %d", sm.LastApplied())
	}
}

func TestKV_DeduplicationIdempotency(t *testing.T) {
	sm := NewKVStateMachine()

	cmd := protocol.Command{
		ClientID:      "client-1",
		RequestID:     100,
		Op:            protocol.OpCAS,
		Key:           "balance",
		ExpectedValue: "",
		Value:         "500",
	}

	// First execution: sets balance to 500, swapped=true
	r1 := sm.Apply(protocol.LogEntry{Index: 1, Term: 1, Command: cmd})
	if !r1.Success || !r1.Swapped || r1.Value != "500" {
		t.Fatalf("first execution failed: %+v", r1)
	}

	// Retry execution with identical (client_id, request_id, command):
	// If it re-executed, CAS would fail because balance is no longer empty!
	// But because of deduplication, it MUST return swapped=true and the cached result!
	r2 := sm.Apply(protocol.LogEntry{Index: 2, Term: 1, Command: cmd})
	if !r2.Success || !r2.Swapped || r2.Value != "500" {
		t.Fatalf("deduplicated retry failed: %+v", r2)
	}
}

func TestKV_DedupePayloadMismatchRejection(t *testing.T) {
	sm := NewKVStateMachine()

	cmd1 := protocol.Command{
		ClientID:  "client-1",
		RequestID: 200,
		Op:        protocol.OpPut,
		Key:       "account",
		Value:     "100",
	}
	r1 := sm.Apply(protocol.LogEntry{Index: 1, Term: 1, Command: cmd1})
	if !r1.Success {
		t.Fatal("first put failed")
	}

	// Reusing request ID 200 with different value
	cmd2 := protocol.Command{
		ClientID:  "client-1",
		RequestID: 200,
		Op:        protocol.OpPut,
		Key:       "account",
		Value:     "999", // different payload
	}
	r2 := sm.Apply(protocol.LogEntry{Index: 2, Term: 1, Command: cmd2})
	if r2.Success || r2.ErrorCode != "PAYLOAD_MISMATCH" {
		t.Fatalf("expected PAYLOAD_MISMATCH error, got: %+v", r2)
	}

	// Verify key value remained unchanged (100)
	val, ok := sm.GetDirect("account")
	if !ok || val != "100" {
		t.Fatalf("expected value to remain 100, got %s", val)
	}
}

func TestKV_SnapshotAndRestore(t *testing.T) {
	sm := NewKVStateMachine()

	// Apply some commands
	sm.Apply(protocol.LogEntry{
		Index: 1, Term: 1,
		Command: protocol.Command{ClientID: "c1", RequestID: 1, Op: protocol.OpPut, Key: "k1", Value: "v1"},
	})
	sm.Apply(protocol.LogEntry{
		Index: 2, Term: 1,
		Command: protocol.Command{ClientID: "c2", RequestID: 5, Op: protocol.OpPut, Key: "k2", Value: "v2"},
	})

	snapBytes, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("failed to take snapshot: %v", err)
	}

	// Restore into fresh state machine
	sm2 := NewKVStateMachine()
	if err := sm2.Restore(snapBytes); err != nil {
		t.Fatalf("failed to restore snapshot: %v", err)
	}

	if sm2.LastApplied() != 2 {
		t.Fatalf("expected lastApplied=2, got %d", sm2.LastApplied())
	}
	if v, ok := sm2.GetDirect("k1"); !ok || v != "v1" {
		t.Fatalf("restored k1 mismatch: %s", v)
	}
	if v, ok := sm2.GetDirect("k2"); !ok || v != "v2" {
		t.Fatalf("restored k2 mismatch: %s", v)
	}

	// Verify dedupe state survived snapshot: retrying c2 request 5 returns cached result
	retryCmd := protocol.Command{ClientID: "c2", RequestID: 5, Op: protocol.OpPut, Key: "k2", Value: "v2"}
	r := sm2.Apply(protocol.LogEntry{Index: 3, Term: 1, Command: retryCmd})
	if !r.Success {
		t.Fatalf("retry after restore failed: %+v", r)
	}
}
