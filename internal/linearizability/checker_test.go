package linearizability

import (
	"testing"

	"raftkv/internal/protocol"
)

func TestChecker_KnownGoodSequential(t *testing.T) {
	c := NewChecker(1000)

	history := []OperationRecord{
		{
			ID: 1, Op: protocol.OpPut, Key: "x", Value: "1",
			InvokeTime: 10, ReturnTime: 20,
		},
		{
			ID: 2, Op: protocol.OpGet, Key: "x",
			InvokeTime: 30, ReturnTime: 40,
			ObservedValue: "1", ObservedFound: true,
		},
		{
			ID: 3, Op: protocol.OpCAS, Key: "x", Expected: "1", Value: "2",
			InvokeTime: 50, ReturnTime: 60,
			ObservedSwapped: true, ObservedValue: "2",
		},
		{
			ID: 4, Op: protocol.OpGet, Key: "x",
			InvokeTime: 70, ReturnTime: 80,
			ObservedValue: "2", ObservedFound: true,
		},
	}

	res := c.Check(history)
	if res.Outcome != OutcomePass {
		t.Fatalf("expected PASS for known good history, got %s: %s", res.Outcome, res.Counterexample)
	}
	if len(res.Witness) != 4 {
		t.Fatalf("expected witness length 4, got %d", len(res.Witness))
	}
}

func TestChecker_ValidOverlappingConcurrent(t *testing.T) {
	c := NewChecker(1000)

	// Op 1 (Put x=1) runs from 10 to 50
	// Op 2 (Put x=2) runs from 20 to 60 (concurrent with Op 1)
	// Op 3 (Get x) runs from 70 to 80 and returns "2"
	history := []OperationRecord{
		{
			ID: 1, Op: protocol.OpPut, Key: "x", Value: "1",
			InvokeTime: 10, ReturnTime: 50,
		},
		{
			ID: 2, Op: protocol.OpPut, Key: "x", Value: "2",
			InvokeTime: 20, ReturnTime: 60,
		},
		{
			ID: 3, Op: protocol.OpGet, Key: "x",
			InvokeTime: 70, ReturnTime: 80,
			ObservedValue: "2", ObservedFound: true,
		},
	}

	res := c.Check(history)
	if res.Outcome != OutcomePass {
		t.Fatalf("expected PASS for concurrent overlap, got %s", res.Outcome)
	}
}

func TestChecker_KnownBadRealTimeViolation(t *testing.T) {
	c := NewChecker(1000)

	// Op 1: Put x=2 completes at time 30
	// Op 2: Get x invokes at time 40 (strictly AFTER Op 1 completes)
	// But Op 2 returns "1"!
	history := []OperationRecord{
		{
			ID: 1, Op: protocol.OpPut, Key: "x", Value: "1",
			InvokeTime: 0, ReturnTime: 10,
		},
		{
			ID: 2, Op: protocol.OpPut, Key: "x", Value: "2",
			InvokeTime: 20, ReturnTime: 30, // completed at 30
		},
		{
			ID: 3, Op: protocol.OpGet, Key: "x",
			InvokeTime: 40, ReturnTime: 50, // started at 40 (2 < 3 in real time)
			ObservedValue: "1", ObservedFound: true, // ILLEGAL: returns stale 1!
		},
	}

	res := c.Check(history)
	if res.Outcome != OutcomeViolation {
		t.Fatalf("expected VIOLATION for real-time stale read, got %s", res.Outcome)
	}
	if res.Counterexample == "" {
		t.Fatal("expected counterexample explanation")
	}
}

func TestChecker_IncorrectCASFails(t *testing.T) {
	c := NewChecker(1000)

	// Key is "10"
	// CAS expects "99", but falsely claims swapped=true
	history := []OperationRecord{
		{
			ID: 1, Op: protocol.OpPut, Key: "bal", Value: "10",
			InvokeTime: 10, ReturnTime: 20,
		},
		{
			ID: 2, Op: protocol.OpCAS, Key: "bal", Expected: "99", Value: "20",
			InvokeTime: 30, ReturnTime: 40,
			ObservedSwapped: true, // FRAUDULENT
		},
	}

	res := c.Check(history)
	if res.Outcome != OutcomeViolation {
		t.Fatalf("expected VIOLATION for invalid CAS, got %s", res.Outcome)
	}
}

func TestChecker_TimedOutOperationHandled(t *testing.T) {
	c := NewChecker(1000)

	// Op 1 (Put x=A) timed out at 20
	// Op 2 (Get x) invoked at 30, returned "A"
	// Legal linearization: Op 1 took effect!
	history := []OperationRecord{
		{
			ID: 1, Op: protocol.OpPut, Key: "x", Value: "A",
			InvokeTime: 10, ReturnTime: 20,
			TimedOut: true,
		},
		{
			ID: 2, Op: protocol.OpGet, Key: "x",
			InvokeTime: 30, ReturnTime: 40,
			ObservedValue: "A", ObservedFound: true,
		},
	}

	res := c.Check(history)
	if res.Outcome != OutcomePass {
		t.Fatalf("expected PASS when timed out op took effect, got %s", res.Outcome)
	}
}

func TestChecker_SearchBoundOverflowReturnsInconclusive(t *testing.T) {
	// Set max search steps very low (e.g. 2 steps)
	c := NewChecker(2)

	history := []OperationRecord{
		{ID: 1, Op: protocol.OpPut, Key: "a", Value: "1", InvokeTime: 10, ReturnTime: 100},
		{ID: 2, Op: protocol.OpPut, Key: "b", Value: "2", InvokeTime: 10, ReturnTime: 100},
		{ID: 3, Op: protocol.OpPut, Key: "c", Value: "3", InvokeTime: 10, ReturnTime: 100},
		{ID: 4, Op: protocol.OpPut, Key: "d", Value: "4", InvokeTime: 10, ReturnTime: 100},
	}

	res := c.Check(history)
	if res.Outcome != OutcomeInconclusive {
		t.Fatalf("expected INCONCLUSIVE on search bound overflow, got %s", res.Outcome)
	}
}

func TestChecker_ExceedsBitmaskCapacityReturnsInconclusive(t *testing.T) {
	c := NewChecker(1000)
	var history []OperationRecord
	for i := 1; i <= 65; i++ {
		history = append(history, OperationRecord{
			ID:         uint64(i),
			Op:         protocol.OpPut,
			Key:        "k",
			Value:      "v",
			InvokeTime: int64(i * 10),
			ReturnTime: int64(i*10 + 5),
		})
	}

	res := c.Check(history)
	if res.Outcome != OutcomeInconclusive {
		t.Fatalf("expected INCONCLUSIVE for history length 65, got %s", res.Outcome)
	}
}
