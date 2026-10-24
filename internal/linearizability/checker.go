package linearizability

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"raftkv/internal/protocol"
)

type CheckOutcome string

const (
	OutcomePass         CheckOutcome = "PASS"
	OutcomeViolation    CheckOutcome = "VIOLATION"
	OutcomeInconclusive CheckOutcome = "INCONCLUSIVE"
)

// OperationRecord represents a logged client operation with invocation/return intervals.
type OperationRecord struct {
	ID              uint64          `json:"id"`
	ClientID        string          `json:"client_id"`
	Op              protocol.OpType `json:"op"`
	Key             string          `json:"key"`
	Value           string          `json:"value,omitempty"`
	Expected        string          `json:"expected,omitempty"`
	InvokeTime      int64           `json:"invoke_time"`
	ReturnTime      int64           `json:"return_time"`
	ObservedValue   string          `json:"observed_value,omitempty"`
	ObservedFound   bool            `json:"observed_found,omitempty"`
	ObservedSwapped bool            `json:"observed_swapped,omitempty"`
	TimedOut        bool            `json:"timed_out"`
}

// CheckResult details the outcome, witness sequence, or counterexample.
type CheckResult struct {
	Outcome        CheckOutcome
	StepsExplored  int
	Witness        []OperationRecord
	Counterexample string
}

// Checker verifies history linearizability against a sequential key-value spec.
type Checker struct {
	MaxSearchSteps int
}

func NewChecker(maxSteps int) *Checker {
	if maxSteps <= 0 {
		maxSteps = 50000
	}
	return &Checker{MaxSearchSteps: maxSteps}
}

// Check searches for a valid sequential linearization respecting real-time precedence.
func (c *Checker) Check(history []OperationRecord) CheckResult {
	if len(history) == 0 {
		return CheckResult{Outcome: OutcomePass}
	}

	// Sort operations by InvokeTime initially
	ops := make([]OperationRecord, len(history))
	copy(ops, history)
	sort.SliceStable(ops, func(i, j int) bool {
		return ops[i].InvokeTime < ops[j].InvokeTime
	})

	for i := range ops {
		if ops[i].TimedOut {
			ops[i].ReturnTime = math.MaxInt64
		}
	}

	// Precompute real-time precedence:
	// A strictly precedes B if A.ReturnTime < B.InvokeTime
	n := len(ops)
	preds := make([]uint64, n) // bitmask of predecessors for each op
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j && ops[j].ReturnTime < ops[i].InvokeTime {
				preds[i] |= (1 << j)
			}
		}
	}

	// Target completed mask: all completed operations MUST be linearized
	var completedMask uint64 = 0
	for i := 0; i < n; i++ {
		if !ops[i].TimedOut {
			completedMask |= (1 << i)
		}
	}

	visited := make(map[string]bool)
	steps := 0
	var witness []OperationRecord

	var search func(mask uint64, state *KVState) bool
	search = func(mask uint64, state *KVState) bool {
		steps++
		if steps > c.MaxSearchSteps {
			return false
		}

		// Success if all completed operations have been successfully linearized
		if (mask & completedMask) == completedMask {
			return true
		}

		memoKey := fmt.Sprintf("%x:%s", mask, state.Key())
		if visited[memoKey] {
			return false
		}
		visited[memoKey] = true

		// Find candidates: unlinearized ops whose predecessors are all linearized
		for i := 0; i < n; i++ {
			bit := uint64(1 << i)
			if (mask & bit) != 0 {
				continue // already linearized
			}

			// Predecessors must be satisfied
			if (preds[i] & ^mask) != 0 {
				continue
			}

			op := ops[i]
			if op.TimedOut {
				// Timed out op option 1: try linearizing it if it matches
				ok, nextState := state.Step(op)
				if ok {
					witness = append(witness, op)
					if search(mask|bit, nextState) {
						return true
					}
					witness = witness[:len(witness)-1]
				}

				// Timed out op option 2: skip linearizing it (treated as having no effect)
				// It simply marks as consumed in mask without updating state
				if search(mask|bit, state) {
					return true
				}
			} else {
				// Completed op: MUST match sequential specification
				ok, nextState := state.Step(op)
				if ok {
					witness = append(witness, op)
					if search(mask|bit, nextState) {
						return true
					}
					witness = witness[:len(witness)-1]
				}
			}
		}

		return false
	}

	initialState := NewKVState()
	passed := search(0, initialState)

	if steps > c.MaxSearchSteps {
		return CheckResult{
			Outcome:       OutcomeInconclusive,
			StepsExplored: steps,
			Counterexample: fmt.Sprintf("search bound exceeded (%d steps) without finding valid order", steps),
		}
	}

	if passed {
		return CheckResult{
			Outcome:       OutcomePass,
			StepsExplored: steps,
			Witness:       witness,
		}
	}

	// Build actionable counterexample
	var sb strings.Builder
	sb.WriteString("Linearizability violation: no legal sequential order exists respecting real-time order.\n")
	sb.WriteString("Operations evaluated:\n")
	for _, o := range ops {
		sb.WriteString(fmt.Sprintf("  #%d [invoke=%d, return=%d] %s %s = %q (found=%v, swapped=%v)\n",
			o.ID, o.InvokeTime, o.ReturnTime, o.Op, o.Key, o.ObservedValue, o.ObservedFound, o.ObservedSwapped))
	}

	return CheckResult{
		Outcome:        OutcomeViolation,
		StepsExplored:  steps,
		Counterexample: sb.String(),
	}
}
