package linearizability

import (
	"fmt"
	"raftkv/internal/protocol"
)

// KVState represents the sequential in-memory key-value state.
type KVState struct {
	data map[string]string
}

func NewKVState() *KVState {
	return &KVState{data: make(map[string]string)}
}

func (s *KVState) Clone() *KVState {
	c := make(map[string]string, len(s.data))
	for k, v := range s.data {
		c[k] = v
	}
	return &KVState{data: c}
}

func (s *KVState) Step(op OperationRecord) (bool, *KVState) {
	next := s.Clone()

	switch op.Op {
	case protocol.OpPut:
		next.data[op.Key] = op.Value
		return true, next

	case protocol.OpGet:
		val, found := next.data[op.Key]
		if found != op.ObservedFound {
			return false, nil
		}
		if found && val != op.ObservedValue {
			return false, nil
		}
		return true, next

	case protocol.OpDelete:
		_, found := next.data[op.Key]
		if found != op.ObservedFound {
			return false, nil
		}
		if found {
			delete(next.data, op.Key)
		}
		return true, next

	case protocol.OpCAS:
		curVal, found := next.data[op.Key]
		var swapped bool
		var finalVal string

		if !found {
			if op.Expected == "" {
				next.data[op.Key] = op.Value
				swapped = true
				finalVal = op.Value
			} else {
				swapped = false
				finalVal = ""
			}
		} else {
			if curVal == op.Expected {
				next.data[op.Key] = op.Value
				swapped = true
				finalVal = op.Value
			} else {
				swapped = false
				finalVal = curVal
			}
		}

		if swapped != op.ObservedSwapped {
			return false, nil
		}
		if !swapped && finalVal != op.ObservedValue {
			return false, nil
		}
		return true, next

	default:
		return false, nil
	}
}

func (s *KVState) Key() string {
	return fmt.Sprintf("%v", s.data)
}
