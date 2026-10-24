package main

import (
	"fmt"
	"testing"

	"raftkv/internal/protocol"
	"raftkv/internal/statemachine"
)

func BenchmarkInMemoryApply(b *testing.B) {
	sm := statemachine.NewKVStateMachine()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.Apply(protocol.LogEntry{
			Index: protocol.Index(i + 1),
			Term:  1,
			Command: protocol.Command{
				Op:    protocol.OpPut,
				Key:   fmt.Sprintf("bench-%d", i),
				Value: "val",
			},
		})
	}
}
