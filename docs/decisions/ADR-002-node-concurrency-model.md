# ADR-002: Node State Ownership and Concurrency Model

## Context
Raft consensus engines manage complex mutable state: terms, roles, log entries, commit indices, peer progress counters, pending client response channels, and state machine state.
Ad-hoc multi-goroutine mutex locking frequently introduces race conditions, deadlocks, and subtle protocol invariant violations (such as reading state that is mid-mutation or holding locks across network calls).

## Decision
We adopt a **single-owner event-loop (actor-style) concurrency model**:
1. All mutations to Raft state, log indexing, and peer progress occur strictly inside the single-owner Raft event loop.
2. Inbound network RPCs (gRPC handlers), timer ticks, and background completions communicate with the Raft actor via a typed event channel (`chan Event`).
3. Handlers submit events to the mailbox and, if a response is expected, wait on a response channel.
4. Outbound RPCs are executed asynchronously by worker goroutines to prevent blocking the event loop; results from outbound RPCs are posted back to the event mailbox as typed response events.
5. Durable WAL operations are invoked synchronously by the event loop at critical protocol boundaries before emitting protocol replies.

## Consequences
- **Positive**: Eliminates data races by design (`go test -race` passes cleanly); state transitions are totally ordered and inspectable; deterministic simulation is straightforward to implement by driving the event loop.
- **Negative**: Long-running synchronous tasks inside the event loop can delay heartbeat processing; storage I/O must remain fast and framed.
- **Status**: ACCEPTED.
