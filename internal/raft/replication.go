package raft

import (
	"context"
	"fmt"

	raftkvv1 "raftkv/gen/raftkv/v1"
	"raftkv/internal/protocol"
)

func (r *Raft) handleHeartbeatTimeout() {
	if r.role != protocol.RoleLeader {
		return
	}
	r.heartbeatTimer.Reset(r.heartbeatInterval)
	r.broadcastAppendEntries()
}

func (r *Raft) broadcastAppendEntries() {
	for _, peer := range r.peers {
		r.sendAppendEntriesToPeer(peer)
	}
}

func (r *Raft) sendAppendEntriesToPeer(peer protocol.NodeID) {
	prevIdx := r.nextIndex[string(peer)] - 1
	prevTerm, _ := r.storage.Term(prevIdx)
	lastIdx, _, _ := r.storage.LastIndexAndTerm()

	var entriesProto []*raftkvv1.LogEntryProto
	if lastIdx >= r.nextIndex[string(peer)] {
		entries, err := r.storage.Entries(r.nextIndex[string(peer)], lastIdx+1)
		if err == nil {
			for _, e := range entries {
				cmdBytes, _ := e.Command.Encode()
				entriesProto = append(entriesProto, &raftkvv1.LogEntryProto{
					Index: uint64(e.Index),
					Term:  uint64(e.Term),
					Data:  cmdBytes,
				})
			}
		}
	}

	term := r.currentTerm
	leaderID := string(r.id)
	leaderCommit := r.commitIndex

	go func(p protocol.NodeID, pIdx protocol.Index, pTerm protocol.Term) {
		ctx, cancel := context.WithTimeout(context.Background(), r.minElectionTimeout)
		defer cancel()

		args := &raftkvv1.AppendEntriesArgs{
			Term:         uint64(term),
			LeaderId:     leaderID,
			PrevLogIndex: uint64(pIdx),
			PrevLogTerm:  uint64(pTerm),
			Entries:      entriesProto,
			LeaderCommit: uint64(leaderCommit),
		}

		reply, err := r.transport.SendAppendEntries(ctx, string(p), args)
		if err != nil {
			return
		}

		match := pIdx + protocol.Index(len(entriesProto))
		r.PostEvent(appendEntriesReplyEvent{
			from:          string(p),
			term:          protocol.Term(reply.Term),
			success:       reply.Success,
			matchIndex:    match,
			conflictIndex: protocol.Index(reply.ConflictIndex),
			conflictTerm:  protocol.Term(reply.ConflictTerm),
		})
	}(peer, prevIdx, prevTerm)
}

func (r *Raft) handleAppendEntries(args *raftkvv1.AppendEntriesArgs) *raftkvv1.AppendEntriesReply {
	reqTerm := protocol.Term(args.Term)

	// 1. Reply false if term < currentTerm (§5.1)
	if reqTerm < r.currentTerm {
		return &raftkvv1.AppendEntriesReply{
			Term:    uint64(r.currentTerm),
			Success: false,
		}
	}

	// 2. If term > currentTerm or valid leader contact, reset election timer and adopt leader (§5.2, §5.3)
	if reqTerm > r.currentTerm {
		r.becomeFollower(reqTerm, protocol.NodeID(args.LeaderId))
	} else if r.role == protocol.RoleCandidate {
		r.becomeFollower(reqTerm, protocol.NodeID(args.LeaderId))
	} else {
		r.leaderID = protocol.NodeID(args.LeaderId)
		r.resetElectionTimeout()
	}

	// 3. Check prevLogIndex and prevLogTerm
	prevLogIndex := protocol.Index(args.PrevLogIndex)
	prevLogTerm := protocol.Term(args.PrevLogTerm)

	lastIdx, _, _ := r.storage.LastIndexAndTerm()
	if prevLogIndex > lastIdx {
		// Log too short: hint conflictIndex = lastIdx + 1
		return &raftkvv1.AppendEntriesReply{
			Term:          uint64(r.currentTerm),
			Success:       false,
			ConflictIndex: uint64(lastIdx + 1),
			ConflictTerm:  0,
		}
	}

	if prevLogIndex > 0 {
		actualTerm, err := r.storage.Term(prevLogIndex)
		if err != nil || actualTerm != prevLogTerm {
			// Term mismatch at prevLogIndex
			conflictIndex := prevLogIndex
			firstIdx, _ := r.storage.FirstIndex()
			for idx := prevLogIndex; idx >= firstIdx; idx-- {
				t, err := r.storage.Term(idx)
				if err != nil || t != actualTerm {
					break
				}
				conflictIndex = idx
			}
			return &raftkvv1.AppendEntriesReply{
				Term:          uint64(r.currentTerm),
				Success:       false,
				ConflictIndex: uint64(conflictIndex),
				ConflictTerm:  uint64(actualTerm),
			}
		}
	}

	// 4. Process entries: detect conflicts, truncate, append
	for i, entryProto := range args.Entries {
		entryIdx := protocol.Index(entryProto.Index)
		entryTerm := protocol.Term(entryProto.Term)

		existingTerm, err := r.storage.Term(entryIdx)
		if err == nil {
			if existingTerm != entryTerm {
				// Invariant check: never truncate at or below commitIndex!
				if entryIdx <= r.commitIndex {
					panic(fmt.Sprintf("invariant violation: attempting to truncate committed log entry: entryIdx=%d, commitIndex=%d, existingTerm=%d, newTerm=%d", entryIdx, r.commitIndex, existingTerm, entryTerm))
				}
				// Suffix conflict detected: truncate from entryIdx
				if err := r.storage.TruncateSuffix(entryIdx); err != nil {
					return &raftkvv1.AppendEntriesReply{Term: uint64(r.currentTerm), Success: false}
				}
				// Append remaining entries
				var toAppend []protocol.LogEntry
				for _, ep := range args.Entries[i:] {
					cmd, _ := protocol.DecodeCommand(ep.Data)
					toAppend = append(toAppend, protocol.LogEntry{
						Index:   protocol.Index(ep.Index),
						Term:    protocol.Term(ep.Term),
						Command: cmd,
					})
				}
				_ = r.storage.AppendEntries(toAppend)
				break
			}
		} else {
			// Entry does not exist locally: append it and all following
			var toAppend []protocol.LogEntry
			for _, ep := range args.Entries[i:] {
				cmd, _ := protocol.DecodeCommand(ep.Data)
				toAppend = append(toAppend, protocol.LogEntry{
					Index:   protocol.Index(ep.Index),
					Term:    protocol.Term(ep.Term),
					Command: cmd,
				})
			}
			_ = r.storage.AppendEntries(toAppend)
			break
		}
	}

	// 5. Update commitIndex (§5.3)
	leaderCommit := protocol.Index(args.LeaderCommit)
	if leaderCommit > r.commitIndex {
		newLastIdx, _, _ := r.storage.LastIndexAndTerm()
		if leaderCommit < newLastIdx {
			r.commitIndex = leaderCommit
		} else {
			r.commitIndex = newLastIdx
		}
		r.applyCommittedEntries()
	}

	return &raftkvv1.AppendEntriesReply{
		Term:    uint64(r.currentTerm),
		Success: true,
	}
}

func (r *Raft) handleAppendEntriesReply(e appendEntriesReplyEvent) {
	if e.term > r.currentTerm {
		r.becomeFollower(e.term, "")
		return
	}

	if r.role != protocol.RoleLeader || e.term != r.currentTerm {
		return
	}

	if e.success {
		// Monotonic match index: ignore stale out-of-order replies
		if e.matchIndex > r.matchIndex[e.from] {
			r.matchIndex[e.from] = e.matchIndex
			r.nextIndex[e.from] = e.matchIndex + 1
			r.maybeAdvanceCommitIndex()
		}
	} else {
		// Backtrack nextIndex using conflict hint
		if e.conflictTerm > 0 {
			// Find last index on leader with conflictTerm
			foundIndex := protocol.Index(0)
			lastIdx, _, _ := r.storage.LastIndexAndTerm()
			firstIdx, _ := r.storage.FirstIndex()
			for idx := lastIdx; idx >= firstIdx; idx-- {
				t, err := r.storage.Term(idx)
				if err == nil && t == e.conflictTerm {
					foundIndex = idx
					break
				}
			}
			if foundIndex > 0 {
				r.nextIndex[e.from] = foundIndex + 1
			} else if e.conflictIndex > 0 {
				r.nextIndex[e.from] = e.conflictIndex
			} else if r.nextIndex[e.from] > 1 {
				r.nextIndex[e.from]--
			}
		} else if e.conflictIndex > 0 {
			r.nextIndex[e.from] = e.conflictIndex
		} else if r.nextIndex[e.from] > 1 {
			r.nextIndex[e.from]--
		}

		if r.nextIndex[e.from] < 1 {
			r.nextIndex[e.from] = 1
		}

		// Retry replication
		r.sendAppendEntriesToPeer(protocol.NodeID(e.from))
	}
}

func (r *Raft) maybeAdvanceCommitIndex() {
	lastIdx, _, _ := r.storage.LastIndexAndTerm()
	totalNodes := len(r.peers) + 1
	majority := (totalNodes / 2) + 1

	for N := lastIdx; N > r.commitIndex; N-- {
		termN, err := r.storage.Term(N)
		if err != nil {
			continue
		}

		// Current-Term Restriction (§5.4.2, Figure 8):
		// Raft never commits log entries from previous terms by counting replicas.
		// Only entries from the leader's current term can be committed by counting replicas!
		if termN != r.currentTerm {
			continue
		}

		replicatedCount := 1 // leader itself
		for _, peer := range r.peers {
			if r.matchIndex[string(peer)] >= N {
				replicatedCount++
			}
		}

		if replicatedCount >= majority {
			r.commitIndex = N
			r.applyCommittedEntries()
			break
		}
	}
}

func (r *Raft) applyCommittedEntries() {
	for r.commitIndex > r.lastApplied {
		r.lastApplied++
		entry, err := r.storage.Entry(r.lastApplied)
		if err == nil && r.applyChan != nil {
			r.applyChan <- entry
		}
	}
}

func (r *Raft) handleInstallSnapshot(args *raftkvv1.InstallSnapshotArgs) *raftkvv1.InstallSnapshotReply {
	reqTerm := protocol.Term(args.Term)
	if reqTerm < r.currentTerm {
		return &raftkvv1.InstallSnapshotReply{Term: uint64(r.currentTerm)}
	}
	if reqTerm > r.currentTerm {
		r.becomeFollower(reqTerm, protocol.NodeID(args.LeaderId))
	} else {
		r.resetElectionTimeout()
	}

	snapIndex := protocol.Index(args.LastIncludedIndex)
	snapTerm := protocol.Term(args.LastIncludedTerm)

	// Save snapshot to disk
	meta := protocol.SnapshotMeta{
		LastIncludedIndex: snapIndex,
		LastIncludedTerm:  snapTerm,
		FormatVersion:     1,
	}
	_ = r.storage.SaveSnapshot(meta, args.Data)
	_ = r.storage.CompactLog(snapIndex, snapTerm)

	if snapIndex > r.commitIndex {
		r.commitIndex = snapIndex
	}
	if snapIndex > r.lastApplied {
		r.lastApplied = snapIndex
	}

	return &raftkvv1.InstallSnapshotReply{Term: uint64(r.currentTerm)}
}

func (r *Raft) handleInstallSnapshotReply(e installSnapshotReplyEvent) {
	if e.term > r.currentTerm {
		r.becomeFollower(e.term, "")
	}
}

func (r *Raft) handleClientCommand(e clientCommandEvent) {
	if r.role != protocol.RoleLeader {
		e.replyCh <- protocol.CommandResult{
			Success:   false,
			ErrorCode: "NOT_LEADER",
		}
		return
	}

	lastIdx, _, _ := r.storage.LastIndexAndTerm()
	newIdx := lastIdx + 1
	entry := protocol.LogEntry{
		Index:   newIdx,
		Term:    r.currentTerm,
		Command: e.cmd,
	}

	if err := r.storage.AppendEntries([]protocol.LogEntry{entry}); err != nil {
		e.replyCh <- protocol.CommandResult{
			Success:   false,
			ErrorCode: "STORAGE_ERROR",
		}
		return
	}

	// Trigger replication immediately
	r.broadcastAppendEntries()

	// Wait for commit in background or return index
	// Note: Client commands will be fully hooked up with state machine applier in Phase 7
	e.replyCh <- protocol.CommandResult{
		Success:     true,
		CommitIndex: newIdx,
	}
}
