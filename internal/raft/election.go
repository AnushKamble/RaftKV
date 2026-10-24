package raft

import (
	"context"

	raftkvv1 "raftkv/gen/raftkv/v1"
	"raftkv/internal/protocol"
)

func (r *Raft) handleElectionTimeout() {
	if r.role == protocol.RoleLeader {
		// Quorum check (§6.2, CheckQuorum):
		// Verify leader has received responses from a majority of nodes (including itself) within election timeout.
		totalNodes := len(r.peers) + 1
		majority := (totalNodes / 2) + 1
		contacted := 1 // leader itself
		for _, p := range r.peers {
			if r.ackedPeers[string(p)] {
				contacted++
			}
		}

		if contacted < majority {
			// Lost quorum: step down immediately
			r.becomeFollower(r.currentTerm, "")
			return
		}

		// Quorum intact: reset peer acks and reset election timer for next check
		r.ackedPeers = make(map[string]bool)
		r.resetElectionTimeout()
		return
	}

	r.currentTerm++
	r.role = protocol.RoleCandidate
	r.votedFor = string(r.id)
	r.leaderID = ""
	if err := r.storage.SaveHardState(r.currentTerm, r.votedFor); err != nil {
		// Disk error: cannot persist candidate state, abort campaign
		return
	}

	r.resetElectionTimeout()
	r.votesReceived = map[string]bool{string(r.id): true}

	totalNodes := len(r.peers) + 1
	if len(r.votesReceived) >= (totalNodes/2)+1 {
		r.becomeLeader()
		return
	}

	lastIdx, lastTerm, _ := r.storage.LastIndexAndTerm()
	term := r.currentTerm
	candidateID := string(r.id)

	for _, peer := range r.peers {
		go func(p protocol.NodeID) {
			ctx, cancel := context.WithTimeout(context.Background(), r.minElectionTimeout)
			defer cancel()

			args := &raftkvv1.RequestVoteArgs{
				Term:         uint64(term),
				CandidateId:  candidateID,
				LastLogIndex: uint64(lastIdx),
				LastLogTerm:  uint64(lastTerm),
			}

			reply, err := r.transport.SendRequestVote(ctx, string(p), args)
			if err != nil {
				return
			}

			r.PostEvent(requestVoteReplyEvent{
				from:        string(p),
				term:        protocol.Term(reply.Term),
				voteGranted: reply.VoteGranted,
			})
		}(peer)
	}
}

func (r *Raft) handleRequestVote(args *raftkvv1.RequestVoteArgs) *raftkvv1.RequestVoteReply {
	reqTerm := protocol.Term(args.Term)

	// 1. Reply false if term < currentTerm (§5.1)
	if reqTerm < r.currentTerm {
		return &raftkvv1.RequestVoteReply{
			Term:        uint64(r.currentTerm),
			VoteGranted: false,
		}
	}

	// 2. If term > currentTerm, adopt term and become follower (§5.1)
	if reqTerm > r.currentTerm {
		r.becomeFollower(reqTerm, "")
	}

	// 3. Log freshness check (§5.4)
	localLastIdx, localLastTerm, _ := r.storage.LastIndexAndTerm()
	candidateIsFresher := false
	if protocol.Term(args.LastLogTerm) > localLastTerm {
		candidateIsFresher = true
	} else if protocol.Term(args.LastLogTerm) == localLastTerm && protocol.Index(args.LastLogIndex) >= localLastIdx {
		candidateIsFresher = true
	}

	// 4. Grant vote if not voted yet (or voted for this candidate) and candidate log is at least as up-to-date
	canVote := (r.votedFor == "" || r.votedFor == args.CandidateId) && candidateIsFresher

	if canVote {
		r.votedFor = args.CandidateId
		if err := r.storage.SaveHardState(r.currentTerm, r.votedFor); err != nil {
			return &raftkvv1.RequestVoteReply{
				Term:        uint64(r.currentTerm),
				VoteGranted: false,
			}
		}
		r.resetElectionTimeout() // Reset timer on granting vote
		return &raftkvv1.RequestVoteReply{
			Term:        uint64(r.currentTerm),
			VoteGranted: true,
		}
	}

	return &raftkvv1.RequestVoteReply{
		Term:        uint64(r.currentTerm),
		VoteGranted: false,
	}
}

func (r *Raft) handleRequestVoteReply(e requestVoteReplyEvent) {
	if e.term > r.currentTerm {
		r.becomeFollower(e.term, "")
		return
	}

	if r.role != protocol.RoleCandidate || e.term != r.currentTerm {
		return // stale or irrelevant reply
	}

	if e.voteGranted {
		r.votesReceived[e.from] = true
		totalNodes := len(r.peers) + 1
		majority := (totalNodes / 2) + 1
		if len(r.votesReceived) >= majority {
			r.becomeLeader()
		}
	}
}
