package raft

import "errors"

// ErrKilled is returned by RPC handlers of a peer that has been shut down.
var ErrKilled = errors.New("raft: peer is shut down")

// RPCService exposes exactly the Raft RPC handlers, with the net/rpc calling
// convention, so it can be registered (as "Raft") on either a simnet.Server
// or a net/rpc server without leaking other methods.
type RPCService struct{ rf *Raft }

// Service returns the RPC endpoint for this peer.
func (rf *Raft) Service() *RPCService { return &RPCService{rf} }

func (s *RPCService) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	if s.rf.killed() {
		return ErrKilled
	}
	s.rf.handleRequestVote(args, reply)
	return nil
}

func (s *RPCService) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	if s.rf.killed() {
		return ErrKilled
	}
	s.rf.handleAppendEntries(args, reply)
	return nil
}

func (s *RPCService) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	if s.rf.killed() {
		return ErrKilled
	}
	s.rf.handleInstallSnapshot(args, reply)
	return nil
}
