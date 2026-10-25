// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout,
// "In Search of an Understandable Consensus Algorithm", 2014).
//
// Implemented: leader election, log replication with fast log backtracking,
// persistence, and log compaction via snapshots (InstallSnapshot).
//
// A service (e.g. a key/value store) calls Start to propose a command. Once
// the command is committed, Raft delivers it on applyCh, in log order, on
// every server. The service periodically calls Snapshot to let Raft discard
// the log prefix the snapshot covers.
package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Peer is anything that can deliver an RPC to another Raft server:
// a simnet.End in tests, a TCP client in production.
type Peer interface {
	Call(serviceMethod string, args, reply any) bool
}

// ApplyMsg is sent on applyCh for every committed entry (CommandValid) or
// when a snapshot from the leader must replace the service's state
// (SnapshotValid).
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

// Entry is one slot of the replicated log.
type Entry struct {
	Index   int
	Term    int
	Command any
}

// Config holds the timing parameters. The election timeout must be several
// heartbeat intervals long so that followers don't time out spuriously.
type Config struct {
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	MaxEntriesPerRPC   int
}

// DefaultConfig works well both in-process and on a LAN.
var DefaultConfig = Config{
	HeartbeatInterval:  50 * time.Millisecond,
	ElectionTimeoutMin: 300 * time.Millisecond,
	ElectionTimeoutMax: 600 * time.Millisecond,
	MaxEntriesPerRPC:   512,
}

type role int

const (
	follower role = iota
	candidate
	leader
)

func (r role) String() string { return [...]string{"follower", "candidate", "leader"}[r] }

// Raft is a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []Peer
	me        int
	persister Persister
	applyCh   chan<- ApplyMsg
	cfg       Config
	dead      atomic.Bool
	killCh    chan struct{}

	// Persistent state (Figure 2). log[0] is a sentinel whose Index/Term are
	// those of the last entry covered by the snapshot.
	currentTerm int
	votedFor    int
	log         []Entry
	snapshot    []byte

	// Volatile state.
	role             role
	leaderID         int
	commitIndex      int
	lastApplied      int
	electionDeadline time.Time
	pendingSnapshot  bool // a snapshot from the leader awaits delivery on applyCh
	applyCond        *sync.Cond

	// Leader state, reinitialised after each election.
	nextIndex     []int
	matchIndex    []int
	backoff       []bool // last RPC to peer failed; wait for the next heartbeat tick
	replCond      []*sync.Cond
	lastHeartbeat time.Time
}

// Make creates a Raft peer. peers[me] is this server (it is never called).
// Persisted state, if any, is restored from persister. Committed commands
// are delivered on applyCh.
func Make(peers []Peer, me int, persister Persister, applyCh chan<- ApplyMsg, cfg Config) *Raft {
	rf := &Raft{
		peers:     peers,
		me:        me,
		persister: persister,
		applyCh:   applyCh,
		cfg:       cfg,
		killCh:    make(chan struct{}),
		votedFor:  -1,
		leaderID:  -1,
		log:       []Entry{{Index: 0, Term: 0}},
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	n := len(peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	rf.backoff = make([]bool, n)
	rf.replCond = make([]*sync.Cond, n)

	rf.readPersist(persister.ReadState())
	rf.snapshot = persister.ReadSnapshot()
	rf.commitIndex = rf.log[0].Index
	rf.lastApplied = rf.log[0].Index
	rf.resetElectionTimerLocked()

	for i := range peers {
		if i == me {
			continue
		}
		rf.replCond[i] = sync.NewCond(&rf.mu)
		go rf.replicator(i)
	}
	go rf.ticker()
	go rf.applier()
	return rf
}

// ---------------------------------------------------------------------------
// Public API

// Start proposes command for the log. It returns immediately with the index
// the command will occupy if it is ever committed, the current term, and
// whether this server believes it is the leader. Commitment is not
// guaranteed: the leader may lose leadership before the entry replicates.
func (rf *Raft) Start(command any) (index, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != leader || rf.dead.Load() {
		return -1, rf.currentTerm, false
	}
	index = rf.lastIndex() + 1
	rf.log = append(rf.log, Entry{Index: index, Term: rf.currentTerm, Command: command})
	rf.persistLocked()
	rf.matchIndex[rf.me] = index
	rf.advanceCommitLocked() // a single-node cluster commits immediately
	for i := range rf.peers {
		if i != rf.me {
			rf.replCond[i].Signal()
		}
	}
	return index, rf.currentTerm, true
}

// GetState returns the current term and whether this server is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == leader
}

// Status is a point-in-time view of the peer, for monitoring.
type Status struct {
	ID          int    `json:"id"`
	Term        int    `json:"term"`
	Role        string `json:"role"`
	LeaderID    int    `json:"leader_id"`
	CommitIndex int    `json:"commit_index"`
	LastApplied int    `json:"last_applied"`
	LastIndex   int    `json:"last_log_index"`
	SnapshotIdx int    `json:"snapshot_index"`
	LogLength   int    `json:"log_entries_in_memory"`
}

func (rf *Raft) Status() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return Status{
		ID: rf.me, Term: rf.currentTerm, Role: rf.role.String(), LeaderID: rf.leaderID,
		CommitIndex: rf.commitIndex, LastApplied: rf.lastApplied, LastIndex: rf.lastIndex(),
		SnapshotIdx: rf.log[0].Index, LogLength: len(rf.log) - 1,
	}
}

// Snapshot tells Raft that the service has a snapshot containing every
// entry up to and including index, so the log prefix can be discarded.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	base := rf.log[0].Index
	if index <= base || index > rf.commitIndex {
		return
	}
	rest := rf.log[index-base+1:]
	newLog := make([]Entry, 1, 1+len(rest))
	newLog[0] = Entry{Index: index, Term: rf.log[index-base].Term}
	rf.log = append(newLog, rest...)
	rf.snapshot = snapshot
	rf.persistLocked()
}

// Kill stops the peer. It is safe to call more than once.
func (rf *Raft) Kill() {
	if rf.dead.Swap(true) {
		return
	}
	close(rf.killCh)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	for _, c := range rf.replCond {
		if c != nil {
			c.Broadcast()
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool { return rf.dead.Load() }

// ---------------------------------------------------------------------------
// Log helpers (all require rf.mu)

func (rf *Raft) lastIndex() int    { return rf.log[len(rf.log)-1].Index }
func (rf *Raft) lastTerm() int     { return rf.log[len(rf.log)-1].Term }
func (rf *Raft) termAt(i int) int  { return rf.log[i-rf.log[0].Index].Term }
func (rf *Raft) entry(i int) Entry { return rf.log[i-rf.log[0].Index] }

// ---------------------------------------------------------------------------
// Persistence

type persistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []Entry
}

func (rf *Raft) persistLocked() {
	if rf.killed() {
		return // a shut-down peer must not touch storage its successor may own
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(persistentState{rf.currentTerm, rf.votedFor, rf.log}); err != nil {
		panic("raft: encoding state: " + err.Error())
	}
	rf.persister.Save(buf.Bytes(), rf.snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	var st persistentState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&st); err != nil {
		panic("raft: decoding state: " + err.Error())
	}
	rf.currentTerm, rf.votedFor, rf.log = st.CurrentTerm, st.VotedFor, st.Log
}

// ---------------------------------------------------------------------------
// State transitions (require rf.mu)

func (rf *Raft) resetElectionTimerLocked() {
	spread := rf.cfg.ElectionTimeoutMax - rf.cfg.ElectionTimeoutMin
	rf.electionDeadline = time.Now().Add(rf.cfg.ElectionTimeoutMin + time.Duration(rand.Int63n(int64(spread)+1)))
}

// stepDownLocked moves to a newer term as a follower (§5.1: "if RPC request
// or response contains term T > currentTerm: set currentTerm = T, convert to
// follower").
func (rf *Raft) stepDownLocked(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.role = follower
	rf.leaderID = -1
	rf.persistLocked()
}

func (rf *Raft) becomeLeaderLocked() {
	rf.role = leader
	rf.leaderID = rf.me
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastIndex() + 1
		rf.matchIndex[i] = 0
		rf.backoff[i] = false
	}
	rf.matchIndex[rf.me] = rf.lastIndex()
	rf.heartbeatLocked() // assert leadership immediately
}

// heartbeatLocked sends an AppendEntries to every follower right away, each
// in its own goroutine. Heartbeats are deliberately independent of the
// per-peer replicators: a replicator may be stuck waiting for a slow reply,
// and that must never starve a follower of heartbeats (which would make it
// time out and depose a perfectly healthy leader).
func (rf *Raft) heartbeatLocked() {
	rf.lastHeartbeat = time.Now()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.backoff[i] = false
		args := rf.heartbeatArgsLocked(i)
		go func(peer int) {
			var reply AppendEntriesReply
			if !rf.peers[peer].Call("Raft.AppendEntries", &args, &reply) {
				return
			}
			rf.mu.Lock()
			rf.handleAppendReplyLocked(peer, &args, &reply)
			rf.mu.Unlock()
		}(i)
	}
	rf.broadcastLocked()
}

// heartbeatArgsLocked builds an empty AppendEntries whose consistency check
// is at the follower's nextIndex (clamped to the snapshot boundary), so a
// heartbeat also helps the leader discover where logs diverge.
func (rf *Raft) heartbeatArgsLocked(peer int) AppendEntriesArgs {
	prev := max(rf.nextIndex[peer]-1, rf.log[0].Index)
	prev = min(prev, rf.lastIndex())
	return AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.termAt(prev),
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) broadcastLocked() {
	for i := range rf.peers {
		if i != rf.me {
			rf.replCond[i].Signal()
		}
	}
}

// advanceCommitLocked commits the highest index replicated on a majority,
// but only if that entry is from the current term (§5.4.2, Figure 8):
// entries from earlier terms are committed indirectly.
func (rf *Raft) advanceCommitLocked() {
	for n := rf.lastIndex(); n > rf.commitIndex; n-- {
		if rf.termAt(n) != rf.currentTerm {
			break // older entries are all from earlier terms
		}
		count := 0
		for i := range rf.peers {
			if rf.matchIndex[i] >= n {
				count++
			}
		}
		if count*2 > len(rf.peers) {
			rf.commitIndex = n
			rf.applyCond.Signal()
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Background goroutines

// ticker starts elections when the election timer fires and drives leader
// heartbeats.
func (rf *Raft) ticker() {
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !rf.killed() {
		select {
		case <-tick.C:
		case <-rf.killCh:
			return
		}
		rf.mu.Lock()
		now := time.Now()
		if rf.role == leader {
			if now.Sub(rf.lastHeartbeat) >= rf.cfg.HeartbeatInterval {
				rf.heartbeatLocked()
			}
		} else if now.After(rf.electionDeadline) {
			rf.startElectionLocked()
		}
		rf.mu.Unlock()
	}
}

// applier delivers committed entries and installed snapshots to the service,
// in order, without holding the lock while blocked on the channel.
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for !rf.killed() && !rf.pendingSnapshot && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		if rf.pendingSnapshot {
			rf.pendingSnapshot = false
			msg := ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotIndex: rf.log[0].Index,
				SnapshotTerm:  rf.log[0].Term,
			}
			rf.mu.Unlock()
			if !rf.send(msg) {
				return
			}
			rf.mu.Lock()
			rf.lastApplied = max(rf.lastApplied, msg.SnapshotIndex)
			rf.mu.Unlock()
			continue
		}

		start := max(rf.lastApplied+1, rf.log[0].Index+1)
		msgs := make([]ApplyMsg, 0, rf.commitIndex-start+1)
		for i := start; i <= rf.commitIndex; i++ {
			e := rf.entry(i)
			msgs = append(msgs, ApplyMsg{CommandValid: true, Command: e.Command, CommandIndex: e.Index, CommandTerm: e.Term})
		}
		rf.mu.Unlock()

		for _, m := range msgs {
			if !rf.send(m) {
				return
			}
		}
		if len(msgs) > 0 {
			rf.mu.Lock()
			rf.lastApplied = max(rf.lastApplied, msgs[len(msgs)-1].CommandIndex)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) send(m ApplyMsg) bool {
	select {
	case rf.applyCh <- m:
		return true
	case <-rf.killCh:
		return false
	}
}

// ---------------------------------------------------------------------------
// Leader election

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) startElectionLocked() {
	rf.role = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.leaderID = -1
	rf.persistLocked()
	rf.resetElectionTimerLocked()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastIndex(),
		LastLogTerm:  rf.lastTerm(),
	}
	votes := 1
	if len(rf.peers) == 1 {
		rf.becomeLeaderLocked()
		return
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			var reply RequestVoteReply
			if !rf.peers[peer].Call("Raft.RequestVote", &args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.stepDownLocked(reply.Term)
				return
			}
			if rf.role != candidate || rf.currentTerm != args.Term || !reply.VoteGranted {
				return
			}
			votes++
			if votes*2 > len(rf.peers) {
				rf.becomeLeaderLocked()
			}
		}(i)
	}
}

func (rf *Raft) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.stepDownLocked(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// §5.4.1 election restriction: only vote for candidates whose log is at
	// least as up-to-date as ours, so every leader holds all committed entries.
	upToDate := args.LastLogTerm > rf.lastTerm() ||
		(args.LastLogTerm == rf.lastTerm() && args.LastLogIndex >= rf.lastIndex())
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		rf.persistLocked()
		rf.resetElectionTimerLocked()
		reply.VoteGranted = true
	}
}

// ---------------------------------------------------------------------------
// Log replication

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

// AppendEntriesReply carries the "fast backup" hints: on a mismatch the
// follower reports the term of its conflicting entry and the first index it
// holds for that term, letting the leader skip a whole term per round trip
// instead of one entry.
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int // -1 if the follower's log is simply too short
	ConflictIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// replicator is a long-lived goroutine per follower. It sleeps until the
// follower is missing entries, then sends one batch at a time (so a lagging
// follower gets each entry roughly once, not once per Start), and a slow
// follower never holds up the others.
func (rf *Raft) replicator(peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		for !rf.killed() && !rf.needsReplicationLocked(peer) {
			rf.replCond[peer].Wait()
		}
		if rf.killed() {
			return
		}
		if rf.nextIndex[peer] <= rf.log[0].Index {
			rf.sendSnapshotLocked(peer)
		} else {
			rf.sendEntriesLocked(peer)
		}
	}
}

func (rf *Raft) needsReplicationLocked(peer int) bool {
	if rf.role != leader {
		return false
	}
	return !rf.backoff[peer] && rf.nextIndex[peer] <= rf.lastIndex()
}

// sendEntriesLocked is called and returns with rf.mu held; it releases the
// lock for the duration of the RPC.
func (rf *Raft) sendEntriesLocked(peer int) {
	prev := rf.nextIndex[peer] - 1
	end := min(rf.lastIndex(), prev+rf.cfg.MaxEntriesPerRPC)
	entries := make([]Entry, end-prev)
	copy(entries, rf.log[prev+1-rf.log[0].Index:end+1-rf.log[0].Index])
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.termAt(prev),
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	rf.mu.Unlock()
	var reply AppendEntriesReply
	ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
	rf.mu.Lock()

	if !ok {
		rf.backoff[peer] = true
		return
	}
	rf.handleAppendReplyLocked(peer, &args, &reply)
}

func (rf *Raft) handleAppendReplyLocked(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.stepDownLocked(reply.Term)
		return
	}
	if rf.role != leader || rf.currentTerm != args.Term {
		return // stale reply from an earlier term
	}
	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[peer] = max(rf.matchIndex[peer], match)
		rf.nextIndex[peer] = max(rf.nextIndex[peer], match+1)
		rf.advanceCommitLocked()
		return
	}

	next := reply.ConflictIndex
	if reply.ConflictTerm != -1 {
		// If we have entries from ConflictTerm, resume right after our last one.
		for i := rf.lastIndex(); i > rf.log[0].Index; i-- {
			t := rf.termAt(i)
			if t == reply.ConflictTerm {
				next = i + 1
				break
			}
			if t < reply.ConflictTerm {
				break
			}
		}
	}
	rf.nextIndex[peer] = min(max(next, rf.matchIndex[peer]+1, 1), rf.lastIndex()+1)
	rf.replCond[peer].Signal()
}

func (rf *Raft) sendSnapshotLocked(peer int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.log[0].Index,
		LastIncludedTerm:  rf.log[0].Term,
		Data:              rf.snapshot,
	}

	rf.mu.Unlock()
	var reply InstallSnapshotReply
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply)
	rf.mu.Lock()

	if !ok {
		rf.backoff[peer] = true
		return
	}
	if reply.Term > rf.currentTerm {
		rf.stepDownLocked(reply.Term)
		return
	}
	if rf.role != leader || rf.currentTerm != args.Term {
		return
	}
	rf.matchIndex[peer] = max(rf.matchIndex[peer], args.LastIncludedIndex)
	rf.nextIndex[peer] = max(rf.nextIndex[peer], args.LastIncludedIndex+1)
}

// acceptLeaderLocked is the common prologue of AppendEntries and
// InstallSnapshot. It returns false if the sender's term is stale.
func (rf *Raft) acceptLeaderLocked(term, leaderID int) bool {
	if term < rf.currentTerm {
		return false
	}
	if term > rf.currentTerm {
		rf.stepDownLocked(term)
	}
	rf.role = follower // a candidate that hears from a same-term leader steps down
	rf.leaderID = leaderID
	rf.resetElectionTimerLocked()
	return true
}

func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ok := rf.acceptLeaderLocked(args.Term, args.LeaderID)
	reply.Term = rf.currentTerm
	if !ok {
		return
	}

	base := rf.log[0].Index
	switch {
	case args.PrevLogIndex < base:
		// The prefix is already in our snapshot (hence committed); ask the
		// leader to continue from just after it.
		reply.ConflictTerm, reply.ConflictIndex = -1, base+1
		return
	case args.PrevLogIndex > rf.lastIndex():
		reply.ConflictTerm, reply.ConflictIndex = -1, rf.lastIndex()+1
		return
	case rf.termAt(args.PrevLogIndex) != args.PrevLogTerm:
		ct := rf.termAt(args.PrevLogIndex)
		i := args.PrevLogIndex
		for i > base+1 && rf.termAt(i-1) == ct {
			i--
		}
		reply.ConflictTerm, reply.ConflictIndex = ct, i
		return
	}

	// Append new entries, truncating only on a real conflict: a delayed,
	// shorter AppendEntries must never erase entries a later one added.
	changed := false
	for i, e := range args.Entries {
		if e.Index > rf.lastIndex() {
			rf.log = append(rf.log, args.Entries[i:]...)
			changed = true
			break
		}
		if rf.termAt(e.Index) != e.Term {
			kept := make([]Entry, e.Index-base)
			copy(kept, rf.log[:e.Index-base])
			rf.log = append(kept, args.Entries[i:]...)
			changed = true
			break
		}
	}
	if changed {
		rf.persistLocked()
	}
	reply.Success = true

	lastNew := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = max(rf.commitIndex, min(args.LeaderCommit, lastNew))
		rf.applyCond.Signal()
	}
}

func (rf *Raft) handleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ok := rf.acceptLeaderLocked(args.Term, args.LeaderID)
	reply.Term = rf.currentTerm
	if !ok || args.LastIncludedIndex <= rf.commitIndex {
		return // we already have everything this snapshot covers
	}

	idx := args.LastIncludedIndex
	newLog := []Entry{{Index: idx, Term: args.LastIncludedTerm}}
	if idx < rf.lastIndex() && rf.termAt(idx) == args.LastIncludedTerm {
		// Keep the suffix that follows the snapshot; it is consistent.
		newLog = append(newLog, rf.log[idx-rf.log[0].Index+1:]...)
	}
	rf.log = newLog
	rf.snapshot = args.Data
	rf.commitIndex = idx
	rf.persistLocked()
	rf.pendingSnapshot = true
	rf.applyCond.Signal()
}
