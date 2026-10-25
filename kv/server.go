package kv

import (
	"bytes"
	"encoding/gob"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/just-manthan/quorum/raft"
)

// RequestTimeout bounds how long a server waits for its proposal to commit
// before telling the client to try elsewhere.
const RequestTimeout = 800 * time.Millisecond

type session struct {
	Seq   int64
	Reply Reply
}

// Server is one replica of the key/value service.
type Server struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	persister    raft.Persister
	maxRaftState int // snapshot when Raft state exceeds this many bytes (-1 = never)
	dead         atomic.Bool

	// Replicated state machine (what a snapshot contains).
	data     map[string]string
	sessions map[int64]session

	lastApplied int
	waiters     map[int]chan applied // log index -> RPC handler waiting for it
}

type applied struct {
	clientID, seq int64
	reply         Reply
}

// StartServer creates a replica. peers[me] is this server's own slot.
func StartServer(peers []raft.Peer, me int, persister raft.Persister, maxRaftState int, cfg raft.Config) *Server {
	s := &Server{
		me:           me,
		applyCh:      make(chan raft.ApplyMsg),
		persister:    persister,
		maxRaftState: maxRaftState,
		data:         map[string]string{},
		sessions:     map[int64]session{},
		waiters:      map[int]chan applied{},
	}
	if snap := persister.ReadSnapshot(); len(snap) > 0 {
		s.restore(snap)
	}
	s.rf = raft.Make(peers, me, persister, s.applyCh, cfg)
	go s.applyLoop()
	return s
}

// Raft exposes the underlying consensus module (for RPC registration and status).
func (s *Server) Raft() *raft.Raft { return s.rf }

// Kill stops the replica.
func (s *Server) Kill() {
	s.dead.Store(true)
	s.rf.Kill()
}

var errKilled = errors.New("kv: server is shut down")

// Do executes one client operation. It is the RPC handler ("KV.Do").
func (s *Server) Do(op *Op, reply *Reply) error {
	if s.dead.Load() {
		return errKilled
	}

	// Fast path for a retried write that was already applied.
	s.mu.Lock()
	if op.Kind != OpGet {
		if sess, ok := s.sessions[op.ClientID]; ok && op.Seq <= sess.Seq {
			if op.Seq == sess.Seq {
				*reply = sess.Reply
			} else {
				reply.Err = OK // an older request; the client has moved on
			}
			s.mu.Unlock()
			return nil
		}
	}
	s.mu.Unlock()

	index, _, isLeader := s.rf.Start(*op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}

	s.mu.Lock()
	ch := make(chan applied, 1)
	s.waiters[index] = ch
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if s.waiters[index] == ch {
			delete(s.waiters, index)
		}
		s.mu.Unlock()
	}()

	select {
	case res := <-ch:
		// A different command may have been committed at our index if we
		// lost leadership; then our op was not applied there.
		if res.clientID != op.ClientID || res.seq != op.Seq {
			reply.Err = ErrWrongLeader
			return nil
		}
		*reply = res.reply
	case <-time.After(RequestTimeout):
		reply.Err = ErrTimeout
	}
	return nil
}

func (s *Server) applyLoop() {
	for msg := range s.applyCh {
		if s.dead.Load() {
			return
		}
		switch {
		case msg.CommandValid:
			s.applyCommand(msg)
		case msg.SnapshotValid:
			s.mu.Lock()
			if msg.SnapshotIndex > s.lastApplied {
				s.restore(msg.Snapshot)
				s.lastApplied = msg.SnapshotIndex
			}
			s.mu.Unlock()
		}
	}
}

func (s *Server) applyCommand(msg raft.ApplyMsg) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if msg.CommandIndex <= s.lastApplied {
		return
	}
	s.lastApplied = msg.CommandIndex
	op, ok := msg.Command.(Op)
	if !ok {
		return
	}

	var reply Reply
	if op.Kind == OpGet {
		reply = s.execute(op)
	} else if sess, ok := s.sessions[op.ClientID]; ok && op.Seq <= sess.Seq {
		reply = sess.Reply // duplicate: don't apply twice
	} else {
		reply = s.execute(op)
		s.sessions[op.ClientID] = session{Seq: op.Seq, Reply: reply}
	}

	if ch, ok := s.waiters[msg.CommandIndex]; ok {
		select {
		case ch <- applied{op.ClientID, op.Seq, reply}:
		default:
		}
	}

	if s.maxRaftState != -1 && s.persister.StateSize() >= s.maxRaftState {
		s.rf.Snapshot(msg.CommandIndex, s.encode())
	}
}

func (s *Server) execute(op Op) Reply {
	switch op.Kind {
	case OpGet:
		if v, ok := s.data[op.Key]; ok {
			return Reply{Err: OK, Value: v}
		}
		return Reply{Err: ErrNoKey}
	case OpPut:
		s.data[op.Key] = op.Value
	case OpAppend:
		s.data[op.Key] += op.Value
	case OpDelete:
		delete(s.data, op.Key)
	case OpCAS:
		if s.data[op.Key] != op.Expect {
			return Reply{Err: ErrCASFailed, Value: s.data[op.Key]}
		}
		s.data[op.Key] = op.Value
	}
	return Reply{Err: OK}
}

type snapshot struct {
	Data     map[string]string
	Sessions map[int64]session
}

func (s *Server) encode() []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(snapshot{s.data, s.sessions}); err != nil {
		panic("kv: encoding snapshot: " + err.Error())
	}
	return buf.Bytes()
}

func (s *Server) restore(data []byte) {
	var snap snapshot
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&snap); err != nil {
		panic("kv: decoding snapshot: " + err.Error())
	}
	s.data, s.sessions = snap.Data, snap.Sessions
	if s.data == nil {
		s.data = map[string]string{}
	}
	if s.sessions == nil {
		s.sessions = map[int64]session{}
	}
}
