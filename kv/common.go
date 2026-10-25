// Package kv is a linearizable, fault-tolerant key/value service built on
// the raft package.
//
// Every operation — reads included — goes through the Raft log, so all
// replicas apply the same operations in the same order. Clients tag each
// request with (ClientID, Seq); servers remember the highest Seq applied per
// client, which makes retries idempotent: a write is applied exactly once
// even if the client resends it to several leaders after timeouts.
package kv

import "encoding/gob"

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
	ErrCASFailed   Err = "ErrCASFailed"
)

type OpKind string

const (
	OpGet    OpKind = "Get"
	OpPut    OpKind = "Put"
	OpAppend OpKind = "Append"
	OpDelete OpKind = "Delete"
	OpCAS    OpKind = "CAS" // compare-and-swap: set Value iff current value == Expect
)

// Op is both the RPC request and the command stored in the Raft log.
type Op struct {
	Kind     OpKind
	Key      string
	Value    string
	Expect   string // for CAS
	ClientID int64
	Seq      int64
}

type Reply struct {
	Err   Err
	Value string
}

func init() {
	gob.Register(Op{})
}
