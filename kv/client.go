package kv

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"

	"github.com/just-manthan/quorum/raft"
)

// RPCService exposes only the client-facing RPC ("KV.Do") of a Server.
type RPCService struct{ s *Server }

// Service returns the RPC endpoint for this replica.
func (s *Server) Service() *RPCService { return &RPCService{s} }

func (r *RPCService) Do(op *Op, reply *Reply) error { return r.s.Do(op, reply) }

// Client talks to a replicated KV cluster. It finds the leader, retries
// through failures, and guarantees each write is applied at most once.
// A Client issues one request at a time; use several Clients for concurrency.
type Client struct {
	mu       sync.Mutex
	servers  []raft.Peer
	clientID int64
	seq      int64
	leader   int // last server known to be leader
}

func NewClient(servers []raft.Peer) *Client {
	var b [8]byte
	rand.Read(b[:])
	return &Client{servers: servers, clientID: int64(binary.LittleEndian.Uint64(b[:]) >> 1)}
}

// Get returns the value for key, or "" if it does not exist.
func (c *Client) Get(key string) string {
	r := c.do(Op{Kind: OpGet, Key: key})
	return r.Value
}

// Lookup is like Get but also reports whether the key exists.
func (c *Client) Lookup(key string) (string, bool) {
	r := c.do(Op{Kind: OpGet, Key: key})
	return r.Value, r.Err == OK
}

func (c *Client) Put(key, value string)    { c.do(Op{Kind: OpPut, Key: key, Value: value}) }
func (c *Client) Append(key, value string) { c.do(Op{Kind: OpAppend, Key: key, Value: value}) }
func (c *Client) Delete(key string)        { c.do(Op{Kind: OpDelete, Key: key}) }

// CompareAndSwap sets key to value iff its current value (missing = "")
// equals expect. It returns whether the swap happened and the value
// observed when it did not.
func (c *Client) CompareAndSwap(key, expect, value string) (bool, string) {
	r := c.do(Op{Kind: OpCAS, Key: key, Expect: expect, Value: value})
	return r.Err == OK, r.Value
}

func (c *Client) do(op Op) Reply {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	op.ClientID, op.Seq = c.clientID, c.seq

	for attempt := 0; ; attempt++ {
		var reply Reply
		ok := c.servers[c.leader].Call("KV.Do", &op, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey || reply.Err == ErrCASFailed) {
			return reply
		}
		c.leader = (c.leader + 1) % len(c.servers)
		if (attempt+1)%len(c.servers) == 0 {
			time.Sleep(50 * time.Millisecond) // went around the cluster; likely mid-election
		}
	}
}
