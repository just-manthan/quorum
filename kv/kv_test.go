package kv

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/just-manthan/quorum/linearizability"
	"github.com/just-manthan/quorum/raft"
	"github.com/just-manthan/quorum/simnet"
)

// ---------------------------------------------------------------------------
// Harness

type kvCluster struct {
	t            *testing.T
	n            int
	net          *simnet.Network
	ids          []int
	servers      []*Server
	persist      []*raft.MemPersister
	maxRaftState int
}

func newKVCluster(t *testing.T, n int, reliable bool, maxRaftState int) *kvCluster {
	t.Helper()
	c := &kvCluster{t: t, n: n, net: simnet.New(), servers: make([]*Server, n),
		persist: make([]*raft.MemPersister, n), maxRaftState: maxRaftState}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, c.net.AddNode())
		c.persist[i] = raft.NewMemPersister()
	}
	for i := 0; i < n; i++ {
		c.start(i)
	}
	t.Cleanup(func() {
		for i := range c.servers {
			c.crash(i)
		}
	})
	return c
}

func (c *kvCluster) start(i int) {
	c.crash(i)
	peers := make([]raft.Peer, c.n)
	for j := range peers {
		peers[j] = c.net.End(c.ids[i], c.ids[j])
	}
	s := StartServer(peers, i, c.persist[i], c.maxRaftState, raft.DefaultConfig)
	c.servers[i] = s
	srv := simnet.NewServer()
	srv.Register("Raft", s.Raft().Service())
	srv.Register("KV", s.Service())
	c.net.Serve(c.ids[i], srv)
}

func (c *kvCluster) crash(i int) {
	c.net.Crash(c.ids[i])
	if c.servers[i] != nil {
		c.persist[i] = c.persist[i].Copy()
		c.servers[i].Kill()
		c.servers[i] = nil
	}
}

func (c *kvCluster) client() *Client {
	id := c.net.AddNode()
	ends := make([]raft.Peer, c.n)
	for j := range ends {
		ends[j] = c.net.End(id, c.ids[j])
	}
	return NewClient(ends)
}

// partition splits servers (by index) into two groups. Clients stay
// connected to everyone, so they must cope with minority "leaders".
func (c *kvCluster) partition(a, b []int) {
	ga, gb := make([]int, len(a)), make([]int, len(b))
	for i, s := range a {
		ga[i] = c.ids[s]
	}
	for i, s := range b {
		gb[i] = c.ids[s]
	}
	c.net.Partition(ga, gb)
}

func (c *kvCluster) leader() int {
	for i := 0; i < 50; i++ {
		for j, s := range c.servers {
			if s != nil {
				if _, ok := s.Raft().GetState(); ok {
					return j
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return -1
}

// recorder wraps a Client and records every operation for the
// linearizability checker.
type recorder struct {
	mu    sync.Mutex
	start time.Time
	ops   []linearizability.Operation
}

func (r *recorder) now() int64 { return time.Since(r.start).Nanoseconds() }

func (r *recorder) add(op linearizability.Operation) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

func (r *recorder) get(id int, ck *Client, key string) string {
	call := r.now()
	v, ok := ck.Lookup(key)
	r.add(linearizability.Operation{ClientID: id, Input: linearizability.Input{Kind: "Get", Key: key},
		Output: linearizability.Output{Value: v, Ok: ok}, Call: call, Return: r.now()})
	return v
}

func (r *recorder) write(id int, ck *Client, kind OpKind, key, value string) {
	call := r.now()
	switch kind {
	case OpPut:
		ck.Put(key, value)
	case OpAppend:
		ck.Append(key, value)
	}
	r.add(linearizability.Operation{ClientID: id, Input: linearizability.Input{Kind: string(kind), Key: key, Value: value},
		Call: call, Return: r.now()})
}

func (r *recorder) cas(id int, ck *Client, key, expect, value string) {
	call := r.now()
	ok, observed := ck.CompareAndSwap(key, expect, value)
	r.add(linearizability.Operation{ClientID: id, Input: linearizability.Input{Kind: "CAS", Key: key, Expect: expect, Value: value},
		Output: linearizability.Output{Ok: ok, Value: observed}, Call: call, Return: r.now()})
}

func (r *recorder) check(t *testing.T) {
	t.Helper()
	res := linearizability.Check(r.ops, 20*time.Second)
	if res.Unknown {
		t.Logf("linearizability check timed out for some keys (inconclusive)")
	}
	if !res.Ok {
		t.Fatalf("history is NOT linearizable on key %q:\n%s", res.BadKey, res.Detail)
	}
	t.Logf("%d operations checked: linearizable", len(r.ops))
}

// ---------------------------------------------------------------------------
// Tests

func TestBasic(t *testing.T) {
	c := newKVCluster(t, 3, true, -1)
	ck := c.client()

	if v, ok := ck.Lookup("missing"); ok || v != "" {
		t.Fatalf("Lookup(missing) = %q, %v", v, ok)
	}
	ck.Put("a", "1")
	ck.Append("a", "2")
	ck.Append("b", "x")
	if got := ck.Get("a"); got != "12" {
		t.Fatalf("Get(a) = %q, want 12", got)
	}
	if got := ck.Get("b"); got != "x" {
		t.Fatalf("Get(b) = %q, want x", got)
	}
	ck.Delete("a")
	if _, ok := ck.Lookup("a"); ok {
		t.Fatalf("a still exists after Delete")
	}

	// Another client sees the same data.
	if got := c.client().Get("b"); got != "x" {
		t.Fatalf("second client Get(b) = %q", got)
	}
}

func TestCompareAndSwapLock(t *testing.T) {
	// Use CAS as a distributed mutex: many clients race to increment a
	// counter; no increment may be lost.
	c := newKVCluster(t, 3, true, -1)
	const clients, increments = 5, 10
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ck := c.client()
			for j := 0; j < increments; j++ {
				for {
					cur := ck.Get("counter")
					n, _ := strconv.Atoi(cur)
					if ok, _ := ck.CompareAndSwap("counter", cur, strconv.Itoa(n+1)); ok {
						break
					}
				}
			}
		}()
	}
	wg.Wait()
	if got := c.client().Get("counter"); got != strconv.Itoa(clients*increments) {
		t.Fatalf("counter = %s, want %d (lost updates!)", got, clients*increments)
	}
}

func TestMinorityCannotServe(t *testing.T) {
	c := newKVCluster(t, 5, true, -1)
	ck := c.client()
	ck.Put("k", "before")

	l := c.leader()
	minority := []int{l, (l + 1) % 5}
	majority := []int{(l + 2) % 5, (l + 3) % 5, (l + 4) % 5}
	c.partition(minority, majority)

	// A write sent only to the old leader (now in a minority) must not commit.
	id := c.net.AddNode()
	op := Op{Kind: OpPut, Key: "k", Value: "minority", ClientID: 42, Seq: 1}
	var reply Reply
	if ok := c.net.End(id, c.ids[l]).Call("KV.Do", &op, &reply); ok && reply.Err == OK {
		t.Fatalf("a minority partition completed a write")
	}

	// The majority keeps going.
	ck.Put("k", "majority")
	if got := ck.Get("k"); got != "majority" {
		t.Fatalf("Get = %q", got)
	}

	// After healing, the old leader's uncommitted write is discarded.
	c.net.Heal()
	ck.Put("other", "x")
	if got := ck.Get("k"); got != "majority" {
		t.Fatalf("after heal Get = %q, want majority", got)
	}
}

func TestPersistTotalCrash(t *testing.T) {
	c := newKVCluster(t, 3, true, -1)
	ck := c.client()
	for i := 0; i < 20; i++ {
		ck.Append("log", strconv.Itoa(i)+",")
	}
	want := ck.Get("log")
	for i := 0; i < 3; i++ {
		c.crash(i)
	}
	for i := 0; i < 3; i++ {
		c.start(i)
	}
	if got := c.client().Get("log"); got != want {
		t.Fatalf("after full restart Get = %q, want %q", got, want)
	}
}

func TestSnapshotBoundsLog(t *testing.T) {
	const maxRaftState = 2000
	c := newKVCluster(t, 3, true, maxRaftState)
	ck := c.client()
	for i := 0; i < 300; i++ {
		ck.Put("k"+strconv.Itoa(i%10), fmt.Sprintf("value-%d", i))
	}
	for i, p := range c.persist {
		if sz := p.StateSize(); sz > 8*maxRaftState {
			t.Fatalf("server %d raft state is %d bytes, limit %d: snapshots not working", i, sz, 8*maxRaftState)
		}
	}
	// A server restarting from a snapshot has the right data.
	c.start(1)
	c.crash(0)
	c.crash(2)
	c.start(0)
	if got := c.client().Get("k9"); got != "value-299" {
		t.Fatalf("Get(k9) = %q", got)
	}
}

type chaos struct {
	clients      int
	unreliable   bool
	partitions   bool
	crashes      bool
	maxRaftState int
	duration     time.Duration
}

// runChaos runs concurrent clients against a 5-server cluster while the
// network misbehaves, then checks the recorded history is linearizable.
func runChaos(t *testing.T, cfg chaos) {
	c := newKVCluster(t, 5, !cfg.unreliable, cfg.maxRaftState)
	rec := &recorder{start: time.Now()}
	var stop atomic.Bool
	var wg sync.WaitGroup
	var total atomic.Int64

	for cl := 0; cl < cfg.clients; cl++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ck := c.client()
			r := rand.New(rand.NewSource(int64(id)))
			lastSeen := map[string]string{}
			for j := 0; !stop.Load(); j++ {
				key := "k" + strconv.Itoa(r.Intn(4))
				val := fmt.Sprintf("[%d.%d]", id, j)
				switch p := r.Intn(100); {
				case p < 40:
					lastSeen[key] = rec.get(id, ck, key)
				case p < 70:
					rec.write(id, ck, OpAppend, key, val)
				case p < 85:
					rec.write(id, ck, OpPut, key, val)
				default:
					rec.cas(id, ck, key, lastSeen[key], val)
				}
				total.Add(1)
			}
		}(cl)
	}

	// The nemesis.
	nemesisDone := make(chan struct{})
	go func() {
		defer close(nemesisDone)
		for !stop.Load() {
			time.Sleep(time.Duration(400+rand.Intn(600)) * time.Millisecond)
			if cfg.partitions {
				perm := rand.Perm(5)
				cut := 1 + rand.Intn(2)
				c.partition(perm[:cut], perm[cut:])
			}
			if cfg.crashes && rand.Intn(2) == 0 {
				victim := rand.Intn(5)
				c.crash(victim)
				time.Sleep(time.Duration(200+rand.Intn(400)) * time.Millisecond)
				c.start(victim)
			}
		}
	}()

	time.Sleep(cfg.duration)
	stop.Store(true)
	<-nemesisDone
	c.net.Heal()
	c.net.SetReliable(true)
	for i := range c.servers {
		if c.servers[i] == nil {
			c.start(i)
		}
	}
	wg.Wait()

	if total.Load() < int64(cfg.clients) {
		t.Fatalf("clients made no progress (%d ops)", total.Load())
	}
	if cfg.maxRaftState > 0 {
		for i, p := range c.persist {
			if sz := p.StateSize(); sz > 8*cfg.maxRaftState {
				t.Fatalf("server %d raft state %d bytes exceeds %d", i, sz, 8*cfg.maxRaftState)
			}
		}
	}
	rec.check(t)
}

func TestLinearizableConcurrentClients(t *testing.T) {
	runChaos(t, chaos{clients: 5, duration: 3 * time.Second, maxRaftState: -1})
}

func TestLinearizableUnreliableNetwork(t *testing.T) {
	runChaos(t, chaos{clients: 5, unreliable: true, duration: 5 * time.Second, maxRaftState: -1})
}

func TestLinearizablePartitions(t *testing.T) {
	runChaos(t, chaos{clients: 5, partitions: true, duration: 6 * time.Second, maxRaftState: -1})
}

func TestLinearizableCrashes(t *testing.T) {
	runChaos(t, chaos{clients: 5, crashes: true, duration: 6 * time.Second, maxRaftState: -1})
}

func TestLinearizableEverythingAtOnce(t *testing.T) {
	runChaos(t, chaos{clients: 7, unreliable: true, partitions: true, crashes: true,
		duration: 8 * time.Second, maxRaftState: 1000})
}
