package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/just-manthan/quorum/simnet"
)

// testConfig uses the default timing; tests are written against it.
var testConfig = DefaultConfig

// cluster is a test harness: n Raft peers on a simulated network, with an
// applier per peer that checks the core safety property on every apply —
// no two peers ever apply different commands at the same index.
type cluster struct {
	t       *testing.T
	n       int
	net     *simnet.Network
	ids     []int
	rafts   []*Raft
	persist []*MemPersister
	gen     []int // incremented on each restart; stale appliers ignore messages

	mu            sync.Mutex
	logs          []map[int]any // per peer: index -> command applied
	lastApplied   []int
	applyErr      []string
	isolated      []bool
	snapshotEvery int // if >0, peers snapshot every this many entries
	start         time.Time
}

func newCluster(t *testing.T, n int, reliable bool, snapshotEvery int) *cluster {
	t.Helper()
	c := &cluster{
		t: t, n: n, net: simnet.New(),
		rafts: make([]*Raft, n), persist: make([]*MemPersister, n), gen: make([]int, n),
		logs: make([]map[int]any, n), lastApplied: make([]int, n), applyErr: make([]string, n), isolated: make([]bool, n),
		snapshotEvery: snapshotEvery, start: time.Now(),
	}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, c.net.AddNode())
		c.persist[i] = NewMemPersister()
	}
	for i := 0; i < n; i++ {
		c.startPeer(i)
	}
	t.Cleanup(c.cleanup)
	return c
}

func (c *cluster) cleanup() {
	for _, rf := range c.rafts {
		if rf != nil {
			rf.Kill()
		}
	}
	for i, e := range c.applyErr {
		if e != "" {
			c.t.Fatalf("peer %d: %s", i, e)
		}
	}
}

// startPeer (re)starts peer i from whatever it had persisted.
func (c *cluster) startPeer(i int) {
	c.crash(i)
	ends := make([]Peer, c.n)
	for j := 0; j < c.n; j++ {
		ends[j] = c.net.End(c.ids[i], c.ids[j])
	}

	c.mu.Lock()
	c.gen[i]++
	gen := c.gen[i]
	c.logs[i] = map[int]any{}
	c.lastApplied[i] = 0
	if snap := c.persist[i].ReadSnapshot(); len(snap) > 0 {
		c.installSnapshotLocked(i, snap)
	}
	c.mu.Unlock()

	applyCh := make(chan ApplyMsg)
	rf := Make(ends, i, c.persist[i], applyCh, testConfig)
	c.rafts[i] = rf
	go c.applier(i, gen, rf, applyCh)

	srv := simnet.NewServer()
	srv.Register("Raft", rf.Service())
	c.net.Serve(c.ids[i], srv)
}

// crash stops peer i. Its persisted state is copied first, so a write the
// dying peer makes afterwards cannot leak into its next incarnation.
func (c *cluster) crash(i int) {
	c.net.Crash(c.ids[i])
	if c.rafts[i] != nil {
		c.persist[i] = c.persist[i].Copy()
		c.rafts[i].Kill()
		c.rafts[i] = nil
	}
}

type snapshotState struct {
	LastIndex int
	Log       map[int]any
}

func (c *cluster) installSnapshotLocked(i int, data []byte) {
	var s snapshotState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&s); err != nil {
		c.t.Fatalf("decode snapshot: %v", err)
	}
	c.logs[i] = s.Log
	c.lastApplied[i] = s.LastIndex
}

func (c *cluster) applier(i, gen int, rf *Raft, ch <-chan ApplyMsg) {
	for msg := range ch {
		c.mu.Lock()
		if c.gen[i] != gen {
			c.mu.Unlock()
			continue
		}
		switch {
		case msg.SnapshotValid:
			if msg.SnapshotIndex > c.lastApplied[i] {
				c.installSnapshotLocked(i, msg.Snapshot)
			}
			c.mu.Unlock()
		case msg.CommandValid:
			if msg.CommandIndex <= c.lastApplied[i] {
				c.mu.Unlock()
				continue
			}
			if msg.CommandIndex != c.lastApplied[i]+1 {
				c.applyErr[i] = fmt.Sprintf("applied index %d out of order (expected %d)", msg.CommandIndex, c.lastApplied[i]+1)
			}
			for j := range c.logs {
				if old, ok := c.logs[j][msg.CommandIndex]; ok && old != msg.Command {
					c.applyErr[i] = fmt.Sprintf("SAFETY VIOLATION: index %d: peer %d applied %v, peer %d applied %v",
						msg.CommandIndex, i, msg.Command, j, old)
				}
			}
			c.logs[i][msg.CommandIndex] = msg.Command
			c.lastApplied[i] = msg.CommandIndex
			var snap []byte
			if c.snapshotEvery > 0 && msg.CommandIndex%c.snapshotEvery == 0 {
				var buf bytes.Buffer
				logCopy := make(map[int]any, len(c.logs[i]))
				for k, v := range c.logs[i] {
					logCopy[k] = v
				}
				gob.NewEncoder(&buf).Encode(snapshotState{msg.CommandIndex, logCopy})
				snap = buf.Bytes()
			}
			c.mu.Unlock()
			if snap != nil {
				rf.Snapshot(msg.CommandIndex, snap)
			}
		default:
			c.mu.Unlock()
		}
	}
}

// checkOneLeader waits for exactly one leader in the newest term among the
// connected peers and returns it.
func (c *cluster) checkOneLeader() int {
	c.t.Helper()
	for iter := 0; iter < 10; iter++ {
		time.Sleep(time.Duration(450+rand.Intn(100)) * time.Millisecond)
		leaders := map[int][]int{}
		for i, rf := range c.rafts {
			if rf == nil || !c.connected(i) {
				continue
			}
			if term, isLeader := rf.GetState(); isLeader {
				leaders[term] = append(leaders[term], i)
			}
		}
		newest := -1
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
			newest = max(newest, term)
		}
		if newest != -1 {
			return leaders[newest][0]
		}
	}
	c.t.Fatalf("expected one leader, got none")
	return -1
}

func (c *cluster) checkNoLeader() {
	c.t.Helper()
	for i, rf := range c.rafts {
		if rf == nil || !c.connected(i) {
			continue
		}
		if _, isLeader := rf.GetState(); isLeader {
			c.t.Fatalf("expected no leader among connected peers, but %d claims to be one", i)
		}
	}
}

func (c *cluster) checkTerms() int {
	c.t.Helper()
	term := -1
	for _, rf := range c.rafts {
		if rf == nil {
			continue
		}
		t, _ := rf.GetState()
		if term == -1 {
			term = t
		} else if t != term {
			c.t.Fatalf("peers disagree on term")
		}
	}
	return term
}

func (c *cluster) disconnect(i int) {
	c.net.Isolate(c.ids[i])
	c.mu.Lock()
	c.isolated[i] = true
	c.mu.Unlock()
}

func (c *cluster) connect(i int) {
	c.net.Reconnect(c.ids[i])
	c.mu.Lock()
	c.isolated[i] = false
	c.mu.Unlock()
}

func (c *cluster) connected(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.isolated[i]
}

// nCommitted reports how many peers have applied index, and the command.
func (c *cluster) nCommitted(index int) (int, any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	var cmd any
	for i := range c.rafts {
		if c.applyErr[i] != "" {
			c.t.Fatal(c.applyErr[i])
		}
		if v, ok := c.logs[i][index]; ok {
			if count > 0 && v != cmd {
				c.t.Fatalf("committed values at index %d disagree: %v vs %v", index, cmd, v)
			}
			count++
			cmd = v
		}
	}
	return count, cmd
}

// one submits cmd to whichever peer is leader and waits until at least
// expected peers have applied it. With retry, it resubmits if the first
// leader fails before committing.
func (c *cluster) one(cmd any, expected int, retry bool) int {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	starts := 0
	for time.Now().Before(deadline) {
		index := -1
		for k := 0; k < c.n; k++ {
			starts = (starts + 1) % c.n
			if rf := c.rafts[starts]; rf != nil && c.connected(starts) {
				if idx, _, ok := rf.Start(cmd); ok {
					index = idx
					break
				}
			}
		}
		if index != -1 {
			t1 := time.Now()
			for time.Since(t1) < 2*time.Second {
				nd, v := c.nCommitted(index)
				if nd > 0 && nd >= expected && v == cmd {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				c.t.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Fatalf("one(%v) failed to reach agreement\n%s", cmd, c.dump())
	return -1
}

// wait waits for at least n peers to commit index, giving up if the term
// moves past startTerm (in which case it returns nil).
func (c *cluster) wait(index, n, startTerm int) any {
	c.t.Helper()
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		if nd, _ := c.nCommitted(index); nd >= n {
			break
		}
		time.Sleep(to)
		to = min(to*2, time.Second)
		if startTerm > -1 {
			for _, rf := range c.rafts {
				if rf == nil {
					continue
				}
				if t, _ := rf.GetState(); t > startTerm {
					return nil
				}
			}
		}
	}
	nd, cmd := c.nCommitted(index)
	if nd < n {
		c.t.Fatalf("only %d peers committed index %d, wanted %d", nd, index, n)
	}
	return cmd
}

func (c *cluster) dump() string {
	out := ""
	for i, rf := range c.rafts {
		if rf == nil {
			out += fmt.Sprintf("  peer %d: down\n", i)
			continue
		}
		c.mu.Lock()
		applied := c.lastApplied[i]
		c.mu.Unlock()
		out += fmt.Sprintf("  peer %d: %+v connected=%v harnessApplied=%d\n", i, rf.Status(), c.connected(i), applied)
	}
	return out
}
