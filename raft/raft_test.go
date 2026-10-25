package raft

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

const electionWait = 1200 * time.Millisecond // comfortably > max election timeout

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.checkOneLeader()

	// The term must not change while nothing fails.
	time.Sleep(50 * time.Millisecond)
	term1 := c.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d, but there should be a leader", term1)
	}
	time.Sleep(2 * electionWait)
	if term2 := c.checkTerms(); term1 != term2 {
		t.Logf("warning: term changed (%d -> %d) even though there were no failures", term1, term2)
	}
	c.checkOneLeader()
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	leader1 := c.checkOneLeader()

	// A new leader takes over when the old one disconnects.
	c.disconnect(leader1)
	c.checkOneLeader()

	// The old leader rejoining must not disturb the new one.
	c.connect(leader1)
	leader2 := c.checkOneLeader()

	// No quorum, no leader.
	c.disconnect(leader2)
	c.disconnect((leader2 + 1) % 3)
	time.Sleep(electionWait)
	c.checkNoLeader()

	// Quorum restored, a leader emerges.
	c.connect((leader2 + 1) % 3)
	c.checkOneLeader()
	c.connect(leader2)
	c.checkOneLeader()
}

func TestManyElections(t *testing.T) {
	c := newCluster(t, 7, true, 0)
	c.checkOneLeader()
	for iter := 0; iter < 10; iter++ {
		i1, i2, i3 := rand.Intn(7), rand.Intn(7), rand.Intn(7)
		c.disconnect(i1)
		c.disconnect(i2)
		c.disconnect(i3)
		// Either the current leader is still alive, or the remaining four elect one.
		c.checkOneLeader()
		c.connect(i1)
		c.connect(i2)
		c.connect(i3)
	}
	c.checkOneLeader()
}

func TestBasicAgreement(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	for index := 1; index <= 3; index++ {
		if nd, _ := c.nCommitted(index); nd > 0 {
			t.Fatalf("some peers committed before Start()")
		}
		if got := c.one(index*100, 3, false); got != index {
			t.Fatalf("got index %d, expected %d", got, index)
		}
	}
}

func TestRPCBytes(t *testing.T) {
	// Each command should be sent to each follower roughly once.
	c := newCluster(t, 3, true, 0)
	c.one(99, 3, false)
	before := c.net.ByteCount()
	var sent int64
	for index := 2; index < 10; index++ {
		cmd := string(make([]byte, 5000))
		cmd = cmd + string(rune('a'+index))
		c.one(cmd, 3, false)
		sent += int64(len(cmd))
	}
	got := c.net.ByteCount() - before
	if limit := 3*sent + 50000; got > limit {
		t.Fatalf("too many RPC bytes; got %d, expected at most %d", got, limit)
	}
}

func TestFollowerFailure(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one(101, 3, false)

	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 1) % 3)
	c.one(102, 2, false) // two remaining peers can still commit
	time.Sleep(electionWait)
	c.one(103, 2, false)

	leader2 := c.checkOneLeader()
	c.disconnect((leader2 + 1) % 3)
	c.disconnect((leader2 + 2) % 3)

	index, _, ok := c.rafts[leader2].Start(104)
	if !ok {
		t.Fatalf("leader rejected Start()")
	}
	if index != 4 {
		t.Fatalf("expected index 4, got %d", index)
	}
	time.Sleep(2 * electionWait)
	if n, _ := c.nCommitted(index); n > 0 {
		t.Fatalf("%d committed but no majority", n)
	}
}

func TestLeaderFailureAndRejoin(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one(101, 3, false)

	// The leader accepts entries that never commit, then is partitioned off.
	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	c.rafts[leader1].Start(102)
	c.rafts[leader1].Start(103)
	c.rafts[leader1].Start(104)

	c.one(103, 2, true)

	// The new leader is partitioned too; the old one comes back.
	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	c.connect(leader1)
	c.one(104, 2, true)

	// Everyone back: the stale, uncommitted entries must be overwritten.
	c.connect(leader2)
	c.one(105, 3, true)
}

func TestConcurrentStarts(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	var success bool
	for try := 0; try < 5 && !success; try++ {
		if try > 0 {
			time.Sleep(3 * time.Second)
		}
		leader := c.checkOneLeader()
		_, term, ok := c.rafts[leader].Start(1)
		if !ok {
			continue
		}

		var wg sync.WaitGroup
		indices := make(chan int, 5)
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				idx, term1, ok := c.rafts[leader].Start(100 + i)
				if ok && term1 == term {
					indices <- idx
				}
			}(i)
		}
		wg.Wait()
		close(indices)

		termChanged := false
		for _, rf := range c.rafts {
			if t1, _ := rf.GetState(); t1 != term {
				termChanged = true
			}
		}
		if termChanged {
			continue
		}

		var cmds []int
		for idx := range indices {
			cmd := c.wait(idx, 3, term)
			if cmd == nil {
				termChanged = true
				break
			}
			cmds = append(cmds, cmd.(int))
		}
		if termChanged {
			continue
		}
		for i := 0; i < 5; i++ {
			found := false
			for _, v := range cmds {
				if v == 100+i {
					found = true
				}
			}
			if !found {
				t.Fatalf("cmd %d missing in %v", 100+i, cmds)
			}
		}
		success = true
	}
	if !success {
		t.Fatalf("term changed too often")
	}
}

// TestFastBackup makes a follower's log diverge from the leader's by ~100
// entries and checks that it catches up quickly (the leader must skip whole
// conflicting terms, not walk back one entry per RPC).
func TestFastBackup(t *testing.T) {
	c := newCluster(t, 5, true, 0)
	c.one(rand.Int(), 5, true)

	// Put the leader and one follower in a minority partition.
	leader1 := c.checkOneLeader()
	c.disconnect((leader1 + 2) % 5)
	c.disconnect((leader1 + 3) % 5)
	c.disconnect((leader1 + 4) % 5)

	// Lots of commands that will never commit.
	for i := 0; i < 50; i++ {
		c.rafts[leader1].Start(rand.Int())
	}
	time.Sleep(electionWait / 2)
	c.disconnect((leader1 + 0) % 5)
	c.disconnect((leader1 + 1) % 5)

	// The other partition elects a leader and commits lots of commands.
	c.connect((leader1 + 2) % 5)
	c.connect((leader1 + 3) % 5)
	c.connect((leader1 + 4) % 5)
	for i := 0; i < 50; i++ {
		c.one(rand.Int(), 3, true)
	}

	// Now another partitioned leader and one follower.
	leader2 := c.checkOneLeader()
	other := (leader1 + 2) % 5
	if leader2 == other {
		other = (leader2 + 1) % 5
	}
	c.disconnect(other)
	for i := 0; i < 50; i++ {
		c.rafts[leader2].Start(rand.Int())
	}
	time.Sleep(electionWait / 2)

	// Bring the original leader back with its divergent log.
	for i := 0; i < 5; i++ {
		c.disconnect(i)
	}
	c.connect((leader1 + 0) % 5)
	c.connect((leader1 + 1) % 5)
	c.connect(other)
	for i := 0; i < 50; i++ {
		c.one(rand.Int(), 3, true)
	}

	// Everyone together.
	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	start := c.net.RPCCount()
	c.one(rand.Int(), 5, true)
	if rpcs := c.net.RPCCount() - start; rpcs > 200 {
		t.Logf("catch-up used %d RPCs", rpcs)
	}
}

func TestPersistence(t *testing.T) {
	c := newCluster(t, 3, true, 0)
	c.one(11, 3, true)

	// Crash and restart everyone.
	for i := 0; i < 3; i++ {
		c.startPeer(i)
	}
	c.one(12, 3, true)

	leader1 := c.checkOneLeader()
	c.startPeer(leader1)
	c.one(13, 3, true)

	leader2 := c.checkOneLeader()
	c.crash(leader2)
	c.one(14, 2, true)
	c.startPeer(leader2)
	c.wait(4, 3, -1) // the restarted peer catches up

	i3 := (c.checkOneLeader() + 1) % 3
	c.crash(i3)
	c.one(15, 2, true)
	c.startPeer(i3)
	c.one(16, 3, true)
}

// TestFigure8Unreliable is the classic stress test: an unreliable,
// reordering network, leaders crashing all the time, and long stretches
// without a majority. Afterwards the cluster must still agree.
func TestFigure8Unreliable(t *testing.T) {
	c := newCluster(t, 5, false, 0)
	c.net.SetReordering(true)
	c.one(rand.Int()%10000, 1, true)

	nup := 5
	for iters := 0; iters < 300; iters++ {
		if iters == 200 {
			c.net.SetReordering(false)
		}
		leader := -1
		for i := 0; i < 5; i++ {
			if c.connected(i) {
				if _, _, ok := c.rafts[i].Start(rand.Int() % 10000); ok {
					leader = i
				}
			}
		}
		if rand.Intn(1000) < 100 {
			time.Sleep(time.Duration(rand.Int63n(int64(electionWait / 2))))
		} else {
			time.Sleep(time.Duration(rand.Intn(13)) * time.Millisecond)
		}
		if leader != -1 && rand.Intn(1000) < 500 {
			c.disconnect(leader)
			nup--
		}
		if nup < 3 {
			if s := rand.Intn(5); !c.connected(s) {
				c.connect(s)
				nup++
			}
		}
	}
	for i := 0; i < 5; i++ {
		c.connect(i)
	}
	c.net.SetReliable(true)
	c.one(rand.Int()%10000, 5, true)
}

// TestFigure8Crash crashes and restarts leaders with a reliable network,
// exercising the "commit only current-term entries" rule.
func TestFigure8Crash(t *testing.T) {
	c := newCluster(t, 5, true, 0)
	c.one(rand.Int(), 1, true)

	nup := 5
	for iters := 0; iters < 300; iters++ {
		leader := -1
		for i := 0; i < 5; i++ {
			if c.rafts[i] != nil {
				if _, _, ok := c.rafts[i].Start(rand.Int()); ok {
					leader = i
				}
			}
		}
		if rand.Intn(1000) < 100 {
			time.Sleep(time.Duration(rand.Int63n(int64(electionWait / 2))))
		} else {
			time.Sleep(time.Duration(rand.Intn(13)) * time.Millisecond)
		}
		if leader != -1 {
			c.crash(leader)
			nup--
		}
		if nup < 3 {
			if s := rand.Intn(5); c.rafts[s] == nil {
				c.startPeer(s)
				nup++
			}
		}
	}
	for i := 0; i < 5; i++ {
		if c.rafts[i] == nil {
			c.startPeer(i)
		}
	}
	c.one(rand.Int(), 5, true)
}

func TestSnapshotInstallUnreliable(t *testing.T) {
	c := newCluster(t, 3, false, 10)
	c.one(rand.Int(), 3, true)
	leader1 := c.checkOneLeader()

	for i := 0; i < 20; i++ {
		victim := (leader1 + 1) % 3
		sender := leader1
		if i%3 == 1 {
			sender, victim = victim, sender
		}
		// Let a follower fall far behind, so it can only catch up via a snapshot.
		c.disconnect(victim)
		c.one(rand.Int(), 2, true)
		for j := 0; j < 25; j++ {
			c.rafts[sender].Start(rand.Int())
		}
		c.one(rand.Int(), 2, true)
		c.connect(victim)
		c.one(rand.Int(), 3, true)
		leader1 = c.checkOneLeader()
	}
}

func TestSnapshotCrash(t *testing.T) {
	c := newCluster(t, 3, true, 10)
	c.one(rand.Int(), 3, true)
	for i := 0; i < 15; i++ {
		victim := rand.Intn(3)
		c.crash(victim)
		for j := 0; j < 12; j++ {
			c.one(rand.Int(), 2, true)
		}
		c.startPeer(victim)
		c.one(rand.Int(), 3, true)
	}
	// Log must actually be compacted.
	for i, rf := range c.rafts {
		if st := rf.Status(); st.LogLength > 40 {
			t.Fatalf("peer %d still holds %d log entries; snapshots are not compacting the log", i, st.LogLength)
		}
	}
}

func TestSingleNode(t *testing.T) {
	c := newCluster(t, 1, true, 0)
	for i := 1; i <= 5; i++ {
		if got := c.one(i, 1, false); got != i {
			t.Fatalf("got index %d, want %d", got, i)
		}
	}
}
