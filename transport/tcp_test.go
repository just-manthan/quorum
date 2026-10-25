package transport_test

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/just-manthan/quorum/kv"
	"github.com/just-manthan/quorum/raft"
	"github.com/just-manthan/quorum/transport"
)

// TestTCPCluster runs a real 3-node cluster over loopback TCP with on-disk
// persistence, kills the leader, and checks no acknowledged write is lost.
func TestTCPCluster(t *testing.T) {
	const n = 3
	lns := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i], addrs[i] = ln, ln.Addr().String()
	}

	peersFor := func() []raft.Peer {
		ps := make([]raft.Peer, n)
		for i, a := range addrs {
			ps[i] = transport.NewPeer(a, 300*time.Millisecond)
		}
		return ps
	}

	servers := make([]*kv.Server, n)
	for i := 0; i < n; i++ {
		p, err := raft.OpenFilePersister(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		servers[i] = kv.StartServer(peersFor(), i, p, 4096, raft.DefaultConfig)
		go transport.Serve(lns[i], map[string]any{"Raft": servers[i].Raft().Service(), "KV": servers[i].Service()})
	}
	defer func() {
		for i := range servers {
			servers[i].Kill()
			lns[i].Close()
		}
	}()

	ck := kv.NewClient(peersFor())
	for i := 0; i < 50; i++ {
		ck.Append("k", strconv.Itoa(i)+" ")
	}
	want := ck.Get("k")

	// Kill the leader by closing its listener and stopping it.
	leader := -1
	for i, s := range servers {
		if _, ok := s.Raft().GetState(); ok {
			leader = i
		}
	}
	if leader == -1 {
		t.Fatal("no leader")
	}
	servers[leader].Kill()
	lns[leader].Close()

	ck.Append("k", "after")
	if got := ck.Get("k"); got != want+"after" {
		t.Fatalf("got %q, want %q", got, want+"after")
	}
}
