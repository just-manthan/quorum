package simnet

import "testing"

type Args struct{ X []int }
type Reply struct{ Sum int }

type Adder struct{}

func (Adder) Add(a *Args, r *Reply) error {
	for _, x := range a.X {
		r.Sum += x
	}
	a.X[0] = -1 // must not be visible to the caller
	return nil
}

func TestCallAndIsolation(t *testing.T) {
	n := New()
	client, server := n.AddNode(), n.AddNode()
	srv := NewServer()
	srv.Register("Adder", Adder{})
	n.Serve(server, srv)

	args := &Args{X: []int{1, 2, 3}}
	var reply Reply
	if !n.End(client, server).Call("Adder.Add", args, &reply) || reply.Sum != 6 {
		t.Fatalf("call failed or wrong sum: %+v", reply)
	}
	if args.X[0] != 1 {
		t.Fatalf("callee mutated caller's memory")
	}
	if n.End(client, server).Call("Adder.Missing", args, &reply) {
		t.Fatalf("unknown method succeeded")
	}
}

func TestPartitionsAndCrashes(t *testing.T) {
	n := New()
	a, b, c := n.AddNode(), n.AddNode(), n.AddNode()
	for _, id := range []int{a, b, c} {
		srv := NewServer()
		srv.Register("Adder", Adder{})
		n.Serve(id, srv)
	}
	call := func(from, to int) bool {
		var r Reply
		return n.End(from, to).Call("Adder.Add", &Args{X: []int{1}}, &r)
	}

	n.Partition([]int{a, b}, []int{c})
	if !call(a, b) || call(a, c) || call(c, b) {
		t.Fatalf("partition not enforced")
	}
	n.Heal()
	if !call(a, c) {
		t.Fatalf("heal did not restore link")
	}
	n.Isolate(b)
	if call(a, b) || call(b, c) {
		t.Fatalf("isolation not enforced")
	}
	n.Reconnect(b)
	n.Crash(c)
	if call(a, c) {
		t.Fatalf("crashed server answered")
	}
}
