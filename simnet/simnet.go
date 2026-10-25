// Package simnet is an in-process, adversarial network used to test
// distributed protocols deterministically-ish and fast.
//
// Every RPC is gob-encoded on the way in and on the way out, so a caller and
// a callee never share memory (the same guarantee a real network gives).
// The network can be made unreliable (random delays, dropped requests,
// dropped replies), links between nodes can be cut to create partitions,
// and whole servers can be removed to simulate crashes.
//
// Handlers use the net/rpc calling convention:
//
//	func (s *Svc) Method(args *Args, reply *Reply) error
//
// so the exact same service objects can be served over real TCP with
// net/rpc in production and over simnet in tests.
package simnet

import (
	"bytes"
	"encoding/gob"
	"errors"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Network is a simulated network connecting a dynamic set of nodes.
type Network struct {
	mu       sync.Mutex
	servers  map[int]*Server // node id -> server currently running there (nil = crashed)
	cut      map[[2]int]bool // directed links that are severed
	isolated map[int]bool    // nodes cut off from everyone
	reliable bool
	reorder  bool // occasionally delay replies a lot, to reorder them
	nextID   int

	rpcCount  atomic.Int64
	byteCount atomic.Int64
}

// New returns an empty, reliable network.
func New() *Network {
	return &Network{
		servers:  map[int]*Server{},
		cut:      map[[2]int]bool{},
		isolated: map[int]bool{},
		reliable: true,
	}
}

// AddNode allocates a new node id. A node can act as a client, a server, or both.
func (n *Network) AddNode() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	id := n.nextID
	n.nextID++
	return id
}

// SetReliable toggles random delays and message loss.
func (n *Network) SetReliable(r bool) { n.mu.Lock(); n.reliable = r; n.mu.Unlock() }

// SetReordering toggles occasional long reply delays (which reorders messages).
func (n *Network) SetReordering(r bool) { n.mu.Lock(); n.reorder = r; n.mu.Unlock() }

// Serve installs srv as the server running on node id (replacing any previous one).
func (n *Network) Serve(id int, srv *Server) { n.mu.Lock(); n.servers[id] = srv; n.mu.Unlock() }

// Crash removes the server running on node id. In-flight replies from it are dropped.
func (n *Network) Crash(id int) { n.mu.Lock(); delete(n.servers, id); n.mu.Unlock() }

// Isolate cuts node id off from every other node (both directions).
func (n *Network) Isolate(id int) { n.mu.Lock(); n.isolated[id] = true; n.mu.Unlock() }

// Reconnect undoes Isolate.
func (n *Network) Reconnect(id int) { n.mu.Lock(); delete(n.isolated, id); n.mu.Unlock() }

// Partition splits the given nodes into groups that can only talk within
// their own group. Nodes not mentioned keep their existing links.
func (n *Network) Partition(groups ...[]int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	group := map[int]int{}
	for g, members := range groups {
		for _, m := range members {
			group[m] = g
		}
	}
	for a, ga := range group {
		for b, gb := range group {
			if a == b {
				continue
			}
			if ga == gb {
				delete(n.cut, [2]int{a, b})
			} else {
				n.cut[[2]int{a, b}] = true
			}
		}
	}
}

// Heal removes every partition and isolation.
func (n *Network) Heal() {
	n.mu.Lock()
	n.cut = map[[2]int]bool{}
	n.isolated = map[int]bool{}
	n.mu.Unlock()
}

// RPCCount returns the total number of RPCs attempted on the network.
func (n *Network) RPCCount() int64 { return n.rpcCount.Load() }

// ByteCount returns the total number of request bytes sent.
func (n *Network) ByteCount() int64 { return n.byteCount.Load() }

func (n *Network) linkedLocked(from, to int) bool {
	return !n.isolated[from] && !n.isolated[to] && !n.cut[[2]int{from, to}]
}

// End returns a client endpoint that sends RPCs from node `from` to node `to`.
func (n *Network) End(from, to int) *End { return &End{net: n, from: from, to: to} }

// End is one directed client endpoint. It is safe for concurrent use.
type End struct {
	net      *Network
	from, to int
}

// Call sends an RPC and waits for the reply. It returns false if the request
// or the reply was lost, or the destination is down or unreachable — exactly
// the ambiguity a real RPC timeout has: the caller cannot tell whether the
// handler ran.
func (e *End) Call(serviceMethod string, args, reply any) bool {
	n := e.net
	n.rpcCount.Add(1)

	n.mu.Lock()
	srv := n.servers[e.to]
	ok := srv != nil && n.linkedLocked(e.from, e.to) && n.linkedLocked(e.to, e.from)
	reliable, reorder := n.reliable, n.reorder
	n.mu.Unlock()

	if !ok {
		// Simulate waiting for a timeout on an unreachable host.
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		return false
	}
	if !reliable {
		time.Sleep(time.Duration(rand.Intn(27)) * time.Millisecond)
		if rand.Intn(10) == 0 { // drop the request
			return false
		}
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(args); err != nil {
		panic("simnet: encoding args: " + err.Error())
	}
	n.byteCount.Add(int64(buf.Len()))

	replyBytes, err := srv.dispatch(serviceMethod, buf.Bytes())
	if err != nil {
		return false
	}

	// The reply only makes it back if the server is still the same instance
	// and the link is still up.
	n.mu.Lock()
	still := n.servers[e.to] == srv && n.linkedLocked(e.to, e.from)
	n.mu.Unlock()
	if !still {
		return false
	}
	if !reliable && rand.Intn(10) == 0 { // drop the reply
		return false
	}
	if reorder && rand.Intn(900) < 600 {
		time.Sleep(time.Duration(200+rand.Intn(1+rand.Intn(2000))) * time.Millisecond)
	}
	if err := gob.NewDecoder(bytes.NewReader(replyBytes)).Decode(reply); err != nil {
		panic("simnet: decoding reply: " + err.Error())
	}
	return true
}

// Server is a set of named services reachable at one node.
type Server struct {
	mu       sync.Mutex
	services map[string]*service
}

// NewServer returns an empty server.
func NewServer() *Server { return &Server{services: map[string]*service{}} }

type service struct {
	rcvr    reflect.Value
	methods map[string]reflect.Method
}

var errorType = reflect.TypeOf((*error)(nil)).Elem()

// Register exposes every method of rcvr with the signature
// func(*Args, *Reply) error under the name "<name>.<Method>".
func (s *Server) Register(name string, rcvr any) {
	svc := &service{rcvr: reflect.ValueOf(rcvr), methods: map[string]reflect.Method{}}
	t := reflect.TypeOf(rcvr)
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		mt := m.Type
		if mt.NumIn() != 3 || mt.NumOut() != 1 || mt.Out(0) != errorType ||
			mt.In(1).Kind() != reflect.Pointer || mt.In(2).Kind() != reflect.Pointer {
			continue
		}
		svc.methods[m.Name] = m
	}
	s.mu.Lock()
	s.services[name] = svc
	s.mu.Unlock()
}

var errNoMethod = errors.New("simnet: unknown method")

func (s *Server) dispatch(serviceMethod string, args []byte) ([]byte, error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		return nil, errNoMethod
	}
	s.mu.Lock()
	svc := s.services[serviceMethod[:dot]]
	s.mu.Unlock()
	if svc == nil {
		return nil, errNoMethod
	}
	m, ok := svc.methods[serviceMethod[dot+1:]]
	if !ok {
		return nil, errNoMethod
	}
	argv := reflect.New(m.Type.In(1).Elem())
	if err := gob.NewDecoder(bytes.NewReader(args)).Decode(argv.Interface()); err != nil {
		panic("simnet: decoding args: " + err.Error())
	}
	replyv := reflect.New(m.Type.In(2).Elem())
	out := m.Func.Call([]reflect.Value{svc.rcvr, argv, replyv})
	if errv := out[0].Interface(); errv != nil {
		return nil, errv.(error)
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(replyv.Interface()); err != nil {
		panic("simnet: encoding reply: " + err.Error())
	}
	return buf.Bytes(), nil
}
