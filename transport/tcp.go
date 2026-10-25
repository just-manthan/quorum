// Package transport carries Raft and KV RPCs over real TCP using net/rpc.
package transport

import (
	"net"
	"net/rpc"
	"sync"
	"time"
)

// Peer is a lazily-connected, auto-reconnecting RPC client for one address.
// It satisfies raft.Peer.
type Peer struct {
	addr    string
	timeout time.Duration

	mu     sync.Mutex
	client *rpc.Client
}

// NewPeer returns a Peer for addr. No connection is made until the first Call.
func NewPeer(addr string, timeout time.Duration) *Peer {
	return &Peer{addr: addr, timeout: timeout}
}

func (p *Peer) getClient() (*rpc.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client != nil {
		return p.client, nil
	}
	conn, err := net.DialTimeout("tcp", p.addr, p.timeout)
	if err != nil {
		return nil, err
	}
	p.client = rpc.NewClient(conn)
	return p.client, nil
}

func (p *Peer) reset(c *rpc.Client) {
	p.mu.Lock()
	if p.client == c {
		p.client.Close()
		p.client = nil
	}
	p.mu.Unlock()
}

// Call performs an RPC with a timeout. Like a simulated network call, it
// returns false on any failure (the handler may or may not have run).
func (p *Peer) Call(serviceMethod string, args, reply any) bool {
	c, err := p.getClient()
	if err != nil {
		return false
	}
	call := c.Go(serviceMethod, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if call.Error != nil {
			if call.Error == rpc.ErrShutdown {
				p.reset(c)
			}
			return false
		}
		return true
	case <-time.After(p.timeout):
		p.reset(c) // the connection may be wedged; start fresh next time
		return false
	}
}

// Serve registers services on a fresh net/rpc server and serves on ln until
// it is closed.
func Serve(ln net.Listener, services map[string]any) error {
	srv := rpc.NewServer()
	for name, svc := range services {
		if err := srv.RegisterName(name, svc); err != nil {
			return err
		}
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go srv.ServeConn(conn)
	}
}
