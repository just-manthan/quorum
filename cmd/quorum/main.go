// Command quorum runs one node of a replicated key/value cluster over TCP,
// with a small HTTP API for clients.
//
//	quorum -id 0 -peers 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 -http :8000 -data ./data/0
//
// HTTP API (any node; requests are forwarded to the leader internally):
//
//	GET    /kv/{key}                -> 200 value | 404
//	PUT    /kv/{key}       (body)   -> 204
//	POST   /kv/{key}/append (body)  -> 204
//	DELETE /kv/{key}                -> 204
//	POST   /kv/{key}/cas   {"expect": "...", "value": "..."} -> {"swapped": bool, "value": "..."}
//	GET    /status                  -> this node's Raft status
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/just-manthan/quorum/kv"
	"github.com/just-manthan/quorum/raft"
	"github.com/just-manthan/quorum/transport"
)

func main() {
	id := flag.Int("id", 0, "index of this node in -peers")
	peersFlag := flag.String("peers", "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002", "comma-separated RPC addresses of all nodes")
	httpAddr := flag.String("http", ":8000", "HTTP listen address for clients")
	dataDir := flag.String("data", "", "directory for durable state (default ./data/<id>)")
	maxRaftState := flag.Int("max-raft-state", 1<<20, "snapshot when the Raft log exceeds this many bytes (-1 disables)")
	flag.Parse()

	addrs := strings.Split(*peersFlag, ",")
	if *id < 0 || *id >= len(addrs) {
		log.Fatalf("-id %d out of range for %d peers", *id, len(addrs))
	}
	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data/%d", *id)
	}
	log.SetPrefix(fmt.Sprintf("[node %d] ", *id))

	persister, err := raft.OpenFilePersister(*dataDir)
	if err != nil {
		log.Fatal(err)
	}

	const rpcTimeout = 500 * time.Millisecond
	peers := make([]raft.Peer, len(addrs))
	for i, a := range addrs {
		peers[i] = transport.NewPeer(a, rpcTimeout)
	}
	server := kv.StartServer(peers, *id, persister, *maxRaftState, raft.DefaultConfig)

	ln, err := net.Listen("tcp", addrs[*id])
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		err := transport.Serve(ln, map[string]any{
			"Raft": server.Raft().Service(),
			"KV":   server.Service(),
		})
		log.Fatal(err)
	}()

	// A small pool of clients serves HTTP requests. Each Client keeps one
	// session on the cluster, so reusing them keeps dedup state bounded.
	pool := make(chan *kv.Client, 16)
	for i := 0; i < cap(pool); i++ {
		clientPeers := make([]raft.Peer, len(addrs))
		for j, a := range addrs {
			clientPeers[j] = transport.NewPeer(a, 2*kv.RequestTimeout)
		}
		pool <- kv.NewClient(clientPeers)
	}
	with := func(f func(*kv.Client)) {
		c := <-pool
		defer func() { pool <- c }()
		f(c)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		with(func(c *kv.Client) {
			v, ok := c.Lookup(r.PathValue("key"))
			if !ok {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			io.WriteString(w, v)
		})
	})
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		with(func(c *kv.Client) { c.Put(r.PathValue("key"), string(body)) })
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /kv/{key}/append", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		with(func(c *kv.Client) { c.Append(r.PathValue("key"), string(body)) })
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		with(func(c *kv.Client) { c.Delete(r.PathValue("key")) })
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /kv/{key}/cas", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Expect, Value string }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var swapped bool
		var observed string
		with(func(c *kv.Client) { swapped, observed = c.CompareAndSwap(r.PathValue("key"), req.Expect, req.Value) })
		writeJSON(w, map[string]any{"swapped": swapped, "value": observed})
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, server.Raft().Status())
	})

	httpSrv := &http.Server{Addr: *httpAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("raft rpc on %s, http on %s, data in %s", addrs[*id], *httpAddr, *dataDir)
		if err := httpSrv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down")
	server.Kill()
	ln.Close()
	httpSrv.Close()
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
