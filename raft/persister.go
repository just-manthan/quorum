package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Persister is Raft's stable storage. Save must durably and atomically
// replace both the Raft state and the snapshot.
type Persister interface {
	Save(state, snapshot []byte)
	ReadState() []byte
	ReadSnapshot() []byte
	StateSize() int
}

// MemPersister keeps state in memory. Tests use Copy to model a crash:
// the restarted server sees exactly what had been saved, nothing more.
type MemPersister struct {
	mu       sync.Mutex
	state    []byte
	snapshot []byte
}

func NewMemPersister() *MemPersister { return &MemPersister{} }

func clone(b []byte) []byte { return append([]byte(nil), b...) }

func (p *MemPersister) Copy() *MemPersister {
	p.mu.Lock()
	defer p.mu.Unlock()
	return &MemPersister{state: clone(p.state), snapshot: clone(p.snapshot)}
}

func (p *MemPersister) Save(state, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state, p.snapshot = clone(state), clone(snapshot)
}

func (p *MemPersister) ReadState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.state)
}

func (p *MemPersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.snapshot)
}

func (p *MemPersister) StateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.state)
}

// FilePersister stores state and snapshot in a single file that is replaced
// atomically (write temp file, fsync, rename, fsync dir), so a crash in the
// middle of Save leaves either the old or the new version — never a mix.
type FilePersister struct {
	mu       sync.Mutex
	path     string
	state    []byte
	snapshot []byte
}

// OpenFilePersister loads (or creates) the persister stored in dir.
func OpenFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := &FilePersister{path: filepath.Join(dir, "raft.state")}
	data, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return p, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) < 8 {
		return nil, fmt.Errorf("persister: corrupt state file %s", p.path)
	}
	n := binary.BigEndian.Uint64(data[:8])
	if uint64(len(data)-8) < n {
		return nil, fmt.Errorf("persister: corrupt state file %s", p.path)
	}
	p.state = data[8 : 8+n]
	p.snapshot = data[8+n:]
	return p, nil
}

func (p *FilePersister) Save(state, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	buf := make([]byte, 8, 8+len(state)+len(snapshot))
	binary.BigEndian.PutUint64(buf, uint64(len(state)))
	buf = append(buf, state...)
	buf = append(buf, snapshot...)

	tmp := p.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		panic(err) // losing durability silently would violate Raft's safety
	}
	if _, err := f.Write(buf); err != nil {
		panic(err)
	}
	if err := f.Sync(); err != nil {
		panic(err)
	}
	f.Close()
	if err := os.Rename(tmp, p.path); err != nil {
		panic(err)
	}
	if d, err := os.Open(filepath.Dir(p.path)); err == nil {
		d.Sync()
		d.Close()
	}
	p.state, p.snapshot = clone(state), clone(snapshot)
}

func (p *FilePersister) ReadState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.state)
}

func (p *FilePersister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return clone(p.snapshot)
}

func (p *FilePersister) StateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.state)
}
