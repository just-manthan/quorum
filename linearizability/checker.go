// Package linearizability checks whether a concurrent history of key/value
// operations is linearizable (Herlihy & Wing, 1990): whether there is a
// single total order of the operations that (1) respects real time — if A
// returned before B was called, A comes first — and (2) is a legal
// sequential execution of a key/value map.
//
// The search is the Wing & Gong backtracking algorithm with memoisation of
// (set of linearized operations, model state), as popularised by Lowe and by
// the Porcupine checker. Linearizability is compositional (it is a "local"
// property), so the history is split by key and each key is checked
// independently, which keeps the search small.
package linearizability

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Input describes a request.
type Input struct {
	Kind   string // "Get", "Put", "Append", "Delete", "CAS"
	Key    string
	Value  string
	Expect string
}

// Output describes the response.
//   - Get: Value and Ok (= key exists)
//   - CAS: Ok (= swapped); Value is the observed value when not swapped
//   - others: ignored
type Output struct {
	Value string
	Ok    bool
}

// Operation is one completed request in the history. Call and Return are
// timestamps from the same monotonic clock.
type Operation struct {
	ClientID int
	Input    Input
	Output   Output
	Call     int64
	Return   int64
}

type state struct {
	value  string
	exists bool
}

// step applies op to s, returning the new state and whether op's observed
// output is consistent with s.
func step(s state, op *Operation) (state, bool) {
	in, out := op.Input, op.Output
	switch in.Kind {
	case "Get":
		return s, out.Ok == s.exists && (!s.exists || out.Value == s.value)
	case "Put":
		return state{in.Value, true}, true
	case "Append":
		return state{s.value + in.Value, true}, true
	case "Delete":
		return state{}, true
	case "CAS":
		if s.value == in.Expect {
			return state{in.Value, true}, out.Ok
		}
		return s, !out.Ok && out.Value == s.value
	}
	panic("linearizability: unknown op " + in.Kind)
}

// Result of a check.
type Result struct {
	Ok      bool
	Unknown bool   // the search ran out of time
	BadKey  string // a key whose history is not linearizable
	Detail  string
}

// Check reports whether history is linearizable, spending at most timeout
// on any single key.
func Check(history []Operation, timeout time.Duration) Result {
	byKey := map[string][]*Operation{}
	for i := range history {
		op := &history[i]
		byKey[op.Input.Key] = append(byKey[op.Input.Key], op)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	res := Result{Ok: true}
	for _, k := range keys {
		ok, done := checkKey(byKey[k], timeout)
		if !done {
			res.Unknown = true
			continue
		}
		if !ok {
			return Result{Ok: false, BadKey: k, Detail: describe(byKey[k])}
		}
	}
	return res
}

type searcher struct {
	ops      []*Operation
	visited  map[string]struct{}
	deadline time.Time
	steps    int
	timedOut bool
}

func checkKey(ops []*Operation, timeout time.Duration) (ok, done bool) {
	sort.Slice(ops, func(i, j int) bool { return ops[i].Call < ops[j].Call })
	s := &searcher{ops: ops, visited: map[string]struct{}{}, deadline: time.Now().Add(timeout)}
	remaining := make([]uint64, (len(ops)+63)/64)
	for i := range ops {
		remaining[i/64] |= 1 << (i % 64)
	}
	ok = s.search(remaining, len(ops), state{})
	return ok, !s.timedOut
}

func (s *searcher) search(remaining []uint64, left int, st state) bool {
	if left == 0 {
		return true
	}
	if s.steps++; s.steps%1024 == 0 && time.Now().After(s.deadline) {
		s.timedOut = true
		return false
	}
	key := memoKey(remaining, st)
	if _, seen := s.visited[key]; seen {
		return false
	}
	s.visited[key] = struct{}{}

	// Any operation called before the earliest return among the remaining
	// ones may be linearized next; nothing else may.
	minRet := int64(1<<63 - 1)
	for i, op := range s.ops {
		if remaining[i/64]&(1<<(i%64)) != 0 && op.Return < minRet {
			minRet = op.Return
		}
	}
	for i, op := range s.ops {
		if op.Call > minRet {
			break // ops are sorted by call time
		}
		bit := uint64(1) << (i % 64)
		if remaining[i/64]&bit == 0 {
			continue
		}
		next, legal := step(st, op)
		if !legal {
			continue
		}
		remaining[i/64] &^= bit
		found := s.search(remaining, left-1, next)
		remaining[i/64] |= bit
		if found || s.timedOut {
			return found
		}
	}
	return false
}

func memoKey(remaining []uint64, st state) string {
	var b strings.Builder
	var buf [8]byte
	for _, w := range remaining {
		binary.LittleEndian.PutUint64(buf[:], w)
		b.Write(buf[:])
	}
	if st.exists {
		b.WriteByte(1)
	} else {
		b.WriteByte(0)
	}
	b.WriteString(st.value)
	return b.String()
}

func describe(ops []*Operation) string {
	var b strings.Builder
	base := ops[0].Call
	for _, op := range ops {
		fmt.Fprintf(&b, "  client %d [%8.3fms, %8.3fms] %s(%q",
			op.ClientID, float64(op.Call-base)/1e6, float64(op.Return-base)/1e6, op.Input.Kind, op.Input.Key)
		if op.Input.Kind != "Get" && op.Input.Kind != "Delete" {
			fmt.Fprintf(&b, ", %q", op.Input.Value)
		}
		fmt.Fprintf(&b, ") -> %q ok=%v\n", op.Output.Value, op.Output.Ok)
		if b.Len() > 8000 {
			b.WriteString("  ...\n")
			break
		}
	}
	return b.String()
}
