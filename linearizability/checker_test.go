package linearizability

import (
	"testing"
	"time"
)

func put(c int, k, v string, call, ret int64) Operation {
	return Operation{ClientID: c, Input: Input{Kind: "Put", Key: k, Value: v}, Call: call, Return: ret}
}

func appendOp(c int, k, v string, call, ret int64) Operation {
	return Operation{ClientID: c, Input: Input{Kind: "Append", Key: k, Value: v}, Call: call, Return: ret}
}

func get(c int, k, v string, ok bool, call, ret int64) Operation {
	return Operation{ClientID: c, Input: Input{Kind: "Get", Key: k}, Output: Output{Value: v, Ok: ok}, Call: call, Return: ret}
}

func cas(c int, k, expect, v string, swapped bool, observed string, call, ret int64) Operation {
	return Operation{ClientID: c, Input: Input{Kind: "CAS", Key: k, Expect: expect, Value: v},
		Output: Output{Ok: swapped, Value: observed}, Call: call, Return: ret}
}

func TestCheck(t *testing.T) {
	cases := []struct {
		name string
		h    []Operation
		want bool
	}{
		{"empty", nil, true},
		{"sequential", []Operation{
			put(0, "x", "1", 0, 10), get(0, "x", "1", true, 20, 30),
		}, true},
		{"missing key reads as absent", []Operation{
			get(0, "x", "", false, 0, 10),
		}, true},
		{"stale read after write completed", []Operation{
			put(0, "x", "1", 0, 10), put(0, "x", "2", 20, 30), get(1, "x", "1", true, 40, 50),
		}, false},
		{"concurrent write may be seen either way", []Operation{
			put(0, "x", "1", 0, 10),
			put(1, "x", "2", 20, 60),
			get(2, "x", "1", true, 30, 40),
			get(2, "x", "2", true, 45, 55),
		}, true},
		{"reads cannot go back in time", []Operation{
			put(0, "x", "1", 0, 10),
			put(1, "x", "2", 20, 80),
			get(2, "x", "2", true, 30, 40),
			get(3, "x", "1", true, 50, 60), // after seeing 2, nobody may see 1 again
		}, false},
		{"appends applied twice are caught", []Operation{
			appendOp(0, "x", "a", 0, 10), get(1, "x", "aa", true, 20, 30),
		}, false},
		{"appends in some order", []Operation{
			appendOp(0, "x", "a", 0, 50), appendOp(1, "x", "b", 0, 50), get(2, "x", "ba", true, 60, 70),
		}, true},
		{"cas", []Operation{
			put(0, "lock", "", 0, 5),
			cas(1, "lock", "", "A", true, "", 10, 30),
			cas(2, "lock", "", "B", false, "A", 10, 30),
		}, true},
		{"cas both won is impossible", []Operation{
			put(0, "lock", "", 0, 5),
			cas(1, "lock", "", "A", true, "", 10, 30),
			cas(2, "lock", "", "B", true, "", 10, 30),
		}, false},
		{"independent keys", []Operation{
			put(0, "x", "1", 0, 10), put(1, "y", "1", 0, 10),
			get(0, "x", "1", true, 20, 30), get(1, "y", "", false, 20, 30),
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := Check(tc.h, time.Second)
			if res.Unknown {
				t.Fatalf("check timed out")
			}
			if res.Ok != tc.want {
				t.Fatalf("got linearizable=%v, want %v\n%s", res.Ok, tc.want, res.Detail)
			}
		})
	}
}

// TestManyConcurrent builds a large linearizable history from a known
// sequential order with heavy overlap, to make sure the search stays fast.
func TestManyConcurrent(t *testing.T) {
	var h []Operation
	val := ""
	now := int64(0)
	for i := 0; i < 400; i++ {
		c := i % 5
		v := string(rune('a' + i%26))
		// Each op overlaps its neighbours by a wide margin.
		call, ret := now, now+100
		if i%2 == 0 {
			val += v
			h = append(h, appendOp(c, "k", v, call, ret))
		} else {
			h = append(h, get(c, "k", val, true, call+60, ret))
		}
		now += 70
	}
	start := time.Now()
	res := Check(h, 10*time.Second)
	if !res.Ok || res.Unknown {
		t.Fatalf("expected linearizable, got %+v", res)
	}
	t.Logf("checked %d ops in %v", len(h), time.Since(start))
}
