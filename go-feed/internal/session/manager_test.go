package session

import (
	"sync"
	"testing"

	"github.com/ndrandal/feed-simulator/go-feed/internal/itch"
	"github.com/ndrandal/feed-simulator/go-feed/internal/symbol"
)

func newTestManager() *Manager {
	return NewManager(symbol.AllSymbols(), 100)
}

func TestResolveTickersSpecific(t *testing.T) {
	m := newTestManager()
	locs, all := m.ResolveTickers([]string{"NEXO", "QBIT"})
	if all {
		t.Fatal("should not be all")
	}
	if len(locs) != 2 {
		t.Fatalf("expected 2 locates, got %d", len(locs))
	}
	locSet := make(map[uint16]bool)
	for _, l := range locs {
		locSet[l] = true
	}
	if !locSet[1] || !locSet[2] {
		t.Fatalf("expected locates 1 and 2, got %v", locs)
	}
}

func TestResolveTickersWildcard(t *testing.T) {
	m := newTestManager()
	locs, all := m.ResolveTickers([]string{"*"})
	if !all {
		t.Fatal("wildcard should set all=true")
	}
	if locs != nil {
		t.Fatalf("wildcard should return nil locates, got %v", locs)
	}
}

func TestResolveTickersUnknown(t *testing.T) {
	m := newTestManager()
	locs, all := m.ResolveTickers([]string{"ZZZZ"})
	if all {
		t.Fatal("should not be all")
	}
	if len(locs) != 0 {
		t.Fatalf("expected 0 locates for unknown ticker, got %d", len(locs))
	}
}

func TestResolveTickersMixed(t *testing.T) {
	m := newTestManager()
	locs, all := m.ResolveTickers([]string{"NEXO", "ZZZZ", "BLITZ"})
	if all {
		t.Fatal("should not be all")
	}
	if len(locs) != 2 {
		t.Fatalf("expected 2 locates (NEXO + BLITZ), got %d", len(locs))
	}
}

func TestResolveTickersWildcardShortCircuits(t *testing.T) {
	m := newTestManager()
	// Wildcard should return immediately even with other tickers
	locs, all := m.ResolveTickers([]string{"NEXO", "*", "BLITZ"})
	if !all {
		t.Fatal("wildcard should short-circuit to all=true")
	}
	if locs != nil {
		t.Fatalf("wildcard should return nil locates, got %v", locs)
	}
}

func TestBroadcastPerMessageTimestamps(t *testing.T) {
	m := newTestManager()
	msgs := []itch.Message{
		{Type: itch.MsgAddOrder, StockLocate: 1},
		{Type: itch.MsgOrderExecuted, StockLocate: 1, ExecGroup: 3},
		{Type: itch.MsgOrderExecuted, StockLocate: 1, ExecGroup: 3},
		{Type: itch.MsgOrderDelete, StockLocate: 1},
		{Type: itch.MsgOrderCancel, StockLocate: 1},
	}
	m.Broadcast(1, "NEXO", msgs)
	if msgs[1].Timestamp != msgs[2].Timestamp {
		t.Fatalf("executions of one aggressor differ: %d vs %d", msgs[1].Timestamp, msgs[2].Timestamp)
	}
	for _, i := range []int{1, 3, 4} {
		if msgs[i].Timestamp <= msgs[i-1].Timestamp {
			t.Fatalf("msg %d ts %d not after msg %d ts %d", i, msgs[i].Timestamp, i-1, msgs[i-1].Timestamp)
		}
	}
}

// Concurrent broadcasters (the 30 symbol runners) must still deliver a
// non-decreasing timestamp sequence on each client's stream.
func TestBroadcastStreamMonotoneUnderConcurrency(t *testing.T) {
	m := newTestManager()
	c := NewClient(nil, 1<<16)
	c.SubscribeAll()
	c.SetFormat(FormatBinary)
	m.clients[c.ID] = c

	const producers, batches = 8, 200
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(loc uint16) {
			defer wg.Done()
			for b := 0; b < batches; b++ {
				m.Broadcast(loc, "NEXO", []itch.Message{
					{Type: itch.MsgOrderDelete, StockLocate: loc},
					{Type: itch.MsgOrderDelete, StockLocate: loc},
				})
			}
		}(uint16(p + 1))
	}
	wg.Wait()
	close(c.sendCh)

	var last int64 = -1
	n := 0
	for frame := range c.sendCh {
		// Frame: Len(2) Type(1) Locate(2) Tracking(2) Timestamp(6) ...
		var ts int64
		for _, b := range frame[7:13] {
			ts = ts<<8 | int64(b)
		}
		if ts < last {
			t.Fatalf("frame %d ts %d < previous %d", n, ts, last)
		}
		last = ts
		n++
	}
	if n != producers*batches*2 {
		t.Fatalf("got %d frames, want %d", n, producers*batches*2)
	}
}
