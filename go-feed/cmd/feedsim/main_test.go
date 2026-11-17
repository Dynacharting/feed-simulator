package main

import (
	"testing"

	"github.com/ndrandal/feed-simulator/go-feed/internal/engine"
	"github.com/ndrandal/feed-simulator/go-feed/internal/itch"
	"github.com/ndrandal/feed-simulator/go-feed/internal/orderbook"
)

// The archive records exactly one trade per fill: one record per E, carrying
// the E's match number, shares, price and aggressor side.
func TestTradeRecordsOnePerFill(t *testing.T) {
	orderbook.SetOrderIDCounter(0)
	orderbook.SetMatchCounter(0)
	sim := orderbook.NewSimulator(engine.NewRNG(7), orderbook.NewBook(1, 0.01), 1, 0.01)
	sim.Initialize(100.00)

	fills, records := 0, 0
	seen := map[uint64]bool{}
	for i := 0; i < 1000; i++ {
		msgs := sim.Step(100.00, 3)
		byMatch := map[uint64]itch.Message{}
		for _, m := range msgs {
			if m.Type == itch.MsgOrderExecuted {
				fills++
				byMatch[m.MatchNumber] = m
			}
		}
		for _, tr := range tradeRecords(1, msgs) {
			records++
			if seen[tr.matchNumber] {
				t.Fatalf("match %d recorded twice", tr.matchNumber)
			}
			seen[tr.matchNumber] = true
			e, ok := byMatch[tr.matchNumber]
			if !ok {
				t.Fatalf("record for match %d has no E", tr.matchNumber)
			}
			if tr.shares != e.Shares || tr.price != e.Price || tr.aggressor != e.Aggressor || tr.aggressor == 0 {
				t.Fatalf("record %+v does not match E %+v", tr, e)
			}
		}
	}
	if fills == 0 || records != fills {
		t.Fatalf("fills=%d records=%d; want equal and non-zero", fills, records)
	}
}

// A P, were one ever on the wire, is not a second record of a fill.
func TestTradeRecordsIgnoresP(t *testing.T) {
	msgs := []itch.Message{
		{Type: itch.MsgOrderExecuted, MatchNumber: 1, Shares: 100, Price: 10, Aggressor: 'B'},
		{Type: itch.MsgTrade, MatchNumber: 1, Shares: 100, Price: 10, Side: 'B'},
	}
	if got := tradeRecords(1, msgs); len(got) != 1 {
		t.Fatalf("got %d records, want 1", len(got))
	}
}
