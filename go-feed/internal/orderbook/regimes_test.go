package orderbook

import (
	"sort"
	"testing"

	"github.com/ndrandal/feed-simulator/go-feed/internal/itch"
)

// These tests judge each regime by its signature on the wire, the way the
// downstream flow detectors (SPEC 2026-11-17-order-flow-pipelines D5) will:
// they replay the message stream into a shadow book and never read regime
// state. Exchange time is modelled as the server's runner produces it — one
// Step per 100 ms tick, messages within a batch microseconds apart, the
// executions of one aggressive order sharing a timestamp.

const (
	tickNs   = int64(100e6) // one Step per 100 ms, as symbolRunner does
	msgGapNs = int64(1000)  // 1 µs between successive stamps within a batch
)

// stamp assigns exchange time to one Step's batch, mirroring itch.Clock:
// adjacent messages sharing a non-zero ExecGroup share a timestamp.
func stamp(msgs []itch.Message, base int64) {
	t := base
	for i := range msgs {
		if i > 0 && msgs[i].ExecGroup != 0 && msgs[i].ExecGroup == msgs[i-1].ExecGroup {
			msgs[i].Timestamp = msgs[i-1].Timestamp
			continue
		}
		t += msgGapNs
		msgs[i].Timestamp = t
	}
}

// runStream initialises a simulator with cfg and returns its stamped stream.
// wander moves the reference price so levels churn.
func runStream(t *testing.T, cfg RegimeConfig, steps int, wander bool) (*Simulator, []itch.Message) {
	t.Helper()
	sim := newTestSimulator()
	sim.SetRegimes(cfg)
	price := 100.00
	all := sim.Initialize(price)
	stamp(all, 0)
	for i := 1; i <= steps; i++ {
		if wander {
			price += float64((i%7)-3) * 0.01
		}
		msgs := sim.Step(price, 3)
		stamp(msgs, int64(i)*tickNs)
		all = append(all, msgs...)
	}
	return sim, all
}

type shadowOrder struct {
	side   byte
	price  float64
	shares int32
	addTs  int64
}

// shadow is a book rebuilt purely from the wire.
type shadow struct {
	orders map[uint64]*shadowOrder
}

func newShadow() *shadow { return &shadow{orders: map[uint64]*shadowOrder{}} }

func (b *shadow) apply(t *testing.T, m itch.Message) {
	t.Helper()
	switch m.Type {
	case itch.MsgAddOrder, itch.MsgAddOrderMPID:
		b.orders[m.OrderRef] = &shadowOrder{side: m.Side, price: m.Price, shares: m.Shares, addTs: m.Timestamp}
	case itch.MsgOrderExecuted, itch.MsgOrderCancel:
		o := b.orders[m.OrderRef]
		if o == nil {
			t.Fatalf("%c for unknown order %d", m.Type, m.OrderRef)
		}
		if m.Shares <= 0 || m.Shares > o.shares {
			t.Fatalf("%c of %d shares against an order holding %d", m.Type, m.Shares, o.shares)
		}
		o.shares -= m.Shares
		if o.shares == 0 {
			delete(b.orders, m.OrderRef)
		}
	case itch.MsgOrderDelete:
		if b.orders[m.OrderRef] == nil {
			t.Fatalf("D for unknown order %d", m.OrderRef)
		}
		delete(b.orders, m.OrderRef)
	case itch.MsgOrderReplace:
		o := b.orders[m.OrigOrderRef]
		if o == nil {
			t.Fatalf("U for unknown order %d", m.OrigOrderRef)
		}
		delete(b.orders, m.OrigOrderRef)
		b.orders[m.OrderRef] = &shadowOrder{side: o.side, price: m.Price, shares: m.Shares, addTs: m.Timestamp}
	}
}

// touch returns the best price on side ('B' highest, 'S' lowest), or 0.
func (b *shadow) touch(side byte) float64 {
	best := 0.0
	for _, o := range b.orders {
		if o.side != side {
			continue
		}
		if best == 0 || (side == 'B' && o.price > best) || (side == 'S' && o.price < best) {
			best = o.price
		}
	}
	return best
}

func (b *shadow) sharesAt(side byte, price float64) int32 {
	var n int32
	for _, o := range b.orders {
		if o.side == side && o.price == price {
			n += o.shares
		}
	}
	return n
}

// countSweeps counts aggressive events (maximal runs of adjacent E sharing a
// timestamp) spanning >= 2 distinct prices, asserting each one's shape: one
// resting side, consecutive match numbers, the first price fully consumed.
func countSweeps(t *testing.T, all []itch.Message) int {
	t.Helper()
	book := newShadow()
	sweeps := 0
	for i := 0; i < len(all); {
		if all[i].Type != itch.MsgOrderExecuted {
			book.apply(t, all[i])
			i++
			continue
		}
		j := i
		for j < len(all) && all[j].Type == itch.MsgOrderExecuted && all[j].Timestamp == all[i].Timestamp {
			j++
		}
		run := all[i:j]
		side := book.orders[run[0].OrderRef].side
		prices := map[float64]bool{}
		for k, m := range run {
			o := book.orders[m.OrderRef]
			if o == nil || o.side != side {
				t.Fatalf("aggressive event at %d hits both sides or an unknown order", i)
			}
			prices[o.price] = true
			if k > 0 && m.MatchNumber != run[k-1].MatchNumber+1 {
				t.Fatalf("aggressive event at %d: match numbers %d then %d are not consecutive",
					i, run[k-1].MatchNumber, m.MatchNumber)
			}
		}
		firstPx := book.orders[run[0].OrderRef].price
		for _, m := range run {
			book.apply(t, m)
		}
		if len(prices) >= 2 {
			sweeps++
			if left := book.sharesAt(side, firstPx); left != 0 {
				t.Fatalf("sweep at %d left %d shares on its first level %.2f", i, left, firstPx)
			}
		}
		i = j
	}
	return sweeps
}

func TestSweepSignature(t *testing.T) {
	_, all := runStream(t, RegimeConfig{SweepRate: 0.05}, 3000, false)
	if n := countSweeps(t, all); n < 50 {
		t.Fatalf("sweep regime at 5%%/Step over 3000 Steps produced %d multi-level sweeps, want >= 50", n)
	}
}

// countAbsorptions applies D5 P3: a touch level's tenure lasts while it is the
// touch; it absorbs when executions there reach 2x its opening displayed size
// while it still holds. Returns tenures that absorbed with >= 2 refills (an add
// at the touch price within 5 ms of an execution there).
func countAbsorptions(t *testing.T, all []itch.Message) int {
	t.Helper()
	type tenure struct {
		px           float64
		d0, executed int32
		lastExecTs   int64
		refills      int
		fired        bool
	}
	book := newShadow()
	ten := map[byte]*tenure{'B': nil, 'S': nil}
	absorbed := 0
	for _, m := range all {
		// Pre-event attribution against the pre-event touch.
		for _, side := range []byte{'B', 'S'} {
			tn := ten[side]
			if tn == nil {
				continue
			}
			switch m.Type {
			case itch.MsgOrderExecuted:
				if o := book.orders[m.OrderRef]; o != nil && o.side == side && o.price == tn.px {
					tn.executed += m.Shares
					tn.lastExecTs = m.Timestamp
				}
			case itch.MsgAddOrder, itch.MsgAddOrderMPID:
				if m.Side == side && m.Price == tn.px && tn.lastExecTs != 0 && m.Timestamp-tn.lastExecTs < 5e6 {
					tn.refills++
				}
			}
		}
		book.apply(t, m)
		for _, side := range []byte{'B', 'S'} {
			px := book.touch(side)
			tn := ten[side]
			if tn == nil || tn.px != px {
				ten[side] = &tenure{px: px, d0: book.sharesAt(side, px)}
				continue
			}
			if !tn.fired && tn.d0 > 0 && tn.executed >= 2*tn.d0 && tn.refills >= 2 {
				tn.fired = true
				absorbed++
			}
		}
	}
	return absorbed
}

func TestIcebergSignature(t *testing.T) {
	_, all := runStream(t, RegimeConfig{IcebergRate: 0.02}, 3000, false)
	if n := countAbsorptions(t, all); n < 10 {
		t.Fatalf("iceberg regime at 2%%/Step over 3000 Steps produced %d refilled absorptions, want >= 10", n)
	}
}

// countFleeting counts adds deleted within 50 ms of exchange time.
func countFleeting(t *testing.T, all []itch.Message) int {
	t.Helper()
	book := newShadow()
	n := 0
	for _, m := range all {
		if m.Type == itch.MsgOrderDelete {
			if o := book.orders[m.OrderRef]; o != nil && m.Timestamp-o.addTs < 50e6 {
				n++
			}
		}
		book.apply(t, m)
	}
	return n
}

func TestFleetingSignature(t *testing.T) {
	_, all := runStream(t, RegimeConfig{FleetingRate: 0.05}, 3000, false)
	// ~150 bursts of 2-6 orders. Ordinary flow can also delete an order added
	// earlier in the same Step, but rarely; the baseline is measured below.
	_, base := runStream(t, RegimeConfig{}, 3000, false)
	n, b := countFleeting(t, all), countFleeting(t, base)
	if n < b+300 {
		t.Fatalf("fleeting regime: %d adds deleted < 50 ms vs %d with regimes off; want >= %d", n, b, b+300)
	}
}

// largeOutcomes finds adds of >= max(5000, 10x median add) and reports how
// many ended fully executed vs deleted.
func largeOutcomes(t *testing.T, all []itch.Message) (adds, filled, pulled int) {
	t.Helper()
	var sizes []int
	for _, m := range all {
		if m.Type == itch.MsgAddOrder || m.Type == itch.MsgAddOrderMPID {
			sizes = append(sizes, int(m.Shares))
		}
	}
	sort.Ints(sizes)
	threshold := int32(10 * sizes[len(sizes)/2])
	if threshold < 5000 {
		threshold = 5000
	}
	book := newShadow()
	large := map[uint64]bool{}
	for _, m := range all {
		switch m.Type {
		case itch.MsgAddOrder, itch.MsgAddOrderMPID:
			if m.Shares >= threshold {
				adds++
				large[m.OrderRef] = true
			}
		case itch.MsgOrderExecuted:
			if large[m.OrderRef] && book.orders[m.OrderRef].shares == m.Shares {
				filled++
				delete(large, m.OrderRef)
			}
		case itch.MsgOrderDelete:
			if large[m.OrderRef] {
				pulled++
				delete(large, m.OrderRef)
			}
		case itch.MsgOrderReplace:
			if large[m.OrigOrderRef] {
				t.Fatalf("large order %d was replaced by ordinary flow", m.OrigOrderRef)
			}
		}
		book.apply(t, m)
	}
	return adds, filled, pulled
}

func TestLargeOrderSignature(t *testing.T) {
	_, all := runStream(t, RegimeConfig{LargeRate: 0.02}, 6000, false)
	adds, filled, pulled := largeOutcomes(t, all)
	// Half the large orders are fated to trade away and half to be pulled.
	// Ordinary flow fills some on its own (~1 in 5 when every one is fated
	// to be pulled), so a third filled is what shows the fill path working.
	if adds < 20 || 3*filled < adds || 4*pulled < adds {
		t.Fatalf("large regime: %d large adds, %d filled, %d pulled; want >= 20 adds, >= 1/3 filled, >= 1/4 pulled", adds, filled, pulled)
	}
}

// With every rate zero the regimes are off: no multi-level sweep and no large
// order appear, and the stream is the pre-regime one.
func TestRegimesOff(t *testing.T) {
	_, all := runStream(t, RegimeConfig{}, 3000, true)
	if n := countSweeps(t, all); n != 0 {
		t.Fatalf("regimes off: %d multi-level sweeps", n)
	}
	if adds, _, _ := largeOutcomes(t, all); adds != 0 {
		t.Fatalf("regimes off: %d large adds", adds)
	}
}

// All regimes at once, at high rates, on a wandering price: the book cap and
// the no-orphan invariant hold, and the book rebuilt from the wire matches the
// simulator's own book order for order.
func TestRegimesKeepBookInvariants(t *testing.T) {
	cfg := RegimeConfig{SweepRate: 0.05, IcebergRate: 0.03, FleetingRate: 0.05, LargeRate: 0.03}
	sim, all := runStream(t, cfg, 6000, true)
	b := sim.Book()
	if b.BidLevels() > MaxLevels || b.AskLevels() > MaxLevels {
		t.Fatalf("book cap broken: %d bid / %d ask levels", b.BidLevels(), b.AskLevels())
	}
	reachable := 0
	for _, l := range b.Depth().Bids {
		reachable += l.Orders
	}
	for _, l := range b.Depth().Asks {
		reachable += l.Orders
	}
	if b.OrderCount() != reachable {
		t.Fatalf("orphans: OrderCount=%d reachable=%d", b.OrderCount(), reachable)
	}
	wire := newShadow()
	for _, m := range all {
		wire.apply(t, m)
	}
	if len(wire.orders) != b.OrderCount() {
		t.Fatalf("wire book holds %d orders, simulator %d", len(wire.orders), b.OrderCount())
	}
	for id, w := range wire.orders {
		o := b.GetOrder(id)
		if o == nil || o.Shares != w.shares || o.Price != w.price || byte(o.Side) != w.side {
			t.Fatalf("order %d: wire %+v, book %+v", id, w, o)
		}
	}
	// Each regime actually ran in this mix.
	if countSweeps(t, all) == 0 || countFleeting(t, all) == 0 {
		t.Fatal("mixed run exercised no sweeps or no fleeting orders")
	}
	if adds, _, _ := largeOutcomes(t, all); adds == 0 {
		t.Fatal("mixed run exercised no large orders")
	}
}
