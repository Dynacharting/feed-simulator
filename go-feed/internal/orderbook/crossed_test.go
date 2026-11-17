package orderbook

import (
	"testing"

	"github.com/ndrandal/feed-simulator/go-feed/internal/engine"
)

// The book is never crossed (ENC-1661, SPEC 2026-11-17-order-flow-pipelines
// D10). Resting orders are priced off the random walk, which runs away from the
// book between trades; without clampPassive a buy lands at or through the ask
// (or a sell through the bid) and the book stays crossed. This drives a long
// seeded walk with every regime at a high rate and checks the simulator's own
// book after every Step and the book a consumer rebuilds from the wire after
// every message.
func TestBookNeverCrosses(t *testing.T) {
	sim := newTestSimulator()
	sim.SetRegimes(RegimeConfig{SweepRate: 0.05, IcebergRate: 0.03, FleetingRate: 0.05, LargeRate: 0.03})
	walk := engine.NewRNG(7)
	price := 100.00
	wire := newShadow()
	for _, m := range sim.Initialize(price) {
		wire.apply(t, m)
	}
	const steps = 20000
	for i := 1; i <= steps; i++ {
		// A walk of up to +/-5 ticks per Step: it outruns the book often.
		price += float64(walk.IntRange(-5, 5)) * 0.01
		if price < 50 {
			price = 50
		}
		msgs := sim.Step(price, 3)
		stamp(msgs, int64(i)*tickNs)
		// The wire book is checked after every message, not just every Step:
		// a fleeting burst adds and deletes within one batch, so a cross it
		// causes is gone by the Step's end but a consumer still sees it.
		for j, m := range msgs {
			wire.apply(t, m)
			if bid, ask := wire.touch('B'), wire.touch('S'); bid > 0 && ask > 0 && bid >= ask {
				t.Fatalf("step %d msg %d (%c): wire-built book crossed: bid %.2f >= ask %.2f", i, j, m.Type, bid, ask)
			}
		}
		b := sim.Book()
		if bid, ask := b.BestBid(), b.BestAsk(); bid > 0 && ask > 0 && bid >= ask {
			t.Fatalf("step %d: simulator book crossed: bid %.2f >= ask %.2f (walk %.2f)", i, bid, ask, price)
		}
	}
}

// clampPassive must leave a non-crossing price alone, so an iceberg still
// improves the touch by one tick when the spread allows.
func TestIcebergStillImprovesTouch(t *testing.T) {
	improved := map[Side]int{}
	for seed := uint64(0); seed < 40; seed++ {
		SetOrderIDCounter(0)
		book := NewBook(1, 0.01)
		sim := NewSimulator(engine.NewRNG(int64(seed)), book, 1, 0.01)
		book.AddOrder(&Order{ID: NextOrderID(), Locate: 1, Side: SideBuy, Price: 99.99, Shares: 500})
		book.AddOrder(&Order{ID: NextOrderID(), Locate: 1, Side: SideSell, Price: 100.03, Shares: 500})
		sim.startIceberg()
		if sim.ice == nil {
			t.Fatalf("seed %d: iceberg did not start", seed)
		}
		want := 100.00
		if sim.ice.side == SideSell {
			want = 100.02
		}
		if diff := sim.ice.price - want; diff > 1e-9 || diff < -1e-9 {
			t.Fatalf("seed %d: %c iceberg at %.2f, want %.2f (one tick inside the touch)", seed, sim.ice.side, sim.ice.price, want)
		}
		improved[sim.ice.side]++
	}
	if improved[SideBuy] == 0 || improved[SideSell] == 0 {
		t.Fatalf("both sides should be exercised: %v", improved)
	}
}

// The clamp itself: a passive order through the opposite touch goes one tick
// behind it, a price that does not cross is untouched, and it never goes
// below one tick.
func TestClampPassive(t *testing.T) {
	book := NewBook(1, 0.01)
	sim := NewSimulator(engine.NewRNG(1), book, 1, 0.01)
	book.AddOrder(&Order{ID: 1, Side: SideBuy, Price: 99.98, Shares: 100})
	book.AddOrder(&Order{ID: 2, Side: SideSell, Price: 100.02, Shares: 100})
	cases := []struct {
		side     Side
		in, want float64
	}{
		{SideBuy, 100.05, 100.01}, {SideBuy, 100.02, 100.01}, {SideBuy, 100.01, 100.01}, {SideBuy, 99.90, 99.90},
		{SideSell, 99.90, 99.99}, {SideSell, 99.98, 99.99}, {SideSell, 99.99, 99.99}, {SideSell, 100.10, 100.10},
	}
	for _, c := range cases {
		if got := sim.clampPassive(c.side, c.in); got-c.want > 1e-9 || c.want-got > 1e-9 {
			t.Errorf("clampPassive(%c, %.2f) = %.4f, want %.2f", c.side, c.in, got, c.want)
		}
	}
	low := NewBook(1, 0.01)
	lsim := NewSimulator(engine.NewRNG(1), low, 1, 0.01)
	low.AddOrder(&Order{ID: 3, Side: SideSell, Price: 0.01, Shares: 100})
	if got := lsim.clampPassive(SideBuy, 0.05); got < 0.01 {
		t.Errorf("clampPassive went below one tick: %.4f", got)
	}
}
