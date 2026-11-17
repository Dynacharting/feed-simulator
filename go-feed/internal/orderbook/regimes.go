package orderbook

import (
	"github.com/ndrandal/feed-simulator/go-feed/internal/itch"
)

// Behaviour regimes (SPEC 2026-11-17-order-flow-pipelines D10(b)).
//
// On top of the random walk of Step's ordinary actions, four seeded regimes
// produce the order-flow shapes the downstream flow detectors look for (D5):
//
//   - sweep:    one aggressive order consumes the whole best level and carries
//     on into the next one (or two, or three). Its executions share one
//     ExecGroup, hence one timestamp, and take consecutive match numbers.
//   - iceberg:  a synthetic iceberg rests at the touch as two displayed child
//     slices backed by a hidden reserve. When a child is executed away, a new
//     child is added at the same price in the same batch — microseconds of
//     exchange time later — until the reserve is spent, so the level absorbs
//     several times its opening displayed size while it holds.
//   - fleeting: a burst of adds near the touch, every one of which is deleted
//     (D) within the same batch, i.e. well under 50 ms after it was added.
//   - large:    a resting order of 8 000–25 000 shares (>= 10x the 100–1 000
//     share ordinary add) joins the touch and later either trades away through
//     executions or is pulled with a delete.
//
// Each regime is gated by a per-Step probability; a zero rate switches it off
// and consumes no randomness, so a Simulator with the zero RegimeConfig (the
// NewSimulator default) behaves exactly as it did before regimes existed.
// Regime state is runtime-only: it is not part of the persisted snapshot, so
// after a restart the orders a regime placed rest on as ordinary orders.

// RegimeConfig holds the per-Step trigger probabilities of each regime.
type RegimeConfig struct {
	SweepRate    float64
	IcebergRate  float64
	FleetingRate float64
	LargeRate    float64
}

// DefaultRegimes are the rates the server runs with unless configured
// otherwise. A normal symbol steps every 100 ms (10 Steps/s), so per symbol
// these are one sweep per ~50 s, one iceberg and one large order per ~100 s,
// and one fleeting burst per ~25 s — occasional, not constant.
func DefaultRegimes() RegimeConfig {
	return RegimeConfig{
		SweepRate:    0.002,
		IcebergRate:  0.001,
		FleetingRate: 0.004,
		LargeRate:    0.001,
	}
}

// Regime tuning. Sizes are in round lots of 100 shares.
const (
	icebergPeakMinLots = 2
	icebergPeakMaxLots = 5
	icebergHitProb     = 0.6 // per Step while an iceberg is active
	icebergMaxAge      = 200 // Steps before the algo gives up and pulls

	fleetingMinOrders = 2
	fleetingMaxOrders = 6

	largeMinLots    = 80  // 8 000 shares
	largeMaxLots    = 250 // 25 000 shares
	largeMinAge     = 30  // Steps before the order's fate is decided
	largeMaxAge     = 150
	largeFillProb   = 0.5 // share of large orders that end by trading away
	largeChunkProb  = 0.5 // per Step, once filling, chance of an aggressive chunk
	largeChunkMin   = 10  // lots per chunk
	largeChunkMax   = 50
	largeMaxDepth   = 3 // a filling chunk may walk at most this many levels
	largeFillBudget = 100
)

// iceberg is the runtime state of one synthetic iceberg.
type iceberg struct {
	side     Side
	price    float64
	peak     int32 // shares per displayed child
	reserve  int32 // hidden shares not yet displayed
	children []uint64
	age      int
}

// largeOrder is the runtime state of one large resting order.
type largeOrder struct {
	id       uint64
	side     Side
	age      int
	lifetime int
	fill     bool // fate: trade away (true) or be pulled (false)
	filling  int  // Steps spent trying to fill
}

// SetRegimes sets the behaviour-regime rates. The zero value disables all.
func (s *Simulator) SetRegimes(c RegimeConfig) {
	s.regimes = c
}

// Regimes returns the configured rates.
func (s *Simulator) Regimes() RegimeConfig {
	return s.regimes
}

// roll reports whether a regime with the given per-Step rate fires. A rate of
// zero never fires and draws nothing from the RNG.
func (s *Simulator) roll(rate float64) bool {
	return rate > 0 && s.rng.Float64() < rate
}

// stepRegimes advances the running regimes and starts new ones.
func (s *Simulator) stepRegimes(currentPrice float64) []itch.Message {
	var msgs []itch.Message
	if s.ice != nil {
		msgs = append(msgs, s.icebergStep()...)
	} else if s.roll(s.regimes.IcebergRate) {
		msgs = append(msgs, s.startIceberg()...)
	}
	if s.large != nil {
		msgs = append(msgs, s.largeStep()...)
	} else if s.roll(s.regimes.LargeRate) {
		msgs = append(msgs, s.startLarge()...)
	}
	if s.roll(s.regimes.SweepRate) {
		_ = s.doSweep
	}
	if s.roll(s.regimes.FleetingRate) {
		msgs = append(msgs, s.doFleeting(currentPrice)...)
	}
	return msgs
}

// isRegimeOrder reports whether id belongs to a running regime. Random
// cancels and replaces leave these alone (a replace would change the id the
// regime tracks); executions may still reach them.
func (s *Simulator) isRegimeOrder(id uint64) bool {
	if s.large != nil && s.large.id == id {
		return true
	}
	if s.ice != nil {
		for _, c := range s.ice.children {
			if c == id {
				return true
			}
		}
	}
	return false
}

// --- shared helpers ---------------------------------------------------------

// levelOrders returns a copy of the order pointers on the first n levels of
// side (fewer if the side is shallower).
func (b *Book) levelOrders(side Side, n int) []PriceLevel {
	b.mu.RLock()
	defer b.mu.RUnlock()
	levels := b.Bids
	if side == SideSell {
		levels = b.Asks
	}
	if n > len(levels) {
		n = len(levels)
	}
	out := make([]PriceLevel, n)
	for i := 0; i < n; i++ {
		out[i] = PriceLevel{Price: levels[i].Price, Orders: append([]*Order(nil), levels[i].Orders...)}
	}
	return out
}

// best returns the best price on side, or 0 when the side is empty.
func (s *Simulator) best(side Side) float64 {
	if side == SideBuy {
		return s.book.BestBid()
	}
	return s.book.BestAsk()
}

func opposite(side Side) Side {
	if side == SideBuy {
		return SideSell
	}
	return SideBuy
}

func (s *Simulator) randomSide() Side {
	if s.rng.Float64() < 0.5 {
		return SideSell
	}
	return SideBuy
}

// fill is one execution an aggressive event will perform.
type fill struct {
	o      *Order
	shares int32
}

// executeEvent performs fills as one aggressive event: every execution shares
// one ExecGroup (so one timestamp) and the event takes a block of consecutive
// match numbers. Any iceberg reloads it triggers follow the executions.
func (s *Simulator) executeEvent(aggressor Side, fills []fill) []itch.Message {
	if len(fills) == 0 {
		return nil
	}
	s.execGroup++
	first := ReserveMatchNumbers(len(fills))
	msgs := make([]itch.Message, 0, len(fills)+2)
	for i, f := range fills {
		msgs = append(msgs, itch.Message{
			Type:        itch.MsgOrderExecuted,
			StockLocate: s.locateCode,
			OrderRef:    f.o.ID,
			Shares:      f.shares,
			MatchNumber: first + uint64(i),
			Price:       f.o.Price,
			Aggressor:   byte(aggressor),
			ExecGroup:   s.execGroup,
		})
		s.book.ReduceOrder(f.o.ID, f.shares)
	}
	return append(msgs, s.icebergReload()...)
}

// --- sweep ------------------------------------------------------------------

// doSweep sends one aggressive order through the full best level and into at
// least one more. It never sweeps a level holding a regime order, and always
// leaves the last level it reaches partly standing, so a side is never emptied.
func (s *Simulator) doSweep() []itch.Message {
	levels := 2
	if r := s.rng.Float64(); r < 0.1 {
		levels = 4
	} else if r < 0.35 {
		levels = 3
	}
	first := s.randomSide() // the resting side to be swept
	for _, side := range []Side{first, opposite(first)} {
		if s.ice != nil && s.ice.side == side {
			continue
		}
		lv := s.book.levelOrders(side, levels+1)
		// Need the swept levels plus one behind them so the side survives.
		usable := 0
		for usable < len(lv)-1 && usable < levels && !s.levelHasRegimeOrder(lv[usable]) {
			usable++
		}
		if usable < 2 {
			continue
		}
		var fills []fill
		for i := 0; i < usable-1; i++ {
			for _, o := range lv[i].Orders {
				fills = append(fills, fill{o, o.Shares})
			}
		}
		// Last level reached: the front order only, partly or fully.
		o := lv[usable-1].Orders[0]
		take := int32(s.rng.IntRange(1, int(o.Shares/100))) * 100
		if take <= 0 {
			take = o.Shares
		}
		fills = append(fills, fill{o, take})
		return s.executeEvent(opposite(side), fills)
	}
	return nil
}

func (s *Simulator) levelHasRegimeOrder(l PriceLevel) bool {
	for _, o := range l.Orders {
		if s.isRegimeOrder(o.ID) {
			return true
		}
	}
	return false
}

// --- iceberg ----------------------------------------------------------------

// startIceberg places a two-slice iceberg at the touch of a random side:
// improving the touch by one tick when the spread allows, else joining it.
func (s *Simulator) startIceberg() []itch.Message {
	side := s.randomSide()
	bid, ask := s.book.BestBid(), s.book.BestAsk()
	if bid == 0 || ask == 0 {
		return nil
	}
	price := bid
	if side == SideSell {
		price = ask
	}
	if ask-bid >= 2*s.tickSize-s.tickSize/2 {
		if side == SideBuy {
			price = snapPrice(bid+s.tickSize, s.tickSize)
		} else {
			price = snapPrice(ask-s.tickSize, s.tickSize)
		}
	}
	peak := int32(s.rng.IntRange(icebergPeakMinLots, icebergPeakMaxLots)) * 100
	ice := &iceberg{side: side, price: price, peak: peak}
	s.ice = ice

	var msgs []itch.Message
	for i := 0; i < 2; i++ {
		msgs = append(msgs, s.addIcebergChild(peak)...)
	}
	if len(ice.children) < 2 {
		// Could not rest (trimmed off the book): give up quietly.
		msgs = append(msgs, s.endIceberg(true)...)
		return msgs
	}
	// Reserve: four times what the level displays now, so the level trades
	// well past twice whatever it displayed when it became the touch.
	shown := int32(0)
	for _, l := range s.book.levelOrders(side, 1) {
		for _, o := range l.Orders {
			shown += o.Shares
		}
	}
	ice.reserve = (4 * shown / peak) * peak
	return msgs
}

func (s *Simulator) addIcebergChild(shares int32) []itch.Message {
	o := &Order{ID: NextOrderID(), Locate: s.locateCode, Side: s.ice.side, Price: s.ice.price, Shares: shares}
	msgs := s.addMsgs(o, s.book.AddOrder(o))
	if s.book.GetOrder(o.ID) != nil {
		s.ice.children = append(s.ice.children, o.ID)
	}
	return msgs
}

// icebergReload replaces every child that has gone (executed away) with a new
// child at the same price, while the reserve lasts and the level still holds.
func (s *Simulator) icebergReload() []itch.Message {
	ice := s.ice
	if ice == nil {
		return nil
	}
	var msgs []itch.Message
	live := ice.children[:0]
	gone := 0
	for _, id := range ice.children {
		if s.book.GetOrder(id) != nil {
			live = append(live, id)
		} else {
			gone++
		}
	}
	ice.children = live
	if gone == 0 {
		return nil
	}
	if len(live) == 0 || s.best(ice.side) != ice.price {
		// The level itself went (evicted, or swept from under us): stop.
		return append(msgs, s.endIceberg(true)...)
	}
	for i := 0; i < gone && ice.reserve > 0; i++ {
		sh := ice.peak
		if sh > ice.reserve {
			sh = ice.reserve
		}
		ice.reserve -= sh
		msgs = append(msgs, s.addIcebergChild(sh)...)
	}
	if ice.reserve == 0 {
		s.ice = nil // spent: the last slices rest on as ordinary orders
	}
	return msgs
}

// icebergStep has an aggressor trade against the iceberg's level, or retires
// the iceberg when its level is no longer the touch or it has run too long.
func (s *Simulator) icebergStep() []itch.Message {
	ice := s.ice
	ice.age++
	if ice.age > icebergMaxAge || s.best(ice.side) != ice.price {
		return s.endIceberg(true)
	}
	if !s.roll(icebergHitProb) {
		return nil
	}
	lv := s.book.levelOrders(ice.side, 1)
	if len(lv) == 0 {
		return s.endIceberg(true)
	}
	orders := lv[0].Orders
	want := int32(s.rng.IntRange(1, 2*int(ice.peak/100))) * 100
	var fills []fill
	for i, o := range orders {
		if want <= 0 {
			break
		}
		take := o.Shares
		if i == len(orders)-1 {
			take -= 100 // the level always holds
		}
		if take > want {
			take = want
		}
		if take <= 0 {
			break
		}
		fills = append(fills, fill{o, take})
		want -= take
	}
	return s.executeEvent(opposite(ice.side), fills)
}

// endIceberg retires the iceberg, pulling its displayed slices if pull.
func (s *Simulator) endIceberg(pull bool) []itch.Message {
	ice := s.ice
	s.ice = nil
	if ice == nil || !pull {
		return nil
	}
	var msgs []itch.Message
	for _, id := range ice.children {
		if s.book.RemoveOrder(id) != nil {
			msgs = append(msgs, itch.Message{Type: itch.MsgOrderDelete, StockLocate: s.locateCode, OrderRef: id})
		}
	}
	return msgs
}

// --- fleeting ---------------------------------------------------------------

// doFleeting adds a short burst of orders 1-3 ticks from mid and deletes every
// one of them within the same batch.
func (s *Simulator) doFleeting(currentPrice float64) []itch.Message {
	n := s.rng.IntRange(fleetingMinOrders, fleetingMaxOrders)
	side := s.randomSide()
	var msgs []itch.Message
	var pending []uint64
	deleteOne := func(i int) {
		id := pending[i]
		pending = append(pending[:i], pending[i+1:]...)
		if s.book.RemoveOrder(id) != nil { // may already be trimmed off
			msgs = append(msgs, itch.Message{Type: itch.MsgOrderDelete, StockLocate: s.locateCode, OrderRef: id})
		}
	}
	for i := 0; i < n; i++ {
		offset := float64(s.rng.IntRange(1, 3)) * s.tickSize
		price := currentPrice - offset
		if side == SideSell {
			price = currentPrice + offset
		}
		price = snapPrice(price, s.tickSize)
		if price < s.tickSize {
			price = s.tickSize
		}
		o := &Order{ID: NextOrderID(), Locate: s.locateCode, Side: side, Price: price,
			Shares: int32(s.rng.IntRange(1, 5)) * 100}
		msgs = append(msgs, s.addMsgs(o, s.book.AddOrder(o))...)
		if s.book.GetOrder(o.ID) != nil {
			pending = append(pending, o.ID)
		}
		if len(pending) > 0 && s.rng.Float64() < 0.5 {
			deleteOne(s.rng.Intn(len(pending)))
		}
	}
	for len(pending) > 0 {
		deleteOne(0)
	}
	return msgs
}

// --- large resting orders ---------------------------------------------------

// startLarge joins the touch of a random side with one large order.
func (s *Simulator) startLarge() []itch.Message {
	side := s.randomSide()
	price := s.best(side)
	if price == 0 {
		return nil
	}
	o := &Order{ID: NextOrderID(), Locate: s.locateCode, Side: side, Price: price,
		Shares: int32(s.rng.IntRange(largeMinLots, largeMaxLots)) * 100}
	if s.rng.Float64() < 0.3 {
		o.MPID = mpids[s.rng.Intn(len(mpids))]
	}
	msgs := s.addMsgs(o, s.book.AddOrder(o))
	if s.book.GetOrder(o.ID) == nil {
		return msgs
	}
	s.large = &largeOrder{
		id:       o.ID,
		side:     side,
		lifetime: s.rng.IntRange(largeMinAge, largeMaxAge),
		fill:     s.rng.Float64() < largeFillProb,
	}
	return msgs
}

// largeStep decides the large order's fate once its lifetime is up: pull it,
// or trade it away in aggressive chunks that take everything ahead of it.
func (s *Simulator) largeStep() []itch.Message {
	lg := s.large
	o := s.book.GetOrder(lg.id)
	if o == nil { // traded away or trimmed off by other activity
		s.large = nil
		return nil
	}
	lg.age++
	if lg.age < lg.lifetime {
		return nil
	}
	if !lg.fill || lg.filling >= largeFillBudget {
		s.large = nil
		s.book.RemoveOrder(lg.id)
		return []itch.Message{{Type: itch.MsgOrderDelete, StockLocate: s.locateCode, OrderRef: lg.id}}
	}
	lg.filling++
	if !s.roll(largeChunkProb) {
		return nil
	}
	if s.ice != nil && s.ice.side == lg.side {
		return nil // never trade through a running iceberg
	}
	lv := s.book.levelOrders(lg.side, largeMaxDepth)
	want := int32(s.rng.IntRange(largeChunkMin, largeChunkMax)) * 100
	var fills []fill
	reached := false
	for _, l := range lv {
		for _, ro := range l.Orders {
			if want <= 0 {
				break
			}
			take := ro.Shares
			if take > want {
				take = want
			}
			fills = append(fills, fill{ro, take})
			want -= take
			if ro.ID == lg.id {
				reached = true
			}
		}
		if reached || want <= 0 {
			break
		}
	}
	if !reached && !priceIn(lv, o.Price) {
		return nil // deeper than largeMaxDepth: wait for the book to come to it
	}
	msgs := s.executeEvent(opposite(lg.side), fills)
	if s.book.GetOrder(lg.id) == nil {
		s.large = nil
	}
	return msgs
}

func priceIn(lv []PriceLevel, price float64) bool {
	for _, l := range lv {
		if l.Price == price {
			return true
		}
	}
	return false
}
