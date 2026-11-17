package orderbook

import (
	"math"

	"github.com/ndrandal/feed-simulator/go-feed/internal/engine"
	"github.com/ndrandal/feed-simulator/go-feed/internal/itch"
)

// Action weights for order book simulation.
var actionWeights = []float64{
	0.30, // Add
	0.20, // Cancel/Delete
	0.15, // Update/Replace
	0.15, // Trade/Execute
	0.20, // Replenish
}

const (
	actionAdd       = 0
	actionCancel    = 1
	actionReplace   = 2
	actionTrade     = 3
	actionReplenish = 4
)

// partialCancelProb is the share of cancels (of orders holding two or more
// lots) that cancel only part of the order (X) rather than all of it (D).
const partialCancelProb = 0.35

// Market maker MPIDs for attributed orders.
var mpids = []string{"GSCO", "MSCO", "JPMS", "CITI", "BARK", "SUSQ", "VIRT", "CITD"}

// Simulator drives simulated order book activity for a single symbol.
type Simulator struct {
	rng        *engine.RNG
	book       *Book
	locateCode uint16
	tickSize   float64

	// execGroup numbers aggressive orders so their executions can be stamped
	// with one timestamp (itch.Message.ExecGroup). Local, not persisted: it
	// only has to distinguish adjacent aggressors within one batch.
	execGroup uint64

	// Behaviour regimes (regimes.go). Zero rates = off.
	regimes RegimeConfig
	ice     *iceberg
	large   *largeOrder
}

// NewSimulator creates a new order book simulator.
func NewSimulator(rng *engine.RNG, book *Book, locateCode uint16, tickSize float64) *Simulator {
	return &Simulator{
		rng:        rng,
		book:       book,
		locateCode: locateCode,
		tickSize:   tickSize,
	}
}

// Book returns the underlying order book.
func (s *Simulator) Book() *Book {
	return s.book
}

// Initialize seeds the book with initial orders around a reference price.
// Creates MaxLevels bid and ask levels with OrdersPerLevel orders each.
func (s *Simulator) Initialize(refPrice float64) []itch.Message {
	var msgs []itch.Message

	for level := 0; level < MaxLevels; level++ {
		offset := float64(level+1) * s.tickSize

		bidPrice := snapPrice(refPrice-offset, s.tickSize)
		askPrice := snapPrice(refPrice+offset, s.tickSize)

		for j := 0; j < OrdersPerLevel; j++ {
			shares := int32(s.rng.IntRange(100, 1000))
			shares = (shares / 100) * 100 // round to lots of 100

			// Bid order
			bidOrder := &Order{
				ID:       NextOrderID(),
				Locate:   s.locateCode,
				Side:     SideBuy,
				Price:    bidPrice,
				Shares:   shares,
				Priority: int32(j),
			}
			// Randomly attribute some orders to market makers
			if s.rng.Float64() < 0.3 {
				bidOrder.MPID = mpids[s.rng.Intn(len(mpids))]
			}
			s.book.AddOrder(bidOrder)
			msgs = append(msgs, s.makeAddOrderMsg(bidOrder))

			// Ask order
			askShares := int32(s.rng.IntRange(100, 1000))
			askShares = (askShares / 100) * 100
			askOrder := &Order{
				ID:       NextOrderID(),
				Locate:   s.locateCode,
				Side:     SideSell,
				Price:    askPrice,
				Shares:   askShares,
				Priority: int32(j),
			}
			if s.rng.Float64() < 0.3 {
				askOrder.MPID = mpids[s.rng.Intn(len(mpids))]
			}
			s.book.AddOrder(askOrder)
			msgs = append(msgs, s.makeAddOrderMsg(askOrder))
		}
	}

	return msgs
}

// Step performs one simulated action cycle and returns generated ITCH messages.
// numActions controls how many actions to take (1-3 for normal, more for stress).
func (s *Simulator) Step(currentPrice float64, numActions int) []itch.Message {
	var msgs []itch.Message

	for i := 0; i < numActions; i++ {
		action := s.rng.WeightedPick(actionWeights)
		var actionMsgs []itch.Message

		switch action {
		case actionAdd:
			actionMsgs = s.doAdd(currentPrice)
		case actionCancel:
			actionMsgs = s.doCancel()
		case actionReplace:
			actionMsgs = s.doReplace(currentPrice)
		case actionTrade:
			actionMsgs = s.doTrade()
		case actionReplenish:
			actionMsgs = s.doReplenish(currentPrice)
		}

		msgs = append(msgs, actionMsgs...)
	}

	return append(msgs, s.stepRegimes(currentPrice)...)
}

// doAdd places a new limit order 1-10 ticks from mid.
func (s *Simulator) doAdd(currentPrice float64) []itch.Message {
	side := SideBuy
	if s.rng.Float64() < 0.5 {
		side = SideSell
	}

	offset := float64(s.rng.IntRange(1, 10)) * s.tickSize
	var price float64
	if side == SideBuy {
		price = snapPrice(currentPrice-offset, s.tickSize)
	} else {
		price = snapPrice(currentPrice+offset, s.tickSize)
	}
	if price < s.tickSize {
		price = s.tickSize
	}

	shares := int32(s.rng.IntRange(1, 10)) * 100

	o := &Order{
		ID:     NextOrderID(),
		Locate: s.locateCode,
		Side:   side,
		Price:  price,
		Shares: shares,
	}
	if s.rng.Float64() < 0.2 {
		o.MPID = mpids[s.rng.Intn(len(mpids))]
	}

	evicted := s.book.AddOrder(o)
	return s.addMsgs(o, evicted)
}

// addMsgs builds the wire messages for adding o: an AddOrder for o, followed by
// an OrderDelete for every previously-resting order that the insert pushed past
// MaxLevels. If o's own level was the one trimmed (o never rested), no message is
// emitted for it at all. Keeping deletes on the wire prevents consumers that
// rebuild the full book from leaking the same orphaned orders the book would.
func (s *Simulator) addMsgs(o *Order, evicted []*Order) []itch.Message {
	msgs := make([]itch.Message, 0, 1+len(evicted))
	selfEvicted := false
	for _, e := range evicted {
		if e.ID == o.ID {
			selfEvicted = true
			continue
		}
		msgs = append(msgs, itch.Message{
			Type:        itch.MsgOrderDelete,
			StockLocate: s.locateCode,
			OrderRef:    e.ID,
		})
	}
	if !selfEvicted {
		// Add appears before the eviction it caused.
		msgs = append([]itch.Message{s.makeAddOrderMsg(o)}, msgs...)
	}
	return msgs
}

// doCancel removes a random order from the book.
func (s *Simulator) doCancel() []itch.Message {
	// Pick a random side
	var o *Order
	totalBid := s.book.TotalBidOrders()
	totalAsk := s.book.TotalAskOrders()
	total := totalBid + totalAsk
	if total == 0 {
		return nil
	}

	idx := s.rng.Intn(total)
	if idx < totalBid {
		o = s.book.RandomBidOrder(idx)
	} else {
		o = s.book.RandomAskOrder(idx - totalBid)
	}
	if o == nil || s.isRegimeOrder(o.ID) {
		return nil
	}

	// Partial cancel: reduce a multi-lot order by a whole number of lots,
	// leaving at least one lot resting, and report it as OrderCancel (X) with
	// the cancelled shares. A full cancel stays OrderDelete (D).
	if o.Shares >= 200 && s.rng.Float64() < partialCancelProb {
		cancelled := int32(s.rng.IntRange(1, int(o.Shares/100)-1)) * 100
		s.book.ReduceOrder(o.ID, cancelled)
		return []itch.Message{
			{
				Type:        itch.MsgOrderCancel,
				StockLocate: s.locateCode,
				OrderRef:    o.ID,
				Shares:      cancelled,
			},
		}
	}

	orderID := o.ID
	removed := s.book.RemoveOrder(orderID)
	if removed == nil {
		return nil
	}

	return []itch.Message{
		{
			Type:        itch.MsgOrderDelete,
			StockLocate: s.locateCode,
			OrderRef:    orderID,
		},
	}
}

// doReplace modifies an existing order's price or size.
func (s *Simulator) doReplace(currentPrice float64) []itch.Message {
	totalBid := s.book.TotalBidOrders()
	totalAsk := s.book.TotalAskOrders()
	total := totalBid + totalAsk
	if total == 0 {
		return nil
	}

	idx := s.rng.Intn(total)
	var o *Order
	if idx < totalBid {
		o = s.book.RandomBidOrder(idx)
	} else {
		o = s.book.RandomAskOrder(idx - totalBid)
	}
	if o == nil || s.isRegimeOrder(o.ID) {
		return nil
	}

	oldID := o.ID
	// New price: shift by -2 to +2 ticks
	shift := float64(s.rng.IntRange(-2, 2)) * s.tickSize
	newPrice := snapPrice(o.Price+shift, s.tickSize)
	if newPrice < s.tickSize {
		newPrice = s.tickSize
	}
	newShares := int32(s.rng.IntRange(1, 10)) * 100

	newOrder := s.book.ReplaceOrder(oldID, newPrice, newShares)
	if newOrder == nil {
		return nil
	}

	return []itch.Message{
		{
			Type:         itch.MsgOrderReplace,
			StockLocate:  s.locateCode,
			OrderRef:     newOrder.ID,
			OrigOrderRef: oldID,
			Shares:       newShares,
			Price:        newPrice,
		},
	}
}

// doTrade executes an aggressive order that crosses the spread against the
// first resting order at the touch.
//
// A displayed fill is reported as OrderExecuted (E) alone. ITCH's Trade (P) is
// for executions against *non-displayed* orders, and every order this book
// holds is displayed, so emitting P here would report the same fill twice
// under one match number. The aggressor side, which E does not carry, rides on
// the simulator-internal Aggressor field for the trade log; ExecGroup ties the
// executions of one aggressive order together so they share a timestamp.
func (s *Simulator) doTrade() []itch.Message {
	bestBid := s.book.BestBid()
	bestAsk := s.book.BestAsk()
	if bestBid == 0 || bestAsk == 0 {
		return nil
	}

	// Randomly pick aggressor side: a buy aggressor hits the ask, a sell
	// aggressor hits the bid.
	aggressor := SideSell
	var o *Order
	if s.rng.Float64() < 0.5 {
		aggressor = SideBuy
		o = s.book.RandomAskOrder(0) // best ask, first order
	} else {
		o = s.book.RandomBidOrder(0) // best bid, first order
	}
	if o == nil {
		return nil
	}
	tradeShares := int32(s.rng.IntRange(1, int(o.Shares/100))) * 100
	if tradeShares <= 0 {
		tradeShares = o.Shares
	}

	return s.executeEvent(aggressor, []fill{{o, tradeShares}})
}

// doReplenish adds liquidity at 1-5 ticks from mid.
func (s *Simulator) doReplenish(currentPrice float64) []itch.Message {
	side := SideBuy
	if s.rng.Float64() < 0.5 {
		side = SideSell
	}

	offset := float64(s.rng.IntRange(1, 5)) * s.tickSize
	var price float64
	if side == SideBuy {
		price = snapPrice(currentPrice-offset, s.tickSize)
	} else {
		price = snapPrice(currentPrice+offset, s.tickSize)
	}
	if price < s.tickSize {
		price = s.tickSize
	}

	shares := int32(s.rng.IntRange(2, 10)) * 100

	o := &Order{
		ID:     NextOrderID(),
		Locate: s.locateCode,
		Side:   side,
		Price:  price,
		Shares: shares,
	}
	if s.rng.Float64() < 0.25 {
		o.MPID = mpids[s.rng.Intn(len(mpids))]
	}

	evicted := s.book.AddOrder(o)
	return s.addMsgs(o, evicted)
}

func (s *Simulator) makeAddOrderMsg(o *Order) itch.Message {
	msgType := itch.MsgAddOrder
	if o.MPID != "" {
		msgType = itch.MsgAddOrderMPID
	}
	return itch.Message{
		Type:        msgType,
		StockLocate: s.locateCode,
		OrderRef:    o.ID,
		Side:        byte(o.Side),
		Shares:      o.Shares,
		Price:       o.Price,
		MPID:        o.MPID,
	}
}

func snapPrice(price, tickSize float64) float64 {
	return math.Round(price/tickSize) * tickSize
}
