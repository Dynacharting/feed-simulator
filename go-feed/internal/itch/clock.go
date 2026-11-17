package itch

import "sync"

// Clock assigns per-message ITCH timestamps (nanoseconds since midnight UTC).
//
// Every message gets its own timestamp, strictly greater than the previous
// one stamped by this Clock — except that adjacent messages sharing a non-zero
// ExecGroup (the executions of one aggressive order) share one timestamp, as
// a real matching engine reports them. The sequence is therefore monotone
// non-decreasing for everything stamped by one Clock.
//
// Clock is safe for concurrent use, but monotonicity on a *stream* also needs
// the caller to deliver messages in stamping order (Manager holds one lock
// across stamp and fan-out for exactly that reason).
type Clock struct {
	mu   sync.Mutex
	last int64
	now  func() int64
}

// dayRollback is how far the wall clock must fall behind the last stamp before
// the Clock treats it as a new trading day (the midnight wrap of
// NanosFromMidnight) rather than as clock jitter to be absorbed.
const dayRollback = 12 * 3600 * int64(1e9)

// NewClock returns a Clock reading NanosFromMidnight.
func NewClock() *Clock { return &Clock{now: NanosFromMidnight} }

// newClockFrom returns a Clock reading now (tests).
func newClockFrom(now func() int64) *Clock { return &Clock{now: now} }

// Stamp sets Timestamp on every message in msgs, in order.
func (c *Clock) Stamp(msgs []Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range msgs {
		if i > 0 && msgs[i].ExecGroup != 0 && msgs[i].ExecGroup == msgs[i-1].ExecGroup {
			msgs[i].Timestamp = msgs[i-1].Timestamp
			continue
		}
		c.last = c.next()
		msgs[i].Timestamp = c.last
	}
}

func (c *Clock) next() int64 {
	t := c.now()
	if t > c.last {
		return t
	}
	if c.last-t > dayRollback {
		return t // new day: the midnight wrap restarts the sequence
	}
	return c.last + 1
}
