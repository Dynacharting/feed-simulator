package itch

import "testing"

func TestClockDistinctWithinBatch(t *testing.T) {
	// A frozen wall clock: every message must still get its own, increasing ts.
	c := newClockFrom(func() int64 { return 1000 })
	msgs := make([]Message, 5)
	c.Stamp(msgs)
	for i := 1; i < len(msgs); i++ {
		if msgs[i].Timestamp <= msgs[i-1].Timestamp {
			t.Fatalf("msg %d ts %d not > msg %d ts %d", i, msgs[i].Timestamp, i-1, msgs[i-1].Timestamp)
		}
	}
	if msgs[0].Timestamp != 1000 {
		t.Fatalf("first ts = %d, want wall clock 1000", msgs[0].Timestamp)
	}
}

func TestClockExecGroupSharesTimestamp(t *testing.T) {
	c := newClockFrom(func() int64 { return 1000 })
	msgs := []Message{
		{Type: MsgAddOrder},
		{Type: MsgOrderExecuted, ExecGroup: 7},
		{Type: MsgOrderExecuted, ExecGroup: 7},
		{Type: MsgOrderExecuted, ExecGroup: 7},
		{Type: MsgOrderExecuted, ExecGroup: 8},
		{Type: MsgOrderDelete},
	}
	c.Stamp(msgs)
	if msgs[1].Timestamp != msgs[2].Timestamp || msgs[2].Timestamp != msgs[3].Timestamp {
		t.Fatalf("one aggressor's executions differ: %d %d %d", msgs[1].Timestamp, msgs[2].Timestamp, msgs[3].Timestamp)
	}
	if msgs[1].Timestamp <= msgs[0].Timestamp {
		t.Fatal("group ts not after preceding message")
	}
	if msgs[4].Timestamp <= msgs[3].Timestamp {
		t.Fatal("a different aggressor must get a new timestamp")
	}
	if msgs[5].Timestamp <= msgs[4].Timestamp {
		t.Fatal("message after a group must get a new timestamp")
	}
}

func TestClockMonotoneAcrossBatchesAndBackwardJitter(t *testing.T) {
	now := int64(5000)
	c := newClockFrom(func() int64 { return now })
	a := make([]Message, 3)
	c.Stamp(a)
	now = 4000 // wall clock steps back a little
	b := make([]Message, 2)
	c.Stamp(b)
	if b[0].Timestamp <= a[2].Timestamp {
		t.Fatalf("batch 2 ts %d not after batch 1 ts %d", b[0].Timestamp, a[2].Timestamp)
	}
	now = 9000 // wall clock moves ahead: follow it
	d := make([]Message, 1)
	c.Stamp(d)
	if d[0].Timestamp != 9000 {
		t.Fatalf("ts = %d, want wall clock 9000", d[0].Timestamp)
	}
}

func TestClockMidnightWrap(t *testing.T) {
	now := int64(86_399_000_000_000) // 23:59:59
	c := newClockFrom(func() int64 { return now })
	a := make([]Message, 1)
	c.Stamp(a)
	now = 1_000 // just past midnight
	b := make([]Message, 1)
	c.Stamp(b)
	if b[0].Timestamp != 1_000 {
		t.Fatalf("after midnight ts = %d, want 1000 (new day restarts)", b[0].Timestamp)
	}
}
