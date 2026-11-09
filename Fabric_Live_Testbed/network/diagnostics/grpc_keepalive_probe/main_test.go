package main

import (
	"testing"
	"time"
)

func ev(name string, sec float64) Event { return Event{Ev: name, MonoNs: int64(sec * 1e9)} }

func TestFrameParserSplitsFramesAcrossWrites(t *testing.T) {
	var got []byte
	p := &frameParser{prefaceLeft: 3, emit: func(typ, flags byte, sid uint32, pl []byte) { got = append(got, typ) }}
	ping := []byte{0, 0, 8, framePing, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}
	goaway := []byte{0, 0, 8, frameGoAway, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 11}
	stream := append(append([]byte("PRI"), ping...), goaway...)
	for _, b := range stream { // one byte at a time
		p.feed([]byte{b})
	}
	if len(got) != 2 || got[0] != framePing || got[1] != frameGoAway {
		t.Fatalf("frames %v", got)
	}
}

// Positive: three pings each just under MinTime after the previous one, no server writes -> GOAWAY at the
// third strike (the first ping after the BDP ping is the first strike).
func TestStrikeReplayPredictsGoAwayOnThirdStrike(t *testing.T) {
	es := []Event{ev("srv_tx_data_or_headers", 1.5), ev("srv_rx_ping", 1.6), // BDP ping after data: reset
		ev("srv_rx_ping", 61.0), ev("srv_rx_ping", 120.4), ev("srv_rx_ping", 179.8)}
	r := StrikeReplay(es, time.Minute, 2)
	if len(r.Strikes) != 3 || r.PredictedAt != int64(179.8e9) {
		t.Fatalf("%+v", r)
	}
}

// Negative: the same pings at or after MinTime -> no strike, no GOAWAY.
func TestStrikeReplayNoGoAwayWhenIntervalsRespectMinTime(t *testing.T) {
	es := []Event{ev("srv_tx_data_or_headers", 1.5), ev("srv_rx_ping", 1.6),
		ev("srv_rx_ping", 61.7), ev("srv_rx_ping", 121.8), ev("srv_rx_ping", 181.9), ev("srv_rx_ping", 242.0)}
	r := StrikeReplay(es, time.Minute, 2)
	if len(r.Strikes) != 0 || r.PredictedAt != 0 {
		t.Fatalf("%+v", r)
	}
}

// Negative: early pings, but a server write before each one resets the count (peer chaincode.keepalive > 0).
func TestStrikeReplayServerWritesResetStrikes(t *testing.T) {
	es := []Event{ev("srv_rx_ping", 1.6)}
	for i := 1; i <= 5; i++ {
		s := 1.6 + float64(i)*59.0
		es = append(es, ev("srv_tx_data_or_headers", s-1), ev("srv_rx_ping", s))
	}
	if r := StrikeReplay(es, time.Minute, 2); r.PredictedAt != 0 {
		t.Fatalf("%+v", r)
	}
}

// The first ping is compared with the zero time and never strikes.
func TestStrikeReplayFirstPingNeverStrikes(t *testing.T) {
	if r := StrikeReplay([]Event{ev("srv_rx_ping", 0.5)}, time.Minute, 2); len(r.Strikes) != 0 {
		t.Fatalf("%+v", r)
	}
}
