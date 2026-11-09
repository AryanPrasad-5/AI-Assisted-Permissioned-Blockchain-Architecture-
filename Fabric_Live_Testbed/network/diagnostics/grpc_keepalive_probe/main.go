// Command grpc_keepalive_probe reproduces E0-0 blocker F2 (chaincode containers closed by the peer with
// GOAWAY ENHANCE_YOUR_CALM "too_many_pings") offline, without Docker or Fabric (E0-0 remediation,
// 2026-11-09).
//
// One process runs, over loopback TCP:
//   - a gRPC server with the keepalive policy Fabric v2.5.16 hard-codes for the peer's chaincode server
//     (internal/peer/node/start.go:1074-1080 via internal/pkg/comm/config.go:258-272):
//     Time 2h, Timeout 20s, MinTime 1m, PermitWithoutStream true;
//   - a client with the keepalive parameters fabric-chaincode-go/v2 v2.0.0 hard-codes
//     (shim/internal/config.go:37-40): Time 1m, Timeout 20s, PermitWithoutStream true;
//   - the ChaincodeSupport.Register bidi stream (fabric-protos-go-apiv2 v0.3.4): REGISTER, REGISTERED,
//     one TRANSACTION/COMPLETED exchange at -tx-at, then idle, as the peer and chaincode were after the
//     one-transaction run (core.yaml chaincode.keepalive: 0, so the idle peer writes nothing).
//
// gRPC is the chaincode's own version (google.golang.org/grpc v1.83.2); handlePing is byte-identical in
// the peer's v1.79.3. The server side of the connection is wrapped: every HTTP/2 frame read or written is
// logged with wall-clock and monotonic time. A sampler logs wall-clock steps (wall minus monotonic
// changing by more than 1 ms). The log is replayed through the server's ping-strike rule
// (http2_server.go handlePing) to predict GOAWAY, and the prediction is compared with the GOAWAY actually
// written.
//
// It sets nothing on the host. Output: JSON lines (-out) and a summary on stdout.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const (
	frameData    = 0x0
	frameHeaders = 0x1
	framePing    = 0x6
	frameGoAway  = 0x7
	prefaceLen   = 24
)

var bdpPayload = [8]byte{2, 4, 16, 16, 9, 14, 7, 7} // grpc internal/transport/bdp_estimator.go:47

// Event is one log line.
type Event struct {
	WallNs int64  `json:"wall_ns"`
	MonoNs int64  `json:"mono_ns"` // monotonic ns since probe start
	Ev     string `json:"ev"`
	Detail string `json:"detail,omitempty"`
	Len    int    `json:"len,omitempty"`
	StepNs int64  `json:"step_ns,omitempty"`
}

type logger struct {
	mu     sync.Mutex
	start  time.Time
	events []Event
	enc    *json.Encoder
}

func (l *logger) add(ev, detail string, n int, step int64) {
	now := time.Now()
	e := Event{WallNs: now.UnixNano(), MonoNs: int64(now.Sub(l.start)), Ev: ev, Detail: detail, Len: n, StepNs: step}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
	if l.enc != nil {
		_ = l.enc.Encode(e)
	}
}

// frameParser splits one direction of an HTTP/2 byte stream into frames.
type frameParser struct {
	buf         []byte
	prefaceLeft int
	emit        func(typ, flags byte, stream uint32, payload []byte)
}

func (f *frameParser) feed(p []byte) {
	f.buf = append(f.buf, p...)
	if f.prefaceLeft > 0 {
		n := min(f.prefaceLeft, len(f.buf))
		f.buf = f.buf[n:]
		f.prefaceLeft -= n
	}
	for len(f.buf) >= 9 {
		l := int(f.buf[0])<<16 | int(f.buf[1])<<8 | int(f.buf[2])
		if len(f.buf) < 9+l {
			return
		}
		f.emit(f.buf[3], f.buf[4], binary.BigEndian.Uint32(f.buf[5:9])&0x7fffffff, f.buf[9:9+l])
		f.buf = f.buf[9+l:]
	}
}

type tapConn struct {
	net.Conn
	rx, tx *frameParser
}

func (c *tapConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.rx.feed(p[:n])
	}
	return n, err
}

func (c *tapConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.tx.feed(p[:n])
	}
	return n, err
}

type tapListener struct {
	net.Listener
	log *logger
}

func (l *tapListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	lg := l.log
	rx := &frameParser{prefaceLeft: prefaceLen, emit: func(typ, flags byte, sid uint32, pl []byte) {
		switch typ {
		case framePing:
			if flags&1 == 0 {
				kind := "keepalive"
				if len(pl) == 8 && [8]byte(pl) == bdpPayload {
					kind = "bdp"
				}
				lg.add("srv_rx_ping", kind, len(pl), 0)
			}
		case frameData:
			lg.add("srv_rx_data", fmt.Sprintf("stream %d", sid), len(pl), 0)
		}
	}}
	tx := &frameParser{emit: func(typ, flags byte, sid uint32, pl []byte) {
		switch typ {
		case frameData, frameHeaders:
			lg.add("srv_tx_data_or_headers", fmt.Sprintf("type %d stream %d", typ, sid), len(pl), 0)
		case framePing:
			if flags&1 == 1 {
				lg.add("srv_tx_ping_ack", "", len(pl), 0)
			}
		case frameGoAway:
			code, debug := uint32(0), ""
			if len(pl) >= 8 {
				code = binary.BigEndian.Uint32(pl[4:8])
				debug = string(pl[8:])
			}
			lg.add("srv_tx_goaway", fmt.Sprintf("code %d debug %q", code, debug), len(pl), 0)
		}
	}}
	return &tapConn{Conn: c, rx: rx, tx: tx}, nil
}

type ccServer struct {
	peer.UnimplementedChaincodeSupportServer
	log       *logger
	txAt      time.Duration
	keepalive time.Duration // emulates the peer's chaincode.keepalive (core/chaincode/handler.go:399-404,450-454)
}

func (s *ccServer) Register(st grpc.BidiStreamingServer[peer.ChaincodeMessage, peer.ChaincodeMessage]) error {
	var sendMu sync.Mutex // the peer serialises sends (serialSendAsync)
	send := func(m *peer.ChaincodeMessage) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return st.Send(m)
	}
	if _, err := st.Recv(); err != nil {
		return err
	}
	if err := send(&peer.ChaincodeMessage{Type: peer.ChaincodeMessage_REGISTERED}); err != nil {
		return err
	}
	go func() {
		time.Sleep(s.txAt)
		s.log.add("srv_send_transaction", "", 0, 0)
		_ = send(&peer.ChaincodeMessage{Type: peer.ChaincodeMessage_TRANSACTION, Txid: "probe-tx"})
	}()
	if s.keepalive > 0 {
		go func() {
			t := time.NewTicker(s.keepalive)
			defer t.Stop()
			for {
				select {
				case <-st.Context().Done():
					return
				case <-t.C:
					s.log.add("srv_send_keepalive_msg", "", 0, 0)
					_ = send(&peer.ChaincodeMessage{Type: peer.ChaincodeMessage_KEEPALIVE})
				}
			}
		}()
	}
	for {
		m, err := st.Recv()
		if err != nil {
			return err
		}
		s.log.add("srv_recv_msg", m.Type.String(), 0, 0)
	}
}

// StrikeReplay applies grpc http2_server.go handlePing to the logged events: a ping after a server
// DATA/HEADERS write resets the count and is not checked; otherwise a ping less than minTime after the
// previous ping (any kind) is a strike; GOAWAY is due when strikes exceed maxStrikes (2). The first
// ping is compared with the zero time, so it can never strike.
type StrikeResult struct {
	Pings          int     `json:"pings"`
	Strikes        []int64 `json:"strike_mono_ns"`
	PredictedAt    int64   `json:"predicted_goaway_mono_ns"` // 0 = no GOAWAY predicted
	ShortIntervals []int64 `json:"short_interval_ns"`
}

func StrikeReplay(events []Event, minTime time.Duration, maxStrikes int) StrikeResult {
	var r StrikeResult
	var lastPing int64 = -1
	reset, strikes := false, 0
	for _, e := range events {
		switch e.Ev {
		case "srv_tx_data_or_headers":
			reset = true
		case "srv_rx_ping":
			r.Pings++
			prev := lastPing
			lastPing = e.MonoNs
			if reset {
				reset, strikes = false, 0
				continue
			}
			if prev >= 0 && e.MonoNs-prev < int64(minTime) {
				strikes++
				r.Strikes = append(r.Strikes, e.MonoNs)
				r.ShortIntervals = append(r.ShortIntervals, e.MonoNs-prev)
			}
			if strikes > maxStrikes && r.PredictedAt == 0 {
				r.PredictedAt = e.MonoNs
			}
		}
	}
	return r
}

func main() {
	dur := flag.Duration("duration", 7*time.Minute, "how long to keep the connection idle")
	clientTime := flag.Duration("client-time", time.Minute, "client keepalive Time (shim: 1m)")
	minTime := flag.Duration("min-time", time.Minute, "server EnforcementPolicy MinTime (Fabric chaincode server: 1m)")
	txAt := flag.Duration("tx-at", 1500*time.Millisecond, "when the server sends the one TRANSACTION")
	peerKA := flag.Duration("peer-keepalive", 0, "emulate the peer's chaincode.keepalive: send a KEEPALIVE message "+
		"this often (0 = off, as core.yaml chaincode.keepalive: 0)")
	out := flag.String("out", "", "JSON lines output file (new)")
	flag.Parse()

	lg := &logger{start: time.Now()}
	if *out != "" {
		fh, err := os.OpenFile(*out, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		defer fh.Close()
		lg.enc = json.NewEncoder(fh)
	}
	lg.add("start", fmt.Sprintf("goos %s go %s client_time %s min_time %s peer_keepalive %s", runtime.GOOS,
		runtime.Version(), *clientTime, *minTime, *peerKA), 0, 0)

	stop := make(chan struct{})
	go func() { // wall-clock step sampler
		prev := int64(0)
		for first := true; ; first = false {
			now := time.Now()
			d := now.UnixNano() - lg.start.UnixNano() - int64(now.Sub(lg.start))
			if !first && (d-prev > 1_000_000 || prev-d > 1_000_000) {
				lg.add("wall_step", "", 0, d-prev)
			}
			prev = d
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	srv := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 2 * time.Hour, Timeout: 20 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: *minTime, PermitWithoutStream: true}),
	)
	peer.RegisterChaincodeSupportServer(srv, &ccServer{log: lg, txAt: *txAt, keepalive: *peerKA})
	go func() { _ = srv.Serve(&tapListener{Listener: ln, log: lg}) }()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: *clientTime, Timeout: 20 * time.Second, PermitWithoutStream: true}))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()
	st, err := peer.NewChaincodeSupportClient(conn).Register(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	_ = st.Send(&peer.ChaincodeMessage{Type: peer.ChaincodeMessage_REGISTER})
	clientErr := ""
	for {
		m, err := st.Recv()
		if err != nil {
			if ctx.Err() == nil {
				clientErr = err.Error()
				lg.add("client_stream_error", clientErr, 0, 0)
			}
			break
		}
		lg.add("client_recv_msg", m.Type.String(), 0, 0)
		switch m.Type {
		case peer.ChaincodeMessage_TRANSACTION:
			_ = st.Send(&peer.ChaincodeMessage{Type: peer.ChaincodeMessage_COMPLETED, Txid: m.Txid})
		case peer.ChaincodeMessage_KEEPALIVE: // the shim echoes KEEPALIVE (shim/handler.go:673-676)
			_ = st.Send(m)
		}
	}
	lg.add("end", "", 0, 0)
	close(stop)
	_ = conn.Close()
	srv.Stop()

	lg.mu.Lock()
	events := append([]Event(nil), lg.events...)
	lg.mu.Unlock()
	var goawayAt int64
	steps, stepSum := 0, int64(0)
	fmt.Println("mono_s    wall_step_ms  event")
	for _, e := range events {
		switch e.Ev {
		case "srv_rx_ping", "srv_tx_goaway", "client_stream_error", "srv_send_transaction":
			fmt.Printf("%8.3f  %12s  %s %s\n", float64(e.MonoNs)/1e9, "", e.Ev, e.Detail)
		case "wall_step":
			steps++
			stepSum += e.StepNs
			fmt.Printf("%8.3f  %+12.3f  wall_step\n", float64(e.MonoNs)/1e9, float64(e.StepNs)/1e6)
		}
		if e.Ev == "srv_tx_goaway" && goawayAt == 0 {
			goawayAt = e.MonoNs
		}
	}
	r := StrikeReplay(events, *minTime, 2)
	keepalives := 0
	for _, e := range events {
		if e.Ev == "srv_rx_ping" && e.Detail == "keepalive" {
			keepalives++
		}
	}
	sum := map[string]any{"goos": runtime.GOOS, "duration_requested_s": dur.Seconds(), "wall_steps": steps,
		"peer_keepalive_s": peerKA.Seconds(), "client_keepalive_pings": keepalives,
		"wall_step_total_ms": float64(stepSum) / 1e6, "pings_received": r.Pings, "strikes_replayed": len(r.Strikes),
		"short_intervals_s": r.ShortIntervals, "goaway_predicted_mono_s": float64(r.PredictedAt) / 1e9,
		"goaway_observed_mono_s": float64(goawayAt) / 1e9, "client_error": clientErr,
		"prediction_matches_observation": (r.PredictedAt == 0) == (goawayAt == 0)}
	b, _ := json.MarshalIndent(sum, "", "  ")
	fmt.Println(string(b))
	lg.add("summary", string(b), 0, 0)
}
