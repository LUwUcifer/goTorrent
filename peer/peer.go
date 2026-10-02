package peer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	KeepAliveInterval = 2 * time.Minute

	WriteTimeout = 30 * time.Second

	DefaultSendQueue = 128

	rateWindow = 20
)

var (
	// ErrProtocol means the remote peer broke the wire protocol. Drop it.
	ErrProtocol = errors.New("peer: protocol violation")

	// ErrClosed is returned by senders once the peer's Run loop has exited.
	ErrClosed = errors.New("peer: connection closed")

	// ErrQueueFull is returned by TrySend when the outbound queue is full.
	ErrQueueFull = errors.New("peer: send queue full")
)

type EventKind uint8

const (
	EventChoke EventKind = iota + 1
	EventUnchoke
	EventInterested
	EventNotInterested
	EventHave     // Event.Index is set; the peer's bitfield is already updated
	EventBitfield // the peer's bitfield was replaced; read it with Peer.Bitfield
	EventRequest  // Event.Request is set; only delivered while we are unchoking
	EventCancel   // Event.Request is set
	EventPiece    // Event.Piece is set; Block is owned by the receiver
)

type Event struct {
	Peer    *Peer
	Kind    EventKind
	Index   uint32
	Request Request
	Piece   Piece
}

type State struct {
	AmChoking      bool
	AmInterested   bool
	PeerChoking    bool
	PeerInterested bool
}

type Config struct {
	NumPieces int

	Events chan<- Event

	SendQueue int

	KeepAlive time.Duration
}

type Peer struct {
	conn      net.Conn
	remote    Handshake
	numPieces int
	maxMsg    uint32
	keepAlive time.Duration
	idle      time.Duration
	events    chan<- Event

	out       chan Message
	done      chan struct{}
	started   atomic.Bool
	runErr    error // valid once done is closed
	closeOnce sync.Once

	sendMu sync.Mutex

	mu             sync.RWMutex
	amChoking      bool
	amInterested   bool
	peerChoking    bool
	peerInterested bool
	have           Bitfield // the peer's pieces

	down rateMeter
	up   rateMeter
}

func New(conn net.Conn, remote Handshake, cfg Config) (*Peer, error) {
	if cfg.Events == nil {
		return nil, errors.New("peer: Config.Events is required")
	}
	if cfg.NumPieces <= 0 {
		return nil, errors.New("peer: Config.NumPieces must be positive")
	}
	queue := cfg.SendQueue
	if queue <= 0 {
		queue = DefaultSendQueue
	}
	ka := cfg.KeepAlive
	if ka <= 0 {
		ka = KeepAliveInterval
	}

	return &Peer{
		conn:        conn,
		remote:      remote,
		numPieces:   cfg.NumPieces,
		maxMsg:      MaxMessageLen(cfg.NumPieces),
		keepAlive:   ka,
		idle:        2*ka + 30*time.Second,
		events:      cfg.Events,
		out:         make(chan Message, queue),
		done:        make(chan struct{}),
		amChoking:   true,
		peerChoking: true,
		have:        NewBitfield(cfg.NumPieces),
	}, nil
}

func (p *Peer) Run(ctx context.Context) (err error) {
	if !p.started.CompareAndSwap(false, true) {
		return errors.New("peer: already running")
	}
	defer func() {
		p.runErr = err
		close(p.done)
	}()

	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errc := make(chan error, 2)
	go func() { errc <- p.readLoop(ctx) }()
	go func() { errc <- p.writeLoop(ctx) }()

	// Whichever goroutine dies first takes the other down with it.
	err = <-errc
	cancel()
	_ = p.conn.Close() // unblocks a Read stuck in the reader
	<-errc

	if perr := parent.Err(); perr != nil {
		err = perr
	}
	return err
}

func (p *Peer) Close() error {
	var err error
	p.closeOnce.Do(func() { err = p.conn.Close() })
	return err
}

func (p *Peer) Done() <-chan struct{} { return p.done }

func (p *Peer) Err() error { return p.runErr }

func (p *Peer) readLoop(ctx context.Context) error {
	first := true
	for {
		if err := p.conn.SetReadDeadline(time.Now().Add(p.idle)); err != nil {
			return err
		}
		m, err := ReadMessage(p.conn, p.maxMsg)
		if err != nil {
			return fmt.Errorf("reading message: %w", err)
		}
		if m.KeepAlive {
			continue
		}
		if err := p.handle(ctx, m, first); err != nil {
			return err
		}
		first = false
	}
}

func (p *Peer) handle(ctx context.Context, m Message, first bool) error {
	if err := m.Validate(); err != nil {
		return fmt.Errorf("%w: %w", ErrProtocol, err)
	}

	switch m.ID {
	case MsgChoke:
		p.mu.Lock()
		p.peerChoking = true
		p.mu.Unlock()
		return p.emit(ctx, Event{Kind: EventChoke})

	case MsgUnchoke:
		p.mu.Lock()
		p.peerChoking = false
		p.mu.Unlock()
		return p.emit(ctx, Event{Kind: EventUnchoke})

	case MsgInterested:
		p.mu.Lock()
		p.peerInterested = true
		p.mu.Unlock()
		return p.emit(ctx, Event{Kind: EventInterested})

	case MsgNotInterested:
		p.mu.Lock()
		p.peerInterested = false
		p.mu.Unlock()
		return p.emit(ctx, Event{Kind: EventNotInterested})

	case MsgHave:
		idx, err := m.ParseHave()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrProtocol, err)
		}
		if uint64(idx) >= uint64(p.numPieces) {
			return fmt.Errorf("%w: have %d out of range (%d pieces)", ErrProtocol, idx, p.numPieces)
		}
		p.mu.Lock()
		p.have.Set(int(idx))
		p.mu.Unlock()
		return p.emit(ctx, Event{Kind: EventHave, Index: idx})

	case MsgBitfield:
		if !first {
			return fmt.Errorf("%w: bitfield is only allowed as the first message", ErrProtocol)
		}
		bf, err := ParseBitfield(m.Payload, p.numPieces)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrProtocol, err)
		}
		p.mu.Lock()
		p.have = bf
		p.mu.Unlock()
		return p.emit(ctx, Event{Kind: EventBitfield})

	case MsgRequest:
		r, err := m.ParseRequest()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrProtocol, err)
		}
		if err := p.checkRequest(r); err != nil {
			return err
		}
		p.mu.RLock()
		choking := p.amChoking
		p.mu.RUnlock()
		if choking {
			return nil
		}
		return p.emit(ctx, Event{Kind: EventRequest, Request: r})

	case MsgCancel:
		r, err := m.ParseRequest()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrProtocol, err)
		}
		if err := p.checkRequest(r); err != nil {
			return err
		}
		return p.emit(ctx, Event{Kind: EventCancel, Request: r})

	case MsgPiece:
		pc, err := m.ParsePiece()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrProtocol, err)
		}
		if uint64(pc.Index) >= uint64(p.numPieces) {
			return fmt.Errorf("%w: piece %d out of range (%d pieces)", ErrProtocol, pc.Index, p.numPieces)
		}
		if len(pc.Block) > MaxBlockSize {
			return fmt.Errorf("%w: block of %d bytes exceeds %d", ErrProtocol, len(pc.Block), MaxBlockSize)
		}
		p.down.Add(len(pc.Block))
		return p.emit(ctx, Event{Kind: EventPiece, Piece: pc})

	default:
		return nil
	}
}

func (p *Peer) checkRequest(r Request) error {
	switch {
	case uint64(r.Index) >= uint64(p.numPieces):
		return fmt.Errorf("%w: request for piece %d out of range (%d pieces)", ErrProtocol, r.Index, p.numPieces)
	case r.Length == 0 || r.Length > MaxBlockSize:
		return fmt.Errorf("%w: request length %d outside 1-%d", ErrProtocol, r.Length, MaxBlockSize)
	}
	return nil
}

func (p *Peer) emit(ctx context.Context, ev Event) error {
	ev.Peer = p
	select {
	case p.events <- ev:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Peer) writeLoop(ctx context.Context) error {
	idle := time.NewTimer(p.keepAlive)
	defer idle.Stop()

	for {
		var m Message
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m = <-p.out:
		case <-idle.C:
			m = NewKeepAlive()
		}

		if err := p.conn.SetWriteDeadline(time.Now().Add(WriteTimeout)); err != nil {
			return err
		}
		if err := WriteMessage(p.conn, m); err != nil {
			return fmt.Errorf("writing %v: %w", m, err)
		}
		if m.Is(MsgPiece) {
			p.up.Add(len(m.Payload) - 8)
		}
		idle.Reset(p.keepAlive)
	}
}

func (p *Peer) Send(ctx context.Context, m Message) error {
	if err := checkSend(m); err != nil {
		return err
	}
	return p.enqueue(ctx, m)
}

// TrySend is Send without blocking: if the queue is full it returns
// ErrQueueFull and queues nothing. Use it from a loop that must not stall
// behind one slow peer.
func (p *Peer) TrySend(m Message) error {
	if err := checkSend(m); err != nil {
		return err
	}
	select {
	case <-p.done:
		return ErrClosed
	default:
	}
	select {
	case p.out <- m:
		return nil
	default:
		return ErrQueueFull
	}
}

// checkSend rejects messages that must go through SetChoking/SetInterested
// and messages with malformed payloads.
func checkSend(m Message) error {
	if !m.KeepAlive {
		switch m.ID {
		case MsgChoke, MsgUnchoke, MsgInterested, MsgNotInterested:
			return fmt.Errorf("peer: send %v via SetChoking/SetInterested", m.ID)
		}
	}
	return m.Validate()
}

func (p *Peer) enqueue(ctx context.Context, m Message) error {
	select {
	case <-p.done:
		return ErrClosed
	default:
	}
	select {
	case p.out <- m:
		return nil
	case <-p.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Peer) SetChoking(ctx context.Context, choke bool) error {
	return p.setFlag(ctx, &p.amChoking, choke, NewChoke(), NewUnchoke())
}

func (p *Peer) SetInterested(ctx context.Context, interested bool) error {
	return p.setFlag(ctx, &p.amInterested, interested, NewInterested(), NewNotInterested())
}

func (p *Peer) setFlag(ctx context.Context, flag *bool, v bool, on, off Message) error {
	p.sendMu.Lock()
	defer p.sendMu.Unlock()

	p.mu.Lock()
	if *flag == v {
		p.mu.Unlock()
		return nil
	}
	*flag = v
	p.mu.Unlock()

	if v {
		return p.enqueue(ctx, on)
	}
	return p.enqueue(ctx, off)
}

func (p *Peer) State() State {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return State{
		AmChoking:      p.amChoking,
		AmInterested:   p.amInterested,
		PeerChoking:    p.peerChoking,
		PeerInterested: p.peerInterested,
	}
}

func (p *Peer) Bitfield() Bitfield {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return Bitfield{bits: append([]byte(nil), p.have.bits...), n: p.have.n}
}

func (p *Peer) Has(i int) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.have.Has(i)
}

func (p *Peer) ID() [20]byte          { return p.remote.PeerID }
func (p *Peer) Remote() Handshake     { return p.remote }
func (p *Peer) RemoteAddr() net.Addr  { return p.conn.RemoteAddr() }
func (p *Peer) DownloadRate() float64 { return p.down.Rate() } // bytes/s of piece data, ~20 s average
func (p *Peer) UploadRate() float64   { return p.up.Rate() }
func (p *Peer) Downloaded() int64     { return p.down.Total() }
func (p *Peer) Uploaded() int64       { return p.up.Total() }

type rateMeter struct {
	mu      sync.Mutex
	total   int64
	buckets [rateWindow]int64
	first   int64
	last    int64
	started bool
}

func (r *rateMeter) advance(sec int64) {
	if !r.started {
		r.started, r.first, r.last = true, sec, sec
		return
	}
	if sec <= r.last {
		return
	}
	if sec-r.last >= rateWindow {
		r.buckets = [rateWindow]int64{}
	} else {
		for s := r.last + 1; s <= sec; s++ {
			r.buckets[s%rateWindow] = 0
		}
	}
	r.last = sec
}

func (r *rateMeter) Add(n int) {
	if n <= 0 {
		return
	}
	sec := time.Now().Unix()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.advance(sec)
	r.buckets[r.last%rateWindow] += int64(n)
	r.total += int64(n)
}

func (r *rateMeter) Rate() float64 {
	sec := time.Now().Unix()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.started {
		return 0
	}
	r.advance(sec)
	var sum int64
	for _, b := range r.buckets {
		sum += b
	}
	span := min(int64(rateWindow), r.last-r.first+1)
	return float64(sum) / float64(span)
}

func (r *rateMeter) Total() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.total
}
