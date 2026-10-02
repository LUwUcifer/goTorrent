package torrent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"time"

	"goTor/peer"
)

var (
	ErrPoolFull   = errors.New("pool: connection limit reached")
	ErrPoolClosed = errors.New("pool: closed")

	errBanned        = errors.New("pool: address is banned")
	errDuplicatePeer = errors.New("pool: already connected to this peer")
)

type poolConfig struct {
	MaxPeers int

	MaxDialing int

	DialTimeout time.Duration

	MaxKnown int

	RetryBase time.Duration
	RetryMax  time.Duration

	MaxFailures int

	MinStable time.Duration
}

func (c poolConfig) withDefaults() poolConfig {
	if c.MaxPeers <= 0 {
		c.MaxPeers = 40
	}
	if c.MaxDialing <= 0 {
		c.MaxDialing = 10
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.MaxKnown <= 0 {
		c.MaxKnown = 4096
	}
	if c.RetryBase <= 0 {
		c.RetryBase = 30 * time.Second
	}
	if c.RetryMax <= 0 {
		c.RetryMax = 15 * time.Minute
	}
	if c.MaxFailures <= 0 {
		c.MaxFailures = 4
	}
	if c.MinStable <= 0 {
		c.MinStable = 30 * time.Second
	}
	return c
}

func (c poolConfig) retryDelay(failures int) time.Duration {
	shift := min(max(failures-1, 0), 6)
	return min(c.RetryBase<<shift, c.RetryMax)
}

type candState uint8

const (
	candQueued  candState = iota + 1 // in the queue, waiting for a free slot
	candBusy                         // dialling or connected
	candWaiting                      // backing off; a timer will requeue it
	candDead                         // given up on for this session
)

type candidate struct {
	state    candState
	failures int
}

// connPool turns the tracker's stream of peer addresses into a bounded set of
// live connections.
//
// Addresses are deduplicated by ip:port in an address book. Ready ones wait in
// a FIFO queue; Run pops them as slots free up and dials up to MaxDialing at a
// time. After the handshake the peer goes to the downloader and runs until
// its connection ends, then its slot is released and the address is retried
// with backoff. After the handshake, peers are also deduplicated by peer ID,
// which catches one remote reached by two routes.
//
// A "slot" is held from the start of a dial until the connection is gone, so
// MaxPeers bounds everything. Inbound connections (AdoptInbound) take slots
// from the same budget.
type connPool struct {
	tor       *Torrent
	dl        *downloader
	ours      peer.Handshake
	numPieces int
	cfg       poolConfig

	wake chan struct{}
	wg   sync.WaitGroup // one count per held slot

	mu      sync.Mutex
	known   map[netip.AddrPort]*candidate
	queue   []netip.AddrPort
	ids     map[[20]byte]struct{} // remote peer IDs currently connected
	slots   int
	dialing int
	closed  bool
}

func newConnPool(tor *Torrent, dl *downloader, id peerID, cfg poolConfig) (*connPool, error) {
	if tor == nil || dl == nil {
		return nil, errors.New("pool: torrent and downloader are required")
	}
	infoHash := tor.localData.infoHash
	if infoHash == (hashBytes{}) {
		return nil, errors.New("pool: torrent metadata not loaded")
	}
	if tor.layout.numPieces <= 0 {
		return nil, errors.New("pool: torrent has no pieces")
	}

	return &connPool{
		tor:       tor,
		dl:        dl,
		ours:      peer.Handshake{InfoHash: infoHash, PeerID: id},
		numPieces: tor.layout.numPieces,
		cfg:       cfg.withDefaults(),
		wake:      make(chan struct{}, 1),
		known:     make(map[netip.AddrPort]*candidate),
		ids:       make(map[[20]byte]struct{}),
	}, nil
}

// Run feeds on tor.Peers() and keeps the pool full until ctx is cancelled,
// then waits for every connection to wind down. The downloader's Run must
// already be running: AddPeer blocks until its loop picks the request up.
func (cp *connPool) Run(ctx context.Context) {
	peers := cp.tor.Peers()
	for {
		cp.fill(ctx)

		select {
		case <-ctx.Done():
			cp.mu.Lock()
			cp.closed = true
			cp.mu.Unlock()
			cp.wg.Wait()
			return

		case batch, ok := <-peers:
			if !ok {
				peers = nil // a nil channel blocks forever
				continue
			}
			if n := cp.Add(batch...); n > 0 {
				slog.Debug("new peer candidates", "offered", len(batch), "added", n)
			}

		case <-cp.wake:
		}
	}
}

// Add offers addresses to the pool and returns how many were new. Banned,
// malformed, already-known and (when the address book is full) surplus
// addresses are ignored.
func (cp *connPool) Add(addrs ...netip.AddrPort) int {
	added := 0

	cp.mu.Lock()
	for _, a := range addrs {
		a = netip.AddrPortFrom(a.Addr().Unmap(), a.Port())
		if !a.IsValid() || a.Port() == 0 || cp.known[a] != nil || cp.dl.IsBanned(a.Addr()) {
			continue
		}
		if len(cp.known) >= cp.cfg.MaxKnown {
			for k, c := range cp.known {
				if c.state == candDead {
					delete(cp.known, k)
				}
			}
			if len(cp.known) >= cp.cfg.MaxKnown {
				continue
			}
		}
		cp.known[a] = &candidate{state: candQueued}
		cp.queue = append(cp.queue, a)
		added++
	}
	cp.mu.Unlock()

	if added > 0 {
		cp.signal()
	}
	return added
}

type poolStats struct {
	Connected int // established (or finishing their handshake)
	Dialing   int
	Queued    int
	Known     int
}

func (cp *connPool) Stats() poolStats {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return poolStats{
		Connected: cp.slots - cp.dialing,
		Dialing:   cp.dialing,
		Queued:    len(cp.queue),
		Known:     len(cp.known),
	}
}

// AdoptInbound hands a connection that has completed peer.Accept to the pool.
// It enforces the shared cap, the ban list and peer-ID deduplication, then
// gives the peer to the downloader. The pool owns conn from here on and closes
// it on any error. Nothing calls this yet; it is the hook for the accept loop.
func (cp *connPool) AdoptInbound(ctx context.Context, conn net.Conn, theirs peer.Handshake) error {
	ap, err := netip.ParseAddrPort(conn.RemoteAddr().String())
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("pool: parsing remote address: %w", err)
	}
	ap = netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())

	err = func() error {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		switch {
		case cp.closed:
			return ErrPoolClosed
		case cp.slots >= cp.cfg.MaxPeers:
			return ErrPoolFull
		case cp.dl.IsBanned(ap.Addr()):
			return errBanned
		case cp.known[ap] != nil:
			return errDuplicatePeer
		}
		cp.slots++
		cp.wg.Add(1)
		cp.known[ap] = &candidate{state: candBusy}
		return nil
	}()
	if err != nil {
		_ = conn.Close()
		return err
	}

	if err := cp.start(ctx, conn, theirs, ap, true); err != nil {
		cp.failed(ctx, ap, err, true)
		cp.signal()
		return err
	}
	return nil
}

func (cp *connPool) signal() {
	select {
	case cp.wake <- struct{}{}:
	default:
	}
}

// fill starts dials while there are free slots, free dial capacity and
// candidates waiting.
func (cp *connPool) fill(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()

	for !cp.closed && cp.slots < cp.cfg.MaxPeers && cp.dialing < cp.cfg.MaxDialing && len(cp.queue) > 0 {
		a := cp.queue[0]
		cp.queue = cp.queue[1:]
		if len(cp.queue) == 0 {
			cp.queue = nil // let the backing array go
		}

		c := cp.known[a]
		if c == nil || c.state != candQueued {
			continue
		}
		c.state = candBusy
		cp.slots++
		cp.dialing++
		cp.wg.Add(1)
		go cp.dial(ctx, a)
	}
}

func (cp *connPool) dial(ctx context.Context, a netip.AddrPort) {
	conn, theirs, err := cp.connect(ctx, a)

	cp.mu.Lock()
	cp.dialing--
	cp.mu.Unlock()

	if err == nil {
		err = cp.start(ctx, conn, theirs, a, false)
	}
	if err != nil {
		cp.failed(ctx, a, err, false)
	}
	cp.signal()
}

func (cp *connPool) connect(ctx context.Context, a netip.AddrPort) (net.Conn, peer.Handshake, error) {
	if cp.dl.IsBanned(a.Addr()) { // may have been banned since it was queued
		return nil, peer.Handshake{}, errBanned
	}

	dctx, cancel := context.WithTimeout(ctx, cp.cfg.DialTimeout)
	defer cancel()

	var d net.Dialer
	conn, err := d.DialContext(dctx, "tcp", a.String())
	if err != nil {
		return nil, peer.Handshake{}, err
	}

	theirs, err := peer.Initiate(ctx, conn, cp.ours, peer.HandshakeTimeout)
	if err != nil {
		_ = conn.Close()
		return nil, peer.Handshake{}, err
	}
	return conn, theirs, nil
}

// start registers a handshaken connection with the downloader and runs it.
// The caller holds a slot. On success the slot is released later by onClosed;
// on error start has closed conn and the caller must call failed. Either way
// start takes ownership of conn.
func (cp *connPool) start(ctx context.Context, conn net.Conn, theirs peer.Handshake, a netip.AddrPort, inbound bool) error {
	cp.mu.Lock()
	if _, dup := cp.ids[theirs.PeerID]; dup {
		cp.mu.Unlock()
		_ = conn.Close()
		return errDuplicatePeer
	}
	cp.ids[theirs.PeerID] = struct{}{}
	cp.mu.Unlock()

	p, err := peer.New(conn, theirs, peer.Config{NumPieces: cp.numPieces, Events: cp.dl.Events()})
	if err == nil {
		// Before Run, so our bitfield is the first message the peer sends.
		err = cp.dl.AddPeer(p)
	}
	if err != nil {
		_ = conn.Close()
		cp.mu.Lock()
		delete(cp.ids, theirs.PeerID)
		cp.mu.Unlock()
		return err
	}

	go func() {
		began := time.Now()
		err := p.Run(ctx)
		slog.Debug("peer disconnected", "peer", a, "after", time.Since(began).Round(time.Second), "error", err)
		cp.onClosed(ctx, a, theirs.PeerID, time.Since(began), inbound)
	}()
	return nil
}

// failed releases the slot of an attempt that never became a running peer and
// decides whether the address is worth another try.
func (cp *connPool) failed(ctx context.Context, a netip.AddrPort, err error, inbound bool) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	cp.releaseSlotLocked()
	if inbound {
		delete(cp.known, a) // its port was ephemeral; nothing to retry
		return
	}
	c := cp.known[a]
	if c == nil {
		return
	}

	if ctx.Err() == nil {
		slog.Debug("peer connect failed", "peer", a, "error", err)
	}
	c.failures++
	permanent := errors.Is(err, peer.ErrSelfConnection) ||
		errors.Is(err, peer.ErrInfoHashMismatch) ||
		errors.Is(err, peer.ErrBadProtocol) ||
		errors.Is(err, errBanned) ||
		errors.Is(err, errDuplicatePeer)
	cp.scheduleRetryLocked(ctx, a, c, permanent)
}

// onClosed releases the slot of a peer whose connection ended.
func (cp *connPool) onClosed(ctx context.Context, a netip.AddrPort, id [20]byte, lived time.Duration, inbound bool) {
	cp.mu.Lock()
	cp.releaseSlotLocked()
	delete(cp.ids, id)

	if c := cp.known[a]; c != nil {
		switch {
		case inbound:
			delete(cp.known, a)
		default:
			// Peers that drop us right away count against the address; ones
			// that served us for a while earn a fresh start.
			if lived < cp.cfg.MinStable {
				c.failures++
			} else {
				c.failures = 0
			}
			cp.scheduleRetryLocked(ctx, a, c, false)
		}
	}
	cp.mu.Unlock()

	cp.signal()
}

// scheduleRetryLocked either buries the address or requeues it after a
// backoff.
func (cp *connPool) scheduleRetryLocked(ctx context.Context, a netip.AddrPort, c *candidate, permanent bool) {
	if permanent || c.failures >= cp.cfg.MaxFailures || ctx.Err() != nil {
		c.state = candDead
		return
	}
	c.state = candWaiting

	time.AfterFunc(cp.cfg.retryDelay(c.failures), func() {
		cp.mu.Lock()
		if cp.closed || cp.known[a] != c || c.state != candWaiting {
			cp.mu.Unlock()
			return
		}
		c.state = candQueued
		cp.queue = append(cp.queue, a)
		cp.mu.Unlock()
		cp.signal()
	})
}

func (cp *connPool) releaseSlotLocked() {
	cp.slots--
	cp.wg.Done()
}
