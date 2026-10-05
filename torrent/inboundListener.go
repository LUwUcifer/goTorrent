package torrent

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"time"

	"goTor/peer"
)

const (
	// maxPendingHandshakes bounds connections that have been accepted but have
	// not finished their handshake. Each holds a goroutine and a socket for up
	// to peer.HandshakeTimeout, so without a cap a flood of silent connections
	// could exhaust file descriptors.
	maxPendingHandshakes = 64

	// maxPendingPerIP stops one address from using up that budget alone.
	maxPendingPerIP = 4

	acceptBackoffMin = 5 * time.Millisecond
	acceptBackoffMax = time.Second
)

var errAlreadyRegistered = errors.New("listener: torrent already registered")

// inboundPool is what the listener needs from a torrent's connection pool: the
// handshake to answer with, and somewhere to hand a connection that has
// passed it. *connPool satisfies it through connPoolEndpoint. Depending on
// this rather than the concrete pool keeps the listener testable without a
// downloader, assembler and picker behind it.
type inboundPool interface {
	identity() peer.Handshake
	AdoptInbound(ctx context.Context, conn net.Conn, theirs peer.Handshake) error
}

// connPoolEndpoint adapts *connPool to inboundPool without changing connPool.
type connPoolEndpoint struct{ *connPool }

func (e connPoolEndpoint) identity() peer.Handshake { return e.ours }

// inboundListener accepts connections on the Client's port and hands each to
// the connPool of the torrent it asks for.
//
// The remote peer speaks first in a BitTorrent handshake and names the torrent
// by info hash, so a connection can't be routed until it has been read. The
// listener therefore runs peer.Accept, which looks the hash up here and replies
// only for torrents it serves. Unknown torrents, malformed handshakes and
// connections to ourselves are closed without a reply. After that the pool
// enforces the connection cap, the ban list and peer-ID deduplication in
// AdoptInbound, and the downloader sends our bitfield as the first message.
//
// One listener serves every torrent that shares the Client.

type inboundListener struct {
	ln  net.Listener
	tcp *net.TCPListener // nil if ln isn't TCP; see unblock

	wg sync.WaitGroup // accepted connections still handshaking

	mu      sync.Mutex
	pools   map[[20]byte]inboundPool
	pending int
	perIP   map[netip.Addr]int
}

func newInboundListener(c *Client) (*inboundListener, error) {
	if c == nil || c.listener == nil {
		return nil, errors.New("listener: client has no listening socket")
	}
	l := &inboundListener{
		ln:    c.listener,
		pools: make(map[[20]byte]inboundPool),
		perIP: make(map[netip.Addr]int),
	}
	l.tcp, _ = c.listener.(*net.TCPListener)
	return l, nil
}

// Register makes the pool's torrent reachable by inbound connections. The info
// hash is taken from the pool itself, so it can't be registered under the
// wrong one.
func (l *inboundListener) Register(p inboundPool) error {
	if p == nil {
		return errors.New("listener: nil pool")
	}
	infoHash := p.identity().InfoHash

	l.mu.Lock()
	defer l.mu.Unlock()
	if _, dup := l.pools[infoHash]; dup {
		return errAlreadyRegistered
	}
	l.pools[infoHash] = p
	return nil
}

// RegisterPool registers a torrent's connPool.
func (l *inboundListener) RegisterPool(cp *connPool) error {
	if cp == nil {
		return errors.New("listener: nil pool")
	}
	return l.Register(connPoolEndpoint{cp})
}

// Unregister stops routing new connections for infoHash. Peers already adopted
// are unaffected.
func (l *inboundListener) Unregister(infoHash [20]byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.pools, infoHash)
}

func (l *inboundListener) poolFor(infoHash [20]byte) inboundPool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.pools[infoHash]
}

// Run accepts connections until ctx is cancelled, then waits for handshakes
// still in progress (they are cancelled by the same ctx). It does not close
// the socket, which belongs to the Client, so Run can be started again.
func (l *inboundListener) Run(ctx context.Context) {
	// A cancelled ctx has to interrupt a blocked Accept. Setting an expired
	// deadline does that without closing the socket.
	if l.tcp != nil {
		_ = l.tcp.SetDeadline(time.Time{}) // clear one left by an earlier Run
	}
	stop := context.AfterFunc(ctx, l.unblock)
	defer stop()

	var delay time.Duration
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}
			// Typically running out of file descriptors. Back off, like
			// net/http does, instead of spinning or giving up.
			if delay == 0 {
				delay = acceptBackoffMin
			} else {
				delay = min(delay*2, acceptBackoffMax)
			}
			slog.Warn("accept failed; retrying", "in", delay, "error", err)

			t := time.NewTimer(delay)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
			}
			continue
		}
		delay = 0
		l.dispatch(ctx, conn)
	}
	l.wg.Wait()
}

func (l *inboundListener) unblock() {
	if l.tcp != nil {
		_ = l.tcp.SetDeadline(time.Now())
		return
	}
	_ = l.ln.Close() // last resort for a non-TCP listener
}

// dispatch admits a connection into the handshake stage, or drops it if too
// many are already waiting.
func (l *inboundListener) dispatch(ctx context.Context, conn net.Conn) {
	ip := remoteIP(conn)
	if !l.admit(ip) {
		slog.Debug("too many pending handshakes; dropping", "remote", conn.RemoteAddr())
		_ = conn.Close()
		return
	}

	l.wg.Go(func() {
		defer l.release(ip)
		l.handshake(ctx, conn)
	})
}

func (l *inboundListener) admit(ip netip.Addr) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.pending >= maxPendingHandshakes || l.perIP[ip] >= maxPendingPerIP {
		return false
	}
	l.pending++
	l.perIP[ip]++
	return true
}

func (l *inboundListener) release(ip netip.Addr) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.pending--
	if l.perIP[ip]--; l.perIP[ip] <= 0 {
		delete(l.perIP, ip)
	}
}

func (l *inboundListener) handshake(ctx context.Context, conn net.Conn) {
	var pool inboundPool
	theirs, err := peer.Accept(ctx, conn, peer.HandshakeTimeout, func(infoHash [20]byte) (peer.Handshake, bool) {
		pool = l.poolFor(infoHash)
		if pool == nil {
			return peer.Handshake{}, false
		}
		return pool.identity(), true
	})
	if err != nil {
		_ = conn.Close()
		slog.Debug("inbound handshake failed", "remote", conn.RemoteAddr(), "error", err)
		return
	}

	// From here the pool owns conn and closes it on any error.
	if err := pool.AdoptInbound(ctx, conn, theirs); err != nil {
		slog.Debug("inbound peer refused", "remote", conn.RemoteAddr(), "error", err)
	}
}

func remoteIP(conn net.Conn) netip.Addr {
	ap, err := netip.ParseAddrPort(conn.RemoteAddr().String())
	if err != nil {
		return netip.Addr{}
	}
	return ap.Addr().Unmap()
}
