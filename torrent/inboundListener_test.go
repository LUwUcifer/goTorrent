package torrent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"goTor/peer"
)

func inboundHS(info, id byte) peer.Handshake {
	var h peer.Handshake
	for i := range h.InfoHash {
		h.InfoHash[i] = info
	}
	for i := range h.PeerID {
		h.PeerID[i] = id
	}
	return h
}

// fakePool stands in for a torrent's connPool. Like the real one, it takes
// ownership of the connection and closes it when it refuses a peer.
type fakePool struct {
	ours    peer.Handshake
	adopted chan peer.Handshake

	mu    sync.Mutex
	err   error // returned from AdoptInbound, which then closes the connection
	conns []net.Conn
}

func newFakePool(ours peer.Handshake) *fakePool {
	return &fakePool{ours: ours, adopted: make(chan peer.Handshake, 8)}
}

func (f *fakePool) identity() peer.Handshake { return f.ours }

func (f *fakePool) AdoptInbound(_ context.Context, conn net.Conn, theirs peer.Handshake) error {
	f.adopted <- theirs

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		_ = conn.Close()
		return f.err
	}
	f.conns = append(f.conns, conn)
	return nil
}

func (f *fakePool) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakePool) closeAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.conns {
		_ = c.Close()
	}
}

func (f *fakePool) waitAdopted(t *testing.T) peer.Handshake {
	t.Helper()
	select {
	case h := <-f.adopted:
		return h
	case <-time.After(2 * time.Second):
		t.Fatal("pool was never given the connection")
		return peer.Handshake{}
	}
}

func (f *fakePool) expectNoAdoption(t *testing.T) {
	t.Helper()
	select {
	case h := <-f.adopted:
		t.Fatalf("pool was given a connection from peer %x that should have been rejected", h.PeerID)
	default:
	}
}

// inboundTestClient binds a Client to an OS-assigned loopback-reachable port.
// It is defined here rather than shared, so this file doesn't depend on any
// other test file being present.
func inboundTestClient(t *testing.T) *Client {
	t.Helper()
	c, err := NewClient(Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

type inboundEnv struct {
	client *Client
	il     *inboundListener
	pool   *fakePool
	cancel context.CancelFunc
	done   chan struct{}
}

// startInbound runs a listener with one registered torrent (info hash 0x11..,
// our peer ID 0xA1..) on a loopback port.
func startInbound(t *testing.T) *inboundEnv {
	t.Helper()

	client := inboundTestClient(t)
	il, err := newInboundListener(client)
	if err != nil {
		t.Fatal(err)
	}
	pool := newFakePool(inboundHS(0x11, 0xA1))
	if err := il.Register(pool); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.closeAll)

	e := &inboundEnv{client: client, il: il, pool: pool}
	e.run(t)
	return e
}

// run starts (or restarts) the accept loop.
func (e *inboundEnv) run(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	e.cancel, e.done = cancel, done

	go func() {
		e.il.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("listener Run did not return after cancel")
		}
	})
}

// stop cancels the accept loop and waits for Run to return.
func (e *inboundEnv) stop(t *testing.T) {
	t.Helper()
	e.cancel()
	select {
	case <-e.done:
	case <-time.After(2 * time.Second):
		t.Fatal("listener Run did not return after cancel")
	}
}

func (e *inboundEnv) dial(t *testing.T) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", e.client.Port()), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// inboundExchange sends our handshake and reads the reply.
func inboundExchange(t *testing.T, conn net.Conn, hs peer.Handshake) peer.Handshake {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if err := peer.WriteHandshake(conn, hs); err != nil {
		t.Fatal(err)
	}
	reply, err := peer.ReadHandshake(conn)
	if err != nil {
		t.Fatalf("reading the handshake reply: %v", err)
	}
	return reply
}

// inboundExpectClosed asserts the server closed the connection without sending anything.
func inboundExpectClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(make([]byte, 1))
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("read %d bytes, err %v; want the connection closed with nothing sent", n, err)
	}
}

func TestInboundKnownTorrentIsAdopted(t *testing.T) {
	e := startInbound(t)
	conn := e.dial(t)

	theirs := inboundHS(0x11, 0xB2)
	theirs.Reserved[5] = 0x10

	reply := inboundExchange(t, conn, theirs)
	if reply.InfoHash != e.pool.ours.InfoHash || reply.PeerID != e.pool.ours.PeerID {
		t.Fatalf("reply = %+v, want the registered torrent's handshake", reply)
	}

	got := e.pool.waitAdopted(t)
	if got.PeerID != theirs.PeerID || !got.SupportsExtensions() {
		t.Fatalf("pool received %+v, want the remote's own handshake intact", got)
	}
}

func TestInboundRejectsWithoutReply(t *testing.T) {
	notBitTorrent := make([]byte, peer.HandshakeLen)
	copy(notBitTorrent, "GET / HTTP/1.1\r\n\r\n")

	cases := []struct {
		name string
		send func(conn net.Conn) error
	}{
		{"unknown torrent", func(c net.Conn) error { return peer.WriteHandshake(c, inboundHS(0x99, 0xB2)) }},
		{"not the bittorrent protocol", func(c net.Conn) error { _, err := c.Write(notBitTorrent); return err }},
		{"connection to ourselves", func(c net.Conn) error { return peer.WriteHandshake(c, inboundHS(0x11, 0xA1)) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := startInbound(t)
			conn := e.dial(t)

			if err := tc.send(conn); err != nil {
				t.Fatal(err)
			}
			inboundExpectClosed(t, conn)
			e.pool.expectNoAdoption(t)
		})
	}
}

func TestInboundRefusedPeerIsClosed(t *testing.T) {
	e := startInbound(t)
	e.pool.setErr(ErrPoolFull)
	conn := e.dial(t)

	// The handshake reply is sent before the pool decides, so it still arrives.
	inboundExchange(t, conn, inboundHS(0x11, 0xB2))
	e.pool.waitAdopted(t)
	inboundExpectClosed(t, conn)
}

func TestInboundUnregister(t *testing.T) {
	e := startInbound(t)

	if err := e.il.Register(e.pool); !errors.Is(err, errAlreadyRegistered) {
		t.Fatalf("second Register: got %v, want errAlreadyRegistered", err)
	}

	e.il.Unregister(e.pool.ours.InfoHash)
	conn := e.dial(t)
	if err := peer.WriteHandshake(conn, inboundHS(0x11, 0xB2)); err != nil {
		t.Fatal(err)
	}
	inboundExpectClosed(t, conn)
	e.pool.expectNoAdoption(t)
}

func TestInboundPendingLimitPerIP(t *testing.T) {
	e := startInbound(t)

	// Connections that never send a handshake occupy handshake slots.
	var silent []net.Conn
	for range maxPendingPerIP {
		silent = append(silent, e.dial(t))
	}

	// One more from the same address is dropped on sight.
	inboundExpectClosed(t, e.dial(t))

	// The ones within the limit are still waiting, not closed.
	_ = silent[0].SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := silent[0].Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("a connection within the limit should still be open and waiting, got %v", err)
	}
}

func TestInboundRunStopsAndCanRestart(t *testing.T) {
	e := startInbound(t)
	e.stop(t)

	// The socket belongs to the Client, so stopping the loop must leave it
	// open, and a new Run must not trip over the deadline that stopped the last.
	e.run(t)
	conn := e.dial(t)
	reply := inboundExchange(t, conn, inboundHS(0x11, 0xB2))
	if reply.PeerID != e.pool.ours.PeerID {
		t.Fatalf("no handshake reply after restart: %+v", reply)
	}
	e.pool.waitAdopted(t)
}
