package tracker

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"time"
)

const (
	udpProtocolID uint64 = 0x41727101980 // magic constant from BEP 15

	udpActionConnect  uint32 = 0
	udpActionAnnounce uint32 = 1
	udpActionError    uint32 = 3

	udpConnectReqSize  = 16
	udpConnectRespSize = 16
	udpAnnounceReqSize = 98
	udpAnnounceHdrSize = 20

	udpBaseTimeout = 15 * time.Second
	udpMaxAttempts = 4 // waits 15s, 30s, 60s, 120s

	udpReadBuf = 8192
)

func init() {
	register("udp", newUDPTracker)
}

type udpTracker struct {
	hostPort string // host:port handed to the dialer
	display  string // scheme://host:port
}

func newUDPTracker(u *url.URL) (Tracker, error) {
	if u.Hostname() == "" {
		return nil, fmt.Errorf("tracker: %q has no host", u.Scheme+"://"+u.Host)
	}
	port, err := strconv.ParseUint(u.Port(), 10, 16)
	if err != nil || port == 0 {
		return nil, fmt.Errorf("tracker: udp tracker %q needs a port in 1-65535", u.Host)
	}
	return &udpTracker{
		hostPort: u.Host,
		display:  u.Scheme + "://" + u.Host,
	}, nil
}

func (t *udpTracker) URL() string { return t.display }

func (t *udpTracker) Announce(ctx context.Context, req AnnounceRequest) (AnnounceResponse, error) {
	var zero AnnounceResponse
	if err := req.Validate(); err != nil {
		return zero, err
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "udp", t.hostPort)
	if err != nil {
		return zero, fmt.Errorf("tracker: dialing %s: %w", t.display, err)
	}
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			slog.Debug("closing connection", "tracker", t.display, "error", err)
		}
	}(conn)

	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()

	v6 := false
	if ua, ok := conn.RemoteAddr().(*net.UDPAddr); ok {
		if ip, ok := netip.AddrFromSlice(ua.IP); ok {
			v6 = ip.Unmap().Is6()
		}
	}

	var lastErr error
	for attempt := range udpMaxAttempts {
		timeout := udpBaseTimeout << attempt

		resp, err := t.attempt(ctx, conn, req, v6, timeout)
		if err == nil {
			return resp, nil
		}
		if ctx.Err() != nil {
			return zero, fmt.Errorf("tracker: announce to %s: %w", t.display, ctx.Err())
		}
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			return zero, err
		}
		lastErr = err
	}
	return zero, fmt.Errorf("tracker: %s did not respond after %d attempts: %w",
		t.display, udpMaxAttempts, lastErr)
}

func (t *udpTracker) attempt(ctx context.Context, conn net.Conn, req AnnounceRequest, v6 bool, timeout time.Duration) (AnnounceResponse, error) {
	var zero AnnounceResponse

	connID, err := t.connect(ctx, conn, timeout)
	if err != nil {
		return zero, err
	}

	txid, err := newTransactionID()
	if err != nil {
		return zero, err
	}

	pkt := buildAnnounce(connID, txid, req)
	buf, err := t.exchange(ctx, conn, pkt, txid, udpActionAnnounce, udpAnnounceHdrSize, timeout)
	if err != nil {
		return zero, err
	}
	return parseAnnounce(buf, v6)
}

func (t *udpTracker) connect(ctx context.Context, conn net.Conn, timeout time.Duration) (uint64, error) {
	txid, err := newTransactionID()
	if err != nil {
		return 0, err
	}

	pkt := make([]byte, udpConnectReqSize)
	binary.BigEndian.PutUint64(pkt[0:8], udpProtocolID)
	binary.BigEndian.PutUint32(pkt[8:12], udpActionConnect)
	binary.BigEndian.PutUint32(pkt[12:16], txid)

	buf, err := t.exchange(ctx, conn, pkt, txid, udpActionConnect, udpConnectRespSize, timeout)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf[8:16]), nil
}

func (t *udpTracker) exchange(ctx context.Context, conn net.Conn, pkt []byte, txid, wantAction uint32, minLen int, timeout time.Duration) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("tracker: %s: %w", t.display, err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if _, err := conn.Write(pkt); err != nil {
		return nil, fmt.Errorf("tracker: sending to %s: %w", t.display, err)
	}

	buf := make([]byte, udpReadBuf)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return nil, fmt.Errorf("tracker: reading from %s: %w", t.display, err)
		}
		if n < 8 {
			continue
		}

		action := binary.BigEndian.Uint32(buf[0:4])
		if binary.BigEndian.Uint32(buf[4:8]) != txid {
			continue
		}

		switch action {
		case udpActionError:
			return nil, &FailureError{Reason: cleanText(string(buf[8:n]))}
		case wantAction:
			if n < minLen {
				return nil, fmt.Errorf("tracker: %s sent a short reply (%d bytes, need %d)", t.display, n, minLen)
			}
			return buf[:n], nil
		default:
			return nil, fmt.Errorf("tracker: %s sent unexpected action %d", t.display, action)
		}
	}
}

func buildAnnounce(connID uint64, txid uint32, r AnnounceRequest) []byte {
	b := make([]byte, udpAnnounceReqSize)
	be := binary.BigEndian

	be.PutUint64(b[0:8], connID)
	be.PutUint32(b[8:12], udpActionAnnounce)
	be.PutUint32(b[12:16], txid)
	copy(b[16:36], r.InfoHash[:])
	copy(b[36:56], r.PeerID[:])
	be.PutUint64(b[56:64], uint64(r.Downloaded))
	be.PutUint64(b[64:72], uint64(r.Left))
	be.PutUint64(b[72:80], uint64(r.Uploaded))
	be.PutUint32(b[80:84], uint32(r.Event))
	be.PutUint32(b[84:88], 0)
	be.PutUint32(b[88:92], r.Key)

	numWant := int32(-1)
	if r.NumWant > 0 {
		numWant = int32(min(r.NumWant, math.MaxInt32))
	}
	be.PutUint32(b[92:96], uint32(numWant))
	be.PutUint16(b[96:98], r.Port)
	return b
}

func parseAnnounce(buf []byte, v6 bool) (AnnounceResponse, error) {
	var resp AnnounceResponse
	be := binary.BigEndian

	resp.Interval = secondsToDuration(int64(be.Uint32(buf[8:12])))
	resp.Leechers = int(min(int64(be.Uint32(buf[12:16])), math.MaxInt32))
	resp.Seeders = int(min(int64(be.Uint32(buf[16:20])), math.MaxInt32))

	peers, err := parseCompactPeers(buf[udpAnnounceHdrSize:], v6)
	if err != nil {
		return resp, err
	}
	resp.Peers = peers
	return resp, nil
}

func newTransactionID() (uint32, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, fmt.Errorf("tracker: generating transaction id: %w", err)
	}
	return binary.BigEndian.Uint32(b[:]), nil
}
