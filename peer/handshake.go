package peer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	protocolString = "BitTorrent protocol"

	// HandshakeLen is the fixed size of a handshake:
	// <pstrlen><pstr><8 reserved><info_hash><peer_id> = 1 + 19 + 8 + 20 + 20.
	HandshakeLen = 1 + len(protocolString) + 8 + 20 + 20

	HandshakeTimeout = 10 * time.Second
)

var (
	ErrBadProtocol      = errors.New("peer: not the BitTorrent protocol")
	ErrInfoHashMismatch = errors.New("peer: handshake info hash mismatch")
	ErrSelfConnection   = errors.New("peer: connected to ourselves")
	ErrUnknownTorrent   = errors.New("peer: handshake for a torrent we are not serving")
)

type Handshake struct {
	Reserved [8]byte // extension flags; all zero until we support extensions
	InfoHash [20]byte
	PeerID   [20]byte
}

func (h Handshake) SupportsExtensions() bool { return h.Reserved[5]&0x10 != 0 } // BEP 10
func (h Handshake) SupportsFast() bool       { return h.Reserved[7]&0x04 != 0 } // BEP 6
func (h Handshake) SupportsDHT() bool        { return h.Reserved[7]&0x01 != 0 } // BEP 5

// Marshal returns the wire form of h.
func (h Handshake) Marshal() [HandshakeLen]byte {
	var buf [HandshakeLen]byte
	buf[0] = byte(len(protocolString))
	n := 1 + copy(buf[1:], protocolString)
	n += copy(buf[n:], h.Reserved[:])
	n += copy(buf[n:], h.InfoHash[:])
	copy(buf[n:], h.PeerID[:])
	return buf
}

func WriteHandshake(w io.Writer, h Handshake) error {
	buf := h.Marshal()
	_, err := w.Write(buf[:])
	return err
}

func ReadHandshake(r io.Reader) (Handshake, error) {
	var buf [HandshakeLen]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return Handshake{}, fmt.Errorf("reading handshake: %w", err)
	}

	end := 1 + len(protocolString)
	if buf[0] != byte(len(protocolString)) || string(buf[1:end]) != protocolString {
		return Handshake{}, ErrBadProtocol
	}

	var h Handshake
	n := end
	n += copy(h.Reserved[:], buf[n:])
	n += copy(h.InfoHash[:], buf[n:])
	copy(h.PeerID[:], buf[n:])
	return h, nil
}

func Initiate(ctx context.Context, conn net.Conn, ours Handshake, timeout time.Duration) (Handshake, error) {
	var theirs Handshake
	err := withDeadline(ctx, conn, timeout, func() error {
		if err := WriteHandshake(conn, ours); err != nil {
			return fmt.Errorf("sending handshake: %w", err)
		}
		var err error
		theirs, err = ReadHandshake(conn)
		return err
	})
	if err != nil {
		return Handshake{}, err
	}

	if theirs.InfoHash != ours.InfoHash {
		return Handshake{}, ErrInfoHashMismatch
	}
	if theirs.PeerID == ours.PeerID {
		return Handshake{}, ErrSelfConnection
	}
	return theirs, nil
}

func Accept(ctx context.Context, conn net.Conn, timeout time.Duration,
	lookup func(infoHash [20]byte) (Handshake, bool)) (Handshake, error) {

	var theirs Handshake
	err := withDeadline(ctx, conn, timeout, func() error {
		var err error
		theirs, err = ReadHandshake(conn)
		if err != nil {
			return err
		}

		ours, ok := lookup(theirs.InfoHash)
		if !ok {
			return ErrUnknownTorrent
		}
		if ours.InfoHash != theirs.InfoHash {
			return ErrInfoHashMismatch
		}
		if ours.PeerID == theirs.PeerID {
			return ErrSelfConnection
		}
		if err := WriteHandshake(conn, ours); err != nil {
			return fmt.Errorf("sending handshake: %w", err)
		}
		return nil
	})
	if err != nil {
		return Handshake{}, err
	}
	return theirs, nil
}

func withDeadline(ctx context.Context, conn net.Conn, timeout time.Duration, fn func() error) error {
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })

	err := fn()

	if !stop() {
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	return conn.SetDeadline(time.Time{})
}
