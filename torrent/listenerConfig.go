package torrent

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
)

type peerID = [20]byte

// todo: automate these as client versions maybe
const majorVersionNumber = "00"
const minorVersionNumber = "1"
const minorMinorVersionNumber = "1"
const clientID = "-GT" + majorVersionNumber + minorVersionNumber + minorMinorVersionNumber + "-"

func getRandString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func newPeerID() peerID {
	var id peerID
	randID := getRandString(12)
	copy(id[:], clientID)
	copy(id[8:], randID)
	return id
}

func peerIDString(id peerID) string {
	return fmt.Sprintf("%q", id[:])
}

type Config struct {
	PortMin uint16
	PortMax uint16
}

func DefaultConfig() Config {
	return Config{PortMin: 6881, PortMax: 6889}
}

func (c Config) validate() error {
	if c.PortMin > c.PortMax {
		return fmt.Errorf("invalid port range %d-%d", c.PortMin, c.PortMax)
	}
	if c.PortMin == 0 && c.PortMax != 0 {
		return errors.New("port 0 (OS-assigned) is only valid as the whole range 0-0")
	}
	return nil
}

type Client struct {
	cfg      Config
	peerID   peerID
	port     uint16
	listener net.Listener
}

func NewClient(cfg Config) (*Client, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	id := newPeerID()

	l, port, err := listenInRange(cfg.PortMin, cfg.PortMax)
	if err != nil {
		return nil, err
	}

	slog.Info("client ready", "peerID", peerIDString(id), "port", port)
	return &Client{cfg: cfg, peerID: id, port: port, listener: l}, nil
}

func (c *Client) PeerID() [20]byte { return c.peerID }

func (c *Client) Port() uint16 { return c.port }

func (c *Client) Close() error { return c.listener.Close() }

func listenInRange(lo, hi uint16) (net.Listener, uint16, error) {
	if lo == 0 && hi == 0 {
		l, err := net.Listen("tcp", ":0")
		if err != nil {
			return nil, 0, fmt.Errorf("listening on an OS-assigned port: %w", err)
		}
		return l, listenerPort(l), nil
	}

	var lastErr error
	// int loop variable so hi == 65535 can't wrap around
	for p := int(lo); p <= int(hi); p++ {
		l, err := net.Listen("tcp", fmt.Sprintf(":%d", p))
		if err == nil {
			return l, uint16(p), nil
		}
		slog.Debug("port unavailable", "port", p, "error", err)
		lastErr = err
	}
	return nil, 0, fmt.Errorf("no free port in range %d-%d: %w", lo, hi, lastErr)
}

func listenerPort(l net.Listener) uint16 {
	if addr, ok := l.Addr().(*net.TCPAddr); ok {
		return uint16(addr.Port)
	}
	return 0
}
