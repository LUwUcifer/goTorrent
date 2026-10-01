package tracker

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

type Tracker interface {
	Announce(ctx context.Context, req AnnounceRequest) (AnnounceResponse, error)

	URL() string
}

type Event uint32

const (
	EventNone      Event = 0 // regular periodic announce
	EventCompleted Event = 1 // download just finished (send once)
	EventStarted   Event = 2 // first announce for this torrent
	EventStopped   Event = 3 // shutting down or removing the torrent
)

func (e Event) String() string {
	switch e {
	case EventNone:
		return "none"
	case EventCompleted:
		return "completed"
	case EventStarted:
		return "started"
	case EventStopped:
		return "stopped"
	default:
		return fmt.Sprintf("event(%d)", uint32(e))
	}
}

func (e Event) httpParam() string {
	if e == EventNone {
		return ""
	}
	return e.String()
}

type AnnounceRequest struct {
	InfoHash [20]byte
	PeerID   [20]byte
	Port     uint16

	Uploaded   int64
	Downloaded int64
	Left       int64

	Event Event

	NumWant int
	Key     uint32

	TrackerID string
}

func (r AnnounceRequest) Validate() error {
	switch {
	case r.Port == 0:
		return errors.New("tracker: announce port must be non-zero")
	case r.Uploaded < 0 || r.Downloaded < 0 || r.Left < 0:
		return errors.New("tracker: negative transfer counter")
	case r.NumWant < 0:
		return errors.New("tracker: NumWant must be >= 0 (0 means tracker default)")
	case r.Event > EventStopped:
		return fmt.Errorf("tracker: unknown event %d", uint32(r.Event))
	}
	return nil
}

type AnnounceResponse struct {
	Interval    time.Duration
	MinInterval time.Duration

	Seeders  int
	Leechers int

	Peers []netip.AddrPort

	TrackerID string

	Warning string
}

const (
	defaultInterval     = 30 * time.Minute
	minAnnounceInterval = 30 * time.Second
	maxAnnounceInterval = 24 * time.Hour
)

func (r AnnounceResponse) NextAnnounce() time.Duration {
	d := r.Interval
	if d <= 0 {
		d = defaultInterval
	}
	if d < r.MinInterval {
		d = r.MinInterval
	}
	if d < minAnnounceInterval {
		d = minAnnounceInterval
	}
	if d > maxAnnounceInterval {
		d = maxAnnounceInterval
	}
	return d
}

type FailureError struct {
	Reason string
}

func (e *FailureError) Error() string {
	return "tracker failure: " + e.Reason
}

var ErrUnsupportedScheme = errors.New("tracker: unsupported URL scheme")

type constructor func(u *url.URL) (Tracker, error)

var constructors = map[string]constructor{}

func register(scheme string, c constructor) {
	constructors[scheme] = c
}

func New(rawURL string) (Tracker, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, fmt.Errorf("tracker: invalid announce URL: %w", stripURLError(err))
	}

	c, ok := constructors[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedScheme, u.Scheme)
	}
	if u.Host == "" {
		return nil, errors.New("tracker: announce URL has no host")
	}
	return c(u)
}
