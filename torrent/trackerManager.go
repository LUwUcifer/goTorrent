package torrent

import (
	"context"
	"errors"
	"goTor/tracker"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	trackerAnnounceTimeout = 45 * time.Second

	trackerStopTimeout = 5 * time.Second

	trackerNumWant = 50

	// maxTrackers caps how many trackers one torrent may run. Every tracker
	// gets its own goroutine and announce schedule, and a hostile .torrent
	// could list thousands.
	maxTrackers = 64

	trackerRetryBase = 30 * time.Second
	trackerRetryMax  = 15 * time.Minute
)

// trackerManager announces the torrent to every tracker it lists, each on its
// own schedule.
//
// Trackers are independent: each has its own interval, backoff and record of
// which one-shot events ("started", "completed") it has been sent, so a slow or
// dead tracker affects nobody else, and every tracker sees accurate transfer
// counters. BEP 12's tiers are deliberately ignored; every URL is announced to.
// Peers found by any tracker go to the same channel, and the connection pool
// deduplicates them.
type trackerManager struct {
	tor  *Torrent
	self peerID
	port uint16
	key  uint32

	loops []*trackerLoop

	completed  atomic.Bool   // the download finished during this session
	completeCh chan struct{} // closed once, by markCompleted
}

// trackerLoop is the announce schedule for one tracker. Its fields are touched
// only by its own goroutine.
type trackerLoop struct {
	m  *trackerManager
	tr tracker.Tracker

	trackerID string
	started   bool // the tracker has accepted a "started"
	completed bool // the tracker has been told we are a seed
	contacted bool // it may have registered us, so it should hear "stopped"
}

func newTrackerManager(tor *Torrent, c *Client) (*trackerManager, error) {
	mi := tor.localData
	if mi.infoHash == (hashBytes{}) {
		return nil, errors.New("torrent metadata not loaded")
	}

	m := &trackerManager{
		tor:        tor,
		self:       c.PeerID(),
		port:       c.Port(),
		key:        rand.Uint32(),
		completeCh: make(chan struct{}),
	}

	// announce-list replaces announce (BEP 12); announce alone is the fallback.
	lists := mi.announceList
	if len(lists) == 0 && mi.announce != "" {
		lists = [][]string{{mi.announce}}
	}
	for _, tr := range buildTrackers(lists) {
		m.loops = append(m.loops, &trackerLoop{m: m, tr: tr})
	}
	if len(m.loops) == 0 {
		return nil, errors.New("torrent has no usable trackers")
	}

	slog.Info("tracker manager ready", "trackers", len(m.loops))
	return m, nil
}

// buildTrackers flattens the announce list into one tracker per distinct URL,
// skipping unusable ones and stopping at maxTrackers.
func buildTrackers(lists [][]string) []tracker.Tracker {
	var out []tracker.Tracker
	seen := make(map[string]struct{})

	for ti, urls := range lists {
		for _, raw := range urls {
			raw = strings.TrimSpace(raw)
			if _, dup := seen[raw]; dup {
				continue
			}
			seen[raw] = struct{}{}

			tr, err := tracker.New(raw)
			if err != nil {
				slog.Warn("skipping tracker", "tier", ti, "error", err)
				continue
			}
			if len(out) >= maxTrackers {
				slog.Warn("too many trackers; ignoring the rest", "limit", maxTrackers)
				return out
			}
			out = append(out, tr)
		}
	}
	return out
}

// StartTrackers launches one announce loop per tracker. The returned channel is
// closed once every loop has finished, which includes sending "stopped" after
// ctx is cancelled, so wait on it before exiting.
func (tor *Torrent) StartTrackers(ctx context.Context, c *Client) (<-chan struct{}, error) {
	m, err := newTrackerManager(tor, c)
	if err != nil {
		return nil, err
	}
	if !tor.trackers.CompareAndSwap(nil, m) {
		return nil, errors.New("trackers already started")
	}

	var wg sync.WaitGroup
	for _, l := range m.loops {
		wg.Go(func() { l.run(ctx) })
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()
	return done, nil
}

// markCompleted is called when the last piece verifies. It wakes every tracker
// loop that still owes its tracker a "completed", so the event goes out
// promptly instead of at the next interval.
func (m *trackerManager) markCompleted() {
	if m.completed.CompareAndSwap(false, true) {
		close(m.completeCh)
	}
}

func (l *trackerLoop) run(ctx context.Context) {
	var wait time.Duration
	failures := 0

	for {
		// completeCh stays closed once the download finishes, so it is level
		// triggered: listen to it only while this tracker is healthy and still
		// owed the event. Otherwise a failing tracker would be retried in a
		// tight loop.
		var done <-chan struct{}
		if failures == 0 && !l.completed {
			done = l.m.completeCh
		}

		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			l.stop()
			return
		case <-done:
		case <-t.C:
		}
		t.Stop()

		ev := l.eventFor()
		stats := l.m.tor.transferStats()

		actx, cancel := context.WithTimeout(ctx, trackerAnnounceTimeout)
		resp, err := l.tr.Announce(actx, l.request(ev, stats))
		cancel()

		if ctx.Err() != nil {
			// The announce may have reached the tracker before we cancelled it.
			l.contacted = true
			l.stop()
			return
		}
		if err != nil {
			failures++
			wait = trackerRetryDelay(failures)
			slog.Warn("announce failed", "tracker", l.tr.URL(), "event", ev, "retryIn", wait, "error", err)
			continue
		}
		failures = 0
		l.contacted = true

		switch ev {
		case tracker.EventStarted:
			l.started = true
			// First contact with nothing left to download already shows us as a
			// seeder, so a "completed" on top would be wrong (and, on private
			// trackers, could count as a snatch).
			if stats.left == 0 {
				l.completed = true
			}
		case tracker.EventCompleted:
			l.completed = true
		}
		if resp.TrackerID != "" {
			l.trackerID = resp.TrackerID
		}

		if resp.Warning != "" {
			slog.Warn("tracker warning", "tracker", l.tr.URL(), "message", resp.Warning)
		}
		slog.Info("announce ok", "tracker", l.tr.URL(), "event", ev, "peers", len(resp.Peers),
			"seeders", resp.Seeders, "leechers", resp.Leechers, "interval", resp.NextAnnounce())

		if len(resp.Peers) > 0 {
			select {
			case l.m.tor.peerCh <- resp.Peers:
			default:
				slog.Debug("peer channel full, dropping batch", "tracker", l.tr.URL(), "peers", len(resp.Peers))
			}
		}

		// An event still owed (completion arrived mid-announce, say) goes out
		// straight away.
		if l.eventFor() != tracker.EventNone {
			wait = 0
		} else {
			wait = resp.NextAnnounce()
		}
	}
}

// eventFor is the event the next announce to this tracker should carry:
// "started" until it has accepted one, then "completed" once if the download
// finished during this session and it hasn't been told, otherwise none.
func (l *trackerLoop) eventFor() tracker.Event {
	switch {
	case !l.started:
		return tracker.EventStarted
	case l.m.completed.Load() && !l.completed:
		return tracker.EventCompleted
	default:
		return tracker.EventNone
	}
}

// stop tells the tracker we are leaving, if it ever heard from us. It runs on
// the loop's own goroutine, so every tracker is told in parallel.
func (l *trackerLoop) stop() {
	if !l.contacted {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), trackerStopTimeout)
	defer cancel()

	req := l.request(tracker.EventStopped, l.m.tor.transferStats())
	req.NumWant = 0
	if _, err := l.tr.Announce(ctx, req); err != nil {
		slog.Debug("stopped announce failed", "tracker", l.tr.URL(), "error", err)
	}
}

func (l *trackerLoop) request(ev tracker.Event, s transferStats) tracker.AnnounceRequest {
	m := l.m
	return tracker.AnnounceRequest{
		InfoHash:   m.tor.localData.infoHash,
		PeerID:     m.self,
		Port:       m.port,
		Uploaded:   s.uploaded,
		Downloaded: s.downloaded,
		Left:       s.left,
		Event:      ev,
		NumWant:    trackerNumWant,
		Key:        m.key,
		TrackerID:  l.trackerID,
	}
}

func trackerRetryDelay(failures int) time.Duration {
	shift := min(max(failures-1, 0), 5)
	return min(trackerRetryBase<<shift, trackerRetryMax)
}
