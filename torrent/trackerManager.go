package torrent

import (
	"context"
	"errors"
	"fmt"
	"goTor/tracker"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

const (
	trackerAnnounceTimeout = 45 * time.Second

	trackerStopTimeout = 5 * time.Second

	trackerNumWant = 50

	// trackerStagger is how long announce waits on one tracker before also
	// starting the next. A dead tracker (typically a silent UDP one) would
	// otherwise cost its whole timeout before the next is even tried.
	trackerStagger = 3 * time.Second

	trackerRetryBase = 30 * time.Second
	trackerRetryMax  = 15 * time.Minute
)

type trackerManager struct {
	tor  *Torrent
	self peerID
	port uint16
	key  uint32

	tiers [][]tracker.Tracker

	trackerIDs    map[tracker.Tracker]string
	announced     map[tracker.Tracker]struct{}
	sentStarted   bool
	sentCompleted bool

	completed atomic.Bool
	kick      chan struct{}
	stagger   time.Duration
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
		trackerIDs: make(map[tracker.Tracker]string),
		announced:  make(map[tracker.Tracker]struct{}),
		kick:       make(chan struct{}, 1),
		stagger:    trackerStagger,
	}

	m.sentCompleted = tor.bytesLeft() == 0

	m.tiers = buildTiers(mi.announceList)
	if len(m.tiers) == 0 && mi.announce != "" {
		m.tiers = buildTiers([][]string{{mi.announce}})
	}
	if len(m.tiers) == 0 {
		return nil, errors.New("torrent has no usable trackers")
	}

	count := 0
	for _, t := range m.tiers {
		count += len(t)
	}
	slog.Info("tracker manager ready", "tiers", len(m.tiers), "trackers", count)
	return m, nil
}

func buildTiers(lists [][]string) [][]tracker.Tracker {
	var tiers [][]tracker.Tracker
	for ti, urls := range lists {
		var tier []tracker.Tracker
		for _, raw := range urls {
			tr, err := tracker.New(raw)
			if err != nil {
				slog.Warn("skipping tracker", "tier", ti, "error", err)
				continue
			}
			tier = append(tier, tr)
		}
		if len(tier) == 0 {
			continue
		}
		rand.Shuffle(len(tier), func(i, j int) { tier[i], tier[j] = tier[j], tier[i] })
		tiers = append(tiers, tier)
	}
	return tiers
}

func (tor *Torrent) StartTrackers(ctx context.Context, c *Client) (<-chan struct{}, error) {
	m, err := newTrackerManager(tor, c)
	if err != nil {
		return nil, err
	}
	if !tor.trackers.CompareAndSwap(nil, m) {
		return nil, errors.New("trackers already started")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.run(ctx)
	}()
	return done, nil
}

func (m *trackerManager) markCompleted() {
	if m.completed.CompareAndSwap(false, true) {
		select {
		case m.kick <- struct{}{}:
		default:
		}
	}
}

func (m *trackerManager) run(ctx context.Context) {
	var wait time.Duration
	failures := 0

	for {
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			m.stop()
			return
		case <-m.kick:
		case <-t.C:
		}
		t.Stop()

		ev := m.nextEvent()
		resp, err := m.announce(ctx, ev)
		if ctx.Err() != nil {
			m.stop()
			return
		}
		if err != nil {
			failures++
			wait = trackerRetryDelay(failures)
			slog.Warn("announce failed", "event", ev, "retryIn", wait, "error", err)
			continue
		}
		failures = 0

		switch ev {
		case tracker.EventStarted:
			m.sentStarted = true
		case tracker.EventCompleted:
			m.sentCompleted = true
		}

		if resp.Warning != "" {
			slog.Warn("tracker warning", "message", resp.Warning)
		}
		slog.Info("announce ok", "event", ev, "peers", len(resp.Peers),
			"seeders", resp.Seeders, "leechers", resp.Leechers, "interval", resp.NextAnnounce())

		if len(resp.Peers) > 0 {
			select {
			case m.tor.peerCh <- resp.Peers:
			default:
				slog.Debug("peer channel full, dropping batch", "peers", len(resp.Peers))
			}
		}

		if m.nextEvent() != tracker.EventNone {
			wait = 0
		} else {
			wait = resp.NextAnnounce()
		}
	}
}

func (m *trackerManager) nextEvent() tracker.Event {
	switch {
	case !m.sentStarted:
		return tracker.EventStarted
	case m.completed.Load() && !m.sentCompleted:
		return tracker.EventCompleted
	default:
		return tracker.EventNone
	}
}

// trackerRef locates one tracker in the tier structure.
type trackerRef struct {
	tier, idx int
	tr        tracker.Tracker
}

// flatTrackers lists every tracker in preference order: tiers in order, and
// within a tier its current order (which keeps recent winners first).
func (m *trackerManager) flatTrackers() []trackerRef {
	var refs []trackerRef
	for ti, tier := range m.tiers {
		for i, tr := range tier {
			refs = append(refs, trackerRef{tier: ti, idx: i, tr: tr})
		}
	}
	return refs
}

// announce asks trackers in preference order and returns the first success.
//
// Trackers are not tried strictly one after another: if the current one hasn't
// answered within m.stagger, the next is started alongside it, and a tracker
// that fails starts the next immediately. A hung tracker therefore delays the
// announce by one stagger interval instead of a full timeout. The first
// success wins and the rest are cancelled.
//
// This relaxes BEP 12 slightly (a lower tier may be announced to while a
// higher one is merely slow), which is what other clients do too.
func (m *trackerManager) announce(ctx context.Context, ev tracker.Event) (tracker.AnnounceResponse, error) {
	var zero tracker.AnnounceResponse

	stats := m.tor.transferStats()
	refs := m.flatTrackers()

	actx, cancel := context.WithTimeout(ctx, trackerAnnounceTimeout)
	defer cancel() // also cancels any announce still in flight when we return

	type result struct {
		n    int
		resp tracker.AnnounceResponse
		err  error
	}
	results := make(chan result, len(refs)) // buffered: late finishers never block

	launched, finished := 0, 0
	inFlight := make([]bool, len(refs))
	launch := func() {
		n := launched
		launched++
		inFlight[n] = true
		// Build the request here: m.request reads manager state, which only
		// this goroutine may touch.
		req := m.request(ev, stats, refs[n].tr)
		go func() {
			resp, err := refs[n].tr.Announce(actx, req)
			results <- result{n: n, resp: resp, err: err}
		}()
	}

	var errs []error
	launch()
	stagger := time.NewTimer(m.stagger)
	defer stagger.Stop()

	for finished < len(refs) {
		select {
		case <-ctx.Done():
			return zero, ctx.Err()

		case <-stagger.C:
			if launched < len(refs) {
				launch()
				stagger.Reset(m.stagger)
			}

		case r := <-results:
			finished++
			inFlight[r.n] = false
			if r.err == nil {
				return m.accept(refs, inFlight, r.n, r.resp), nil
			}
			slog.Debug("tracker failed", "tracker", refs[r.n].tr.URL(), "error", r.err)
			errs = append(errs, r.err)
			if launched < len(refs) {
				launch()
				stagger.Reset(m.stagger)
			}
		}
	}
	return zero, fmt.Errorf("all trackers failed: %w", errors.Join(errs...))
}

// accept records the winning tracker: it moves to the front of its tier and
// its tracker ID is kept. Trackers still mid-announce when we moved on are
// remembered too, since they may have registered us before being cancelled and
// should hear "stopped" at shutdown.
func (m *trackerManager) accept(refs []trackerRef, inFlight []bool, win int, resp tracker.AnnounceResponse) tracker.AnnounceResponse {
	w := refs[win]
	tier := m.tiers[w.tier]
	copy(tier[1:w.idx+1], tier[:w.idx])
	tier[0] = w.tr

	m.announced[w.tr] = struct{}{}
	if resp.TrackerID != "" {
		m.trackerIDs[w.tr] = resp.TrackerID
	}
	for n, flying := range inFlight {
		if flying {
			m.announced[refs[n].tr] = struct{}{}
		}
	}
	return resp
}

func (m *trackerManager) stop() {
	if len(m.announced) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), trackerStopTimeout)
	defer cancel()

	stats := m.tor.transferStats()
	var wg sync.WaitGroup
	for tr := range m.announced {
		req := m.request(tracker.EventStopped, stats, tr)
		req.NumWant = 0
		wg.Go(func() {
			if _, err := tr.Announce(ctx, req); err != nil {
				slog.Debug("stopped announce failed", "tracker", tr.URL(), "error", err)
			}
		})
	}
	wg.Wait()
}

func (m *trackerManager) request(ev tracker.Event, s transferStats, tr tracker.Tracker) tracker.AnnounceRequest {
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
		TrackerID:  m.trackerIDs[tr],
	}
}

func trackerRetryDelay(failures int) time.Duration {
	shift := min(max(failures-1, 0), 5)
	return min(trackerRetryBase<<shift, trackerRetryMax)
}
