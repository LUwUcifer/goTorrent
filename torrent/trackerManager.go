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

func (m *trackerManager) announce(ctx context.Context, ev tracker.Event) (tracker.AnnounceResponse, error) {
	stats := m.tor.transferStats()
	var errs []error

	for _, tier := range m.tiers {
		for i, tr := range tier {
			tctx, cancel := context.WithTimeout(ctx, trackerAnnounceTimeout)
			resp, err := tr.Announce(tctx, m.request(ev, stats, tr))
			cancel()

			if ctx.Err() != nil {
				return tracker.AnnounceResponse{}, ctx.Err()
			}
			if err != nil {
				slog.Debug("tracker failed", "tracker", tr.URL(), "error", err)
				errs = append(errs, err)
				continue
			}

			copy(tier[1:i+1], tier[:i])
			tier[0] = tr

			m.announced[tr] = struct{}{}
			if resp.TrackerID != "" {
				m.trackerIDs[tr] = resp.TrackerID
			}
			return resp, nil
		}
	}
	return tracker.AnnounceResponse{}, fmt.Errorf("all trackers failed: %w", errors.Join(errs...))
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
