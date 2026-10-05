package torrent

import (
	"cmp"
	"context"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"goTor/peer"
)

const (
	defaultChokeInterval   = 10 * time.Second
	defaultUnchokeSlots    = 4
	defaultOptimisticEvery = 30 * time.Second

	// minRechokeGap rate-limits the early rechokes triggered by interest
	// changes, so a peer toggling interested/not-interested can't make us
	// rerun the algorithm in a loop.
	minRechokeGap = time.Second
)

// chokablePeer is what the choker needs from a connection. *peer.Peer
// satisfies it; tests use fakes.
type chokablePeer interface {
	State() peer.State
	DownloadRate() float64 // bytes/s they send us
	UploadRate() float64   // bytes/s we send them
	SetChoking(ctx context.Context, choke bool) error
	Done() <-chan struct{}
}

type chokerConfig struct {
	// Interval is how often the regular unchoke set is recomputed.
	Interval time.Duration

	// Slots is how many interested peers are unchoked by rate. One more is
	// unchoked optimistically, so up to Slots+1 peers are unchoked at once.
	Slots int

	// OptimisticEvery is how long the optimistic unchoke stays on one peer.
	OptimisticEvery time.Duration

	// Seeding reports whether the download is complete. Nil means never.
	Seeding func() bool
}

func (c chokerConfig) withDefaults() chokerConfig {
	if c.Interval <= 0 {
		c.Interval = defaultChokeInterval
	}
	if c.Slots <= 0 {
		c.Slots = defaultUnchokeSlots
	}
	if c.OptimisticEvery <= 0 {
		c.OptimisticEvery = defaultOptimisticEvery
	}
	return c
}

type chokeCand struct {
	p    chokablePeer
	rate float64
}

// choker decides which peers we upload to.
//
// Every Interval it unchokes the Slots interested peers with the best rate and
// chokes everyone else:
//   - while downloading, rank by how fast they send to us (tit-for-tat:
//     reward whoever reciprocates);
//   - once seeding there is nothing to reciprocate, so rank by how fast we
//     can push data to them, which keeps upload capacity busy.
//
// One further interested peer is unchoked optimistically and kept for
// OptimisticEvery, then replaced. That gives newcomers, who have nothing to
// trade yet, a chance to prove themselves, and finds faster partners than the
// regular set contains.
//
// A peer enters the choker when it first sends interested or not-interested,
// since a peer that has never shown interest stays choked anyway. Wire it up
// next to the uploader through downloaderConfig.OnUploadEvent:
//
//	OnUploadEvent: func(ev peer.Event) { up.Handle(ev); ch.Handle(ev) }
//
// It does not know which peers have stopped sending to us (snubbing), so a
// peer that stalls while we download can keep a regular slot until its rate
// decays out of the 20-second window.
type choker struct {
	cfg  chokerConfig
	kick chan struct{}

	mu    sync.Mutex
	peers map[chokablePeer]struct{}

	// Owned by whoever calls rechoke (Run, or a test); not shared.
	optimistic      chokablePeer
	optimisticSince time.Time
	lastRun         time.Time
}

func newChoker(cfg chokerConfig) *choker {
	return &choker{
		cfg:   cfg.withDefaults(),
		kick:  make(chan struct{}, 1),
		peers: make(map[chokablePeer]struct{}),
	}
}

// Handle notes a peer whose interest changed and asks for a prompt rechoke, so
// the first interested peer doesn't wait out a whole interval. It never blocks.
func (c *choker) Handle(ev peer.Event) {
	if ev.Peer == nil {
		return
	}
	switch ev.Kind {
	case peer.EventInterested, peer.EventNotInterested:
		c.track(ev.Peer)
		select {
		case c.kick <- struct{}{}:
		default:
		}
	}
}

// track adds a peer and forgets it again when its connection ends.
func (c *choker) track(p chokablePeer) {
	c.mu.Lock()
	if _, ok := c.peers[p]; ok {
		c.mu.Unlock()
		return
	}
	c.peers[p] = struct{}{}
	c.mu.Unlock()

	go func() {
		<-p.Done()
		c.mu.Lock()
		delete(c.peers, p)
		c.mu.Unlock()
	}()
}

// Run rechokes every Interval until ctx is cancelled.
func (c *choker) Run(ctx context.Context) {
	tick := time.NewTicker(c.cfg.Interval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-tick.C:
			c.rechoke(ctx, now)
		case <-c.kick:
			if now := time.Now(); now.Sub(c.lastRun) >= minRechokeGap {
				c.rechoke(ctx, now)
			}
		}
	}
}

// rechoke recomputes who is unchoked and tells every tracked peer. Telling a
// peer something it already knows is free: SetChoking only sends on a change.
func (c *choker) rechoke(ctx context.Context, now time.Time) {
	c.mu.Lock()
	all := make([]chokablePeer, 0, len(c.peers))
	for p := range c.peers {
		all = append(all, p)
	}
	c.mu.Unlock()

	seeding := c.cfg.Seeding != nil && c.cfg.Seeding()

	var cands []chokeCand
	for _, p := range all {
		if !p.State().PeerInterested {
			continue
		}
		rate := p.DownloadRate()
		if seeding {
			rate = p.UploadRate()
		}
		cands = append(cands, chokeCand{p: p, rate: rate})
	}

	// Shuffle first so peers with equal rates (everyone, at the start) are
	// picked fairly instead of by map order, then rank best first.
	rand.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] })
	slices.SortStableFunc(cands, func(a, b chokeCand) int { return cmp.Compare(b.rate, a.rate) })

	keep := make(map[chokablePeer]bool, c.cfg.Slots+1)
	for i := 0; i < len(cands) && i < c.cfg.Slots; i++ {
		keep[cands[i].p] = true
	}
	c.pickOptimistic(cands, keep, now)
	if c.optimistic != nil {
		keep[c.optimistic] = true
	}

	for _, p := range all {
		_ = p.SetChoking(ctx, !keep[p])
	}
	c.lastRun = now
}

// pickOptimistic keeps the current optimistic peer while it is still
// interested, outside the regular set and within its time, and otherwise picks
// a new one at random from the interested peers left over.
func (c *choker) pickOptimistic(cands []chokeCand, keep map[chokablePeer]bool, now time.Time) {
	var rest []chokablePeer
	currentOK := false
	for _, cd := range cands {
		if keep[cd.p] {
			continue
		}
		rest = append(rest, cd.p)
		if cd.p == c.optimistic {
			currentOK = true
		}
	}

	expired := c.optimistic == nil || now.Sub(c.optimisticSince) >= c.cfg.OptimisticEvery
	if currentOK && !expired {
		return
	}

	// Rotating: prefer someone other than the peer we just had.
	if currentOK && len(rest) > 1 {
		rest = slices.DeleteFunc(rest, func(p chokablePeer) bool { return p == c.optimistic })
	}
	if len(rest) == 0 {
		c.optimistic = nil
		return
	}
	c.optimistic = rest[rand.IntN(len(rest))]
	c.optimisticSince = now
}
