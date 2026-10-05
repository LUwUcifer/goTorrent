package torrent

import (
	"context"
	"log/slog"
	"sync"

	"goTor/peer"
)

const (
	defaultUploadWorkers    = 4
	defaultMaxQueuedPerPeer = 64
	defaultMaxBadRequests   = 16

	// maxCancelMarks bounds the remembered cancels per peer. A cancel for a
	// request that was already served leaves a mark behind, and a peer could
	// otherwise grow the set without limit.
	maxCancelMarks = 256

	uploadQueueSize = 512
)

type uploaderConfig struct {
	// Workers is the number of goroutines reading blocks from disk.
	Workers int

	// MaxQueuedPerPeer caps one peer's requests waiting for a worker; further
	// requests are dropped, and the peer re-requests after its own timeout.
	MaxQueuedPerPeer int

	// MaxBadRequests is how many invalid requests get a peer dropped.
	MaxBadRequests int
}

func (c uploaderConfig) withDefaults() uploaderConfig {
	if c.Workers <= 0 {
		c.Workers = defaultUploadWorkers
	}
	if c.MaxQueuedPerPeer <= 0 {
		c.MaxQueuedPerPeer = defaultMaxQueuedPerPeer
	}
	if c.MaxBadRequests <= 0 {
		c.MaxBadRequests = defaultMaxBadRequests
	}
	return c
}

// upKey identifies one request exactly as the wire does, so a cancel matches
// only the request it withdraws.
type upKey struct{ index, begin, length uint32 }

type upPeer struct {
	queued    int // requests handed to the workers and not yet picked up
	bad       int
	cancelled map[upKey]struct{}
}

type serveJob struct {
	p   *peer.Peer
	key upKey
}

// uploader answers peers' block requests from storage.
//
// Wire it up by passing Handle as downloaderConfig.OnUploadEvent. The peer
// package already drops requests from peers we are choking and rejects ones
// with an out-of-range piece or an impossible length, so Handle sees only
// requests we are willing to serve, and checks the rest itself: that the range
// lies inside the piece and that we actually have the piece.
//
// Reading from disk can block, so Handle (which runs on the downloader's loop)
// only validates and queues. Workers do the reads and send the blocks. A peer
// that falls behind never holds a worker: sends are non-blocking, and a request
// that can't be queued is dropped, since the peer will ask again.
//
// Nothing is served to a peer until something unchokes it. That is the
// choking algorithm's job, and until it exists a peer has to be unchoked by
// hand with Peer.SetChoking(ctx, false).
type uploader struct {
	tor   *Torrent
	lay   *layout
	asm   *pieceAssembler
	store pieceReader
	cfg   uploaderConfig

	jobs chan serveJob

	mu    sync.Mutex
	peers map[*peer.Peer]*upPeer
}

func newUploader(tor *Torrent, asm *pieceAssembler, store pieceReader, cfg uploaderConfig) *uploader {
	return &uploader{
		tor:   tor,
		lay:   &tor.layout,
		asm:   asm,
		store: store,
		cfg:   cfg.withDefaults(),
		jobs:  make(chan serveJob, uploadQueueSize),
		peers: make(map[*peer.Peer]*upPeer),
	}
}

// Run starts the workers and blocks until ctx is cancelled.
func (u *uploader) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for range u.cfg.Workers {
		wg.Go(func() { u.worker(ctx) })
	}
	wg.Wait()
}

// Handle processes one peer event. It never blocks. Interested and
// not-interested are the choking algorithm's business and are ignored here.
func (u *uploader) Handle(ev peer.Event) {
	switch ev.Kind {
	case peer.EventRequest:
		u.onRequest(ev.Peer, ev.Request)
	case peer.EventCancel:
		u.onCancel(ev.Peer, ev.Request)
	}
}

func (u *uploader) onRequest(p *peer.Peer, r peer.Request) {
	index := int(r.Index)
	size := u.lay.pieceSize(index) // 0 if out of range
	begin, length := int64(r.Begin), int64(r.Length)

	var problem string
	switch {
	case size == 0:
		problem = "piece out of range"
	case length <= 0 || length > peer.MaxBlockSize:
		problem = "bad block length"
	case begin+length > size:
		problem = "range extends past the end of the piece"
	case !u.asm.Has(index):
		problem = "piece we have not verified"
	}

	key := upKey{r.Index, r.Begin, r.Length}

	u.mu.Lock()
	up := u.peerLocked(p)
	delete(up.cancelled, key) // a new request supersedes an earlier cancel of the same block

	if problem != "" {
		up.bad++
		bad := up.bad
		u.mu.Unlock()

		slog.Debug("rejected block request",
			"peer", p.RemoteAddr(), "reason", problem, "piece", r.Index, "begin", r.Begin, "length", r.Length)
		if bad >= u.cfg.MaxBadRequests {
			slog.Warn("dropping peer for repeated invalid requests", "peer", p.RemoteAddr(), "count", bad)
			_ = p.Close()
		}
		return
	}

	if up.queued >= u.cfg.MaxQueuedPerPeer {
		u.mu.Unlock()
		slog.Debug("too many queued requests; dropping one", "peer", p.RemoteAddr())
		return
	}
	up.queued++
	u.mu.Unlock()

	select {
	case u.jobs <- serveJob{p: p, key: key}:
	default:
		u.mu.Lock()
		up.queued--
		u.mu.Unlock()
		slog.Debug("upload queue full; dropping a request", "peer", p.RemoteAddr())
	}
}

// onCancel withdraws a request. If a worker hasn't picked it up yet it will be
// skipped, saving the disk read and the bandwidth.
func (u *uploader) onCancel(p *peer.Peer, r peer.Request) {
	u.mu.Lock()
	defer u.mu.Unlock()

	up := u.peerLocked(p)
	if len(up.cancelled) >= maxCancelMarks {
		clear(up.cancelled)
	}
	up.cancelled[upKey{r.Index, r.Begin, r.Length}] = struct{}{}
}

// peerLocked returns the peer's state, creating it on first sight and arranging
// for it to be forgotten when the connection ends.
func (u *uploader) peerLocked(p *peer.Peer) *upPeer {
	up := u.peers[p]
	if up == nil {
		up = &upPeer{cancelled: make(map[upKey]struct{})}
		u.peers[p] = up

		go func() {
			<-p.Done()
			u.mu.Lock()
			delete(u.peers, p)
			u.mu.Unlock()
		}()
	}
	return up
}

func (u *uploader) worker(ctx context.Context) {
	buf := make([]byte, peer.MaxBlockSize) // one read buffer per worker; NewPiece copies out of it
	for {
		select {
		case <-ctx.Done():
			return
		case j := <-u.jobs:
			u.serve(j, buf)
		}
	}
}

func (u *uploader) serve(j serveJob, buf []byte) {
	if u.dequeue(j) {
		return // cancelled, or the peer is gone
	}
	p := j.p

	// BEP 3: choking a peer discards the requests it has pending.
	if p.State().AmChoking {
		return
	}
	select {
	case <-p.Done():
		return
	default:
	}

	block := buf[:j.key.length]
	n, err := u.store.ReadPieceAt(block, int(j.key.index), int64(j.key.begin))
	if err != nil || n != len(block) {
		slog.Warn("could not read a block we have verified",
			"piece", j.key.index, "begin", j.key.begin, "length", j.key.length, "error", err)
		return
	}

	if err := p.TrySend(peer.NewPiece(j.key.index, j.key.begin, block)); err != nil {
		slog.Debug("not sending block", "peer", p.RemoteAddr(), "reason", err)
		return
	}
	u.tor.uploaded.Add(int64(len(block))) // counted when queued, not when written
}

// dequeue accounts for a job leaving the queue. It reports whether the job
// should be skipped because it was cancelled or its peer has gone.
func (u *uploader) dequeue(j serveJob) (skip bool) {
	u.mu.Lock()
	defer u.mu.Unlock()

	up := u.peers[j.p]
	if up == nil {
		return true
	}
	up.queued--
	if _, ok := up.cancelled[j.key]; ok {
		delete(up.cancelled, j.key)
		return true
	}
	return false
}
