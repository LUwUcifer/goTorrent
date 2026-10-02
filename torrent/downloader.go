package torrent

import (
	"context"
	"errors"
	"log/slog"
	"net/netip"
	"runtime"
	"slices"
	"sync"
	"time"

	"goTor/peer"
)

var ErrDownloaderStopped = errors.New("downloader: stopped")

const (
	maxUnsolicited = 256 // blocks we never asked for (or cancelled) before dropping a peer
	bufferPause    = time.Second
	storagePause   = 5 * time.Second
	maxCancelled   = 1024 // remembered cancelled requests per peer
)

type downloaderConfig struct {
	// Pipeline is how many block requests to keep outstanding per unchoked
	// peer. 5-10 keeps a typical link full without hoarding blocks that a
	// faster peer could serve.
	Pipeline int

	// RequestTimeout is how long a request may stay unanswered before the
	// peer is considered stalled.
	RequestTimeout time.Duration

	// SnubFor is how long a stalled peer gets no new work.
	SnubFor time.Duration

	// MaxTimeouts is how many consecutive stalls get a peer dropped. Any
	// answered request resets the count.
	MaxTimeouts int

	// MaxStrikes is how many suspected-corruption strikes get an address
	// banned. A piece with a single source bans it outright.
	MaxStrikes int

	// EndgameBlocks is the number of missing blocks at or below which, once
	// every remaining piece is already assigned, idle peers also request
	// blocks that other peers are fetching. 0 means 32; negative disables
	// endgame mode.
	EndgameBlocks int

	// EndgameMaxRequesters caps how many peers may request one block at the
	// same time. 0 means 3.
	EndgameMaxRequesters int

	Tick    time.Duration // how often timeouts are checked
	Workers int           // goroutines hashing and writing completed pieces
}

func (c downloaderConfig) withDefaults() downloaderConfig {
	if c.Pipeline <= 0 {
		c.Pipeline = 8
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 30 * time.Second
	}
	if c.SnubFor <= 0 {
		c.SnubFor = time.Minute
	}
	if c.MaxTimeouts <= 0 {
		c.MaxTimeouts = 3
	}
	if c.MaxStrikes <= 0 {
		c.MaxStrikes = 3
	}
	if c.Tick <= 0 {
		c.Tick = time.Second
	}
	if c.Workers <= 0 {
		c.Workers = min(runtime.NumCPU(), 4)
	}
	if c.EndgameBlocks == 0 {
		c.EndgameBlocks = 32
	}
	if c.EndgameMaxRequesters <= 0 {
		c.EndgameMaxRequesters = 3
	}
	return c
}

// peerKey is the identity used with the piece picker and as the "source" of
// blocks in the assembler.
func peerKey(p *peer.Peer) string { return p.RemoteAddr().String() }

type blockRef struct{ piece, block int }

type reqKey struct{ index, begin uint32 }

type inflightReq struct {
	sent   time.Time
	length uint32
}

// dlPeer is the downloader's view of one connection. It is touched only by
// the downloader's loop goroutine.
type dlPeer struct {
	p    *peer.Peer
	key  string
	addr netip.Addr // invalid if the remote address didn't parse

	queue    []blockRef // blocks of owned pieces not yet requested
	inflight map[reqKey]inflightReq

	timeouts     int
	unsolicited  int
	snubbedUntil time.Time
	dead         bool

	// cancelled remembers requests we withdrew, with their length, so a block
	// already on the wire when the cancel went out is accepted, not punished.
	cancelled map[reqKey]uint32
}

type blockJob struct {
	p     *peer.Peer
	key   string
	index int
	begin int64
	data  []byte
}

type blockOutcome struct {
	job blockJob
	res BlockResult
	err error
}

// downloader schedules block requests across peers.
//
// Concurrency: one loop goroutine (Run) owns all per-peer state and receives
// every peer event, so there is no locking on that state. The one expensive
// step, hashing and writing a completed piece inside AddBlock, is handed to a
// small worker pool so it never stalls the loop; results come back on a
// channel.
type downloader struct {
	tor    *Torrent
	lay    *layout
	asm    *pieceAssembler
	picker *piecePicker
	cfg    downloaderConfig

	events  chan peer.Event
	cmds    chan func()
	stop    chan struct{} // closed when Run returns
	jobs    chan blockJob
	results chan blockOutcome

	done     chan struct{} // closed when every piece is verified
	doneOnce sync.Once

	// Loop-owned.
	ctx         context.Context
	peers       map[*peer.Peer]*dlPeer
	pausedUntil time.Time // no new pieces are assigned before this
	strikes     map[netip.Addr]int

	endgame     bool  // idle peers may duplicate other peers' requests
	endgameSeen bool  // endgame has been on at some point: duplicates may exist
	tail        []int // unverified pieces, cached while few remain

	banMu  sync.RWMutex
	banned map[netip.Addr]struct{}
}

func newDownloader(tor *Torrent, asm *pieceAssembler, picker *piecePicker, cfg downloaderConfig) *downloader {
	cfg = cfg.withDefaults()
	return &downloader{
		tor:     tor,
		lay:     &tor.layout,
		asm:     asm,
		picker:  picker,
		cfg:     cfg,
		events:  make(chan peer.Event, 256),
		cmds:    make(chan func()),
		stop:    make(chan struct{}),
		jobs:    make(chan blockJob, 64),
		results: make(chan blockOutcome, 64),
		done:    make(chan struct{}),
		peers:   make(map[*peer.Peer]*dlPeer),
		strikes: make(map[netip.Addr]int),
		banned:  make(map[netip.Addr]struct{}),
	}
}

// Events is the channel to pass as peer.Config.Events for every peer given to
// AddPeer.
func (d *downloader) Events() chan<- peer.Event { return d.events }

// Done is closed once every piece is verified.
func (d *downloader) Done() <-chan struct{} { return d.done }

// IsBanned reports whether an address was banned for sending bad data. The
// dialer and the listener should refuse banned addresses.
func (d *downloader) IsBanned(addr netip.Addr) bool {
	d.banMu.RLock()
	defer d.banMu.RUnlock()
	_, ok := d.banned[addr.Unmap()]
	return ok
}

// AddPeer starts managing a handshaken peer. Call it before the peer's Run so
// our bitfield is the first thing it sends. Run must already be running.
// The peer is forgotten automatically when its connection ends.
func (d *downloader) AddPeer(p *peer.Peer) error {
	return d.do(func() {
		dp := &dlPeer{p: p, key: peerKey(p), inflight: make(map[reqKey]inflightReq)}
		if ap, err := netip.ParseAddrPort(dp.key); err == nil {
			dp.addr = ap.Addr().Unmap()
		}
		if dp.addr.IsValid() && d.IsBanned(dp.addr) {
			_ = p.Close()
			return
		}

		d.peers[p] = dp
		d.sendBitfield(dp)

		go func() {
			<-p.Done()
			_ = d.do(func() {
				d.remove(dp)
				d.fillAll()
			})
		}()
	})
}

func (d *downloader) do(f func()) error {
	select {
	case d.cmds <- f:
		return nil
	case <-d.stop:
		return ErrDownloaderStopped
	}
}

// Run is the scheduling loop. It returns when ctx is cancelled.
func (d *downloader) Run(ctx context.Context) {
	d.ctx = ctx
	defer close(d.stop)

	for range d.cfg.Workers {
		go d.worker(ctx)
	}

	tick := time.NewTicker(d.cfg.Tick)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-d.events:
			d.onEvent(ev)
		case r := <-d.results:
			d.onResult(r)
		case f := <-d.cmds:
			f()
		case now := <-tick.C:
			d.onTick(now)
		}
	}
}

func (d *downloader) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case j := <-d.jobs:
			select {
			case d.results <- d.process(j):
			case <-ctx.Done():
				return
			}
		}
	}
}

func (d *downloader) process(j blockJob) blockOutcome {
	res, err := d.asm.AddBlock(j.index, j.begin, j.data, j.key)
	return blockOutcome{job: j, res: res, err: err}
}

// ---- events from peers ----

func (d *downloader) onEvent(ev peer.Event) {
	dp := d.peers[ev.Peer]
	if dp == nil || dp.dead {
		return
	}
	switch ev.Kind {
	case peer.EventBitfield:
		d.picker.PeerBitfield(dp.key, ev.Peer.Bitfield())
		d.refresh(dp)

	case peer.EventHave:
		d.picker.PeerHave(dp.key, int(ev.Index))
		d.refresh(dp)

	case peer.EventUnchoke:
		d.fill(dp)

	case peer.EventChoke:
		// A choke makes the remote discard every request it hasn't served
		// (BEP 3), so there is nothing to cancel, only bookkeeping to undo.
		d.abandon(dp, false)
		d.fillAll()

	case peer.EventPiece:
		d.onBlock(dp, ev.Piece)
	}
	// Interested, not-interested, request and cancel concern uploading.
}

func (d *downloader) onBlock(dp *dlPeer, pc peer.Piece) {
	k := reqKey{pc.Index, pc.Begin}

	var want uint32
	answered := false
	if req, ok := dp.inflight[k]; ok {
		want, answered = req.length, true
	} else if n, ok := dp.cancelled[k]; ok {
		// We withdrew this request (a stall, or a duplicate won elsewhere)
		// but the bytes were already on the wire. They're good data; take them.
		want = n
		delete(dp.cancelled, k)
	} else {
		if dp.unsolicited++; dp.unsolicited > maxUnsolicited {
			d.dropPeer(dp, "too many unsolicited blocks")
		}
		return
	}
	if uint32(len(pc.Block)) != want {
		d.dropPeer(dp, "block has the wrong length")
		return
	}

	if answered {
		delete(dp.inflight, k)
		dp.timeouts = 0 // a late block doesn't prove the peer is responsive
	}
	d.tor.downloaded.Add(int64(len(pc.Block)))

	// Other peers may be fetching the same block: duplicates in endgame, or
	// the re-request that followed a stall (when this is a late arrival).
	// Outside those cases there's nobody to cancel, so skip the scan.
	freed := false
	if d.endgameSeen || !answered {
		freed = d.cancelElsewhere(dp, k)
	}

	job := blockJob{p: dp.p, key: dp.key, index: int(pc.Index), begin: int64(pc.Begin), data: pc.Block}
	select {
	case d.jobs <- job:
	default: // pool saturated: do it inline rather than block
		d.onResult(d.process(job))
	}

	d.fill(dp) // refill the slot now, without waiting for hashing
	if freed {
		d.fillAll() // peers whose duplicate was cancelled have a free slot
	}
}

func (d *downloader) onResult(r blockOutcome) {
	idx := r.job.index
	dp := d.peers[r.job.p] // nil if the peer left while the block was in a worker

	switch {
	case errors.Is(r.err, ErrBufferFull):
		// No room to start another piece. Hand the piece back and stop
		// starting new ones until a piece completes and frees memory.
		d.pause(bufferPause)
		if dp != nil {
			d.dropPiece(dp, idx)
		}

	case errors.Is(r.err, ErrBadBlock):
		// Blocks are only requested at valid offsets and length-checked on
		// arrival, so this is our bug, not the peer's.
		slog.Error("assembler rejected a block we requested", "piece", idx, "begin", r.job.begin, "error", r.err)
		if dp != nil {
			d.dropPiece(dp, idx)
		}

	case r.err != nil:
		// The piece verified but couldn't be written. The assembler already
		// discarded it; fetch it again once the disk has had a moment.
		slog.Error("storing piece failed; will retry", "piece", idx, "error", r.err)
		d.picker.Reset(idx)
		d.pause(storagePause)

	default:
		switch r.res.Status {
		case PieceComplete:
			d.onPieceComplete(idx)
		case PieceCorrupt:
			d.onCorrupt(idx, r.res.Sources)
		}
	}

	if dp != nil {
		d.fill(dp)
	}
}

func (d *downloader) onPieceComplete(idx int) {
	d.picker.Verified(idx)
	d.tor.pieceVerified(idx)
	d.pausedUntil = time.Time{} // buffer memory was just freed
	d.evalEndgame()

	for _, dp := range d.peers {
		if !dp.p.Has(idx) {
			_ = dp.p.TrySend(peer.NewHave(uint32(idx)))
		}
		d.refresh(dp) // we may no longer need anything from this peer
	}

	if d.asm.Done() {
		slog.Info("download complete", "pieces", d.lay.numPieces)
		d.doneOnce.Do(func() { close(d.done) })
	}
}

func (d *downloader) onCorrupt(idx int, sources []string) {
	d.picker.Reset(idx) // the assembler already discarded the buffer
	slog.Warn("piece failed verification", "piece", idx, "sources", sources)

	if len(sources) == 1 {
		// Only one peer touched this piece, so it's the one lying.
		d.ban(sources[0], "sent a corrupt piece")
	} else {
		// Can't tell who is at fault; make everyone involved a suspect.
		for _, s := range sources {
			d.strike(s)
		}
	}
	d.fillAll()
}

// ---- scheduling ----

// refresh updates our interest in a peer and starts requesting if it's useful.
func (d *downloader) refresh(dp *dlPeer) {
	if dp.dead {
		return
	}
	want := d.picker.Interested(dp.key)
	if err := dp.p.SetInterested(d.ctx, want); err != nil {
		return // connection is going away; its Done handler cleans up
	}
	if want {
		d.fill(dp)
	}
}

// fill tops the peer's pipeline up to cfg.Pipeline outstanding requests.
func (d *downloader) fill(dp *dlPeer) {
	if dp.dead {
		return
	}
	now := time.Now()
	if now.Before(dp.snubbedUntil) || dp.p.State().PeerChoking {
		return
	}

	for len(dp.inflight) < d.cfg.Pipeline {
		if len(dp.queue) == 0 {
			if d.nextPiece(dp, now) {
				continue // the new piece may have contributed no blocks
			}
			// Nothing left to assign. In endgame, help with blocks that
			// other peers are slow to deliver.
			if !d.endgame || !d.duplicateBlock(dp) {
				return
			}
			continue
		}

		b := dp.queue[0]
		begin := uint32(int64(b.block) * blockSize)
		n := uint32(d.lay.blockLen(b.piece, b.block))
		if err := dp.p.TrySend(peer.NewRequest(uint32(b.piece), begin, n)); err != nil {
			return // queue full or closed; the next event or tick retries
		}
		dp.queue = dp.queue[1:]
		dp.inflight[reqKey{uint32(b.piece), begin}] = inflightReq{sent: now, length: n}
	}
}

// nextPiece takes a piece from the picker and queues its missing blocks. It
// returns false when there is nothing to assign.
func (d *downloader) nextPiece(dp *dlPeer, now time.Time) bool {
	if now.Before(d.pausedUntil) {
		return false
	}
	idx, ok := d.picker.Pick(dp.key)
	if !ok {
		return false
	}
	if d.asm.Has(idx) { // picker and assembler disagree; trust the assembler
		d.picker.Verified(idx)
		return true
	}
	// MissingBlocks skips blocks already buffered, so a piece resumed after
	// its owner left costs only what's actually missing.
	for _, b := range d.asm.MissingBlocks(idx) {
		dp.queue = append(dp.queue, blockRef{piece: idx, block: b})
	}
	return true
}

func (d *downloader) fillAll() {
	for _, dp := range d.peers {
		d.fill(dp)
	}
}

func (d *downloader) pause(dur time.Duration) {
	if until := time.Now().Add(dur); until.After(d.pausedUntil) {
		d.pausedUntil = until
	}
}

// abandon voids everything a peer has outstanding and returns its pieces to
// the picker, keeping any blocks already buffered. With sendCancel, the
// remote is told to forget its queued requests as well.
func (d *downloader) abandon(dp *dlPeer, sendCancel bool) {
	if sendCancel {
		for k, r := range dp.inflight {
			d.cancelReq(dp, k, r)
		}
	}
	clear(dp.inflight)
	dp.queue = nil
	d.picker.ReleaseAll(dp.key)
}

// dropPiece takes one piece away from a peer.
func (d *downloader) dropPiece(dp *dlPeer, idx int) {
	kept := dp.queue[:0]
	for _, b := range dp.queue {
		if b.piece != idx {
			kept = append(kept, b)
		}
	}
	dp.queue = kept

	for k, r := range dp.inflight {
		if int(k.index) == idx {
			d.cancelReq(dp, k, r)
		}
	}

	if len(d.asm.MissingBlocks(idx)) == d.lay.numBlocks(idx) {
		d.picker.Reset(idx) // nothing buffered, so no reason to favour it
	} else {
		d.picker.Release(idx)
	}
}

// ---- timeouts ----

func (d *downloader) onTick(now time.Time) {
	d.evalEndgame()
	released := false
	for _, dp := range d.peers {
		if d.expire(dp, now) {
			released = true
		}
		d.fill(dp) // also retries after pauses and snubs
	}
	if released {
		d.fillAll()
	}
}

// expire handles a peer whose oldest request has gone unanswered too long. It
// reports whether the peer's pieces were released.
func (d *downloader) expire(dp *dlPeer, now time.Time) bool {
	if dp.dead {
		return false
	}
	stalled := false
	for _, r := range dp.inflight {
		if now.Sub(r.sent) >= d.cfg.RequestTimeout {
			stalled = true
			break
		}
	}
	if !stalled {
		return false
	}

	dp.timeouts++
	slog.Warn("peer stalled", "peer", dp.key, "timeouts", dp.timeouts, "outstanding", len(dp.inflight))

	if dp.timeouts >= d.cfg.MaxTimeouts {
		d.dropPeer(dp, "repeatedly stalled")
		return true
	}

	// Cancel everything, free its pieces for other peers (buffered blocks
	// are kept), and stop feeding it for a while.
	d.abandon(dp, true)
	dp.snubbedUntil = now.Add(d.cfg.SnubFor)
	return true
}

// ---- dropping and banning ----

// remove forgets a peer. It is idempotent.
func (d *downloader) remove(dp *dlPeer) {
	if d.peers[dp.p] != dp {
		return
	}
	dp.dead = true
	delete(d.peers, dp.p)
	d.picker.PeerGone(dp.key) // releases its pieces, keeping their progress
}

func (d *downloader) dropPeer(dp *dlPeer, reason string) {
	slog.Warn("dropping peer", "peer", dp.key, "reason", reason)
	_ = dp.p.Close()
	d.remove(dp)
}

// strike counts a suspicion against the source's address and bans it at
// cfg.MaxStrikes.
func (d *downloader) strike(key string) {
	ap, err := netip.ParseAddrPort(key)
	if err != nil {
		return
	}
	addr := ap.Addr().Unmap()
	d.strikes[addr]++
	if d.strikes[addr] >= d.cfg.MaxStrikes {
		d.ban(key, "too many suspected corrupt pieces")
	}
}

// ban refuses an address for the rest of the session and disconnects it. The
// ban is by IP, not ip:port, so reconnecting from another port doesn't help.
func (d *downloader) ban(key, reason string) {
	var addr netip.Addr
	if ap, err := netip.ParseAddrPort(key); err == nil {
		addr = ap.Addr().Unmap()
		d.banMu.Lock()
		d.banned[addr] = struct{}{}
		d.banMu.Unlock()
		slog.Warn("banned peer address", "addr", addr, "reason", reason)
	}

	for _, dp := range d.peers {
		if dp.key == key || (addr.IsValid() && dp.addr == addr) {
			d.dropPeer(dp, reason)
		}
	}
}

func (d *downloader) sendBitfield(dp *dlPeer) {
	if d.asm.CompleteCount() == 0 {
		return
	}
	bf, err := peer.ParseBitfield(d.asm.BitfieldBytes(), d.lay.numPieces)
	if err != nil {
		slog.Error("building our bitfield", "error", err)
		return
	}
	if err := dp.p.TrySend(peer.NewBitfieldMessage(bf)); err != nil {
		slog.Debug("sending bitfield", "peer", dp.key, "error", err)
	}
}

// ---- cancelling ----

// cancelReq withdraws one outstanding request: it tells the remote, forgets the
// request, and remembers it so a block that was already in flight is accepted.
func (d *downloader) cancelReq(dp *dlPeer, k reqKey, r inflightReq) {
	_ = dp.p.TrySend(peer.NewCancel(k.index, k.begin, r.length))
	delete(dp.inflight, k)

	if dp.cancelled == nil {
		dp.cancelled = make(map[reqKey]uint32)
	}
	if len(dp.cancelled) >= maxCancelled {
		clear(dp.cancelled) // bounded; a peer can't grow this by stalling
	}
	dp.cancelled[k] = r.length
}

// cancelElsewhere withdraws every other peer's claim on block k, which just
// arrived from "from": outstanding requests are cancelled and queued ones are
// dropped. It reports whether any peer was affected.
func (d *downloader) cancelElsewhere(from *dlPeer, k reqKey) bool {
	piece, blk := int(k.index), int(int64(k.begin)/blockSize)
	affected := false

	for _, q := range d.peers {
		if q == from || q.dead {
			continue
		}
		if r, ok := q.inflight[k]; ok {
			d.cancelReq(q, k, r)
			affected = true
		}
		n := len(q.queue)
		q.queue = slices.DeleteFunc(q.queue, func(b blockRef) bool {
			return b.piece == piece && b.block == blk
		})
		if len(q.queue) != n {
			affected = true
		}
	}
	return affected
}

// ---- endgame ----

// evalEndgame turns endgame mode on or off. It runs on every tick and after
// every verified piece.
func (d *downloader) evalEndgame() {
	on := d.cfg.EndgameBlocks > 0 && d.inEndgame()

	switch {
	case on && !d.endgame:
		slog.Info("entering endgame", "piecesLeft", d.lay.numPieces-d.asm.CompleteCount())
		d.endgameSeen = true
	case !on && d.endgame:
		slog.Info("leaving endgame")
	}
	d.endgame = on
}

// inEndgame reports whether every unverified piece is already assigned to a
// peer and only a few blocks are still missing. A piece nobody owns means
// ordinary scheduling still has work to hand out, so duplicating would only
// waste bandwidth.
func (d *downloader) inEndgame() bool {
	remaining := d.lay.numPieces - d.asm.CompleteCount()
	// Every unverified piece is missing at least one block, so this is a cheap
	// way to rule out most of a download without scanning anything.
	if remaining <= 0 || remaining > d.cfg.EndgameBlocks {
		return false
	}
	if _, _, unassigned := d.picker.Counts(); unassigned > 0 {
		return false
	}

	// Only a handful of pieces remain; cache them so a tick doesn't rescan the
	// whole torrent. Completion is monotonic, so a length change is the signal.
	if len(d.tail) != remaining {
		d.tail = d.tail[:0]
		for i := 0; i < d.lay.numPieces; i++ {
			if !d.asm.Has(i) {
				d.tail = append(d.tail, i)
			}
		}
	}
	blocks := 0
	for _, i := range d.tail {
		blocks += len(d.asm.MissingBlocks(i))
	}
	return blocks > 0 && blocks <= d.cfg.EndgameBlocks
}

// duplicateBlock queues, for dp, a block that other peers hold: either
// requested from them already or waiting in their queues. It prefers blocks
// with the fewest requesters, so idle peers spread across the remaining work
// rather than all chasing the same block, and skips blocks dp's remote lacks
// or already has at the cap. It reports whether it queued something.
func (d *downloader) duplicateBlock(dp *dlPeer) bool {
	requesters := make(map[reqKey]int) // 0 = queued by its owner, not yet requested
	refs := make(map[reqKey]blockRef)

	for _, q := range d.peers {
		if q == dp || q.dead {
			continue
		}
		for k := range q.inflight {
			requesters[k]++
			refs[k] = blockRef{piece: int(k.index), block: int(int64(k.begin) / blockSize)}
		}
		for _, b := range q.queue {
			k := reqKey{uint32(b.piece), uint32(int64(b.block) * blockSize)}
			if _, ok := requesters[k]; !ok {
				requesters[k] = 0
				refs[k] = b
			}
		}
	}

	// Never duplicate onto ourselves.
	for k := range dp.inflight {
		delete(requesters, k)
	}
	for _, b := range dp.queue {
		delete(requesters, reqKey{uint32(b.piece), uint32(int64(b.block) * blockSize)})
	}

	var best reqKey
	bestN := -1
	for k, n := range requesters {
		if n >= d.cfg.EndgameMaxRequesters || !dp.p.Has(int(k.index)) {
			continue
		}
		if bestN < 0 || n < bestN {
			best, bestN = k, n
		}
	}
	if bestN < 0 {
		return false
	}
	dp.queue = append(dp.queue, refs[best])
	return true
}
