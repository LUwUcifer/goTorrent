package torrent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"goTor/peer"
)

// SessionConfig describes one torrent to run.
type SessionConfig struct {
	TorrentPath string
	DownloadDir string

	// PortMin..PortMax is the range of TCP ports tried for incoming
	// connections. Both zero selects the traditional 6881-6889.
	PortMin, PortMax uint16

	// MaxPeers caps simultaneous peer connections; 0 selects the default.
	MaxPeers int
}

// SessionState is what the session is doing right now.
type SessionState int

const (
	StateDownloading SessionState = iota
	StateSeeding
	StatePausing // winding down; trackers are being told we are leaving
	StatePaused
	StateStopped
)

func (s SessionState) String() string {
	switch s {
	case StateDownloading:
		return "downloading"
	case StateSeeding:
		return "seeding"
	case StatePausing:
		return "pausing"
	case StatePaused:
		return "paused"
	case StateStopped:
		return "stopped"
	default:
		return fmt.Sprintf("state(%d)", int(s))
	}
}

// Progress is a point-in-time view of the session for a UI.
type Progress struct {
	Name     string
	InfoHash string
	State    SessionState

	TotalBytes int64
	DoneBytes  int64 // verified bytes, including data found on disk at startup

	PiecesDone  int
	PiecesTotal int

	Downloaded int64 // bytes received from peers since the program started
	Uploaded   int64 // bytes sent to peers since the program started

	DownRate float64 // bytes/s, smoothed
	UpRate   float64

	Peers int // established connections
	Port  uint16
}

// Session runs a single torrent: it owns the storage and piece state, and
// starts and stops the networking (trackers, peer connections, listener,
// downloader, uploader, choker) around them.
//
// Pausing tears the networking down completely, trackers hear "stopped", and
// resuming builds it again and re-announces. What was downloaded survives
// because the piece assembler and the files on disk stay open across a pause.
type Session struct {
	cfg    SessionConfig
	tor    *Torrent
	client *Client
	cache  *FileCache
	store  *Storage
	asm    *pieceAssembler

	mu     sync.Mutex
	run    *activation // nil while paused
	closed bool

	rates rateSampler
}

// activation is one running period between Resume and Pause.
type activation struct {
	cancel   context.CancelFunc
	done     chan struct{} // closed when everything has wound down
	pool     *connPool
	stopping atomic.Bool
}

// OpenSession reads the torrent, binds the listening port, opens storage, and
// hashes whatever is already on disk so an interrupted download resumes where
// it left off. onCheck, if non-nil, reports that hashing as (done, total)
// pieces. The returned session is paused; call Resume to start transferring.
func OpenSession(ctx context.Context, cfg SessionConfig, onCheck func(done, total int)) (*Session, error) {
	if cfg.TorrentPath == "" || cfg.DownloadDir == "" {
		return nil, errors.New("session: torrent path and download directory are required")
	}

	tor := NewTorrent()
	if err := tor.fileInfoParser(cfg.TorrentPath, cfg.DownloadDir); err != nil {
		return nil, fmt.Errorf("reading torrent: %w", err)
	}

	ports := Config{PortMin: cfg.PortMin, PortMax: cfg.PortMax}
	if ports == (Config{}) {
		ports = DefaultConfig()
	}
	client, err := NewClient(ports)
	if err != nil {
		return nil, fmt.Errorf("opening a listening port: %w", err)
	}

	cache := NewFileCache(0)
	store, err := OpenStorage(&tor.layout, tor.destPath, cache, StorageConfig{})
	if err != nil {
		cache.Close()
		_ = client.Close()
		return nil, err
	}

	asm, err := newPieceAssembler(&tor.layout, tor.localData.infoDict.pieces, store, 0)
	if err != nil {
		store.Close()
		cache.Close()
		_ = client.Close()
		return nil, err
	}

	s := &Session{cfg: cfg, tor: tor, client: client, cache: cache, store: store, asm: asm}

	if _, err := tor.resumeFromDisk(ctx, store, asm, onCheck); err != nil {
		s.closeResources()
		return nil, fmt.Errorf("checking existing files: %w", err)
	}
	return s, nil
}

// Resume starts (or restarts) transferring. It is a no-op if already running.
func (s *Session) Resume() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case s.closed:
		return errors.New("session: stopped")
	case s.run != nil && s.run.stopping.Load():
		return errors.New("session: still pausing")
	case s.run != nil:
		return nil
	}

	run, err := s.activate()
	if err != nil {
		return err
	}
	s.run = run
	slog.Info("session running", "torrent", s.name(), "port", s.client.Port())
	return nil
}

// Pause stops all network activity and waits for it to finish. That includes
// telling the trackers we are leaving, so it can take a few seconds. Calling it
// while already paused does nothing.
func (s *Session) Pause() {
	s.mu.Lock()
	run := s.run
	s.mu.Unlock()
	if run == nil {
		return
	}

	run.stopping.Store(true)
	run.cancel()
	<-run.done

	s.mu.Lock()
	if s.run == run {
		s.run = nil
	}
	s.mu.Unlock()
	slog.Info("session paused", "torrent", s.name())
}

// Stop pauses, then releases the files and the listening port. The session
// cannot be used afterwards. It is safe to call more than once.
func (s *Session) Stop() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true // no Resume can start new work from here on
	s.mu.Unlock()

	s.Pause()
	s.closeResources()
}

func (s *Session) closeResources() {
	s.store.Close()
	s.cache.Close()
	_ = s.client.Close()
}

// Snapshot reports current progress. It is cheap and safe to call from any
// goroutine; call it about once a second so the transfer rates are smooth.
func (s *Session) Snapshot() Progress {
	s.mu.Lock()
	run, closed := s.run, s.closed
	s.mu.Unlock()

	lay := &s.tor.layout
	down, up := s.tor.downloaded.Load(), s.tor.uploaded.Load()
	downRate, upRate := s.rates.sample(time.Now(), down, up)

	state := StateDownloading
	switch {
	case closed:
		state = StateStopped
	case run == nil:
		state = StatePaused
	case run.stopping.Load():
		state = StatePausing
	case s.tor.bytesLeft() == 0:
		state = StateSeeding
	}

	peers := 0
	if run != nil {
		peers = run.pool.Stats().Connected
	}

	return Progress{
		Name:        s.name(),
		InfoHash:    fmt.Sprintf("%x", s.tor.localData.infoHash[:]),
		State:       state,
		TotalBytes:  lay.totalLength,
		DoneBytes:   min(lay.totalLength, s.tor.verified.Load()),
		PiecesDone:  s.asm.CompleteCount(),
		PiecesTotal: lay.numPieces,
		Downloaded:  down,
		Uploaded:    up,
		DownRate:    downRate,
		UpRate:      upRate,
		Peers:       peers,
		Port:        s.client.Port(),
	}
}

func (s *Session) name() string {
	// Every file's path starts with the torrent's name.
	if files := s.tor.layout.files; len(files) > 0 && len(files[0].path) > 0 {
		return files[0].path[0]
	}
	return s.tor.torrName
}

// activate builds and starts the networking. The caller holds s.mu.
func (s *Session) activate() (*activation, error) {
	// The assembler is the source of truth for which pieces are done. A piece
	// that finished while the last activation was shutting down was recorded
	// there but its result was dropped, so rebuild the byte count from it.
	s.reconcileVerified()

	picker := newPiecePicker(s.tor.layout.numPieces, -1)
	picker.SyncVerified(s.asm)

	up := newUploader(s.tor, s.asm, s.store, uploaderConfig{})
	ch := newChoker(chokerConfig{Seeding: func() bool { return s.tor.bytesLeft() == 0 }})
	dl := newDownloader(s.tor, s.asm, picker, downloaderConfig{
		OnUploadEvent: func(ev peer.Event) {
			up.Handle(ev)
			ch.Handle(ev)
		},
	})

	pool, err := newConnPool(s.tor, dl, s.client.PeerID(), poolConfig{MaxPeers: s.cfg.MaxPeers})
	if err != nil {
		return nil, err
	}
	il, err := newInboundListener(s.client)
	if err != nil {
		return nil, err
	}
	if err := il.RegisterPool(pool); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	// The trackers get a context of their own. They send "stopped" when it is
	// cancelled, and that announce should carry the final uploaded/downloaded
	// counts, so it must go out only after the peers, uploader and listener have
	// finished. Cancelling cancel() stops those; the goroutine below cancels the
	// trackers once they are done.
	trackerCtx, stopTrackers := context.WithCancel(context.Background())

	trackersDone, err := s.tor.StartTrackers(trackerCtx, s.client)
	if err != nil {
		cancel()
		stopTrackers()
		return nil, fmt.Errorf("starting trackers: %w", err)
	}

	a := &activation{cancel: cancel, done: make(chan struct{}), pool: pool}

	// The downloader goes first: AddPeer, which the pool and listener end in,
	// waits for its loop.
	var wg sync.WaitGroup
	wg.Go(func() { dl.Run(ctx) })
	wg.Go(func() { up.Run(ctx) })
	wg.Go(func() { ch.Run(ctx) })
	wg.Go(func() { pool.Run(ctx) })
	wg.Go(func() { il.Run(ctx) })

	go func() {
		wg.Wait()      // peers, uploader and listener have all stopped
		stopTrackers() // now "stopped" goes out with the final counters
		<-trackersDone // wait for those announces to finish
		s.tor.trackers.Store(nil)
		close(a.done)
	}()
	return a, nil
}

// reconcileVerified recomputes the verified byte count from the assembler.
// Only call it while nothing is running.
func (s *Session) reconcileVerified() {
	var sum int64
	for i := range s.tor.layout.numPieces {
		if s.asm.Has(i) {
			sum += s.tor.layout.pieceSize(i)
		}
	}
	s.tor.verified.Store(sum)
}

// rateSampler turns the torrent's running byte counters into smoothed rates.
type rateSampler struct {
	mu       sync.Mutex
	last     time.Time
	lastDown int64
	lastUp   int64
	down, up float64
}

func (r *rateSampler) sample(now time.Time, down, up int64) (float64, float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.last.IsZero() {
		r.last, r.lastDown, r.lastUp = now, down, up
		return 0, 0
	}
	dt := now.Sub(r.last).Seconds()
	if dt < 0.4 {
		return r.down, r.up // too soon for a meaningful sample
	}

	const alpha = 0.3 // weight of the newest sample
	r.down = alpha*float64(down-r.lastDown)/dt + (1-alpha)*r.down
	r.up = alpha*float64(up-r.lastUp)/dt + (1-alpha)*r.up
	r.last, r.lastDown, r.lastUp = now, down, up
	return r.down, r.up
}
