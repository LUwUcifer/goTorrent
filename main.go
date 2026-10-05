package main

// Command goTorrent downloads one torrent, shows its progress, and keeps
// seeding until told to quit.
//
//	goTorrent [options] path/to/file.torrent
//
// While it runs: p pauses or resumes, q quits. Ctrl-C also quits.

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"goTor/torrent"
)

const (
	defaultDownloadDir = "downloads"
	defaultPort        = 6881
	refreshEvery       = 500 * time.Millisecond
)

type options struct {
	torrentPath string
	outputDir   string
	port        int
	maxPeers    int
	logFile     string
	debug       bool
}

const usage = `Usage: goTorrent [options] <file.torrent>

Downloads the torrent, then keeps seeding until you quit.

Options:
  -o dir         download directory (default ./downloads)
  -port n        first of nine TCP ports tried for incoming connections (default 6881)
  -max-peers n   maximum simultaneous peers (default 40)
  -log file      write logs to file (default: no logs)
  -debug         verbose logging; goes to goTorrent.log unless -log is given

While running:  p  pause / resume     q  quit     (Ctrl-C also quits)
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func parseArgs(args []string, stderr io.Writer) (options, error) {
	var o options
	fs := flag.NewFlagSet("goTorrent", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	fs.StringVar(&o.outputDir, "o", defaultDownloadDir, "download directory")
	fs.IntVar(&o.port, "port", defaultPort, "first port to try")
	fs.IntVar(&o.maxPeers, "max-peers", 0, "maximum simultaneous peers")
	fs.StringVar(&o.logFile, "log", "", "log file")
	fs.BoolVar(&o.debug, "debug", false, "verbose logging")

	// The flag package stops at the first non-flag argument; keep parsing so
	// options may come after the torrent path too.
	var positional []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return o, err
		}
		args = fs.Args()
		if len(args) > 0 {
			positional = append(positional, args[0])
			args = args[1:]
		}
	}

	if len(positional) != 1 {
		fs.Usage()
		return o, errors.New("expected exactly one .torrent file")
	}
	o.torrentPath = positional[0]

	if o.port < 1 || o.port > 65535 {
		return o, fmt.Errorf("-port must be between 1 and 65535, got %d", o.port)
	}
	if o.maxPeers < 0 {
		return o, errors.New("-max-peers must not be negative")
	}
	return o, nil
}

// setupLogging sends slog output to a file, or discards it. Logs must never
// go to the terminal: the progress display redraws in place.
func setupLogging(o options) (closeFn func(), err error) {
	path := o.logFile
	if path == "" && o.debug {
		path = "goTorrent.log"
	}
	if path == "" {
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
		return func() {}, nil
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening log file: %w", err)
	}
	level := slog.LevelInfo
	if o.debug {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(f, &slog.HandlerOptions{Level: level})))
	return func() { _ = f.Close() }, nil
}

func run(args []string, stdout, stderr io.Writer) int {
	o, err := parseArgs(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 2
	}

	closeLog, err := setupLogging(o)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	defer closeLog()

	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	display := newUI(stdout, isTerminal(os.Stdout))

	fmt.Fprintf(stdout, "Opening %s\n", o.torrentPath)
	s, err := torrent.OpenSession(ctx, torrent.SessionConfig{
		TorrentPath: o.torrentPath,
		DownloadDir: o.outputDir,
		PortMin:     uint16(o.port),
		PortMax:     uint16(min(o.port+8, 65535)),
		MaxPeers:    o.maxPeers,
	}, display.checkProgress)
	if err != nil {
		if ctx.Err() != nil {
			fmt.Fprintln(stderr, "\ninterrupted")
			return 130
		}
		fmt.Fprintf(stderr, "\nerror: %v\n", err)
		return 1
	}
	defer s.Stop() // a safety net; shutdown below already stops it

	if err := s.Resume(); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}

	// Keyboard: single keys in raw mode where possible, otherwise a letter and
	// Enter. Nothing at all when stdin isn't a terminal.
	keys := make(chan byte, 8)
	restoreTerminal := func() {}
	if restore, ok := enableRawInput(); ok {
		restoreTerminal = restore
		go readKeys(os.Stdin, keys)
	} else if isTerminal(os.Stdin) {
		go readLines(os.Stdin, keys)
	}
	defer restoreTerminal()

	shutdown := func() int {
		// Register for the next signal before letting go of the first handler.
		// Done the other way round (or from a goroutine), there is a moment when
		// neither is installed and a second Ctrl-C kills the process with the
		// terminal still in raw mode.
		forceQuitOnSignal(restoreTerminal)
		stopSignals() // the first signal has been handled

		display.note("Stopping: telling trackers we are leaving (Ctrl-C again to force quit)...")
		s.Stop()
		display.finish(s.Snapshot())
		return 0
	}

	var busy atomic.Bool // a pause or resume is in progress
	tick := time.NewTicker(refreshEvery)
	defer tick.Stop()

	announcedDone := s.Snapshot().State == torrent.StateSeeding
	for {
		select {
		case <-ctx.Done():
			return shutdown()

		case k := <-keys:
			switch k {
			case 'q', 'Q':
				return shutdown()
			case 'p', 'P', ' ':
				togglePause(s, display, &busy)
			}

		case <-tick.C:
			snap := s.Snapshot()
			display.draw(snap)
			if !announcedDone && snap.State == torrent.StateSeeding {
				announcedDone = true
				display.note("Download complete. Seeding until you quit (q).")
			}
		}
	}
}

// togglePause pauses a running session or resumes a paused one. Pausing waits
// for the trackers, so it runs in the background and further presses are
// ignored until it finishes.
func togglePause(s *torrent.Session, display *ui, busy *atomic.Bool) {
	if !busy.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer busy.Store(false)
		switch s.Snapshot().State {
		case torrent.StateDownloading, torrent.StateSeeding:
			s.Pause()
		case torrent.StatePaused:
			if err := s.Resume(); err != nil {
				display.note("Could not resume: " + err.Error())
			}
		}
	}()
}

// forceQuitOnSignal arranges for a second Ctrl-C during shutdown to restore the
// terminal (it may be in raw mode) and leave at once. The signal is registered
// before it returns; only the waiting happens in the background.
func forceQuitOnSignal(restoreTerminal func()) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		restoreTerminal()
		fmt.Fprintln(os.Stderr, "\nforced quit")
		os.Exit(130)
	}()
}

// readKeys forwards raw single-byte key presses.
func readKeys(r io.Reader, out chan<- byte) {
	buf := make([]byte, 1)
	for {
		if _, err := r.Read(buf); err != nil {
			return
		}
		select {
		case out <- buf[0]:
		default: // nobody is listening; drop it
		}
	}
}

// readLines is the fallback when raw mode is unavailable: the first letter of
// each line typed.
func readLines(r io.Reader, out chan<- byte) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		select {
		case out <- line[0]:
		default:
		}
	}
}

// isTerminal reports whether f is an interactive terminal (a character device).
func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
