package main

import (
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"
	"unicode"

	"goTor/torrent"
)

const (
	barWidth      = 30
	maxNameWidth  = 60
	plainInterval = 10 * time.Second // how often a non-terminal gets a status line
)

// ui draws progress on stdout.
//
// On a terminal it keeps a small block of lines on screen and redraws it in
// place; notes are printed above it. Anything else (a pipe, a file) gets plain
// lines instead, a few seconds apart, with no escape codes. Every line is kept
// well under 80 columns, so the cursor arithmetic below never meets a wrapped
// line.
//
// Safe for concurrent use: notes arrive from the pause/resume goroutine while
// the main loop is drawing.
type ui struct {
	mu  sync.Mutex
	w   io.Writer
	tty bool

	drawn int // lines of the live block currently on screen

	lastPct int // last percentage printed while hashing existing files

	lastPlain time.Time
	lastState torrent.SessionState
	haveState bool
}

func newUI(w io.Writer, tty bool) *ui {
	return &ui{w: w, tty: tty, lastPct: -1}
}

// checkProgress reports the startup hash check of files already on disk. It
// matches the onCheck callback of torrent.OpenSession and is called once per
// piece, so it only prints when the percentage changes.
func (u *ui) checkProgress(done, total int) {
	if total <= 0 {
		return
	}
	pct := done * 100 / total

	u.mu.Lock()
	defer u.mu.Unlock()

	if pct == u.lastPct && done < total {
		return
	}
	u.lastPct = pct

	if u.tty {
		fmt.Fprintf(u.w, "\r\x1b[2KChecking existing files: %3d%% (%d/%d pieces)", pct, done, total)
		if done >= total {
			fmt.Fprintln(u.w)
		}
		return
	}
	if pct%10 == 0 || done >= total {
		fmt.Fprintf(u.w, "Checking existing files: %d%%\n", pct)
	}
}

// note prints a message on its own line, above the live block.
func (u *ui) note(msg string) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.clearBlockLocked()
	fmt.Fprintln(u.w, clean(msg))
}

// draw shows the current progress. Call it every half second or so.
func (u *ui) draw(p torrent.Progress) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if !u.tty {
		u.drawPlainLocked(p)
		return
	}

	var b strings.Builder
	if u.drawn > 0 {
		fmt.Fprintf(&b, "\x1b[%dA", u.drawn) // back to the top of the block
	}
	lines := renderLines(p)
	for _, l := range lines {
		b.WriteString("\r\x1b[2K") // each line is cleared before it is rewritten
		b.WriteString(l)
		b.WriteByte('\n')
	}
	u.drawn = len(lines)
	_, _ = io.WriteString(u.w, b.String()) // one write, so the redraw doesn't flicker
}

// finish removes the live block and prints a one-line summary.
func (u *ui) finish(p torrent.Progress) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.clearBlockLocked()
	fmt.Fprintf(u.w, "%s: %s of %s verified (%.1f%%). This session: downloaded %s, uploaded %s.\n",
		clean(p.Name), humanBytes(p.DoneBytes), humanBytes(p.TotalBytes), percent(p),
		humanBytes(p.Downloaded), humanBytes(p.Uploaded))
}

func (u *ui) clearBlockLocked() {
	if u.tty && u.drawn > 0 {
		fmt.Fprintf(u.w, "\x1b[%dA\r\x1b[J", u.drawn) // up, then erase to the end of the screen
		u.drawn = 0
	}
}

func (u *ui) drawPlainLocked(p torrent.Progress) {
	now := time.Now()
	if u.haveState && p.State == u.lastState && now.Sub(u.lastPlain) < plainInterval {
		return
	}
	u.haveState, u.lastState, u.lastPlain = true, p.State, now

	fmt.Fprintf(u.w, "%s [%s] %.1f%%  down %s  up %s  peers %d\n",
		truncate(clean(p.Name), maxNameWidth), p.State, percent(p),
		humanRate(p.DownRate), humanRate(p.UpRate), p.Peers)
}

// renderLines builds the live block. It is pure so it can be tested directly.
func renderLines(p torrent.Progress) []string {
	pct := percent(p)

	tail := "eta --"
	if p.State == torrent.StateSeeding {
		tail = "uploaded " + humanBytes(p.Uploaded)
	} else if left := p.TotalBytes - p.DoneBytes; p.State == torrent.StateDownloading && p.DownRate >= 1 && left > 0 {
		tail = "eta " + humanETA(float64(left)/p.DownRate)
	}

	pause := "p pause"
	switch p.State {
	case torrent.StatePaused:
		pause = "p resume"
	case torrent.StatePausing:
		pause = "pausing..."
	}

	return []string{
		fmt.Sprintf("%s  [%s]", truncate(clean(p.Name), maxNameWidth), p.State),
		fmt.Sprintf("%s %5.1f%%", bar(pct/100, barWidth), pct),
		fmt.Sprintf("%s / %s   pieces %d/%d", humanBytes(p.DoneBytes), humanBytes(p.TotalBytes), p.PiecesDone, p.PiecesTotal),
		fmt.Sprintf("down %s   up %s   peers %d   %s", humanRate(p.DownRate), humanRate(p.UpRate), p.Peers, tail),
		fmt.Sprintf("port %d   %s   q quit", p.Port, pause),
	}
}

func percent(p torrent.Progress) float64 {
	if p.TotalBytes <= 0 {
		return 0
	}
	return min(100, float64(p.DoneBytes)/float64(p.TotalBytes)*100)
}

func bar(frac float64, width int) string {
	filled := int(math.Round(min(max(frac, 0), 1) * float64(width)))
	return "[" + strings.Repeat("#", filled) + strings.Repeat("-", width-filled) + "]"
}

// clean replaces control characters. The torrent's name and error text come
// from outside, and an escape sequence in either could rewrite the screen.
func clean(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return '?'
		}
		return r
	}, s)
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

func humanBytes(n int64) string {
	if n < 0 {
		n = 0
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit && exp < 4; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTP"[exp])
}

func humanRate(bytesPerSec float64) string {
	if math.IsNaN(bytesPerSec) || bytesPerSec < 0 {
		bytesPerSec = 0
	}
	return humanBytes(int64(min(bytesPerSec, math.MaxInt64/2))) + "/s"
}

func humanETA(secs float64) string {
	if math.IsNaN(secs) || secs > 99*3600 {
		return ">99h"
	}
	d := time.Duration(secs) * time.Second
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	case d >= time.Minute:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
}
