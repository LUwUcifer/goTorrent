package torrent

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
)

const maxRecheckWorkers = 4

type pieceReader interface {
	ReadPieceAt(p []byte, index int, begin int64) (int, error)
}

var _ pieceReader = (*Storage)(nil)

// recheckPieces reads every piece from r and returns which ones hash to their
// expected SHA-1. Pieces whose data is absent or short (IsMissing) are simply
// "not present"; any other read error aborts the whole check, because
// guessing could mean overwriting data we merely failed to read.
//
// It never creates files: Storage reads open files read-only.
//
// workers <= 0 picks a default. progress, if non-nil, is called after each
// piece with the count done so far; calls are serialized, so it needs no
// locking of its own, but it should be quick.
func recheckPieces(ctx context.Context, lay *layout, hashes []hashBytes, r pieceReader,
	workers int, progress func(done, total int)) ([]bool, error) {

	switch {
	case lay == nil || lay.numPieces <= 0 || lay.pieceLength <= 0:
		return nil, errors.New("recheck: empty layout")
	case len(hashes) != lay.numPieces:
		return nil, fmt.Errorf("recheck: %d hashes for %d pieces", len(hashes), lay.numPieces)
	case r == nil:
		return nil, errors.New("recheck: nil storage")
	}

	if workers <= 0 {
		workers = min(runtime.NumCPU(), maxRecheckWorkers)
	}
	workers = max(1, min(workers, lay.numPieces))

	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	good := make([]bool, lay.numPieces) // each worker writes distinct indices

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error

		progMu sync.Mutex
		done   int
	)
	fail := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	jobs := make(chan int)
	for w := 0; w < workers; w++ {
		wg.Go(func() {
			buf := make([]byte, lay.pieceLength) // reused for every piece this worker checks
			for i := range jobs {
				if ctx.Err() != nil {
					continue // drain; the feeder is about to stop anyway
				}
				ok, err := checkPiece(lay, hashes[i], r, buf, i)
				if err != nil {
					fail(fmt.Errorf("recheck: piece %d: %w", i, err))
					continue
				}
				good[i] = ok

				if progress != nil {
					progMu.Lock()
					done++
					progress(done, lay.numPieces)
					progMu.Unlock()
				}
			}
		})
	}

feed:
	for i := 0; i < lay.numPieces; i++ {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	return good, nil
}

func checkPiece(lay *layout, want hashBytes, r pieceReader, buf []byte, index int) (bool, error) {
	size := int(lay.pieceSize(index))
	n, err := r.ReadPieceAt(buf[:size], index, 0)
	if err != nil {
		if IsMissing(err) {
			return false, nil
		}
		return false, err
	}
	if n != size {
		return false, nil
	}
	return sha1.Sum(buf[:size]) == want, nil
}

// resumeFromDisk hashes whatever is already on disk and records every piece
// that verifies: it marks them complete in asm (so late blocks for them are
// ignored) and credits their bytes to the torrent. It returns how many pieces
// were found. Run it once, at startup, before downloading.
//
// Credited bytes go straight into tor.verified rather than through
// pieceVerified, on purpose: resuming a finished download must not trigger the
// "completed" tracker event, which means the download finished during this
// session. Finish this before calling StartTrackers, so a fully resumed
// torrent announces as a seeder (left=0) from its first "started" announce.
//
// If every piece verifies, the torrent is a seed: send asm.BitfieldBytes() to
// each peer after the handshake.
func (tor *Torrent) resumeFromDisk(ctx context.Context, store pieceReader, asm *pieceAssembler,
	progress func(done, total int)) (int, error) {

	good, err := recheckPieces(ctx, &tor.layout, tor.localData.infoDict.pieces, store, 0, progress)
	if err != nil {
		return 0, err
	}

	found := 0
	for i, ok := range good {
		if ok && asm.MarkComplete(i) {
			tor.verified.Add(tor.layout.pieceSize(i))
			found++
		}
	}

	slog.Info("resume check finished",
		"piecesFound", found, "pieces", tor.layout.numPieces, "bytesLeft", tor.bytesLeft())
	return found, nil
}
