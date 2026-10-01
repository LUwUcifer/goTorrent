package torrent

import "fmt"

// pieceSpans maps the byte range [begin, begin+length) inside piece index to
// the file segments that hold it, in stream order. begin is relative to the
// start of the piece, matching the begin field of request/piece messages.
//
// A range may touch several files when the piece crosses file boundaries.
// Zero-length files are never returned. A zero-length range returns no spans
// and no error, provided the index and begin are valid.

func (l *layout) pieceSpans(index int, begin, length int64) ([]fileSpan, error) {
	size := l.pieceSize(index)
	if size == 0 {
		return nil, fmt.Errorf("piece %d out of range (%d pieces)", index, l.numPieces)
	}
	// size >= 1 here, so size-length cannot overflow.
	if begin < 0 || length < 0 || length > size || begin > size-length {
		return nil, fmt.Errorf("range [%d, +%d) is outside piece %d (size %d)", begin, length, index, size)
	}
	return l.spansFor(l.pieceOffset(index)+begin, length)
}
