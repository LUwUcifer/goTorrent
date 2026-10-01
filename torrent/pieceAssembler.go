package torrent

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
)

// DefaultMaxBuffered bounds the memory held by partially assembled pieces.
const DefaultMaxBuffered = 64 << 20

var (
	ErrBadBlock   = errors.New("piece: invalid block")
	ErrBufferFull = errors.New("piece: assembly buffer full")
)

// pieceWriter is where verified pieces go. *Storage implements it.
type pieceWriter interface {
	WritePieceAt(p []byte, index int, begin int64) (int, error)
}

var _ pieceWriter = (*Storage)(nil)

type BlockStatus uint8

const (
	BlockStored BlockStatus = iota
	BlockDuplicate
	PieceComplete
	PieceCorrupt
)

func (s BlockStatus) String() string {
	switch s {
	case BlockStored:
		return "stored"
	case BlockDuplicate:
		return "duplicate"
	case PieceComplete:
		return "piece-complete"
	case PieceCorrupt:
		return "piece-corrupt"
	default:
		return fmt.Sprintf("status(%d)", uint8(s))
	}
}

type BlockResult struct {
	Status  BlockStatus
	Sources []string
}

type partialPiece struct {
	buf      []byte
	got      []bool // per block
	received int
	sources  []string

	// verifying is set by the goroutine that received the last block. From then
	// on the fields above are read-only, so that goroutine can hash and write
	// buf without holding the assembler's lock.
	verifying bool
}

// pieceAssembler collects blocks into pieces, verifies each completed piece
// against its SHA-1 from the metainfo, and writes it to storage only if the
// hash matches. It is safe for concurrent use by many peer goroutines.
type pieceAssembler struct {
	lay         *layout
	hashes      []hashBytes
	store       pieceWriter
	maxBuffered int64
	pool        sync.Pool

	mu            sync.Mutex
	parts         map[int]*partialPiece
	buffered      int64
	complete      []bool
	completeCount int
}

func newPieceAssembler(lay *layout, hashes []hashBytes, store pieceWriter, maxBuffered int64) (*pieceAssembler, error) {
	switch {
	case lay == nil || lay.numPieces <= 0 || lay.pieceLength <= 0:
		return nil, errors.New("piece: empty layout")
	case len(hashes) != lay.numPieces:
		return nil, fmt.Errorf("piece: %d hashes for %d pieces", len(hashes), lay.numPieces)
	case store == nil:
		return nil, errors.New("piece: nil storage")
	}

	if maxBuffered <= 0 {
		maxBuffered = DefaultMaxBuffered
	}
	maxBuffered = max(maxBuffered, lay.pieceLength)

	return &pieceAssembler{
		lay:         lay,
		hashes:      hashes,
		store:       store,
		maxBuffered: maxBuffered,
		parts:       make(map[int]*partialPiece),
		complete:    make([]bool, lay.numPieces),
	}, nil
}

// AddBlock stores one block of piece index at offset begin (relative to the
// piece, as in request and piece messages). data is copied, so the caller may
// reuse it. source identifies who sent the block (a peer address, say) and is
// reported back if the piece turns out corrupt.
//
// Blocks must be the standard size: begin a multiple of 16 KiB and data
// exactly that block's length (shorter for a piece's final block). Anything
// else returns ErrBadBlock.
//
// When a block completes its piece, AddBlock hashes it and, on a match,
// writes it to storage before returning PieceComplete. A storage failure
// returns the error, discards the buffered piece and leaves it incomplete, so
// it is downloaded again.
func (a *pieceAssembler) AddBlock(index int, begin int64, data []byte, source string) (BlockResult, error) {
	if err := a.checkBlock(index, begin, len(data)); err != nil {
		return BlockResult{}, err
	}
	blk := int(begin / blockSize)

	a.mu.Lock()
	if a.complete[index] {
		a.mu.Unlock()
		return BlockResult{Status: BlockDuplicate}, nil
	}

	pp := a.parts[index]
	if pp == nil {
		size := a.lay.pieceSize(index)
		if a.buffered+size > a.maxBuffered {
			a.mu.Unlock()
			return BlockResult{}, ErrBufferFull
		}
		pp = &partialPiece{
			buf: a.getBuf(int(size)),
			got: make([]bool, a.lay.numBlocks(index)),
		}
		a.parts[index] = pp
		a.buffered += size
	}

	if pp.verifying || pp.got[blk] {
		a.mu.Unlock()
		return BlockResult{Status: BlockDuplicate}, nil
	}

	copy(pp.buf[int(begin):], data)
	pp.got[blk] = true
	pp.received++
	if source != "" && !slices.Contains(pp.sources, source) {
		pp.sources = append(pp.sources, source)
	}

	if pp.received < len(pp.got) {
		a.mu.Unlock()
		return BlockResult{Status: BlockStored}, nil
	}
	pp.verifying = true
	a.mu.Unlock()

	return a.finish(index, pp)
}

func (a *pieceAssembler) finish(index int, pp *partialPiece) (BlockResult, error) {
	ok := sha1.Sum(pp.buf) == a.hashes[index]

	var werr error
	if ok {
		var n int
		n, werr = a.store.WritePieceAt(pp.buf, index, 0)
		if werr == nil && n != len(pp.buf) {
			werr = fmt.Errorf("short write: %d of %d bytes", n, len(pp.buf))
		}
	}

	a.mu.Lock()
	delete(a.parts, index)
	a.buffered -= int64(len(pp.buf))
	if ok && werr == nil {
		a.complete[index] = true
		a.completeCount++
	}
	a.mu.Unlock()

	sources := pp.sources
	a.putBuf(pp.buf)

	switch {
	case !ok:
		slog.Warn("piece failed hash check; discarding", "piece", index, "sources", sources)
		return BlockResult{Status: PieceCorrupt, Sources: sources}, nil
	case werr != nil:
		return BlockResult{}, fmt.Errorf("storage: writing verified piece %d: %w", index, werr)
	default:
		return BlockResult{Status: PieceComplete}, nil
	}
}

func (a *pieceAssembler) checkBlock(index int, begin int64, n int) error {
	size := a.lay.pieceSize(index)
	switch {
	case size == 0:
		return fmt.Errorf("%w: piece %d out of range (%d pieces)", ErrBadBlock, index, a.lay.numPieces)
	case begin < 0 || begin >= size || begin%blockSize != 0:
		return fmt.Errorf("%w: begin %d is not a block boundary inside piece %d (size %d)", ErrBadBlock, begin, index, size)
	}
	if want := a.lay.blockLen(index, int(begin/blockSize)); n != want {
		return fmt.Errorf("%w: block at %d in piece %d is %d bytes, want %d", ErrBadBlock, begin, index, n, want)
	}
	return nil
}

func (a *pieceAssembler) Has(index int) bool {
	if index < 0 || index >= a.lay.numPieces {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.complete[index]
}

func (a *pieceAssembler) CompleteCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.completeCount
}

func (a *pieceAssembler) Done() bool {
	return a.CompleteCount() == a.lay.numPieces
}

func (a *pieceAssembler) MarkComplete(index int) bool {
	if index < 0 || index >= a.lay.numPieces {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.complete[index] {
		return false
	}
	if pp := a.parts[index]; pp != nil {
		if pp.verifying {
			return false
		}
		a.dropLocked(index, pp)
	}
	a.complete[index] = true
	a.completeCount++
	return true
}

func (a *pieceAssembler) BitfieldBytes() []byte {
	a.mu.Lock()
	defer a.mu.Unlock()

	out := make([]byte, (a.lay.numPieces+7)/8)
	for i, ok := range a.complete {
		if ok {
			out[i/8] |= 0x80 >> (i % 8)
		}
	}
	return out
}

func (a *pieceAssembler) MissingBlocks(index int) []int {
	if index < 0 || index >= a.lay.numPieces {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.complete[index] {
		return nil
	}
	pp := a.parts[index]
	n := a.lay.numBlocks(index)
	missing := make([]int, 0, n)
	for b := range n {
		if pp == nil || !pp.got[b] {
			missing = append(missing, b)
		}
	}
	return missing
}

func (a *pieceAssembler) Discard(index int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	pp := a.parts[index]
	if pp == nil || pp.verifying {
		return false
	}
	a.dropLocked(index, pp)
	return true
}

func (a *pieceAssembler) BufferedBytes() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.buffered
}

func (a *pieceAssembler) dropLocked(index int, pp *partialPiece) {
	delete(a.parts, index)
	a.buffered -= int64(len(pp.buf))
	a.putBuf(pp.buf)
}

func (a *pieceAssembler) getBuf(size int) []byte {
	if v := a.pool.Get(); v != nil {
		if b := *(v.(*[]byte)); cap(b) >= size {
			return b[:size]
		}
	}
	return make([]byte, size, int(a.lay.pieceLength))
}

func (a *pieceAssembler) putBuf(b []byte) {
	b = b[:cap(b)]
	a.pool.Put(&b)
}
