package torrent

import (
	"math/rand/v2"
	"sync"
)

// DefaultRandomFirst is how many pieces we complete by random choice before
// switching to rarest-first. Early on a peer wants something to trade quickly
// more than it wants the rarest piece, and random choice avoids every new
// peer piling onto the same pieces.
const DefaultRandomFirst = 4

// pieceSet is anything that can say whether it holds a piece. peer.Bitfield
// satisfies it, so the picker needs no dependency on the peer package.
type pieceSet interface {
	Has(index int) bool
}

// piecePicker decides which piece each peer should download next.
//
// Policy:
//   - A piece is assigned to one peer at a time (its owner), so peers don't
//     fetch the same piece twice. Endgame mode (step 25) is the deliberate
//     exception and will add its own path.
//   - Pieces that were started and then released, because their owner choked
//     us, timed out or left, are preferred. Their blocks are already buffered
//     in the assembler, and finishing them frees that memory.
//   - Otherwise, until DefaultRandomFirst pieces are complete, pick at random
//     among what the peer has; after that, pick the rarest (lowest
//     availability), breaking ties at random.
//
// The picker keeps its own copy of each peer's bitfield and applies changes as
// differences, so it can't double-count a piece when a have message is
// processed after a bitfield snapshot that already included it.
//
// Pick and Interested scan every piece, O(pieces). That is fine per piece
// picked, but don't call them per block.
//
// Safe for concurrent use. Peers are identified by an opaque string, such as
// their remote address; the same string must be used for every call about a
// peer.
type piecePicker struct {
	mu          sync.Mutex
	n           int
	randomFirst int

	have      []bool // pieces we have verified
	haveCount int
	owner     []*pickerPeer // nil = not in flight
	started   []bool        // work was begun and not discarded
	avail     []int32       // connected peers that have each piece

	peers map[string]*pickerPeer
}

type pickerPeer struct {
	has   []bool
	owned map[int]struct{}
}

// newPiecePicker creates a picker. randomFirst < 0 selects DefaultRandomFirst;
// 0 means rarest-first from the start.
func newPiecePicker(numPieces, randomFirst int) *piecePicker {
	if randomFirst < 0 {
		randomFirst = DefaultRandomFirst
	}
	return &piecePicker{
		n:           numPieces,
		randomFirst: randomFirst,
		have:        make([]bool, numPieces),
		owner:       make([]*pickerPeer, numPieces),
		started:     make([]bool, numPieces),
		avail:       make([]int32, numPieces),
		peers:       make(map[string]*pickerPeer),
	}
}

func (p *piecePicker) peerLocked(key string) *pickerPeer {
	pp := p.peers[key]
	if pp == nil {
		pp = &pickerPeer{has: make([]bool, p.n), owned: make(map[int]struct{})}
		p.peers[key] = pp
	}
	return pp
}

// PeerBitfield sets what a peer has, replacing anything recorded before. Pass a
// snapshot (peer.Bitfield from Peer.Bitfield) rather than the live peer, so
// the picker isn't calling into a peer's lock once per piece.
func (p *piecePicker) PeerBitfield(peer string, has pieceSet) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pp := p.peerLocked(peer)
	for i := 0; i < p.n; i++ {
		now := has.Has(i)
		if now == pp.has[i] {
			continue
		}
		if now {
			p.avail[i]++
		} else {
			p.avail[i]--
		}
		pp.has[i] = now
	}
}

// PeerHave records a have message. It is idempotent.
func (p *piecePicker) PeerHave(peer string, index int) {
	if index < 0 || index >= p.n {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	pp := p.peerLocked(peer)
	if !pp.has[index] {
		pp.has[index] = true
		p.avail[index]++
	}
}

// PeerGone removes a disconnected peer: its pieces stop counting toward
// availability, and any pieces it owned go back up for grabs (keeping their
// "started" mark, since their blocks are still buffered).
func (p *piecePicker) PeerGone(peer string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pp := p.peers[peer]
	if pp == nil {
		return
	}
	p.releaseAllLocked(pp)
	for i, has := range pp.has {
		if has {
			p.avail[i]--
		}
	}
	delete(p.peers, peer)
}

// ReleaseAll returns every piece the peer owns, for when it chokes us or stalls
// and its outstanding requests are void. Its availability is kept.
func (p *piecePicker) ReleaseAll(peer string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pp := p.peers[peer]; pp != nil {
		p.releaseAllLocked(pp)
	}
}

func (p *piecePicker) releaseAllLocked(pp *pickerPeer) {
	for i := range pp.owned {
		p.owner[i] = nil
	}
	clear(pp.owned)
}

func (p *piecePicker) releaseLocked(index int) {
	if o := p.owner[index]; o != nil {
		delete(o.owned, index)
		p.owner[index] = nil
	}
}

// Pick assigns the peer a piece to download and returns its index. The piece
// stays owned by this peer until Verified, Release or Reset, or until the peer
// is released or removed. It returns false if the peer is unknown or has
// nothing we need that isn't already being fetched.
func (p *piecePicker) Pick(peer string) (int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pp := p.peers[peer]
	if pp == nil {
		return 0, false
	}

	needRandom := p.haveCount < p.randomFirst

	var resumed, fresh rarest
	freshRandom, freshCount := -1, 0

	for i := 0; i < p.n; i++ {
		if p.have[i] || p.owner[i] != nil || !pp.has[i] {
			continue
		}
		if p.started[i] {
			resumed.offer(i, p.avail[i])
			continue
		}
		fresh.offer(i, p.avail[i])
		if needRandom {
			freshCount++
			if rand.IntN(freshCount) == 0 {
				freshRandom = i
			}
		}
	}

	var idx int
	switch {
	case resumed.n > 0:
		idx = resumed.idx
	case fresh.n > 0 && needRandom:
		idx = freshRandom
	case fresh.n > 0:
		idx = fresh.idx
	default:
		return 0, false
	}

	p.owner[idx] = pp
	pp.owned[idx] = struct{}{}
	p.started[idx] = true
	return idx, true
}

// rarest tracks the lowest-availability candidate seen so far, choosing
// uniformly among ties (reservoir sampling).
type rarest struct {
	idx   int
	avail int32
	n     int // candidates tied at the current minimum; 0 = none yet
}

func (r *rarest) offer(i int, avail int32) {
	switch {
	case r.n == 0 || avail < r.avail:
		r.idx, r.avail, r.n = i, avail, 1
	case avail == r.avail:
		r.n++
		if rand.IntN(r.n) == 0 {
			r.idx = i
		}
	}
}

// Verified records that piece index is complete and on disk: it stops being
// pickable and its owner, if any, is freed. Use it for pieces that verify
// during the download and for pieces found by the resume check.
func (p *piecePicker) Verified(index int) {
	if index < 0 || index >= p.n {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	p.releaseLocked(index)
	p.started[index] = false
	if !p.have[index] {
		p.have[index] = true
		p.haveCount++
	}
}

// SyncVerified marks every piece that have reports as verified. After
// resumeFromDisk, pass the assembler.
func (p *piecePicker) SyncVerified(have pieceSet) {
	for i := 0; i < p.n; i++ {
		if have.Has(i) {
			p.Verified(i)
		}
	}
}

// Release gives a piece back without forgetting progress, for when its owner
// stalls or chokes us. Blocks already buffered for it are kept, and the piece
// is preferred next time someone who has it asks.
func (p *piecePicker) Release(index int) {
	if index < 0 || index >= p.n {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.releaseLocked(index)
}

func (p *piecePicker) Reset(index int) {
	if index < 0 || index >= p.n {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.releaseLocked(index)
	p.started[index] = false
}

// Interested reports whether the peer has any piece we still lack
func (p *piecePicker) Interested(peer string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	pp := p.peers[peer]
	if pp == nil {
		return false
	}
	for i := 0; i < p.n; i++ {
		if pp.has[i] && !p.have[i] {
			return true
		}
	}
	return false
}

func (p *piecePicker) Owned(peer string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pp := p.peers[peer]; pp != nil {
		return len(pp.owned)
	}
	return 0
}

func (p *piecePicker) Availability(index int) int {
	if index < 0 || index >= p.n {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return int(p.avail[index])
}

// Counts reports verified, in-flight and still-unassigned pieces.
func (p *piecePicker) Counts() (have, inFlight, missing int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := 0; i < p.n; i++ {
		switch {
		case p.have[i]:
			have++
		case p.owner[i] != nil:
			inFlight++
		default:
			missing++
		}
	}
	return have, inFlight, missing
}
