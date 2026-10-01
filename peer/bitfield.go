package peer

import (
	"fmt"
	"math/bits"
)

// Bitfield records which pieces a peer has. Piece 0 is the high bit of the
// first byte, as on the wire. Spare bits in the last byte are always zero.
//
// Copies share storage, so Set through a copy is visible in the original.
type Bitfield struct {
	bits []byte
	n    int // number of pieces
}

func NewBitfield(numPieces int) Bitfield {
	if numPieces < 0 {
		numPieces = 0
	}
	return Bitfield{bits: make([]byte, (numPieces+7)/8), n: numPieces}
}

func ParseBitfield(payload []byte, numPieces int) (Bitfield, error) {
	want := (numPieces + 7) / 8
	if len(payload) != want {
		return Bitfield{}, fmt.Errorf("%w: bitfield is %d bytes, want %d for %d pieces",
			ErrBadPayload, len(payload), want, numPieces)
	}
	if spare := want*8 - numPieces; spare > 0 {
		mask := byte(0xFF) >> (8 - spare)
		if payload[want-1]&mask != 0 {
			return Bitfield{}, fmt.Errorf("%w: bitfield has spare bits set", ErrBadPayload)
		}
	}

	b := NewBitfield(numPieces)
	copy(b.bits, payload)
	return b, nil
}

func (b Bitfield) Len() int { return b.n }

func (b Bitfield) Has(i int) bool {
	if i < 0 || i >= b.n {
		return false
	}
	return b.bits[i/8]&(0x80>>(i%8)) != 0
}

func (b Bitfield) Set(i int) {
	if i < 0 || i >= b.n {
		return
	}
	b.bits[i/8] |= 0x80 >> (i % 8)
}

func (b Bitfield) Count() int {
	total := 0
	for _, x := range b.bits {
		total += bits.OnesCount8(x)
	}
	return total
}

func (b Bitfield) Complete() bool { return b.Count() == b.n }

func (b Bitfield) Bytes() []byte { return b.bits }
