package peer

import (
	"bytes"
	"errors"
	"testing"
)

func TestBitfieldSetHas(t *testing.T) {
	b := NewBitfield(10)
	if b.Len() != 10 || len(b.Bytes()) != 2 {
		t.Fatalf("Len=%d, bytes=%d; want 10 and 2", b.Len(), len(b.Bytes()))
	}

	b.Set(0)
	b.Set(9)
	b.Set(10)
	b.Set(-1)
	b.Set(15)

	for i, want := range map[int]bool{0: true, 1: false, 8: false, 9: true, 10: false, -1: false, 15: false} {
		if got := b.Has(i); got != want {
			t.Errorf("Has(%d) = %v, want %v", i, got, want)
		}
	}
	if b.Count() != 2 {
		t.Errorf("Count = %d, want 2", b.Count())
	}

	if want := []byte{0x80, 0x40}; !bytes.Equal(b.Bytes(), want) {
		t.Errorf("wire bytes = %x, want %x", b.Bytes(), want)
	}
}

func TestBitfieldComplete(t *testing.T) {
	b := NewBitfield(3)
	if b.Complete() {
		t.Fatal("empty bitfield reported complete")
	}
	b.Set(0)
	b.Set(1)
	b.Set(2)
	if !b.Complete() {
		t.Fatal("full bitfield not reported complete")
	}
	if !bytes.Equal(b.Bytes(), []byte{0xE0}) {
		t.Fatalf("wire bytes = %x, want e0", b.Bytes())
	}
}

func TestParseBitfield(t *testing.T) {
	b, err := ParseBitfield([]byte{0x80, 0x40}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !b.Has(0) || !b.Has(9) || b.Has(1) || b.Count() != 2 {
		t.Fatalf("parsed bitfield wrong: count=%d", b.Count())
	}

	if _, err := ParseBitfield([]byte{0xFF}, 8); err != nil {
		t.Fatalf("8 pieces, all set: %v", err)
	}

	bad := []struct {
		name    string
		payload []byte
		pieces  int
	}{
		{"too short", []byte{0x80}, 10},
		{"too long", []byte{0x80, 0x40, 0}, 10},
		{"spare bit set", []byte{0x80, 0x41}, 10},
		{"all spare bits set", []byte{0x00, 0x3F}, 10},
		{"empty for non-empty torrent", nil, 1},
	}
	for _, tc := range bad {
		if _, err := ParseBitfield(tc.payload, tc.pieces); !errors.Is(err, ErrBadPayload) {
			t.Errorf("%s: got %v, want ErrBadPayload", tc.name, err)
		}
	}
}

func TestParseBitfieldCopiesPayload(t *testing.T) {
	payload := []byte{0x80}
	b, err := ParseBitfield(payload, 8)
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 0
	if !b.Has(0) {
		t.Fatal("bitfield aliases the message payload; it must copy")
	}
}

func TestBitfieldMessage(t *testing.T) {
	b := NewBitfield(8)
	b.Set(3)
	msg := NewBitfieldMessage(b)

	if !msg.Is(MsgBitfield) || !bytes.Equal(msg.Payload, []byte{0x10}) {
		t.Fatalf("message = %v %x", msg, msg.Payload)
	}

	b.Set(4)
	if !bytes.Equal(msg.Payload, []byte{0x10}) {
		t.Fatalf("message payload changed to %x after the bitfield was modified", msg.Payload)
	}

	got, err := ParseBitfield(msg.Payload, 8)
	if err != nil || !got.Has(3) || got.Has(4) {
		t.Fatalf("round trip: %v, has3=%v has4=%v", err, got.Has(3), got.Has(4))
	}
}

func TestNewBitfieldNegative(t *testing.T) {
	b := NewBitfield(-5)
	if b.Len() != 0 || len(b.Bytes()) != 0 || !b.Complete() {
		t.Fatalf("negative size should behave as an empty bitfield: len=%d", b.Len())
	}
}
