package peer

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"testing"
)

func sameMessage(a, b Message) bool {
	return a.KeepAlive == b.KeepAlive && a.ID == b.ID && bytes.Equal(a.Payload, b.Payload)
}

func TestMessageEncoding(t *testing.T) {
	tests := []struct {
		name string
		msg  Message
		want []byte
	}{
		{"keep-alive", NewKeepAlive(), []byte{0, 0, 0, 0}},
		{"choke", NewChoke(), []byte{0, 0, 0, 1, 0}},
		{"unchoke", NewUnchoke(), []byte{0, 0, 0, 1, 1}},
		{"interested", NewInterested(), []byte{0, 0, 0, 1, 2}},
		{"not interested", NewNotInterested(), []byte{0, 0, 0, 1, 3}},
		{"have", NewHave(5), []byte{0, 0, 0, 5, 4, 0, 0, 0, 5}},
		{"request", NewRequest(1, 2, 3),
			[]byte{0, 0, 0, 13, 6, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3}},
		{"cancel", NewCancel(1, 2, 3),
			[]byte{0, 0, 0, 13, 8, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3}},
		{"piece", NewPiece(7, 16384, []byte{0xAA, 0xBB}),
			[]byte{0, 0, 0, 11, 7, 0, 0, 0, 7, 0, 0, 0x40, 0, 0xAA, 0xBB}},
		{"bitfield", NewBitfieldMessage(mustBitfield(t, []byte{0x80, 0x40}, 10)),
			[]byte{0, 0, 0, 3, 5, 0x80, 0x40}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteMessage(&buf, tc.msg); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(buf.Bytes(), tc.want) {
				t.Fatalf("encoded %x, want %x", buf.Bytes(), tc.want)
			}

			got, err := ReadMessage(&buf, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if !sameMessage(got, tc.msg) {
				t.Fatalf("round trip: got %v %x, want %v %x", got, got.Payload, tc.msg, tc.msg.Payload)
			}
			if err := got.Validate(); err != nil {
				t.Fatalf("a message we built failed Validate: %v", err)
			}
		})
	}
}

func mustBitfield(t *testing.T, payload []byte, n int) Bitfield {
	t.Helper()
	b, err := ParseBitfield(payload, n)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestReadMessageStream(t *testing.T) {
	var buf bytes.Buffer
	for _, m := range []Message{NewKeepAlive(), NewHave(9), NewPiece(1, 0, []byte("data")), NewChoke()} {
		if err := WriteMessage(&buf, m); err != nil {
			t.Fatal(err)
		}
	}

	r := bufio.NewReader(&buf)
	want := []Message{NewKeepAlive(), NewHave(9), NewPiece(1, 0, []byte("data")), NewChoke()}
	for i, w := range want {
		got, err := ReadMessage(r, 1024)
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if !sameMessage(got, w) {
			t.Fatalf("message %d: got %v, want %v", i, got, w)
		}
	}
	if _, err := ReadMessage(r, 1024); !errors.Is(err, io.EOF) {
		t.Fatalf("after the last message got %v, want io.EOF", err)
	}
}

func TestReadMessageLimits(t *testing.T) {
	t.Run("oversized length is rejected before reading the body", func(t *testing.T) {
		r := bytes.NewReader([]byte{0x40, 0, 0, 0})
		if _, err := ReadMessage(r, 1000); !errors.Is(err, ErrMessageTooLarge) {
			t.Fatalf("got %v, want ErrMessageTooLarge", err)
		}
	})

	t.Run("exactly at the limit is fine", func(t *testing.T) {
		var buf bytes.Buffer
		if err := WriteMessage(&buf, NewPiece(0, 0, make([]byte, 99))); err != nil { // 1+8+99 = 108
			t.Fatal(err)
		}
		if _, err := ReadMessage(&buf, 108); err != nil {
			t.Fatalf("got %v, want success at the limit", err)
		}
	})

	t.Run("clean disconnect", func(t *testing.T) {
		if _, err := ReadMessage(bytes.NewReader(nil), 100); !errors.Is(err, io.EOF) {
			t.Fatalf("got %v, want io.EOF", err)
		}
	})

	t.Run("truncated header", func(t *testing.T) {
		if _, err := ReadMessage(bytes.NewReader([]byte{0, 0}), 100); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
		}
	})

	t.Run("truncated body", func(t *testing.T) {
		r := bytes.NewReader([]byte{0, 0, 0, 10, 4, 1, 2})
		if _, err := ReadMessage(r, 100); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("got %v, want io.ErrUnexpectedEOF", err)
		}
	})

	t.Run("header but no body at all", func(t *testing.T) {
		r := bytes.NewReader([]byte{0, 0, 0, 5})
		if _, err := ReadMessage(r, 100); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("got %v, want io.ErrUnexpectedEOF (not a clean EOF)", err)
		}
	})
}

func TestUnknownMessageIDIsReturned(t *testing.T) {
	r := bytes.NewReader([]byte{0, 0, 0, 3, 42, 0xDE, 0xAD})
	m, err := ReadMessage(r, 100)
	if err != nil {
		t.Fatal(err)
	}
	if m.KeepAlive || m.ID != 42 || !bytes.Equal(m.Payload, []byte{0xDE, 0xAD}) {
		t.Fatalf("got %v %x", m, m.Payload)
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("unknown IDs must pass Validate so they can be ignored: %v", err)
	}
}

func TestMaxMessageLen(t *testing.T) {
	if got, want := MaxMessageLen(10), uint32(1+8+MaxBlockSize); got != want {
		t.Errorf("small torrent: got %d, want %d (piece message dominates)", got, want)
	}
	if got, want := MaxMessageLen(8_000_000), uint32(1+1_000_000); got != want {
		t.Errorf("huge torrent: got %d, want %d (bitfield dominates)", got, want)
	}
}

func TestParsers(t *testing.T) {
	if idx, err := NewHave(42).ParseHave(); err != nil || idx != 42 {
		t.Errorf("ParseHave = %d, %v", idx, err)
	}

	if r, err := NewRequest(1, 2, 3).ParseRequest(); err != nil || r != (Request{1, 2, 3}) {
		t.Errorf("ParseRequest = %+v, %v", r, err)
	}
	if r, err := NewCancel(4, 5, 6).ParseRequest(); err != nil || r != (Request{4, 5, 6}) {
		t.Errorf("ParseRequest on cancel = %+v, %v", r, err)
	}

	p, err := NewPiece(7, 16384, []byte{1, 2, 3}).ParsePiece()
	if err != nil || p.Index != 7 || p.Begin != 16384 || !bytes.Equal(p.Block, []byte{1, 2, 3}) {
		t.Errorf("ParsePiece = %+v, %v", p, err)
	}

	if p, err := NewPiece(0, 0, nil).ParsePiece(); err != nil || len(p.Block) != 0 {
		t.Errorf("empty piece = %+v, %v", p, err)
	}
}

func TestParsersRejectBadInput(t *testing.T) {
	wrong := []struct {
		name string
		call func() error
	}{
		{"have from choke", func() error { _, err := NewChoke().ParseHave(); return err }},
		{"have from keep-alive", func() error { _, err := NewKeepAlive().ParseHave(); return err }},
		{"request from have", func() error { _, err := NewHave(1).ParseRequest(); return err }},
		{"piece from request", func() error { _, err := NewRequest(1, 2, 3).ParsePiece(); return err }},
	}
	for _, tc := range wrong {
		if err := tc.call(); !errors.Is(err, ErrWrongMessage) {
			t.Errorf("%s: got %v, want ErrWrongMessage", tc.name, err)
		}
	}

	bad := []struct {
		name string
		call func() error
	}{
		{"short have", func() error {
			_, err := Message{ID: MsgHave, Payload: []byte{1, 2}}.ParseHave()
			return err
		}},
		{"short request", func() error {
			_, err := Message{ID: MsgRequest, Payload: make([]byte, 11)}.ParseRequest()
			return err
		}},
		{"long cancel", func() error {
			_, err := Message{ID: MsgCancel, Payload: make([]byte, 13)}.ParseRequest()
			return err
		}},
		{"short piece", func() error {
			_, err := Message{ID: MsgPiece, Payload: make([]byte, 7)}.ParsePiece()
			return err
		}},
	}
	for _, tc := range bad {
		if err := tc.call(); !errors.Is(err, ErrBadPayload) {
			t.Errorf("%s: got %v, want ErrBadPayload", tc.name, err)
		}
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		msg  Message
		ok   bool
	}{
		{"keep-alive", NewKeepAlive(), true},
		{"choke empty", Message{ID: MsgChoke}, true},
		{"choke with payload", Message{ID: MsgChoke, Payload: []byte{1}}, false},
		{"interested with payload", Message{ID: MsgInterested, Payload: []byte{1}}, false},
		{"port ok", Message{ID: MsgPort, Payload: []byte{0x1A, 0xE1}}, true},
		{"port short", Message{ID: MsgPort, Payload: []byte{0x1A}}, false},
		{"bitfield any length", Message{ID: MsgBitfield, Payload: []byte{1, 2, 3}}, true},
		{"unknown id", Message{ID: 99, Payload: []byte{1, 2, 3}}, true},
	}
	for _, tc := range tests {
		if err := tc.msg.Validate(); (err == nil) != tc.ok {
			t.Errorf("%s: Validate() = %v, want ok=%v", tc.name, err, tc.ok)
		}
	}
}

func TestKeepAliveIsNotChoke(t *testing.T) {
	if NewKeepAlive().Is(MsgChoke) {
		t.Fatal("a keep-alive must not report as a choke")
	}
	if !NewChoke().Is(MsgChoke) {
		t.Fatal("a choke must report as a choke")
	}
}

func TestMsgIDString(t *testing.T) {
	if MsgPiece.String() != "piece" || MsgNotInterested.String() != "not-interested" {
		t.Error("unexpected names for known IDs")
	}
	if got := MsgID(42).String(); got != "msg(42)" {
		t.Errorf("unknown ID name = %q", got)
	}
}
