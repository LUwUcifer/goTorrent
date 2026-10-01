package peer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

func testHandshake(infoByte, idByte byte) Handshake {
	var h Handshake
	for i := range h.InfoHash {
		h.InfoHash[i] = infoByte
	}
	for i := range h.PeerID {
		h.PeerID[i] = idByte
	}
	return h
}

func TestHandshakeMarshal(t *testing.T) {
	h := testHandshake(0xAA, 0xBB)
	h.Reserved[5] = 0x10

	want := []byte{19}
	want = append(want, "BitTorrent protocol"...)
	want = append(want, 0, 0, 0, 0, 0, 0x10, 0, 0)
	want = append(want, bytes.Repeat([]byte{0xAA}, 20)...)
	want = append(want, bytes.Repeat([]byte{0xBB}, 20)...)

	got := h.Marshal()
	if len(got) != HandshakeLen || HandshakeLen != 68 {
		t.Fatalf("handshake is %d bytes, want 68", len(got))
	}
	if !bytes.Equal(got[:], want) {
		t.Fatalf("marshal =\n%x\nwant\n%x", got[:], want)
	}
}

func TestHandshakeRoundTrip(t *testing.T) {
	h := testHandshake(1, 2)
	h.Reserved = [8]byte{0, 0, 0, 0, 0, 0x10, 0, 0x05}

	var buf bytes.Buffer
	if err := WriteHandshake(&buf, h); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != HandshakeLen {
		t.Fatalf("wrote %d bytes, want %d", buf.Len(), HandshakeLen)
	}

	got, err := ReadHandshake(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got != h {
		t.Fatalf("got %+v, want %+v", got, h)
	}
	if !got.SupportsExtensions() || !got.SupportsFast() || !got.SupportsDHT() {
		t.Errorf("reserved flags not decoded: ext=%v fast=%v dht=%v",
			got.SupportsExtensions(), got.SupportsFast(), got.SupportsDHT())
	}
	if testHandshake(1, 2).SupportsExtensions() {
		t.Error("an all-zero reserved field must not advertise extensions")
	}
}

func TestReadHandshakeConsumesOnlyTheHandshake(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteHandshake(&buf, testHandshake(1, 2)); err != nil {
		t.Fatal(err)
	}
	if err := WriteMessage(&buf, NewHave(3)); err != nil {
		t.Fatal(err)
	}

	if _, err := ReadHandshake(&buf); err != nil {
		t.Fatal(err)
	}
	m, err := ReadMessage(&buf, 100)
	if err != nil || !m.Is(MsgHave) {
		t.Fatalf("message after the handshake was lost: %v, %v", m, err)
	}
}

func TestReadHandshakeRejects(t *testing.T) {
	good := testHandshake(1, 2).Marshal()

	badLen := good
	badLen[0] = 18
	badName := good
	badName[1] = 'X'

	for name, data := range map[string][]byte{
		"wrong pstrlen":  badLen[:],
		"wrong protocol": badName[:],
		"http not bt":    []byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\nxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
		"all zero bytes": make([]byte, HandshakeLen),
	} {
		if _, err := ReadHandshake(bytes.NewReader(data)); !errors.Is(err, ErrBadProtocol) {
			t.Errorf("%s: got %v, want ErrBadProtocol", name, err)
		}
	}

	if _, err := ReadHandshake(bytes.NewReader(good[:30])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("truncated: got %v, want io.ErrUnexpectedEOF", err)
	}
	if _, err := ReadHandshake(bytes.NewReader(nil)); !errors.Is(err, io.EOF) {
		t.Errorf("empty: got %v, want io.EOF", err)
	}
}

type acceptResult struct {
	h   Handshake
	err error
}

func TestInitiateAcceptHappyPath(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ours := testHandshake(0x11, 0xA1)
	ours.Reserved[5] = 0x10
	remote := testHandshake(0x11, 0xB2)

	acc := make(chan acceptResult, 1)
	go func() {
		h, err := Accept(context.Background(), server, 2*time.Second, func(ih [20]byte) (Handshake, bool) {
			if ih != remote.InfoHash {
				return Handshake{}, false
			}
			return remote, true
		})
		acc <- acceptResult{h, err}
	}()

	theirs, err := Initiate(context.Background(), client, ours, 2*time.Second)
	if err != nil {
		t.Fatalf("Initiate: %v", err)
	}
	if theirs.PeerID != remote.PeerID {
		t.Errorf("Initiate saw peer id %x, want %x", theirs.PeerID, remote.PeerID)
	}

	got := <-acc
	if got.err != nil {
		t.Fatalf("Accept: %v", got.err)
	}
	if got.h.PeerID != ours.PeerID || !got.h.SupportsExtensions() {
		t.Errorf("Accept saw %+v, want our peer id and extension bit", got.h)
	}

	// Deadlines are cleared, and the connection is usable for messages.
	go func() { _ = WriteMessage(server, NewUnchoke()) }()
	m, err := ReadMessage(client, 100)
	if err != nil || !m.Is(MsgUnchoke) {
		t.Fatalf("message after handshake: %v, %v", m, err)
	}
}

func TestInitiateRejectsMismatchedInfoHash(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() {
		if _, err := ReadHandshake(server); err != nil {
			return
		}
		_ = WriteHandshake(server, testHandshake(0x99, 0xB2)) // different torrent
	}()

	_, err := Initiate(context.Background(), client, testHandshake(0x11, 0xA1), 2*time.Second)
	if !errors.Is(err, ErrInfoHashMismatch) {
		t.Fatalf("got %v, want ErrInfoHashMismatch", err)
	}
}

func TestInitiateDetectsSelfConnection(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ours := testHandshake(0x11, 0xA1)
	go func() {
		if _, err := ReadHandshake(server); err != nil {
			return
		}
		_ = WriteHandshake(server, ours) // same info hash and the same peer id
	}()

	if _, err := Initiate(context.Background(), client, ours, 2*time.Second); !errors.Is(err, ErrSelfConnection) {
		t.Fatalf("got %v, want ErrSelfConnection", err)
	}
}

func TestAcceptUnknownTorrentSendsNothing(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	go func() { _ = WriteHandshake(client, testHandshake(0x11, 0xA1)) }()

	_, err := Accept(context.Background(), server, 2*time.Second, func([20]byte) (Handshake, bool) {
		return Handshake{}, false
	})
	if !errors.Is(err, ErrUnknownTorrent) {
		t.Fatalf("got %v, want ErrUnknownTorrent", err)
	}

	// Nothing was written back: a read must hit the deadline, not return data.
	_ = client.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	buf := make([]byte, 1)
	if n, err := client.Read(buf); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read %d bytes, err %v; want nothing sent to an unknown torrent's peer", n, err)
	}
}

func TestAcceptDetectsSelfConnection(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ours := testHandshake(0x11, 0xA1)
	go func() { _ = WriteHandshake(client, ours) }()

	_, err := Accept(context.Background(), server, 2*time.Second, func([20]byte) (Handshake, bool) {
		return ours, true
	})
	if !errors.Is(err, ErrSelfConnection) {
		t.Fatalf("got %v, want ErrSelfConnection", err)
	}
}

func TestInitiateTimesOutOnSilentPeer(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close() // nobody ever reads from server

	start := time.Now()
	_, err := Initiate(context.Background(), client, testHandshake(0x11, 0xA1), 100*time.Millisecond)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("got %v, want a deadline error", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("timeout not honoured: took %v", took)
	}
}

func TestInitiateHonoursContext(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)

	start := time.Now()
	_, err := Initiate(ctx, client, testHandshake(0x11, 0xA1), 10*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("cancellation not honoured: took %v", took)
	}
}
