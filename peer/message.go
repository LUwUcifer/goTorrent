package peer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

const (
	BlockSize = 16 * 1024

	MaxBlockSize = 32 * 1024
)

type MsgID uint8

const (
	MsgChoke         MsgID = 0
	MsgUnchoke       MsgID = 1
	MsgInterested    MsgID = 2
	MsgNotInterested MsgID = 3
	MsgHave          MsgID = 4
	MsgBitfield      MsgID = 5
	MsgRequest       MsgID = 6
	MsgPiece         MsgID = 7
	MsgCancel        MsgID = 8
	MsgPort          MsgID = 9 // DHT port (BEP 5); framed and validated, not acted on
)

func (id MsgID) String() string {
	switch id {
	case MsgChoke:
		return "choke"
	case MsgUnchoke:
		return "unchoke"
	case MsgInterested:
		return "interested"
	case MsgNotInterested:
		return "not-interested"
	case MsgHave:
		return "have"
	case MsgBitfield:
		return "bitfield"
	case MsgRequest:
		return "request"
	case MsgPiece:
		return "piece"
	case MsgCancel:
		return "cancel"
	case MsgPort:
		return "port"
	default:
		return fmt.Sprintf("msg(%d)", uint8(id))
	}
}

var (
	// ErrMessageTooLarge means a length prefix exceeded the limit given to
	// ReadMessage. The connection should be dropped.
	ErrMessageTooLarge = errors.New("peer: message too large")

	// ErrBadPayload means a message's payload doesn't match its type.
	ErrBadPayload = errors.New("peer: malformed message payload")

	// ErrWrongMessage means a parser was called on the wrong message type.
	ErrWrongMessage = errors.New("peer: wrong message type")
)

type Message struct {
	KeepAlive bool
	ID        MsgID
	Payload   []byte
}

func (m Message) Is(id MsgID) bool {
	return !m.KeepAlive && m.ID == id
}

func (m Message) String() string {
	if m.KeepAlive {
		return "keep-alive"
	}
	return fmt.Sprintf("%v(%d bytes)", m.ID, len(m.Payload))
}

func ReadMessage(r io.Reader, maxLen uint32) (Message, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Message{}, err
	}

	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		return Message{KeepAlive: true}, nil
	}
	if n > maxLen {
		return Message{}, fmt.Errorf("%w: %d bytes, limit %d", ErrMessageTooLarge, n, maxLen)
	}

	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return Message{}, err
	}
	return Message{ID: MsgID(body[0]), Payload: body[1:]}, nil
}

func WriteMessage(w io.Writer, m Message) error {
	if m.KeepAlive {
		_, err := w.Write([]byte{0, 0, 0, 0})
		return err
	}
	if uint64(len(m.Payload)) >= math.MaxUint32 {
		return fmt.Errorf("%w: payload of %d bytes", ErrMessageTooLarge, len(m.Payload))
	}

	buf := make([]byte, 5+len(m.Payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(1+len(m.Payload)))
	buf[4] = byte(m.ID)
	copy(buf[5:], m.Payload)
	_, err := w.Write(buf)
	return err
}

func MaxMessageLen(numPieces int) uint32 {
	piece := 1 + 8 + MaxBlockSize
	bitfield := 1 + (numPieces+7)/8
	return uint32(max(piece, bitfield))
}

func (m Message) Validate() error {
	if m.KeepAlive {
		return nil
	}

	var want int
	switch m.ID {
	case MsgChoke, MsgUnchoke, MsgInterested, MsgNotInterested:
		want = 0
	case MsgHave:
		want = 4
	case MsgRequest, MsgCancel:
		want = 12
	case MsgPort:
		want = 2
	case MsgPiece:
		if len(m.Payload) < 8 {
			return fmt.Errorf("%w: piece payload is %d bytes, want at least 8", ErrBadPayload, len(m.Payload))
		}
		return nil
	default: // bitfield, or an ID we don't know
		return nil
	}

	if len(m.Payload) != want {
		return fmt.Errorf("%w: %v payload is %d bytes, want %d", ErrBadPayload, m.ID, len(m.Payload), want)
	}
	return nil
}

func NewKeepAlive() Message     { return Message{KeepAlive: true} }
func NewChoke() Message         { return Message{ID: MsgChoke} }
func NewUnchoke() Message       { return Message{ID: MsgUnchoke} }
func NewInterested() Message    { return Message{ID: MsgInterested} }
func NewNotInterested() Message { return Message{ID: MsgNotInterested} }

func NewHave(index uint32) Message {
	p := make([]byte, 4)
	binary.BigEndian.PutUint32(p, index)
	return Message{ID: MsgHave, Payload: p}
}

func NewBitfieldMessage(b Bitfield) Message {
	return Message{ID: MsgBitfield, Payload: append([]byte(nil), b.bits...)}
}

func NewRequest(index, begin, length uint32) Message {
	return newRequestLike(MsgRequest, index, begin, length)
}

func NewCancel(index, begin, length uint32) Message {
	return newRequestLike(MsgCancel, index, begin, length)
}

func newRequestLike(id MsgID, index, begin, length uint32) Message {
	p := make([]byte, 12)
	binary.BigEndian.PutUint32(p[0:4], index)
	binary.BigEndian.PutUint32(p[4:8], begin)
	binary.BigEndian.PutUint32(p[8:12], length)
	return Message{ID: id, Payload: p}
}

func NewPiece(index, begin uint32, block []byte) Message {
	p := make([]byte, 8+len(block))
	binary.BigEndian.PutUint32(p[0:4], index)
	binary.BigEndian.PutUint32(p[4:8], begin)
	copy(p[8:], block)
	return Message{ID: MsgPiece, Payload: p}
}

type Request struct {
	Index  uint32
	Begin  uint32
	Length uint32
}

type Piece struct {
	Index uint32
	Begin uint32
	Block []byte
}

func (m Message) ParseHave() (uint32, error) {
	if !m.Is(MsgHave) {
		return 0, fmt.Errorf("%w: %v is not have", ErrWrongMessage, m)
	}
	if err := m.Validate(); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(m.Payload), nil
}

func (m Message) ParseRequest() (Request, error) {
	if !m.Is(MsgRequest) && !m.Is(MsgCancel) {
		return Request{}, fmt.Errorf("%w: %v is not request or cancel", ErrWrongMessage, m)
	}
	if err := m.Validate(); err != nil {
		return Request{}, err
	}
	return Request{
		Index:  binary.BigEndian.Uint32(m.Payload[0:4]),
		Begin:  binary.BigEndian.Uint32(m.Payload[4:8]),
		Length: binary.BigEndian.Uint32(m.Payload[8:12]),
	}, nil
}

func (m Message) ParsePiece() (Piece, error) {
	if !m.Is(MsgPiece) {
		return Piece{}, fmt.Errorf("%w: %v is not piece", ErrWrongMessage, m)
	}
	if err := m.Validate(); err != nil {
		return Piece{}, err
	}
	return Piece{
		Index: binary.BigEndian.Uint32(m.Payload[0:4]),
		Begin: binary.BigEndian.Uint32(m.Payload[4:8]),
		Block: m.Payload[8:],
	}, nil
}
