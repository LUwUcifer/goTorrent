package bencoder

import (
	"bufio"
	"errors"
	"io"
)

type Decoder struct {
	decodeBuffer *bufio.Reader
}

func NewDecoder(reader io.Reader) *Decoder {
	return &Decoder{
		decodeBuffer: bufio.NewReader(reader),
	}
}

func (d *Decoder) Decode() (any, error) {
	b, err := d.decodeBuffer.ReadByte()
	if err != nil {
		return nil, err
	}

	switch b {
	case 'i':
		return d.decodeInt()
	case 'l':
		return d.decodeList()
	case 'd':
		return d.decodeDict()
	case '1', '2', '3', '4', '5', '6', '7', '8', '9':
		err := d.decodeBuffer.UnreadByte()
		if err != nil {
			return nil, err
		}
		return d.decodeString()
	case 'e':
		_ = d.decodeBuffer.UnreadByte()
		return nil, nil
	default:
		return nil, errors.New("undefined Bencode Type")
	}
}
