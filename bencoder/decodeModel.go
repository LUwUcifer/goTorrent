package bencoder

import (
	"fmt"
	"io"
)

const maxDepth = 64

type Decoder struct {
	data  []byte
	pos   int
	depth int

	// Raw byte span of the value under the top-level "info" key.
	infoStart, infoEnd int
	infoFound          bool
}

func NewDecoder(reader io.Reader) *Decoder {
	data, err := io.ReadAll(reader)
	d := &Decoder{data: data}
	if err != nil {
		d.data = nil
		d.pos = -1
	}
	return d
}

func NewDecoderBytes(data []byte) *Decoder {
	return &Decoder{data: data}
}

func (d *Decoder) InfoBytes() ([]byte, bool) {
	if !d.infoFound {
		return nil, false
	}
	return d.data[d.infoStart:d.infoEnd], true
}

func (d *Decoder) Remaining() int {
	if d.pos < 0 {
		return 0
	}
	return len(d.data) - d.pos
}

func (d *Decoder) Decode() (any, error) {
	if d.pos < 0 {
		return nil, fmt.Errorf("bencode: failed to read input")
	}
	return d.decodeValue()
}

func (d *Decoder) decodeValue() (any, error) {
	if d.pos >= len(d.data) {
		return nil, io.ErrUnexpectedEOF
	}

	c := d.data[d.pos]
	switch {
	case c == 'i':
		d.pos++
		n, err := d.decodeInt()
		if err != nil {
			return nil, err
		}
		return n, nil

	case c == 'l':
		d.pos++
		l, err := d.decodeList()
		if err != nil {
			return nil, err
		}
		return l, nil

	case c == 'd':
		d.pos++
		m, err := d.decodeDict()
		if err != nil {
			return nil, err
		}
		return m, nil

	case isNumber(c):
		s, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		return s, nil

	default:
		return nil, fmt.Errorf("bencode: unexpected byte %q at offset %d", c, d.pos)
	}
}

func (d *Decoder) enter() error {
	d.depth++
	if d.depth > maxDepth {
		return fmt.Errorf("bencode: nesting deeper than %d", maxDepth)
	}
	return nil
}

func (d *Decoder) leave() { d.depth-- }
