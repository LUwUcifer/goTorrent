package bencoder

import (
	"errors"
	"fmt"
	"io"
	"strconv"
)

var (
	errBadStringLen = errors.New("bencode: invalid string length")
	errBadInt       = errors.New("bencode: invalid integer")
)

func (d *Decoder) decodeString() (string, error) {
	start := d.pos
	for d.pos < len(d.data) && isNumber(d.data[d.pos]) {
		d.pos++
	}
	if d.pos == start || d.pos >= len(d.data) || d.data[d.pos] != ':' {
		return "", errBadStringLen
	}

	digits := d.data[start:d.pos]
	if len(digits) > 1 && digits[0] == '0' {
		return "", errBadStringLen // leading zeros are not canonical
	}
	n, err := strconv.Atoi(string(digits))
	if err != nil || n < 0 {
		return "", errBadStringLen
	}

	d.pos++ // skip ':'
	if n > len(d.data)-d.pos {
		return "", io.ErrUnexpectedEOF // claimed length exceeds input
	}

	s := string(d.data[d.pos : d.pos+n])
	d.pos += n
	return s, nil
}

func (d *Decoder) decodeInt() (int64, error) {
	start := d.pos
	if d.pos < len(d.data) && d.data[d.pos] == '-' {
		d.pos++
	}

	digitsStart := d.pos
	for d.pos < len(d.data) && isNumber(d.data[d.pos]) {
		d.pos++
	}
	if d.pos >= len(d.data) {
		return 0, io.ErrUnexpectedEOF
	}
	if d.pos == digitsStart || d.data[d.pos] != 'e' {
		return 0, errBadInt // "ie", "i-e", or a stray character
	}

	digits := d.data[digitsStart:d.pos]
	if len(digits) > 1 && digits[0] == '0' {
		return 0, errBadInt // "i007e"
	}
	if digitsStart > start && digits[0] == '0' {
		return 0, errBadInt // "i-0e"
	}

	n, err := strconv.ParseInt(string(d.data[start:d.pos]), 10, 64)
	if err != nil {
		return 0, errBadInt // overflow
	}

	d.pos++
	return n, nil
}

func (d *Decoder) decodeList() ([]any, error) {
	if err := d.enter(); err != nil {
		return nil, err
	}
	defer d.leave()

	list := make([]any, 0)
	for {
		if d.pos >= len(d.data) {
			return nil, io.ErrUnexpectedEOF
		}
		if d.data[d.pos] == 'e' {
			d.pos++
			return list, nil
		}
		item, err := d.decodeValue()
		if err != nil {
			return nil, err
		}
		list = append(list, item)
	}
}

func (d *Decoder) decodeDict() (map[string]any, error) {
	if err := d.enter(); err != nil {
		return nil, err
	}
	defer d.leave()

	dict := make(map[string]any)
	for {
		if d.pos >= len(d.data) {
			return nil, io.ErrUnexpectedEOF
		}
		if d.data[d.pos] == 'e' {
			d.pos++
			return dict, nil
		}

		key, err := d.decodeString()
		if err != nil {
			return nil, fmt.Errorf("bencode: bad dictionary key: %w", err)
		}
		if _, dup := dict[key]; dup {
			return nil, fmt.Errorf("bencode: duplicate dictionary key %q", key)
		}

		valueStart := d.pos
		value, err := d.decodeValue()
		if err != nil {
			return nil, err
		}

		if key == "info" && d.depth == 1 && !d.infoFound {
			d.infoStart, d.infoEnd, d.infoFound = valueStart, d.pos, true
		}

		dict[key] = value
	}
}

func isNumber(ch byte) bool {
	return ch >= '0' && ch <= '9'
}
