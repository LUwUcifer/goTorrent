package bencoder

import (
	"errors"
	"strings"
)

func (d *Decoder) decodeString() (any, error) {
	var strLen int
	var bencodeStr strings.Builder
	for {
		numCheck, err := d.decodeBuffer.ReadByte()
		if err != nil {
			return nil, err
		}

		if isNumber(numCheck) {
			strLen = strLen*10 + int(numCheck-'0')
		} else if numCheck == ':' {
			break
		} else {
			err := d.decodeBuffer.UnreadByte()
			if err != nil {
				return nil, err
			}
			break
		}
	}
	for i := 0; i < strLen; i++ {
		charCheck, err := d.decodeBuffer.ReadByte()
		if err != nil {
			return nil, err
		}
		bencodeStr.WriteString(string(charCheck))
	}
	return bencodeStr.String(), nil
}

func (d *Decoder) decodeInt() (any, error) {
	var bencodeInt int64
	var isNegative = false
	bit, err := d.decodeBuffer.Peek(1)
	if err != nil {
		return nil, err
	}
	if bit[0] == '-' {
		isNegative = true
		_, _ = d.decodeBuffer.ReadByte()
	}
	for {
		bit[0], err = d.decodeBuffer.ReadByte()
		if err != nil {
			return nil, err
		}
		if isNumber(bit[0]) {
			bencodeInt = bencodeInt*10 + int64(bit[0]-'0')
		} else if bit[0] == 'e' {
			break
		} else {
			return nil, errors.New("undefined Bencode digit")
		}
	}

	if isNegative {
		bencodeInt = -bencodeInt
	}
	return bencodeInt, nil
}

func (d *Decoder) decodeList() ([]any, error) {
	bencodeList := make([]any, 0)
	for {
		bit, err := d.decodeBuffer.Peek(1)
		if err != nil {
			return nil, err
		}
		if bit[0] == 'e' {
			_, err := d.decodeBuffer.ReadByte()
			if err != nil {
				return nil, err
			}
			break
		}
		bencodeData, err := d.Decode()
		if err != nil {
			return nil, err
		}
		bencodeList = append(bencodeList, bencodeData)
	}
	return bencodeList, nil
}

func (d *Decoder) decodeDict() (map[string]any, error) {
	var bencodeDict = make(map[string]any)
	for {
		bit, err := d.decodeBuffer.Peek(1)
		if err != nil {
			return nil, err
		}
		if bit[0] == 'e' {
			_, err := d.decodeBuffer.ReadByte()
			if err != nil {
				return nil, err
			}
			break
		}
		key, err := d.decodeString()
		if err != nil {
			return nil, err
		}
		var mapKey = key.(string)
		value, err := d.Decode()
		if err != nil {
			return nil, err
		}
		bencodeDict[mapKey] = value
	}
	return bencodeDict, nil
}

func isNumber(ch byte) bool {
	if ch >= '0' && ch <= '9' {
		return true
	}
	return false
}
