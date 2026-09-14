package bencoder

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

//All testcases are derived from
//https://github.com/nrk/bencoder/blob/master/tests/Bencoder/BencodeTest.php

func TestDecoder_Decode(t *testing.T) {
	type fields struct {
		decodeBuffer *bufio.Reader
	}
	tests := []struct {
		name    string
		fields  fields
		want    any
		wantErr bool
	}{
		{
			name: "test_integer_decoding_1",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("i0e")),
			},
			want:    int64(0),
			wantErr: false,
		},
		{
			name: "test_integer_decoding_2",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("i1e")),
			},
			want:    int64(1),
			wantErr: false,
		},
		{
			name: "test_integer_decoding_3",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("i42e")),
			},
			want:    int64(42),
			wantErr: false,
		},
		{
			name: "test_integer_decoding_4",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("i-42e")),
			},
			want:    int64(-42),
			wantErr: false,
		},
		{
			name: "test_integer_decoding_5",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("i-9223372036854775808e")),
			},
			want:    int64(-9223372036854775808),
			wantErr: false,
		},
		{
			name: "test_string_decoding_1",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("0:")),
			},
			want:    "",
			wantErr: false,
		},
		{
			name: "test_string_decoding_2",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("2:  ")),
			},
			want:    "  ",
			wantErr: false,
		},
		{
			name: "test_string_decoding_3",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("15:this is a test.")),
			},
			want:    "this is a test.",
			wantErr: false,
		},
		{
			name: "test_string_decoding_4",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("9:123456789")),
			},
			want:    "123456789",
			wantErr: false,
		},
		{
			name: "test_list_decoding_1",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("le")),
			},
			want:    []any{},
			wantErr: false,
		},
		{
			name: "test_list_decoding_2",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("li1ei2ei3ee")),
			},
			want:    []any{int64(1), int64(2), int64(3)},
			wantErr: false,
		},
		{
			name: "test_list_decoding_3",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("llee")),
			},
			want:    []any{[]any{}},
			wantErr: false,
		},
		{
			name: "test_list_decoding_4",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("ll1:ai1eel1:bi2eel1:ci3eee")),
			},
			want: []any{
				[]any{"a", int64(1)},
				[]any{"b", int64(2)},
				[]any{"c", int64(3)},
			},
			wantErr: false,
		},
		{
			name: "test_dict_decoding_1",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("de")),
			},
			want:    map[string]any{},
			wantErr: false,
		},
		{
			name: "test_dict_decoding_2",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("d1:ai1e1:bi2e1:ci3ee")),
			},
			want: map[string]any{
				"a": int64(1),
				"b": int64(2),
				"c": int64(3),
			},
			wantErr: false,
		},
		{
			name: "test_dict_decoding_2",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("d1:al1:ni1ee1:bl1:ni2eee")),
			},
			want: map[string]any{
				"a": []any{"n", int64(1)},
				"b": []any{"n", int64(2)},
			},
			wantErr: false,
		},
		{
			name: "test_dict_decoding_3",
			fields: fields{
				decodeBuffer: bufio.NewReader(bytes.NewBufferString("d1:ad1:ni1ee1:bd1:ni2eee")),
			},
			want: map[string]any{
				"a": map[string]any{"n": int64(1)},
				"b": map[string]any{"n": int64(2)},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Decoder{
				decodeBuffer: tt.fields.decodeBuffer,
			}
			got, err := d.Decode()
			if (err != nil) != tt.wantErr {
				t.Errorf("Decode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Decode() got = %v, want %v", got, tt.want)
			}
		})
	}
}
