package bencoder

import (
	"reflect"
	"strings"
	"testing"
)

//All testcases are derived from
//https://github.com/nrk/bencoder/blob/master/tests/Bencoder/BencodeTest.php

func TestDecoder_Decode(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    any
		wantErr bool
	}{
		{name: "test_integer_decoding_1", input: "i0e", want: int64(0)},
		{name: "test_integer_decoding_2", input: "i1e", want: int64(1)},
		{name: "test_integer_decoding_3", input: "i42e", want: int64(42)},
		{name: "test_integer_decoding_4", input: "i-42e", want: int64(-42)},
		{name: "test_integer_decoding_5", input: "i-9223372036854775808e", want: int64(-9223372036854775808)},
		{name: "test_string_decoding_1", input: "0:", want: ""},
		{name: "test_string_decoding_2", input: "2:  ", want: "  "},
		{name: "test_string_decoding_3", input: "15:this is a test.", want: "this is a test."},
		{name: "test_string_decoding_4", input: "9:123456789", want: "123456789"},
		{name: "test_list_decoding_1", input: "le", want: []any{}},
		{name: "test_list_decoding_2", input: "li1ei2ei3ee", want: []any{int64(1), int64(2), int64(3)}},
		{name: "test_list_decoding_3", input: "llee", want: []any{[]any{}}},
		{
			name:  "test_list_decoding_4",
			input: "ll1:ai1eel1:bi2eel1:ci3eee",
			want: []any{
				[]any{"a", int64(1)},
				[]any{"b", int64(2)},
				[]any{"c", int64(3)},
			},
		},
		{name: "test_dict_decoding_1", input: "de", want: map[string]any{}},
		{
			name:  "test_dict_decoding_2",
			input: "d1:ai1e1:bi2e1:ci3ee",
			want:  map[string]any{"a": int64(1), "b": int64(2), "c": int64(3)},
		},
		{
			name:  "test_dict_decoding_3",
			input: "d1:al1:ni1ee1:bl1:ni2eee",
			want: map[string]any{
				"a": []any{"n", int64(1)},
				"b": []any{"n", int64(2)},
			},
		},
		{
			name:  "test_dict_decoding_4",
			input: "d1:ad1:ni1ee1:bd1:ni2eee",
			want: map[string]any{
				"a": map[string]any{"n": int64(1)},
				"b": map[string]any{"n": int64(2)},
			},
		},
		{
			// Raw bytes >= 0x80 must survive untouched; torrent "pieces" fields are full of them.
			name:  "binary_string_is_not_utf8_mangled",
			input: "3:\xff\x00\x80",
			want:  "\xff\x00\x80",
		},

		// Malformed input must be rejected, not guessed at.
		{name: "err_empty_input", input: "", wantErr: true},
		{name: "err_unknown_type", input: "x", wantErr: true},
		{name: "err_unterminated_int", input: "i42", wantErr: true},
		{name: "err_empty_int", input: "ie", wantErr: true},
		{name: "err_negative_zero", input: "i-0e", wantErr: true},
		{name: "err_int_leading_zero", input: "i007e", wantErr: true},
		{name: "err_int_overflow", input: "i9223372036854775808e", wantErr: true},
		{name: "err_string_longer_than_input", input: "5:abc", wantErr: true},
		{name: "err_string_length_leading_zero", input: "01:a", wantErr: true},
		{name: "err_unterminated_list", input: "li1e", wantErr: true},
		{name: "err_unterminated_dict", input: "d1:ai1e", wantErr: true},
		{name: "err_dict_key_not_a_string", input: "di1ei2ee", wantErr: true},
		{name: "err_duplicate_dict_key", input: "d1:ai1e1:ai2ee", wantErr: true},
		{name: "err_nesting_too_deep", input: strings.Repeat("l", 65) + strings.Repeat("e", 65), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewDecoderBytes([]byte(tt.input)).Decode()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Decode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Decode() got = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestNewDecoderFromReader(t *testing.T) {
	got, err := NewDecoder(strings.NewReader("li1e3:abce")).Decode()
	if err != nil {
		t.Fatal(err)
	}
	if want := []any{int64(1), "abc"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestDecoder_InfoBytes(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string // "" means no info span expected
	}{
		{
			name:  "top-level info is captured byte for byte",
			input: "d8:announce3:url4:infod4:name1:x6:lengthi5eee",
			want:  "d4:name1:x6:lengthi5ee",
		},
		{
			// A nested key called "info" must not be mistaken for the real one.
			name:  "nested info key is ignored",
			input: "d5:otherd4:infoi1ee4:infod1:ai1eee",
			want:  "d1:ai1ee",
		},
		{
			name:  "no info key",
			input: "d1:ai1ee",
			want:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dec := NewDecoderBytes([]byte(tt.input))
			if _, err := dec.Decode(); err != nil {
				t.Fatal(err)
			}
			got, ok := dec.InfoBytes()
			if tt.want == "" {
				if ok {
					t.Fatalf("InfoBytes() = %q, want none", got)
				}
				return
			}
			if !ok || string(got) != tt.want {
				t.Fatalf("InfoBytes() = %q, %v; want %q", got, ok, tt.want)
			}
		})
	}
}

func TestDecoder_Remaining(t *testing.T) {
	dec := NewDecoderBytes([]byte("i1ei2e"))
	if _, err := dec.Decode(); err != nil {
		t.Fatal(err)
	}
	if got := dec.Remaining(); got != 3 {
		t.Fatalf("Remaining() = %d, want 3 (the unread \"i2e\")", got)
	}
}
