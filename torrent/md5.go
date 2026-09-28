package torrent

import (
	"crypto/md5"
	"encoding/hex"
	"log/slog"
	"strings"
)

func parseMD5(raw map[string]any, attrs ...any) (sum md5Hex, ok bool) {
	s, present := raw["md5sum"].(string)
	if !present {
		slog.Debug("md5sum not provided, skipping", attrs...)
		return sum, false
	}

	if len(s) != len(sum) {
		slog.Warn("md5sum has wrong length, ignoring", append(attrs, "length", len(s))...)
		return sum, false
	}

	s = strings.ToLower(s)
	if _, err := hex.DecodeString(s); err != nil {
		slog.Warn("md5sum is not valid hex, ignoring", append(attrs, "error", err)...)
		return sum, false
	}

	copy(sum[:], s)
	return sum, true
}

func hasMD5(h md5Hex) bool {
	return h != md5Hex{}
}

func md5Raw(h md5Hex) (raw [md5.Size]byte, ok bool) {
	if !hasMD5(h) {
		return raw, false
	}
	if _, err := hex.Decode(raw[:], h[:]); err != nil {
		return raw, false
	}
	return raw, true
}
