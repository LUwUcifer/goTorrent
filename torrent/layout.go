package torrent

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"
)

const (
	// blockSize is the request size used on the wire (16 KiB).
	blockSize = 16 * 1024

	// maxPieceLength is a sanity cap so a hostile torrent can't make us
	// allocate absurd piece buffers. Arbitrary.
	maxPieceLength = 128 << 20
)

// fileEntry is one file in the torrent, in the unified model. Single-file and
// multi-file torrents both become a []fileEntry.
type fileEntry struct {
	path   []string
	length int64

	// offset is where this file starts in the torrent's concatenated byte
	// stream (the stream that pieces are cut from).
	offset int64
	md5sum md5Hex
}

type layout struct {
	files       []fileEntry
	totalLength int64
	pieceLength int64
	numPieces   int
}

func buildLayout(info infoDict) (layout, error) {
	var l layout

	if info.pieceLength <= 0 || info.pieceLength > maxPieceLength {
		return l, fmt.Errorf("invalid piece length %d", info.pieceLength)
	}
	l.pieceLength = info.pieceLength

	if info.multipleFiles {
		root := info.multFileInfo.name
		if err := checkSegment(root); err != nil {
			return l, fmt.Errorf("torrent name: %w", err)
		}
		if len(info.multFileInfo.files) == 0 {
			return l, errors.New("multi-file torrent has no files")
		}
		for i, f := range info.multFileInfo.files {
			if len(f.path) == 0 {
				return l, fmt.Errorf("file %d has an empty path", i)
			}
			path := make([]string, 0, len(f.path)+1)
			path = append(path, root)
			for _, seg := range f.path {
				if err := checkSegment(seg); err != nil {
					return l, fmt.Errorf("file %d path: %w", i, err)
				}
				path = append(path, seg)
			}
			if err := l.add(path, f.length, f.md5sum); err != nil {
				return l, fmt.Errorf("file %d: %w", i, err)
			}
		}
	} else {
		sf := info.singleFileInfo
		if err := checkSegment(sf.name); err != nil {
			return l, fmt.Errorf("torrent name: %w", err)
		}
		if err := l.add([]string{sf.name}, sf.length, sf.md5sum); err != nil {
			return l, err
		}
	}

	if l.totalLength == 0 {
		return l, errors.New("torrent has zero total length")
	}

	expected := l.totalLength / l.pieceLength
	if l.totalLength%l.pieceLength != 0 {
		expected++
	}
	if int64(len(info.pieces)) != expected {
		return l, fmt.Errorf("piece count mismatch: have %d hashes, total length %d with piece length %d needs %d",
			len(info.pieces), l.totalLength, l.pieceLength, expected)
	}
	l.numPieces = len(info.pieces)

	return l, nil
}

func (l *layout) add(path []string, length int64, sum md5Hex) error {
	if length < 0 {
		return fmt.Errorf("negative length %d", length)
	}
	if length > math.MaxInt64-l.totalLength {
		return errors.New("total length overflows int64")
	}
	l.files = append(l.files, fileEntry{
		path:   path,
		length: length,
		offset: l.totalLength,
		md5sum: sum,
	})
	l.totalLength += length
	return nil
}

// checkSegment rejects path elements that could escape the destination
// directory or are otherwise unsafe to create on disk.
func checkSegment(seg string) error {
	switch {
	case seg == "":
		return errors.New("empty path segment")
	case seg == "." || seg == "..":
		return fmt.Errorf("illegal path segment %q", seg)
	case strings.ContainsAny(seg, "/\\\x00"):
		return fmt.Errorf("path segment %q contains a separator or NUL", seg)
	}
	return nil
}

func (f fileEntry) fsPath(dest string) string {
	return filepath.Join(append([]string{dest}, f.path...)...)
}

func (l *layout) pieceOffset(i int) int64 {
	return int64(i) * l.pieceLength
}

func (l *layout) pieceSize(i int) int64 {
	if i < 0 || i >= l.numPieces {
		return 0
	}
	if i == l.numPieces-1 {
		return l.totalLength - l.pieceOffset(i)
	}
	return l.pieceLength
}

func (l *layout) numBlocks(i int) int {
	size := l.pieceSize(i)
	return int((size + blockSize - 1) / blockSize)
}

func (l *layout) blockLen(i, b int) int {
	size := l.pieceSize(i)
	begin := int64(b) * blockSize
	if b < 0 || begin >= size {
		return 0
	}
	return int(min(blockSize, size-begin))
}

type fileSpan struct {
	fileIndex  int
	fileOffset int64 // offset within that file
	length     int64
}

func (l *layout) spansFor(offset, length int64) ([]fileSpan, error) {
	if offset < 0 || length < 0 || length > l.totalLength || offset > l.totalLength-length {
		return nil, fmt.Errorf("range [%d, +%d) is outside the torrent", offset, length)
	}

	i := sort.Search(len(l.files), func(i int) bool {
		return l.files[i].offset+l.files[i].length > offset
	})

	var spans []fileSpan
	for remaining := length; remaining > 0 && i < len(l.files); i++ {
		f := l.files[i]
		if f.length == 0 {
			continue
		}
		within := offset - f.offset
		n := min(f.length-within, remaining)
		spans = append(spans, fileSpan{fileIndex: i, fileOffset: within, length: n})
		offset += n
		remaining -= n
	}
	return spans, nil
}
