package torrent

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
)

var (
	ErrInsufficientSpace = errors.New("storage: not enough free disk space")
	ErrStorageClosed     = errors.New("storage: closed")
)

type StorageConfig struct {
	Preallocate bool

	SkipSpaceCheck bool
}

type Storage struct {
	lay    *layout
	root   string
	paths  []string // absolute path per layout.files entry
	cache  *FileCache
	closed atomic.Bool
}

// OpenStorage validates dest and prepares the on-disk tree. It creates
// directories and zero-length files immediately, but opens nothing else
// unless cfg.Preallocate is set. Existing files are kept untouched, so a
// restart can hash-check them.
func OpenStorage(lay *layout, dest string, cache *FileCache, cfg StorageConfig) (*Storage, error) {
	if lay == nil || len(lay.files) == 0 {
		return nil, errors.New("storage: empty layout")
	}
	if cache == nil {
		return nil, errors.New("storage: nil file cache")
	}

	root, err := filepath.Abs(dest)
	if err != nil {
		return nil, fmt.Errorf("storage: resolving %q: %w", dest, err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("storage: creating %s: %w", root, err)
	}
	if err := checkWritable(root); err != nil {
		return nil, fmt.Errorf("storage: %s is not writable: %w", root, err)
	}

	s := &Storage{lay: lay, root: root, cache: cache, paths: make([]string, len(lay.files))}

	var need int64
	for i, f := range lay.files {
		p := f.fsPath(root)
		s.paths[i] = p

		st, err := os.Stat(p)
		switch {
		case err == nil:
			if !st.Mode().IsRegular() {
				return nil, fmt.Errorf("storage: %s exists and is not a regular file", p)
			}
			if st.Size() > f.length {
				slog.Warn("existing file is larger than the torrent expects; extra bytes ignored",
					"path", p, "size", st.Size(), "expected", f.length)
			}
			need += max(0, f.length-st.Size())
		case errors.Is(err, fs.ErrNotExist):
			need += f.length
		default:
			return nil, fmt.Errorf("storage: stat %s: %w", p, err)
		}
	}

	if !cfg.SkipSpaceCheck && need > 0 {
		if avail, ok := freeSpace(root); ok && uint64(need) > avail {
			return nil, fmt.Errorf("%w: need %d bytes, %d available in %s", ErrInsufficientSpace, need, avail, root)
		}
	}

	// Directory tree for every file, plus the zero-length files themselves:
	// no piece ever touches them, so nothing else would create them.
	for i, f := range lay.files {
		if err := os.MkdirAll(filepath.Dir(s.paths[i]), 0o755); err != nil {
			return nil, fmt.Errorf("storage: creating directory for %s: %w", s.paths[i], err)
		}
		if f.length == 0 {
			fh, err := os.OpenFile(s.paths[i], os.O_CREATE|os.O_WRONLY, 0o644)
			if err != nil {
				return nil, fmt.Errorf("storage: creating empty file %s: %w", s.paths[i], err)
			}
			if err := fh.Close(); err != nil {
				return nil, fmt.Errorf("storage: closing %s: %w", s.paths[i], err)
			}
		}
	}

	if cfg.Preallocate {
		for i, f := range lay.files {
			if f.length == 0 {
				continue
			}
			if err := s.preallocate(i); err != nil {
				return nil, fmt.Errorf("storage: preallocating %s: %w", s.paths[i], err)
			}
		}
	}

	slog.Info("storage ready", "root", root, "files", len(lay.files),
		"bytesNeeded", need, "preallocate", cfg.Preallocate)
	return s, nil
}

func (s *Storage) preallocate(i int) error {
	h, err := s.cache.acquire(s.paths[i], true)
	if err != nil {
		return err
	}
	defer s.cache.release(h)
	return allocate(h.f, s.lay.files[i].length)
}

func checkWritable(dir string) error {
	f, err := os.CreateTemp(dir, ".gotor-probe-*")
	if err != nil {
		return err
	}
	name := f.Name()
	_, werr := f.Write([]byte{0})
	cerr := f.Close()
	rerr := os.Remove(name)
	return errors.Join(werr, cerr, rerr)
}

// Root is the absolute destination directory.
func (s *Storage) Root() string { return s.root }

// Close drops this torrent's handles from the shared cache. The cache itself
// stays open for other torrents.
func (s *Storage) Close() {
	if s.closed.CompareAndSwap(false, true) {
		s.cache.drop(s.paths)
	}
}

// IsMissing reports whether err means the data simply isn't on disk yet (file
// absent, or shorter than the requested range). A resume hash check should
// treat this as "piece not present", not as a failure.
func IsMissing(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, io.ErrUnexpectedEOF)
}

// ReadAt reads len(p) bytes from the torrent stream at offset off, crossing
// file boundaries as needed. It is all-or-error: a range outside the torrent
// is an error, and data past a file's current end yields io.ErrUnexpectedEOF
// (see IsMissing). Safe for concurrent use.
func (s *Storage) ReadAt(p []byte, off int64) (int, error) {
	if s.closed.Load() {
		return 0, ErrStorageClosed
	}
	spans, err := s.lay.spansFor(off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	return s.readSpans(p, spans)
}

// WriteAt writes p at offset off in the torrent stream, creating files lazily
// and crossing file boundaries as needed. Safe for concurrent use, provided
// callers don't write overlapping ranges.
func (s *Storage) WriteAt(p []byte, off int64) (int, error) {
	if s.closed.Load() {
		return 0, ErrStorageClosed
	}
	spans, err := s.lay.spansFor(off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	return s.writeSpans(p, spans)
}

func (s *Storage) ReadPieceAt(p []byte, index int, begin int64) (int, error) {
	if s.closed.Load() {
		return 0, ErrStorageClosed
	}
	spans, err := s.lay.pieceSpans(index, begin, int64(len(p)))
	if err != nil {
		return 0, err
	}
	return s.readSpans(p, spans)
}

func (s *Storage) WritePieceAt(p []byte, index int, begin int64) (int, error) {
	if s.closed.Load() {
		return 0, ErrStorageClosed
	}
	spans, err := s.lay.pieceSpans(index, begin, int64(len(p)))
	if err != nil {
		return 0, err
	}
	return s.writeSpans(p, spans)
}

func (s *Storage) readSpans(p []byte, spans []fileSpan) (int, error) {
	n := 0
	for _, sp := range spans {
		chunk := p[n : n+int(sp.length)]
		m, err := s.readFile(sp.fileIndex, chunk, sp.fileOffset)
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (s *Storage) writeSpans(p []byte, spans []fileSpan) (int, error) {
	n := 0
	for _, sp := range spans {
		chunk := p[n : n+int(sp.length)]
		m, err := s.writeFile(sp.fileIndex, chunk, sp.fileOffset)
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (s *Storage) readFile(i int, p []byte, off int64) (int, error) {
	h, err := s.cache.acquire(s.paths[i], false)
	if err != nil {
		return 0, err
	}
	defer s.cache.release(h)

	n, err := h.f.ReadAt(p, off)
	if err == io.EOF {
		if n == len(p) {
			return n, nil
		}
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

func (s *Storage) writeFile(i int, p []byte, off int64) (int, error) {
	h, err := s.cache.acquire(s.paths[i], true)
	if err != nil {
		return 0, err
	}
	defer s.cache.release(h)
	return h.f.WriteAt(p, off)
}
