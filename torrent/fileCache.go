package torrent

import (
	"container/list"
	"errors"
	"log/slog"
	"os"
	"sync"
)

const DefaultMaxOpenFiles = 128

var ErrCacheClosed = errors.New("storage: file cache closed")

// FileCache is a bounded LRU of open file handles, meant to be created once
// and shared by every torrent so the descriptor cap is global.
//
// ReadAt/WriteAt on *os.File are safe for concurrent use (pread/pwrite), so
// handles need no per-file mutex; a reference count is enough to make sure a
// handle is never closed under an in-flight I/O call.
//
// The cap is soft: if every cached handle is in use, new opens exceed it
// briefly and are trimmed as handles are released. Callers hold at most one
// handle at a time, so this can't deadlock.
type FileCache struct {
	mu     sync.Mutex
	max    int
	m      map[string]*handle
	lru    *list.List // front = most recently used; values are *handle
	closed bool
}

type handle struct {
	path    string
	f       *os.File
	rw      bool
	refs    int
	retired bool // no longer in the cache; close when refs reaches 0
	elem    *list.Element
}

func NewFileCache(maxOpen int) *FileCache {
	if maxOpen <= 0 {
		maxOpen = DefaultMaxOpenFiles
	}
	return &FileCache{
		max: maxOpen,
		m:   make(map[string]*handle),
		lru: list.New(),
	}
}

func openFile(path string, write bool) (*os.File, error) {
	if write {
		return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	}
	return os.Open(path) // read-only, never creates
}

// acquire returns an open handle for path with its refcount raised; the
// caller must call release. A read-only request is satisfied by any cached
// handle. A write request upgrades a cached read-only handle by opening a
// read-write one and retiring the old one.
func (c *FileCache) acquire(path string, write bool) (*handle, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrCacheClosed
	}
	if h := c.m[path]; h != nil && (h.rw || !write) {
		h.refs++
		c.lru.MoveToFront(h.elem)
		c.mu.Unlock()
		return h, nil
	}
	c.mu.Unlock()

	// Open outside the lock so a slow open doesn't stall other files.
	f, err := openFile(path, write)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = f.Close()
		return nil, ErrCacheClosed
	}

	var toClose []*handle
	if h := c.m[path]; h != nil {
		if h.rw || !write {
			// Someone else opened it while we were; use theirs.
			h.refs++
			c.lru.MoveToFront(h.elem)
			c.mu.Unlock()
			_ = f.Close()
			return h, nil
		}
		// Upgrade: retire the read-only handle.
		c.unlinkLocked(h)
		if h.refs == 0 {
			toClose = append(toClose, h)
		}
	}

	h := &handle{path: path, f: f, rw: write, refs: 1}
	h.elem = c.lru.PushFront(h)
	c.m[path] = h
	toClose = append(toClose, c.evictLocked()...)
	c.mu.Unlock()

	closeHandles(toClose)
	return h, nil
}

func (c *FileCache) release(h *handle) {
	c.mu.Lock()
	h.refs--
	var toClose []*handle
	switch {
	case h.refs > 0:
	case h.retired:
		toClose = []*handle{h}
	default:
		toClose = c.evictLocked()
	}
	c.mu.Unlock()
	closeHandles(toClose)
}

// unlinkLocked removes h from the cache and marks it retired.
func (c *FileCache) unlinkLocked(h *handle) {
	if c.m[h.path] == h {
		delete(c.m, h.path)
	}
	if h.elem != nil {
		c.lru.Remove(h.elem)
		h.elem = nil
	}
	h.retired = true
}

// evictLocked unlinks idle least-recently-used handles until the cache is
// within its cap, returning them for the caller to close after unlocking.
func (c *FileCache) evictLocked() []*handle {
	var out []*handle
	for e := c.lru.Back(); e != nil && len(c.m) > c.max; {
		prev := e.Prev()
		if h := e.Value.(*handle); h.refs == 0 {
			c.unlinkLocked(h)
			out = append(out, h)
		}
		e = prev
	}
	return out
}

// drop removes the given paths from the cache. Handles still in use are
// closed when their last reference is released.
func (c *FileCache) drop(paths []string) {
	c.mu.Lock()
	var toClose []*handle
	for _, p := range paths {
		if h := c.m[p]; h != nil {
			c.unlinkLocked(h)
			if h.refs == 0 {
				toClose = append(toClose, h)
			}
		}
	}
	c.mu.Unlock()
	closeHandles(toClose)
}

// Close closes every handle (in-use ones when released) and rejects further
// acquires.
func (c *FileCache) Close() {
	c.mu.Lock()
	c.closed = true
	var toClose []*handle
	for _, h := range c.m {
		c.unlinkLocked(h)
		if h.refs == 0 {
			toClose = append(toClose, h)
		}
	}
	c.mu.Unlock()
	closeHandles(toClose)
}

func closeHandles(hs []*handle) {
	for _, h := range hs {
		if err := h.f.Close(); err != nil {
			slog.Debug("closing file", "path", h.path, "error", err)
		}
	}
}
