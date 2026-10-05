package torrent

import (
	"net/netip"
	"sync/atomic"
)

type Torrent struct {
	torrPath string
	destPath string
	torrName string

	localData metaInfo
	layout    layout

	uploaded   atomic.Int64
	downloaded atomic.Int64
	verified   atomic.Int64

	peerCh chan []netip.AddrPort

	trackers atomic.Pointer[trackerManager]
}

func NewTorrent() *Torrent {
	return &Torrent{peerCh: make(chan []netip.AddrPort, maxTrackers)}
}

func (tor *Torrent) Peers() <-chan []netip.AddrPort { return tor.peerCh }

type transferStats struct {
	uploaded, downloaded, left int64
}

func (tor *Torrent) transferStats() transferStats {
	return transferStats{
		uploaded:   tor.uploaded.Load(),
		downloaded: tor.downloaded.Load(),
		left:       tor.bytesLeft(),
	}
}

func (tor *Torrent) bytesLeft() int64 {
	return max(0, tor.layout.totalLength-tor.verified.Load())
}

func (tor *Torrent) pieceVerified(index int) {
	size := tor.layout.pieceSize(index)
	if size == 0 {
		return
	}
	if tor.verified.Add(size) >= tor.layout.totalLength {
		if m := tor.trackers.Load(); m != nil {
			m.markCompleted()
		}
	}
}
