package tracker

import (
	"encoding/binary"
	"fmt"
	"net/netip"
)

const (
	compactPeerSize4 = 4 + 2  // IPv4 address + big-endian port
	compactPeerSize6 = 16 + 2 // IPv6 address + big-endian port
)

func parseCompactPeers(b []byte, v6 bool) ([]netip.AddrPort, error) {
	size := compactPeerSize4
	if v6 {
		size = compactPeerSize6
	}
	if len(b)%size != 0 {
		return nil, fmt.Errorf("tracker: compact peer list is %d bytes, not a multiple of %d", len(b), size)
	}

	peers := make([]netip.AddrPort, 0, len(b)/size)
	for i := 0; i < len(b); i += size {
		addr, ok := netip.AddrFromSlice(b[i : i+size-2])
		if !ok {
			continue
		}
		port := binary.BigEndian.Uint16(b[i+size-2 : i+size])

		ap := netip.AddrPortFrom(addr.Unmap(), port)
		if usablePeer(ap) {
			peers = append(peers, ap)
		}
	}
	return peers, nil
}

func usablePeer(ap netip.AddrPort) bool {
	return ap.Port() != 0 && !ap.Addr().IsUnspecified()
}
