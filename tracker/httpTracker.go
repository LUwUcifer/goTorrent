package tracker

import (
	"context"
	"errors"
	"fmt"
	"goTor/bencoder"
	"io"
	"math"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

const (
	httpTimeout = 30 * time.Second

	maxResponseSize = 1 << 20

	userAgent = "goTor/0.1"

	maxTextLen = 256
)

func init() {
	register("http", newHTTPTracker)
	register("https", newHTTPTracker)
}

type httpTracker struct {
	url     *url.URL
	display string // scheme://host only; path and query may hold a passkey
	client  *http.Client
}

func newHTTPTracker(u *url.URL) (Tracker, error) {
	return &httpTracker{
		url:     u,
		display: u.Scheme + "://" + u.Host,
		client:  &http.Client{Timeout: httpTimeout},
	}, nil
}

func (t *httpTracker) URL() string { return t.display }

func (t *httpTracker) Announce(ctx context.Context, req AnnounceRequest) (AnnounceResponse, error) {
	var zero AnnounceResponse
	if err := req.Validate(); err != nil {
		return zero, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, t.announceURL(req), nil)
	if err != nil {
		return zero, fmt.Errorf("tracker: building request for %s: %w", t.display, stripURLError(err))
	}
	httpReq.Header.Set("User-Agent", userAgent)

	resp, err := t.client.Do(httpReq)
	if err != nil {
		return zero, fmt.Errorf("tracker: announce to %s failed: %w", t.display, stripURLError(err))
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("tracker: announce to %s failed: %v\n", t.display, err)
		}
	}(resp.Body)

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize+1))
	if err != nil {
		return zero, fmt.Errorf("tracker: reading response from %s: %w", t.display, stripURLError(err))
	}
	if len(body) > maxResponseSize {
		return zero, fmt.Errorf("tracker: response from %s exceeds %d bytes", t.display, maxResponseSize)
	}

	if resp.StatusCode != http.StatusOK {
		_, perr := parseHTTPResponse(body)
		if _, ok := errors.AsType[*FailureError](perr); ok {
			return zero, perr
		}
		return zero, fmt.Errorf("tracker: %s returned %s", t.display, resp.Status)
	}

	return parseHTTPResponse(body)
}

func (t *httpTracker) announceURL(req AnnounceRequest) string {
	var b strings.Builder

	b.WriteString("info_hash=")
	b.WriteString(escapeBytes(req.InfoHash[:]))
	b.WriteString("&peer_id=")
	b.WriteString(escapeBytes(req.PeerID[:]))

	fmt.Fprintf(&b, "&port=%d&uploaded=%d&downloaded=%d&left=%d&compact=1&key=%08x",
		req.Port, req.Uploaded, req.Downloaded, req.Left, req.Key)

	if ev := req.Event.httpParam(); ev != "" {
		b.WriteString("&event=")
		b.WriteString(ev)
	}
	if req.NumWant > 0 {
		fmt.Fprintf(&b, "&numwant=%d", req.NumWant)
	}
	if req.TrackerID != "" {
		b.WriteString("&trackerid=")
		b.WriteString(url.QueryEscape(req.TrackerID))
	}

	u := *t.url
	u.Fragment = ""
	if u.RawQuery != "" {
		u.RawQuery += "&" + b.String()
	} else {
		u.RawQuery = b.String()
	}
	return u.String()
}

const upperHex = "0123456789ABCDEF"

// escapeBytes percent-encodes every byte except the RFC 3986 unreserved set.
func escapeBytes(data []byte) string {
	var b strings.Builder
	b.Grow(len(data) * 3)
	for _, c := range data {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(upperHex[c>>4])
			b.WriteByte(upperHex[c&0x0F])
		}
	}
	return b.String()
}

func parseHTTPResponse(body []byte) (AnnounceResponse, error) {
	var resp AnnounceResponse

	decoded, err := bencoder.NewDecoderBytes(body).Decode()
	if err != nil {
		return resp, fmt.Errorf("tracker: decoding response: %w", err)
	}
	m, ok := decoded.(map[string]any)
	if !ok {
		return resp, errors.New("tracker: response is not a dictionary")
	}

	if reason, ok := m["failure reason"].(string); ok {
		return resp, &FailureError{Reason: cleanText(reason)}
	}

	if s, ok := m["warning message"].(string); ok {
		resp.Warning = cleanText(s)
	}
	if s, ok := m["tracker id"].(string); ok {
		resp.TrackerID = cleanText(s)
	}
	if n, ok := m["interval"].(int64); ok {
		resp.Interval = secondsToDuration(n)
	}
	if n, ok := m["min interval"].(int64); ok {
		resp.MinInterval = secondsToDuration(n)
	}
	if n, ok := m["complete"].(int64); ok && n > 0 {
		resp.Seeders = int(min(n, math.MaxInt32))
	}
	if n, ok := m["incomplete"].(int64); ok && n > 0 {
		resp.Leechers = int(min(n, math.MaxInt32))
	}

	switch p := m["peers"].(type) {
	case nil:
	case string:
		peers, err := parseCompactPeers([]byte(p), false)
		if err != nil {
			return resp, err
		}
		resp.Peers = append(resp.Peers, peers...)
	case []any:
		resp.Peers = append(resp.Peers, parseDictPeers(p)...)
	default:
		return resp, fmt.Errorf("tracker: unexpected type %T for peers", p)
	}

	if p6, ok := m["peers6"].(string); ok {
		peers, err := parseCompactPeers([]byte(p6), true)
		if err != nil {
			return resp, err
		}
		resp.Peers = append(resp.Peers, peers...)
	}

	return resp, nil
}

func parseDictPeers(list []any) []netip.AddrPort {
	peers := make([]netip.AddrPort, 0, len(list))
	for _, item := range list {
		d, ok := item.(map[string]any)
		if !ok {
			continue
		}
		ipStr, _ := d["ip"].(string)
		port, _ := d["port"].(int64)

		addr, err := netip.ParseAddr(ipStr)
		if err != nil || port <= 0 || port > math.MaxUint16 {
			continue
		}
		ap := netip.AddrPortFrom(addr.WithZone("").Unmap(), uint16(port))
		if usablePeer(ap) {
			peers = append(peers, ap)
		}
	}
	return peers
}

func secondsToDuration(n int64) time.Duration {
	if n < 0 {
		return 0
	}
	return time.Duration(min(n, math.MaxInt32)) * time.Second
}

func cleanText(s string) string {
	if len(s) > maxTextLen {
		s = s[:maxTextLen]
	}
	return strings.ToValidUTF8(s, "?")
}

func stripURLError(err error) error {
	if ue, ok := errors.AsType[*url.Error](err); ok {
		return ue.Err
	}
	return err
}
