package server

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitforge/internal/gitobj"
	"gitforge/internal/store"
)

// chanListener implements net.Listener over a channel so fallback connections
// can be handed off seamlessly to Go's standard http.Server.
type chanListener struct {
	ch     chan net.Conn
	closed chan struct{}
	addr   net.Addr
	once   sync.Once
}

func newChanListener(addr net.Addr) *chanListener {
	return &chanListener{
		ch:     make(chan net.Conn),
		closed: make(chan struct{}),
		addr:   addr,
	}
}

func (l *chanListener) Accept() (net.Conn, error) {
	select {
	case c, ok := <-l.ch:
		if !ok {
			return nil, net.ErrClosed
		}
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *chanListener) Close() error {
	l.once.Do(func() {
		close(l.closed)
	})
	return nil
}

func (l *chanListener) Addr() net.Addr {
	return l.addr
}

type prefixConn struct {
	net.Conn
	r io.Reader
}

type fallbackConn struct {
	net.Conn
	ready chan struct{}
	once  sync.Once
}

func (c *fallbackConn) markReady() { c.once.Do(func() { close(c.ready) }) }

type fallbackConnKey struct{}

// trackedFastConn carries its activity flag atomically so the hot read/response
// path never takes the server-wide mutex; the mutex only guards membership.
type trackedFastConn struct {
	net.Conn
	active atomic.Bool
}

type readPendingConn struct {
	net.Conn
	err error
}

func (c *readPendingConn) Read(p []byte) (int, error) {
	if c.err != nil {
		err := c.err
		c.err = nil
		return 0, err
	}
	n, err := c.Conn.Read(p)
	if n > 0 && err != nil {
		c.err = err
		return n, nil
	}
	return n, err
}

func (c *prefixConn) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

// FastServer manages direct keep-alive connections with zero-allocation fast paths
// for benchmark hot paths, delegating complex/exotic requests to a fallback http.Server.
type FastServer struct {
	Router       *Router
	Forge        *store.Forge
	fallbackLn   *chanListener
	fallbackSrv  *http.Server
	mu           sync.Mutex
	listener     net.Listener
	conns        map[*trackedFastConn]struct{}
	changed      chan struct{}
	stopping     atomic.Bool
	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error

	ForceClose bool

	FastReadHits  atomic.Uint64
	FastWriteHits atomic.Uint64
	FastPushHits  atomic.Uint64
	FastCloneHits atomic.Uint64
	FallbackHits  atomic.Uint64
}

var connBufPool = sync.Pool{
	New: func() any {
		return &connBuf{}
	},
}

type connBuf struct {
	rbuf [65536]byte
	wbuf [16384]byte
	fbuf [8192]byte
}

func NewFastServer(router *Router) *FastServer {
	fs := &FastServer{
		Router:       router,
		Forge:        router.Forge,
		conns:        make(map[*trackedFastConn]struct{}),
		changed:      make(chan struct{}),
		shutdownDone: make(chan struct{}),
	}
	return fs
}

func (fs *FastServer) Start(tcpLn net.Listener) error {
	fs.mu.Lock()
	if fs.stopping.Load() || fs.listener != nil {
		fs.mu.Unlock()
		return http.ErrServerClosed
	}
	fs.listener = tcpLn
	fs.fallbackLn = newChanListener(tcpLn.Addr())
	fs.fallbackSrv = &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Context().Value(fallbackConnKey{}).(*fallbackConn).markReady()
			fs.Router.ServeHTTP(w, r)
		}),
		MaxHeaderBytes:    64 * 1024,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       120 * time.Second,
		WriteTimeout:      30 * time.Second,
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return context.WithValue(ctx, fallbackConnKey{}, c.(*fallbackConn))
		},
		ConnState: func(c net.Conn, state http.ConnState) {
			if state == http.StateClosed || state == http.StateHijacked {
				c.(*fallbackConn).markReady()
			}
		},
	}
	if fs.ForceClose {
		fs.fallbackSrv.SetKeepAlivesEnabled(false)
	}
	fs.mu.Unlock()

	go func() {
		_ = fs.fallbackSrv.Serve(fs.fallbackLn)
	}()

	for {
		conn, err := tcpLn.Accept()
		if err != nil {
			if fs.stopping.Load() {
				return http.ErrServerClosed
			}
			return err
		}
		tracked := &trackedFastConn{Conn: conn}
		fs.mu.Lock()
		if fs.stopping.Load() {
			fs.mu.Unlock()
			conn.Close()
			continue
		}
		fs.conns[tracked] = struct{}{}
		fs.mu.Unlock()
		go fs.handleConn(tracked)
	}
}

func (fs *FastServer) Shutdown(ctx context.Context) error {
	fs.shutdownOnce.Do(func() {
		fs.mu.Lock()
		fs.stopping.Store(true)
		listener, fallback := fs.listener, fs.fallbackSrv
		var idle []*trackedFastConn
		for c := range fs.conns {
			if !c.active.Load() {
				idle = append(idle, c)
			}
		}
		fs.mu.Unlock()
		if listener != nil {
			_ = listener.Close()
		}
		for _, c := range idle {
			_ = c.Close()
		}
		go func() {
			for {
				fs.mu.Lock()
				if len(fs.conns) == 0 {
					fs.mu.Unlock()
					break
				}
				changed := fs.changed
				fs.mu.Unlock()
				<-changed
			}
			if fallback != nil {
				fs.shutdownErr = fallback.Shutdown(context.Background())
			}
			close(fs.shutdownDone)
		}()
	})
	select {
	case <-fs.shutdownDone:
		return fs.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (fs *FastServer) handoff(conn net.Conn, unread []byte) {
	var c net.Conn = conn
	if len(unread) > 0 {
		cp := make([]byte, len(unread))
		copy(cp, unread)
		c = &prefixConn{
			Conn: conn,
			r:    io.MultiReader(bytes.NewReader(cp), conn),
		}
	}
	fallback := &fallbackConn{Conn: c, ready: make(chan struct{})}
	select {
	case fs.fallbackLn.ch <- fallback:
		<-fallback.ready
	case <-fs.fallbackLn.closed:
		_ = conn.Close()
	}
}

type fastHeaderPair struct{ name, value []byte }

// memResponseWriter captures the shared router's response for in-memory
// dispatch on a validated fast-path connection.
type memResponseWriter struct {
	hdr        http.Header
	statusCode int
	body       []byte
	wroteHdr   bool
}

func (m *memResponseWriter) Header() http.Header {
	if m.hdr == nil {
		m.hdr = make(http.Header)
	}
	return m.hdr
}

func (m *memResponseWriter) Write(b []byte) (int, error) {
	if !m.wroteHdr {
		m.WriteHeader(http.StatusOK)
	}
	m.body = append(m.body, b...)
	return len(b), nil
}

func (m *memResponseWriter) WriteHeader(code int) {
	if !m.wroteHdr {
		m.statusCode = code
		m.wroteHdr = true
	}
}

// dispatchInMemory serves a fully validated request that has no fast route
// (REST lists/mutations, GraphQL, MCP, HEAD, domain errors) through the same
// router the standard server uses, then frames the captured response with
// net/http's body rules and keeps the connection on the fast path. Without
// this, one unrouted setup request would hand the whole pooled connection to
// the fallback server and every later hot-path request on it would pay
// net/http parsing and timer costs. Exotic or malformed requests never reach
// here: the parser hands those connections off before dispatch.
func (fs *FastServer) dispatchInMemory(conn net.Conn, buf *connBuf, method, target, host []byte, hdrs []fastHeaderPair, body []byte) (bool, bool) {
	switch string(method) {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
	default:
		return false, false
	}
	u, err := url.ParseRequestURI(string(target))
	if err != nil {
		return false, false
	}
	header := make(http.Header, len(hdrs))
	for _, h := range hdrs {
		key := textproto.CanonicalMIMEHeaderKey(string(h.name))
		header[key] = append(header[key], string(h.value))
	}
	req := &http.Request{
		Method:        string(method),
		URL:           u,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        header,
		Host:          string(host),
		RequestURI:    string(target),
		ContentLength: int64(len(body)),
	}
	if len(body) > 0 {
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	fs.FallbackHits.Add(1)
	m := &memResponseWriter{statusCode: http.StatusOK, body: buf.wbuf[:0]}
	fs.Router.ServeHTTP(m, req)

	w, sendBody := frameMemResponse(buf.fbuf[:0], string(method), m, fs.ForceClose)
	if err := conn.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return true, false
	}
	keepAlive := !fs.ForceClose && !headerValuesContain(m.hdr["Connection"], "close")
	if !sendBody || len(m.body) == 0 {
		_, err := writeFull(conn, w)
		return true, err == nil && keepAlive
	}
	if len(w)+len(m.body) <= len(buf.fbuf) {
		w = append(w, m.body...)
		_, err := writeFull(conn, w)
		return true, err == nil && keepAlive
	}
	if _, err := writeFull(conn, w); err != nil {
		return true, false
	}
	_, err = writeFull(conn, m.body)
	return true, err == nil && keepAlive
}

// frameMemResponse serializes a captured router response with net/http's
// body rules, returning the header block (through the blank line) and whether
// the captured body may be transmitted. 304 drops Content-Type/Content-Length/
// Transfer-Encoding; 1xx/204 drop Content-Length/Transfer-Encoding; HEAD keeps
// the representation headers (Content-Length describes the body a GET would
// return) but transmits no body. Content-Length is always recomputed from the
// captured bytes; Transfer-Encoding is never emitted.
func frameMemResponse(w []byte, method string, m *memResponseWriter, forceClose bool) ([]byte, bool) {
	status := m.statusCode
	isHead := method == "HEAD"
	statusNoBody := status < 200 || status == http.StatusNoContent || status == http.StatusNotModified
	sendBody := !isHead && !statusNoBody
	w = append(w, "HTTP/1.1 "...)
	w = strconv.AppendInt(w, int64(status), 10)
	w = append(w, ' ')
	w = append(w, http.StatusText(status)...)
	w = append(w, "\r\n"...)
	keys := make([]string, 0, len(m.hdr)+1)
	for k := range m.hdr {
		if k == "Content-Length" || k == "Transfer-Encoding" {
			continue
		}
		if status == http.StatusNotModified && k == "Content-Type" {
			continue
		}
		keys = append(keys, k)
	}
	if !statusNoBody {
		keys = append(keys, "Content-Length")
	}
	slices.Sort(keys)
	for _, k := range keys {
		if k == "Content-Length" {
			w = append(w, "Content-Length: "...)
			w = strconv.AppendInt(w, int64(len(m.body)), 10)
			w = append(w, "\r\n"...)
			continue
		}
		for _, v := range m.hdr[k] {
			w = append(w, k...)
			w = append(w, ": "...)
			w = append(w, v...)
			w = append(w, "\r\n"...)
		}
	}
	if forceClose {
		w = append(w, fastCloseHeader...)
	}
	w = append(w, "\r\n"...)
	return w, sendBody
}

func headerValuesContain(vals []string, want string) bool {
	for _, v := range vals {
		if strings.EqualFold(v, want) {
			return true
		}
	}
	return false
}

func (fs *FastServer) handleConn(conn net.Conn) {
	tracked, _ := conn.(*trackedFastConn)
	if tracked != nil {
		defer func() {
			fs.mu.Lock()
			delete(fs.conns, tracked)
			close(fs.changed)
			fs.changed = make(chan struct{})
			fs.mu.Unlock()
		}()
	}
	conn = &readPendingConn{Conn: conn}
	buf := connBufPool.Get().(*connBuf)
	defer connBufPool.Put(buf)

	rpos := 0
	rlen := 0
	waitingIdle := false
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	for {
		// Read until we have at least complete HTTP headers (\r\n\r\n or \n\n)
		hdrEnd := -1
		for {
			unread := buf.rbuf[rpos:rlen]
			end := bytes.Index(unread, []byte("\r\n\r\n"))
			if lf := bytes.Index(unread, []byte("\n\n")); lf >= 0 && (end < 0 || lf < end) {
				fs.handoff(conn, unread)
				return
			}
			if end >= 0 {
				hdrEnd = rpos + end
				break
			}
			// Shift remaining bytes to front if buffer is getting full or empty
			if rpos >= rlen {
				rpos = 0
				rlen = 0
			} else if rpos > 0 {
				copy(buf.rbuf[0:], buf.rbuf[rpos:rlen])
				rlen -= rpos
				rpos = 0
			}
			if rlen >= len(buf.rbuf) {
				// Header larger than 64 KiB: hand off to net/http
				fs.handoff(conn, buf.rbuf[rpos:rlen])
				return
			}
			n, err := conn.Read(buf.rbuf[rlen:])
			if err != nil {
				if err == io.EOF && rlen > rpos {
					fs.handoff(conn, buf.rbuf[rpos:rlen])
					return
				}
				_ = conn.Close()
				return
			}
			rlen += n
			if n > 0 && waitingIdle {
				_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
				waitingIdle = false
			}
			if n > 0 && tracked != nil {
				if fs.stopping.Load() && !tracked.active.Load() {
					conn.Close()
					return
				}
				tracked.active.Store(true)
			}
		}

		requestStart := rpos
		_ = conn.SetReadDeadline(time.Time{})
		rawHdr := buf.rbuf[rpos:hdrEnd]
		bodyStart := hdrEnd + 4

		// Parse request line: METHOD PATH PROTO
		sp1 := bytes.IndexByte(rawHdr, ' ')
		if sp1 <= 0 {
			fs.handoff(conn, buf.rbuf[rpos:rlen])
			return
		}
		method := rawHdr[:sp1]
		restReq := rawHdr[sp1+1:]
		sp2 := bytes.IndexByte(restReq, ' ')
		if sp2 <= 0 {
			fs.handoff(conn, buf.rbuf[rpos:rlen])
			return
		}
		target := restReq[:sp2]
		if target[0] != '/' || !plainFastTarget(target) {
			fs.handoff(conn, buf.rbuf[requestStart:rlen])
			return
		}
		version := restReq[sp2+1:]
		if nl := bytes.IndexByte(version, '\n'); nl >= 0 {
			version = version[:nl]
		}
		if !bytes.Equal(bytes.TrimSuffix(version, []byte("\r")), []byte("HTTP/1.1")) {
			fs.handoff(conn, buf.rbuf[requestStart:rlen])
			return
		}

		// Parse headers in one single scan without allocations
		var (
			contentLength   int = -1
			authHeader      []byte
			gitProtocol     []byte
			hostHeader      []byte
			hasAuth         bool
			hasGitProtocol  bool
			hasConnection   bool
			hasHost         bool
			connectionClose bool
			unsupported     bool
			nHdrs           int
			hdrs            [32]fastHeaderPair
		)

		lineStart := bytes.IndexByte(restReq[sp2:], '\n')
		if lineStart >= 0 {
			lines := restReq[sp2+lineStart+1:]
			for len(lines) > 0 {
				var line []byte
				nl := bytes.IndexByte(lines, '\n')
				if nl >= 0 {
					line = bytes.TrimSuffix(lines[:nl], []byte("\r"))
					lines = lines[nl+1:]
				} else {
					line = lines
					lines = nil
				}
				if len(line) == 0 {
					continue
				}
				colon := bytes.IndexByte(line, ':')
				if colon <= 0 || line[0] == ' ' || line[0] == '\t' {
					unsupported = true
					break
				}
				hname := line[:colon]
				hval := bytes.Trim(line[colon+1:], " 	")
				if !validFastHeader(hname, line[colon+1:]) {
					unsupported = true
					break
				}
				if nHdrs >= len(hdrs) {
					// More headers than the in-memory dispatch can carry: let
					// net/http own the connection.
					unsupported = true
					break
				}
				hdrs[nHdrs] = fastHeaderPair{name: hname, value: hval}
				nHdrs++
				// Only names starting with these letters can match the fast
				// classifier; skip the EqualFold chain for all other headers.
				switch hname[0] | 0x20 {
				case 'c', 'a', 'g', 'h', 't', 'e', 'u':
				default:
					continue
				}

				if bytes.EqualFold(hname, []byte("Content-Length")) {
					n, err := strconv.Atoi(string(hval))
					if err != nil || n < 0 || len(hval) == 0 || hval[0] < '0' || hval[0] > '9' || contentLength >= 0 {
						unsupported = true
						break
					}
					contentLength = n
				} else if bytes.EqualFold(hname, []byte("Authorization")) {
					if hasAuth {
						unsupported = true
						break
					}
					hasAuth = true
					authHeader = hval
				} else if bytes.EqualFold(hname, []byte("Git-Protocol")) {
					if hasGitProtocol {
						unsupported = true
						break
					}
					hasGitProtocol = true
					gitProtocol = hval
				} else if bytes.EqualFold(hname, []byte("Host")) {
					if hasHost || !validFastHost(hval) {
						unsupported = true
						break
					}
					hasHost = true
					hostHeader = hval
				} else if bytes.EqualFold(hname, []byte("Connection")) {
					if hasConnection {
						unsupported = true
						break
					}
					hasConnection = true
					if bytes.EqualFold(hval, []byte("close")) {
						connectionClose = true
					} else if !bytes.EqualFold(hval, []byte("keep-alive")) {
						unsupported = true
						break
					}
				} else if bytes.EqualFold(hname, []byte("Transfer-Encoding")) || bytes.EqualFold(hname, []byte("Content-Encoding")) || bytes.EqualFold(hname, []byte("Expect")) || bytes.EqualFold(hname, []byte("Upgrade")) {
					unsupported = true
					break
				}
			}
		}

		if unsupported || !hasHost || connectionClose {
			fs.handoff(conn, buf.rbuf[rpos:rlen])
			return
		}

		if contentLength < 0 {
			contentLength = 0
		}

		// Ensure body is fully buffered if Content-Length > 0
		for (rlen - bodyStart) < contentLength {
			if len(buf.rbuf)-rlen < contentLength-(rlen-bodyStart) {
				// Body exceeds remaining buffer space: hand off
				fs.handoff(conn, buf.rbuf[rpos:rlen])
				return
			}
			n, err := conn.Read(buf.rbuf[rlen:])
			if err != nil {
				if err == io.EOF {
					fs.handoff(conn, buf.rbuf[requestStart:rlen])
					return
				}
				_ = conn.Close()
				return
			}
			rlen += n
		}

		bodyBytes := buf.rbuf[bodyStart : bodyStart+contentLength]
		rpos = bodyStart + contentLength

		// Attempt fast path
		handled, keepAlive := fs.tryFastPath(conn, buf, method, target, authHeader, gitProtocol, bodyBytes)
		if !handled {
			// Fully validated but unrouted (or a domain error needing the
			// shared handler): serve it in memory and keep the connection
			// fast. Anything the parser could not validate handed off above.
			ok, ka := fs.dispatchInMemory(conn, buf, method, target, hostHeader, hdrs[:nHdrs], bodyBytes)
			if !ok {
				fs.handoff(conn, buf.rbuf[requestStart:rlen])
				return
			}
			keepAlive = ka
		}

		if !keepAlive || fs.ForceClose {
			_ = conn.Close()
			return
		}
		waitingIdle = rpos == rlen
		if waitingIdle {
			_ = conn.SetReadDeadline(time.Now().Add(120 * time.Second))
		} else {
			_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		}
		if tracked != nil {
			if fs.stopping.Load() {
				conn.Close()
				return
			}
			tracked.active.Store(rpos < rlen)
		}
	}
}

func plainFastTarget(target []byte) bool {
	for _, c := range target {
		if c <= ' ' || c >= 127 || c == '%' || c == '#' {
			return false
		}
	}
	return true
}

var fastTchar [256]bool
var fastHostChar [256]bool

func init() {
	for c := 'a'; c <= 'z'; c++ {
		fastTchar[c] = true
		fastHostChar[c] = true
	}
	for c := 'A'; c <= 'Z'; c++ {
		fastTchar[c] = true
		fastHostChar[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		fastTchar[c] = true
		fastHostChar[c] = true
	}
	for _, c := range "!#$%&'*+-.^_`|~" {
		fastTchar[c] = true
	}
	for _, c := range ".-:_[]" {
		fastHostChar[c] = true
	}
}

func validFastHeader(name, value []byte) bool {
	for _, c := range name {
		if !fastTchar[c] {
			return false
		}
	}
	for _, c := range value {
		if c < 32 && c != '	' || c == 127 {
			return false
		}
	}
	return true
}

func validFastHost(host []byte) bool {
	if len(host) == 0 {
		return false
	}
	for _, c := range host {
		if !fastHostChar[c] {
			return false
		}
	}
	return true
}

func fastErrorFrame(status, body string) []byte {
	b := []byte("HTTP/1.1 " + status + "\r\nContent-Type: application/json; charset=utf-8\r\nX-GitHub-Request-Id: 1\r\nContent-Length: ")
	b = strconv.AppendInt(b, int64(len(body)), 10)
	b = append(b, "\r\n\r\n"...)
	return append(b, body...)
}

var fastHealth = store.BuildHTTPResponse("HTTP/1.1 200 OK", "application/json; charset=utf-8", "", healthzBody)

var fastUnauthorized = fastErrorFrame("401 Unauthorized", "{\"message\":\"Requires authentication\"}\n")
var fastNotFound = fastErrorFrame("404 Not Found", "{\"message\":\"Not Found\"}\n")

func writeFull(conn net.Conn, p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n, err := conn.Write(p)
		total += n
		p = p[n:]
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}

var fastCloseHeader = []byte("Connection: close\r\n")

// unwrapWriteConn strips the read/tracking wrappers, which intercept only
// Read, exposing the real network connection for vectored writes.
func unwrapWriteConn(conn net.Conn) net.Conn {
	for {
		switch v := conn.(type) {
		case *readPendingConn:
			conn = v.Conn
		case *trackedFastConn:
			conn = v.Conn
		default:
			return conn
		}
	}
}

// writeFrame emits an immutable cached frame with a fresh request ID. Small
// frames are flattened into the connection's scratch buffer and sent with a
// single write (the cheapest syscall pattern under emulation); large frames
// (pack bodies) go out zero-copy via writev. The header region is bounded so
// binary pack bodies are never scanned for framing on the hot path. Partial
// writes are completed; any error leaves the caller to close the connection.
func (fs *FastServer) writeFrame(conn net.Conn, buf *connBuf, frame []byte) (int, error) {
	const marker = "\r\nX-GitHub-Request-Id: 1\r\n"
	window := frame
	if len(window) > 1024 {
		window = window[:1024]
	}
	headerEnd := bytes.Index(window, []byte("\r\n\r\n"))
	if headerEnd < 0 {
		headerEnd = bytes.Index(frame, []byte("\r\n\r\n"))
		if headerEnd < 0 {
			return 0, io.ErrUnexpectedEOF
		}
	}
	i := bytes.Index(frame[:headerEnd+2], []byte(marker))
	var tail int
	if i < 0 {
		i = bytes.Index(frame[:headerEnd+2], []byte("\r\n"))
		if i < 0 {
			return 0, io.ErrUnexpectedEOF
		}
		tail = i + 2
	} else {
		tail = i + len(marker)
	}
	const idPrefix = "\r\nX-GitHub-Request-Id: "
	id := fs.Forge.NextRequestID()
	var midBuf [80]byte
	mid := append(midBuf[:0], idPrefix...)
	mid = append(mid, id...)
	mid = append(mid, "\r\n"...)
	total := i + len(mid) + len(frame) - tail
	if fs.ForceClose {
		total += len(fastCloseHeader)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return 0, err
	}
	if buf != nil && total <= len(buf.fbuf) {
		w := buf.fbuf[:0]
		w = append(w, frame[:i]...)
		w = append(w, mid...)
		if fs.ForceClose {
			w = append(w, fastCloseHeader...)
		}
		w = append(w, frame[tail:]...)
		return writeFull(conn, w)
	}
	var bufs net.Buffers
	if fs.ForceClose {
		bufs = net.Buffers{frame[:i], mid, fastCloseHeader, frame[tail:]}
	} else {
		bufs = net.Buffers{frame[:i], mid, frame[tail:]}
	}
	n, err := writeBuffersFull(conn, bufs)
	return n, err
}

// writeBuffersFull emits the frame parts with a single vectored write on real
// TCP/Unix connections (writev handles partial writes internally). For any
// other connection type it falls back to per-buffer writeFull loops, because
// net.Buffers' generic writer path does not retry short writes.
func writeBuffersFull(conn net.Conn, bufs net.Buffers) (int, error) {
	// The read/tracking wrappers only intercept Read; writes pass straight
	// through, so the underlying connection can take the writev path.
	wc := unwrapWriteConn(conn)
	switch wc.(type) {
	case *net.TCPConn, *net.UnixConn:
		n, err := bufs.WriteTo(wc)
		return int(n), err
	}
	total := 0
	for _, b := range bufs {
		n, err := writeFull(conn, b)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (fs *FastServer) tryFastPath(conn net.Conn, buf *connBuf, method, target, authHeader, gitProtocol, body []byte) (handled bool, keepAlive bool) {
	// Separate path and query
	path := target
	var query []byte
	if q := bytes.IndexByte(target, '?'); q >= 0 {
		path = target[:q]
		query = target[q+1:]
	}

	// 1. GET /healthz
	if bytes.Equal(method, []byte("GET")) && bytes.Equal(path, []byte("/healthz")) {
		_, err := fs.writeFrame(conn, buf, fastHealth)
		return true, err == nil
	}

	// 2. GET /api/v3/repos/{owner}/{repo}/issues/{number}
	if bytes.Equal(method, []byte("GET")) && bytes.HasPrefix(path, []byte("/api/v3/repos/")) {
		after := path[len("/api/v3/repos/"):]
		s1 := bytes.IndexByte(after, '/')
		if s1 > 0 {
			owner := after[:s1]
			rem := after[s1+1:]
			s2 := bytes.IndexByte(rem, '/')
			if s2 > 0 {
				repoName := rem[:s2]
				tail := rem[s2+1:]
				if bytes.HasPrefix(tail, []byte("issues/")) {
					numBytes := tail[len("issues/"):]
					if bytes.IndexByte(numBytes, '/') < 0 {
						if num, ok := parsePosIntBytes(numBytes); ok {
							if !fs.Forge.CheckAuthBytes(authHeader) {
								_, err := fs.writeFrame(conn, buf, fastUnauthorized)
								return true, err == nil
							}
							repo := fs.Forge.GetRepoBytes(owner, repoName)
							if repo == nil {
								_, err := fs.writeFrame(conn, buf, fastNotFound)
								return true, err == nil
							}
							iss := repo.GetIssue(num)
							if iss == nil {
								_, err := fs.writeFrame(conn, buf, fastNotFound)
								return true, err == nil
							}
							fs.FastReadHits.Add(1)
							_, err := fs.writeFrame(conn, buf, iss.HTTP200Frame())
							return true, err == nil
						}
					}
				}
			}
		}
	}

	// 3. POST /api/v3/repos/{owner}/{repo}/issues
	if bytes.Equal(method, []byte("POST")) && bytes.HasPrefix(path, []byte("/api/v3/repos/")) {
		after := path[len("/api/v3/repos/"):]
		s1 := bytes.IndexByte(after, '/')
		if s1 > 0 {
			owner := after[:s1]
			rem := after[s1+1:]
			s2 := bytes.IndexByte(rem, '/')
			if s2 > 0 {
				repoName := rem[:s2]
				tail := rem[s2+1:]
				if bytes.Equal(tail, []byte("issues")) {
					if !fs.Forge.CheckAuthBytes(authHeader) {
						_, err := fs.writeFrame(conn, buf, fastUnauthorized)
						return true, err == nil
					}
					repo := fs.Forge.GetRepoBytes(owner, repoName)
					if repo == nil {
						_, err := fs.writeFrame(conn, buf, fastNotFound)
						return true, err == nil
					}
					title, bodyStr, ok := store.TryParseSimpleIssueJSON(body)
					if ok {
						iss, err := repo.CreateIssue(title, bodyStr, nil)
						if err != nil {
							return false, false // delegate domain error formatting
						}
						fs.FastWriteHits.Add(1)
						w := buf.wbuf[:0]
						w = append(w, "HTTP/1.1 201 Created\r\nContent-Type: application/json; charset=utf-8\r\nX-GitHub-Request-Id: 1\r\nContent-Length: "...)
						w = strconv.AppendInt(w, int64(len(iss.RestJSON)), 10)
						w = append(w, "\r\n\r\n"...)
						w = append(w, iss.RestJSON...)
						_, writeErr := fs.writeFrame(conn, buf, w)
						return true, writeErr == nil
					}
				}
			}
		}
	}

	// 4. Git Smart HTTP: /{owner}/{repo}.git/...
	if bytes.HasPrefix(path, []byte("/")) {
		rest := path[1:]
		slash1 := bytes.IndexByte(rest, '/')
		if slash1 > 0 {
			owner := rest[:slash1]
			rest2 := rest[slash1+1:]
			slash2 := bytes.IndexByte(rest2, '/')
			var repoSeg, subpath []byte
			if slash2 < 0 {
				repoSeg = rest2
			} else {
				repoSeg = rest2[:slash2]
				subpath = rest2[slash2:]
			}
			if bytes.HasSuffix(repoSeg, []byte(".git")) && len(repoSeg) > 4 {
				repoName := repoSeg[:len(repoSeg)-4]
				if !fs.Forge.CheckAuthBytes(authHeader) {
					return false, false
				}
				repo := fs.Forge.GetRepoBytes(owner, repoName)
				if repo == nil {
					return false, false
				}

				// GET info/refs?service=git-upload-pack (v2)
				if bytes.Equal(method, []byte("GET")) && bytes.Equal(subpath, []byte("/info/refs")) && bytes.Equal(query, []byte("service=git-upload-pack")) {
					if isV2Bytes(gitProtocol) {
						_, err := fs.writeFrame(conn, buf, store.UploadV2HTTP200)
						return true, err == nil
					}
				}

				// GET info/refs?service=git-receive-pack
				if bytes.Equal(method, []byte("GET")) && bytes.Equal(subpath, []byte("/info/refs")) && bytes.Equal(query, []byte("service=git-receive-pack")) {
					_, err := fs.writeFrame(conn, buf, repo.Git().ReceiveV0HTTP200())
					return true, err == nil
				}

				// POST git-upload-pack
				if bytes.Equal(method, []byte("POST")) && bytes.Equal(subpath, []byte("/git-upload-pack")) {
					snap := repo.Git()
					// Check for default ls-refs (peel, symrefs, unborn)
					if isDefaultLsRefsBody(body, snap) {
						fs.FastCloneHits.Add(1)
						_, err := fs.writeFrame(conn, buf, snap.LsRefsHTTP200())
						return true, err == nil
					}
					// Check for depth-1 shallow clone of HEAD
					if snap.HasHead && isDepth1HeadFetch(body, snap) {
						if frame, ok := snap.Depth1FetchHTTP200(); ok {
							fs.FastCloneHits.Add(1)
							_, err := fs.writeFrame(conn, buf, frame)
							return true, err == nil
						}
					}
				}

				// POST git-receive-pack
				if bytes.Equal(method, []byte("POST")) && bytes.Equal(subpath, []byte("/git-receive-pack")) {
					if resp, ok := tryFastReceivePack(repo, body, buf); ok {
						fs.FastPushHits.Add(1)
						_, err := fs.writeFrame(conn, buf, resp)
						return true, err == nil
					}
				}
			}
		}
	}

	return false, false
}

func parsePosIntBytes(b []byte) (int, bool) {
	if len(b) == 0 {
		return 0, false
	}
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
		if n > 1_000_000_000 {
			return 0, false
		}
	}
	return n, n > 0
}

func isV2Bytes(gp []byte) bool {
	return bytes.Equal(gp, []byte("version=2"))
}

func isDefaultLsRefsBody(b []byte, snap *store.GitSnapshot) bool {
	var packets [16]gitobj.Packet
	pkts, end, err := gitobj.DecodePktLines(packets[:0], b, 0)
	if err != nil || end != len(b) {
		return false
	}
	command, delimiter, peel, symrefs, unborn, flush, objectFormat := false, false, false, false, false, false, false
	var prefixes []string
	for _, pkt := range pkts {
		switch pkt.Kind {
		case gitobj.PktDelim:
			if delimiter || !command || flush {
				return false
			}
			delimiter = true
		case gitobj.PktFlush:
			if !delimiter || flush {
				return false
			}
			flush = true
		case gitobj.PktData:
			if flush {
				return false
			}
			line := gitobj.TrimLF(pkt.Payload)
			if !delimiter {
				if bytes.Equal(line, []byte("command=ls-refs")) && !command {
					command = true
				} else if bytes.Equal(line, []byte("object-format=sha1")) && !objectFormat {
					objectFormat = true
				} else if !bytes.HasPrefix(line, []byte("agent=")) {
					return false
				}
				continue
			}
			switch string(line) {
			case "peel":
				if peel {
					return false
				}
				peel = true
			case "symrefs":
				if symrefs {
					return false
				}
				symrefs = true
			case "unborn":
				if unborn {
					return false
				}
				unborn = true
			default:
				if !bytes.HasPrefix(line, []byte("ref-prefix ")) {
					return false
				}
				prefixes = append(prefixes, string(line[len("ref-prefix "):]))
			}
		default:
			return false
		}
	}
	if !(command && delimiter && peel && symrefs && unborn && flush) {
		return false
	}
	if len(prefixes) == 0 {
		return true
	}
	matches := func(name string) bool {
		for _, prefix := range prefixes {
			if strings.HasPrefix(name, prefix) {
				return true
			}
		}
		return false
	}
	if !matches("HEAD") {
		return false
	}
	for _, ref := range snap.SortedRefs {
		if !matches(ref.Name) {
			return false
		}
	}
	return true
}

func isDepth1HeadFetch(b []byte, snap *store.GitSnapshot) bool {
	var packets [16]gitobj.Packet
	pkts, end, err := gitobj.DecodePktLines(packets[:0], b, 0)
	if err != nil || end != len(b) {
		return false
	}
	command, delimiter, want, depth, done, flush, objectFormat, includeTag := false, false, false, false, false, false, false, false
	for _, pkt := range pkts {
		switch pkt.Kind {
		case gitobj.PktDelim:
			if delimiter || !command || flush {
				return false
			}
			delimiter = true
		case gitobj.PktFlush:
			if !delimiter || flush {
				return false
			}
			flush = true
		case gitobj.PktData:
			if flush {
				return false
			}
			line := gitobj.TrimLF(pkt.Payload)
			if !delimiter {
				if bytes.Equal(line, []byte("command=fetch")) && !command {
					command = true
				} else if bytes.Equal(line, []byte("object-format=sha1")) && !objectFormat {
					objectFormat = true
				} else if !bytes.HasPrefix(line, []byte("agent=")) {
					return false
				}
				continue
			}
			switch {
			case bytes.HasPrefix(line, []byte("want ")):
				oid, ok := gitobj.ParseOIDBytes(bytes.TrimSpace(line[5:]))
				if !ok || oid != snap.HeadOID {
					return false
				}
				want = true
			case bytes.Equal(line, []byte("deepen 1")):
				if depth {
					return false
				}
				depth = true
			case bytes.Equal(line, []byte("done")):
				if done {
					return false
				}
				done = true
			case bytes.Equal(line, []byte("thin-pack")), bytes.Equal(line, []byte("ofs-delta")), bytes.Equal(line, []byte("no-progress")):
			case bytes.Equal(line, []byte("include-tag")) && !includeTag:
				includeTag = true
				for _, ref := range snap.SortedRefs {
					if strings.HasPrefix(ref.Name, "refs/tags/") {
						return false
					}
				}
			default:
				return false
			}
		default:
			return false
		}
	}
	return command && delimiter && want && depth && done && flush
}

// tryFastReceivePack handles the 1-ref push command list + pack in a zero-allocation wire frame.
func tryFastReceivePack(repo *store.Repository, body []byte, buf *connBuf) ([]byte, bool) {
	var packets [16]gitobj.Packet
	pkts, packOffset, err := gitobj.DecodePktLines(packets[:0], body, 1)
	if err != nil || len(pkts) < 2 {
		return nil, false
	}
	if pkts[len(pkts)-1].Kind != gitobj.PktFlush {
		return nil, false
	}
	// For standard 1-command push
	if len(pkts) != 2 || pkts[0].Kind != gitobj.PktData {
		return nil, false
	}
	rawLine := gitobj.TrimLF(pkts[0].Payload)
	var sideband, reportStatus, reportStatusV2 bool
	if nul := bytes.IndexByte(rawLine, 0); nul >= 0 {
		caps := rawLine[nul+1:]
		rawLine = rawLine[:nul]
		for _, cap := range bytes.Fields(caps) {
			switch string(cap) {
			case "report-status":
				reportStatus = true
			case "report-status-v2":
				reportStatus = true
				reportStatusV2 = true
			case "side-band-64k", "side-band":
				sideband = true
			case "delete-refs", "quiet", "atomic", "ofs-delta", "object-format=sha1":
			default:
				if !bytes.HasPrefix(cap, []byte("agent=")) {
					return nil, false
				}
			}
		}
	}
	if !reportStatus {
		return nil, false
	}
	if len(rawLine) < 83 || rawLine[40] != ' ' || rawLine[81] != ' ' {
		return nil, false
	}
	oldOID, ok1 := gitobj.ParseOIDBytes(rawLine[0:40])
	newOID, ok2 := gitobj.ParseOIDBytes(rawLine[41:81])
	refName := string(rawLine[82:])
	if !ok1 || !ok2 || refName == "" {
		return nil, false
	}

	packBytes := body[packOffset:]
	unpackErr, statuses := repo.ReceivePack([]store.PushCommand{{OldOID: oldOID, NewOID: newOID, Ref: refName}}, packBytes, false)

	var repBuf [512]byte
	report := repBuf[:0]
	var lineBuf [256]byte
	if unpackErr != "" {
		report = gitobj.AppendPktString(report, "unpack "+unpackErr+"\n")
	} else {
		report = gitobj.AppendPktString(report, "unpack ok\n")
	}
	var prefix, reason string
	if unpackErr != "" {
		prefix, reason = "ng ", " unpacker error"
	} else if len(statuses) != 1 || !statuses[0].OK {
		prefix = "ng "
		if len(statuses) == 1 {
			reason = statuses[0].Reason
		}
		if reason == "" {
			reason = "failed"
		}
		reason = " " + reason
	} else {
		prefix = "ok "
	}
	b := append(lineBuf[:0], prefix...)
	b = append(b, refName...)
	b = append(b, reason...)
	b = append(b, '\n')
	report = gitobj.AppendPktLine(report, b)
	if reportStatusV2 && unpackErr == "" && len(statuses) == 1 && statuses[0].OK {
		// Mirror the standard handler's report-status-v2 option lines exactly.
		b = append(lineBuf[:0], "option refname "...)
		b = append(b, refName...)
		b = append(b, '\n')
		report = gitobj.AppendPktLine(report, b)
		b = append(lineBuf[:0], "option old-oid "...)
		b = statuses[0].OldOID.AppendHex(b)
		b = append(b, '\n')
		report = gitobj.AppendPktLine(report, b)
		b = append(lineBuf[:0], "option new-oid "...)
		b = statuses[0].NewOID.AppendHex(b)
		b = append(b, '\n')
		report = gitobj.AppendPktLine(report, b)
	}
	report = append(report, "0000"...)

	var payload []byte
	if sideband {
		var sbBuf [512]byte
		payload = gitobj.AppendSidebandData(sbBuf[:0], 1, report)
		payload = append(payload, "0000"...)
	} else {
		payload = report
	}

	w := buf.wbuf[:0]
	w = append(w, "HTTP/1.1 200 OK\r\nContent-Type: application/x-git-receive-pack-result\r\nCache-Control: no-cache\r\nContent-Length: "...)
	w = strconv.AppendInt(w, int64(len(payload)), 10)
	w = append(w, "\r\n\r\n"...)
	w = append(w, payload...)
	return w, true
}
