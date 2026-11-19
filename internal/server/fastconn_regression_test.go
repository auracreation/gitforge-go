package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"gitforge/internal/gitobj"
	"gitforge/internal/store"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"
)

type fastAuditConn struct{ bytes.Buffer }

func (c *fastAuditConn) Close() error                     { return nil }
func (c *fastAuditConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *fastAuditConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *fastAuditConn) SetDeadline(time.Time) error      { return nil }
func (c *fastAuditConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fastAuditConn) SetWriteDeadline(time.Time) error { return nil }

type eofWithDataConn struct {
	fastAuditConn
	reads [][]byte
}

func (c *eofWithDataConn) Read(p []byte) (int, error) {
	if len(c.reads) == 0 {
		return 0, io.EOF
	}
	n := copy(p, c.reads[0])
	c.reads = c.reads[1:]
	if len(c.reads) == 0 {
		return n, io.EOF
	}
	return n, nil
}

func TestFastConnProcessesBytesReturnedWithEOF(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reads  []string
		status int
	}{
		{"header", []string{"GET /healthz HTTP/1.1\r\nHost: example\r\n\r\n"}, http.StatusOK},
		{"body", []string{"POST /api/v3/repos/a/b/issues HTTP/1.1\r\nHost: example\r\nContent-Length: 2\r\n\r\n", "{}"}, http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &eofWithDataConn{}
			for _, read := range tc.reads {
				c.reads = append(c.reads, []byte(read))
			}
			fs := NewFastServer(NewRouter(store.NewForge("audit-token")))
			fs.handleConn(c)
			r, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(c.Bytes())), nil)
			if err != nil {
				t.Fatalf("complete request produced no valid response: %v", err)
			}
			defer r.Body.Close()
			if r.StatusCode != tc.status {
				t.Fatalf("status %d, want %d", r.StatusCode, tc.status)
			}
		})
	}
}

func TestFastServerShutdownClosesListenerAndIdleConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	fs := NewFastServer(NewRouter(store.NewForge("audit-token")))
	serveDone := make(chan error, 1)
	go func() { serveDone <- fs.Start(listener) }()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = io.WriteString(conn, "GET /healthz HTTP/1.1\r\nHost: example\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	r, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.Copy(io.Discard, r.Body); err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = fs.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-serveDone:
	case <-time.After(200 * time.Millisecond):
		t.Error("listener remains open after shutdown")
	}
	conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, err = conn.Read(make([]byte, 1)); err != io.EOF {
		t.Errorf("idle connection read = %v, want EOF", err)
	}
}

type observedListener struct {
	net.Listener
	read chan struct{}
}

func (l *observedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &observedConn{Conn: c, read: l.read}, nil
}

type observedConn struct {
	net.Conn
	read chan struct{}
}

func (c *observedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		select {
		case c.read <- struct{}{}:
		default:
		}
	}
	return n, err
}

func TestFastServerShutdownWaitsForActiveRequest(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	l := &observedListener{Listener: base, read: make(chan struct{}, 1)}
	fs := NewFastServer(NewRouter(store.NewForge("audit-token")))
	go func() { _ = fs.Start(l) }()
	c, err := net.DialTimeout("tcp", base.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	_, err = io.WriteString(c, "POST /api/v3/repos/a/b/issues HTTP/1.1\r\nHost: example\r\nContent-Length: 2\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-l.read:
	case <-time.After(time.Second):
		t.Fatal("server did not read request")
	}
	shutdown := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { shutdown <- fs.Shutdown(ctx) }()
	select {
	case err := <-shutdown:
		t.Fatalf("shutdown returned before active body arrived: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err = io.WriteString(c, "{}"); err != nil {
		t.Fatal(err)
	}
	r, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status %d", r.StatusCode)
	}
	select {
	case err := <-shutdown:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestFastServerShutdownTimeoutStillDrains(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	l := &observedListener{Listener: base, read: make(chan struct{}, 1)}
	fs := NewFastServer(NewRouter(store.NewForge("audit-token")))
	go func() { _ = fs.Start(l) }()
	c, err := net.DialTimeout("tcp", base.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	_, err = io.WriteString(c, "POST /api/v3/repos/a/b/issues HTTP/1.1\r\nHost: example\r\nContent-Length: 2\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-l.read:
	case <-time.After(time.Second):
		t.Fatal("server did not read request")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := fs.Shutdown(ctx); err != context.DeadlineExceeded {
		t.Fatalf("shutdown error %v", err)
	}
	if _, err = io.WriteString(c, "{}"); err != nil {
		t.Fatal(err)
	}
	r, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status %d", r.StatusCode)
	}
	select {
	case <-fs.fallbackLn.closed:
	case <-time.After(time.Second):
		t.Fatal("fallback listener remained open after active request finished")
	}
}

func TestFastServerShutdownWaitsForFallbackMutation(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()
	l := &observedListener{Listener: base, read: make(chan struct{}, 1)}
	f := store.NewForge("audit-token")
	if _, err = f.CreateRepo("audit", "", true, "main"); err != nil {
		t.Fatal(err)
	}
	fs := NewFastServer(NewRouter(f))
	go func() { _ = fs.Start(l) }()
	c, err := net.DialTimeout("tcp", base.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	body := `{"title":"active"}`
	req := "POST /api/v3/repos/" + f.Viewer.Login + "/audit/issues HTTP/1.1\r\nHost: example\r\nAuthorization: Bearer audit-token\r\nTransfer-Encoding: chunked\r\n\r\n" + strconv.FormatInt(int64(len(body)), 16) + "\r\n" + body + "\r\n"
	if _, err = io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	select {
	case <-l.read:
	case <-time.After(time.Second):
		t.Fatal("server did not read request")
	}
	deadline := time.After(time.Second)
	for {
		fs.mu.Lock()
		count, changed := len(fs.conns), fs.changed
		fs.mu.Unlock()
		if count == 0 {
			break
		}
		select {
		case <-changed:
		case <-deadline:
			t.Fatal("fallback did not accept connection")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	shutdown := make(chan error, 1)
	go func() { shutdown <- fs.Shutdown(ctx) }()
	select {
	case err := <-shutdown:
		c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		r, readErr := http.ReadResponse(bufio.NewReader(c), nil)
		if readErr != nil {
			t.Fatalf("shutdown returned while fallback request active: %v; response: %v", err, readErr)
		}
		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)
		t.Fatalf("shutdown returned while fallback request active: %v; response: %d %q", err, r.StatusCode, body)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err = io.WriteString(c, "0\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	r, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusCreated {
		t.Fatalf("status %d", r.StatusCode)
	}
	select {
	case err := <-shutdown:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestAuditFastFetchDepth(t *testing.T) {
	var head gitobj.OID
	head[0] = 1
	for _, depth := range []int{1, 10} {
		body := gitobj.AppendPktString(nil, "command=fetch\n")
		body = append(body, "0001"...)
		body = gitobj.AppendPktString(body, "want "+head.Hex()+"\n")
		body = gitobj.AppendPktString(body, "deepen "+strconv.Itoa(depth)+"\n")
		body = gitobj.AppendPktString(body, "done\n")
		body = append(body, "0000"...)
		if got := isDepth1HeadFetch(body, &store.GitSnapshot{HeadOID: head}); got != (depth == 1) {
			t.Errorf("depth=%d classified as depth-one fast path: %t", depth, got)
		}
	}
}

func TestAuditFastIssueNumberRejectsOverflow(t *testing.T) {
	if number, ok := parsePosIntBytes([]byte("18446744073709551617")); ok {
		t.Fatalf("overflowing issue number accepted as %d", number)
	}
}

func TestAuditFastAuthResponseFraming(t *testing.T) {
	f := store.NewForge("audit-token")
	fs := NewFastServer(NewRouter(f))
	c := &fastAuditConn{}
	handled, _ := fs.tryFastPath(c, &connBuf{}, []byte("GET"), []byte("/api/v3/repos/a/b/issues/1"), nil, nil, nil)
	if !handled {
		t.Fatal("request did not use fast path")
	}
	response, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(c.Bytes())), nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("invalid HTTP framing: advertised=%d actual=%d error=%v", response.ContentLength, len(body), err)
	}
}

type shortFastAuditConn struct{ fastAuditConn }

func (c *shortFastAuditConn) Write(p []byte) (int, error) {
	if len(p) > 3 {
		p = p[:3]
	}
	return c.fastAuditConn.Write(p)
}

type failedWriteAuditConn struct{ fastAuditConn }

func (c *failedWriteAuditConn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	_, _ = c.fastAuditConn.Write(p[:1])
	return 1, io.ErrClosedPipe
}

func TestFastWriteFailureDoesNotRedispatch(t *testing.T) {
	fs := NewFastServer(NewRouter(store.NewForge("audit-token")))
	conn := &failedWriteAuditConn{}
	handled, keepAlive := fs.tryFastPath(conn, &connBuf{}, []byte("GET"), []byte("/healthz"), nil, nil, nil)
	if !handled || keepAlive {
		t.Fatalf("handled=%v keepAlive=%v", handled, keepAlive)
	}
}

func TestAuditFastAuthShortWrites(t *testing.T) {
	f := store.NewForge("audit-token")
	fs := NewFastServer(NewRouter(f))
	c := &shortFastAuditConn{}
	handled, _ := fs.tryFastPath(c, &connBuf{}, []byte("GET"), []byte("/api/v3/repos/a/b/issues/1"), nil, nil, nil)
	if !handled {
		t.Fatal("request did not use fast path")
	}
	response, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(c.Bytes())), nil)
	if err != nil {
		t.Fatalf("incomplete response: %v", err)
	}
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatalf("incomplete body: %v", err)
	}
}

func TestAuditFastFetchRejectsNegotiation(t *testing.T) {
	var head gitobj.OID
	head[0] = 1
	body := gitobj.AppendPktString(nil, "command=fetch\n")
	body = append(body, "0001"...)
	body = gitobj.AppendPktString(body, "want "+head.Hex()+"\n")
	body = gitobj.AppendPktString(body, "deepen 1\n")
	body = gitobj.AppendPktString(body, "have "+head.Hex()+"\n")
	body = gitobj.AppendPktString(body, "done\n")
	body = append(body, "0000"...)
	if isDepth1HeadFetch(body, &store.GitSnapshot{HeadOID: head}) {
		t.Fatal("negotiated fetch used depth-one clone cache")
	}
}

func TestAuditFastFetchRealClientRequest(t *testing.T) {
	var head gitobj.OID
	head[0] = 1
	b := gitobj.AppendPktString(nil, "command=fetch\n")
	b = gitobj.AppendPktString(b, "agent=git/2.39.5\n")
	b = gitobj.AppendPktString(b, "object-format=sha1\n")
	b = append(b, "0001"...)
	for _, line := range []string{"thin-pack", "no-progress", "include-tag", "ofs-delta", "deepen 1", "want " + head.Hex(), "want " + head.Hex(), "done"} {
		b = gitobj.AppendPktString(b, line+"\n")
	}
	b = append(b, "0000"...)
	snap := &store.GitSnapshot{HeadOID: head}
	if !isDepth1HeadFetch(b, snap) {
		t.Fatal("real depth-one request was not eligible for a repository without tags")
	}
	snap.SortedRefs = []store.RefEntry{{Name: "refs/tags/v1"}}
	if isDepth1HeadFetch(b, snap) {
		t.Fatal("include-tag request with tags used a tag-free cached pack")
	}
}

func startFastWire(t *testing.T) net.Conn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := NewFastServer(NewRouter(store.NewForge("audit-token")))
	go func() { _ = srv.Start(listener) }()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); listener.Close(); srv.Shutdown(context.Background()) })
	return conn
}

func startStandardWire(t *testing.T) net.Conn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: NewRouter(store.NewForge("audit-token")), MaxHeaderBytes: 64 * 1024}
	go func() { _ = srv.Serve(listener) }()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); listener.Close(); srv.Shutdown(context.Background()) })
	return conn
}

func TestFastWireMatchesStandardMalformedAuthority(t *testing.T) {
	for _, tc := range []struct{ name, request string }{
		{"percent", "GET /healthz HTTP/1.1\r\nHost: ex%zz\r\n\r\n"},
		{"bracket", "GET /healthz HTTP/1.1\r\nHost: [::1\r\n\r\n"},
		{"space", "GET /healthz HTTP/1.1\r\nHost: bad host\r\n\r\n"},
		{"hash", "GET /healthz HTTP/1.1\r\nHost: bad#host\r\n\r\n"},
		{"at", "GET /healthz HTTP/1.1\r\nHost: user@host\r\n\r\n"},
		{"slash", "GET /healthz HTTP/1.1\r\nHost: bad/host\r\n\r\n"},
		{"non-ascii", "GET /healthz HTTP/1.1\r\nHost: café\r\n\r\n"},
		{"extra-cr", "GET /healthz HTTP/1.1\r\nHost: example\r\r\n\r\n"},
		{"unsupported-transfer", "POST /healthz HTTP/1.1\r\nHost: example\r\nTransfer-Encoding: unknown\r\n\r\n"},
		{"oversized-header", "GET /healthz HTTP/1.1\r\nHost: example\r\nX-Large: " + string(bytes.Repeat([]byte("x"), 70*1024)) + "\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := func(c net.Conn) int {
				t.Helper()
				if _, err := io.WriteString(c, tc.request); err != nil {
					t.Fatal(err)
				}
				r, err := http.ReadResponse(bufio.NewReader(c), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Body.Close()
				return r.StatusCode
			}
			want := status(startStandardWire(t))
			if got := status(startFastWire(t)); got != want {
				t.Fatalf("fast status %d; net/http status %d", got, want)
			}
		})
	}
}

func TestFastWireMatchesStandardPrematureEOF(t *testing.T) {
	for _, tc := range []struct{ name, request string }{
		{"partial-header", "GET /healthz HTTP/1.1\r\nHost: example"},
		{"partial-body", "POST /api/v3/repos/a/b/issues HTTP/1.1\r\nHost: example\r\nContent-Length: 10\r\n\r\n{"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			read := func(c net.Conn) (int, string, error) {
				if _, err := io.WriteString(c, tc.request); err != nil {
					return 0, "", err
				}
				if err := c.(*net.TCPConn).CloseWrite(); err != nil {
					return 0, "", err
				}
				r, err := http.ReadResponse(bufio.NewReader(c), nil)
				if err != nil {
					return 0, "", err
				}
				defer r.Body.Close()
				body, err := io.ReadAll(r.Body)
				return r.StatusCode, string(body), err
			}
			wantStatus, wantBody, wantErr := read(startStandardWire(t))
			gotStatus, gotBody, gotErr := read(startFastWire(t))
			if (wantErr == nil) != (gotErr == nil) || gotStatus != wantStatus || gotBody != wantBody {
				t.Fatalf("fast=(%d,%q,%v), standard=(%d,%q,%v)", gotStatus, gotBody, gotErr, wantStatus, wantBody, wantErr)
			}
		})
	}
}

type shortDeadlineListener struct{ net.Listener }

func (l shortDeadlineListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return shortDeadlineConn{Conn: c}, nil
}

type shortDeadlineConn struct{ net.Conn }

func (c shortDeadlineConn) SetReadDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		deadline = time.Now().Add(100 * time.Millisecond)
	}
	return c.Conn.SetReadDeadline(deadline)
}

func TestFastWireReadAndIdleDeadlines(t *testing.T) {
	for _, tc := range []struct{ name, request string }{
		{"partial-header", "GET /heal"},
		{"idle-after-response", "GET /healthz HTTP/1.1\r\nHost: example\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			fs := NewFastServer(NewRouter(store.NewForge("audit-token")))
			go func() { _ = fs.Start(shortDeadlineListener{base}) }()
			c, err := net.DialTimeout("tcp", base.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Close(); base.Close(); fs.Shutdown(context.Background()) })
			c.SetDeadline(time.Now().Add(700 * time.Millisecond))
			if _, err := io.WriteString(c, tc.request); err != nil {
				t.Fatal(err)
			}
			if tc.name == "idle-after-response" {
				r, err := http.ReadResponse(bufio.NewReader(c), nil)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					t.Fatal(err)
				}
				r.Body.Close()
			}
			if _, err := c.Read(make([]byte, 1)); err != io.EOF {
				t.Fatalf("expected deadline to close connection, got %v", err)
			}
		})
	}
}

func TestFastWireHeaderDeadlineDoesNotLimitBody(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fs := NewFastServer(NewRouter(store.NewForge("audit-token")))
	go func() { _ = fs.Start(shortDeadlineListener{base}) }()
	c, err := net.DialTimeout("tcp", base.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(); base.Close(); fs.Shutdown(context.Background()) })
	c.SetDeadline(time.Now().Add(time.Second))
	if _, err := io.WriteString(c, "GET /healthz HTTP/1.1\r\nHost: example\r\nContent-Length: 2\r\n\r\n{"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(180 * time.Millisecond)
	if _, err := io.WriteString(c, "}"); err != nil {
		t.Fatal(err)
	}
	r, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		t.Fatalf("status %d", r.StatusCode)
	}
}

func TestFastWireResetDoesNotBreakNextConnection(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fs := NewFastServer(NewRouter(store.NewForge("audit-token")))
	go func() { _ = fs.Start(base) }()
	defer func() { base.Close(); fs.Shutdown(context.Background()) }()
	bad, err := net.DialTimeout("tcp", base.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(bad, "POST /healthz HTTP/1.1\r\nHost: example\r\nContent-Length: 10\r\n\r\n{"); err != nil {
		t.Fatal(err)
	}
	if err := bad.(*net.TCPConn).SetLinger(0); err != nil {
		t.Fatal(err)
	}
	bad.Close()
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	r, err := client.Get("http://" + base.Addr().String() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		t.Fatalf("status %d", r.StatusCode)
	}
}

func TestFastWireUnicodeBodyAndPipelineFraming(t *testing.T) {
	f := store.NewForge("audit-token")
	if _, err := f.CreateRepo("unicode", "", true, "main"); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fs := NewFastServer(NewRouter(f))
	go func() { _ = fs.Start(listener) }()
	c, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(); listener.Close(); fs.Shutdown(context.Background()) })
	c.SetDeadline(time.Now().Add(2 * time.Second))
	body := `{"title":"café 🍀","body":"文字"}`
	request := "POST /api/v3/repos/" + f.Viewer.Login + "/unicode/issues HTTP/1.1\r\nHost: example\r\nAuthorization: Bearer audit-token\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n" + body + "GET /healthz HTTP/1.1\r\nHost: example\r\n\r\n"
	if _, err := io.WriteString(c, request); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(c)
	r, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil || r.ContentLength != int64(len(data)) {
		t.Fatalf("body length %d, declared %d, err %v", len(data), r.ContentLength, err)
	}
	var issue struct {
		Title string `json:"title"`
		Body  string `json:"body"`
	}
	if err := json.Unmarshal(data, &issue); err != nil || issue.Title != "café 🍀" || issue.Body != "文字" {
		t.Fatalf("issue %+v, err %v", issue, err)
	}
	next, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Body.Close()
	if next.StatusCode != http.StatusOK {
		t.Fatalf("next status %d", next.StatusCode)
	}
}

func TestFastWireRejectsConflictingContentLength(t *testing.T) {
	conn := startFastWire(t)
	_, err := io.WriteString(conn, "POST /healthz HTTP/1.1\r\nHost: example\r\nContent-Length: 2\r\nContent-Length: 0\r\n\r\nxx")
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("conflicting Content-Length returned %d, expected 400", response.StatusCode)
	}
}

func TestFastWireHeadKeepsNextResponseAligned(t *testing.T) {
	conn := startFastWire(t)
	_, err := io.WriteString(conn, "HEAD /api/v3/user HTTP/1.1\r\nHost: example\r\nAuthorization: Bearer audit-token\r\n\r\nGET /healthz HTTP/1.1\r\nHost: example\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	head, err := http.ReadResponse(reader, &http.Request{Method: "HEAD"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(head.Body); err != nil {
		t.Fatal(err)
	}
	head.Body.Close()
	next, err := http.ReadResponse(reader, &http.Request{Method: "GET"})
	if err != nil {
		t.Fatalf("next response misframed after HEAD: %v", err)
	}
	defer next.Body.Close()
	if next.StatusCode != http.StatusOK {
		t.Fatalf("next response status %d", next.StatusCode)
	}
}

func TestFastWireDuplicateAuthorizationUsesFirstValue(t *testing.T) {
	c := startFastWire(t)
	_, err := io.WriteString(c, "GET /api/v3/repos/a/b/issues/1 HTTP/1.1\r\nHost: example\r\nAuthorization: Bearer wrong\r\nAuthorization: Bearer audit-token\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	r, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", r.StatusCode)
	}
}

func TestAuditV2HeaderExact(t *testing.T) {
	if !isV2Bytes([]byte("version=2")) {
		t.Fatal("expected version 2")
	}
	if isV2Bytes([]byte("version=20")) {
		t.Fatal("version 20 was classified as version 2")
	}
}

func TestFastWireGitBasicAuthentication(t *testing.T) {
	c := startFastWire(t)
	auth := base64.StdEncoding.EncodeToString([]byte("user:audit-token"))
	_, err := io.WriteString(c, "GET /a/missing.git/info/refs?service=git-upload-pack HTTP/1.1\r\nHost: example\r\nAuthorization: Basic "+auth+"\r\nGit-Protocol: version=2\r\n\r\n")
	if err != nil {
		t.Fatal(err)
	}
	r, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	if r.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("Repository not found")) {
		t.Fatalf("status %d body %q", r.StatusCode, body)
	}
}

func TestFastWirePushCapabilityNegotiation(t *testing.T) {
	for _, tc := range []struct{ name, caps, expect string }{
		{"no-report", "", ""},
		{"report-status", "report-status", "ok refs/heads/main\n"},
		{"report-status-v2", "report-status-v2", "option refname refs/heads/main\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := store.NewForge("audit-token")
			repo, err := f.CreateRepo("push-audit", "", true, "main")
			if err != nil {
				t.Fatal(err)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			fs := NewFastServer(NewRouter(f))
			go func() { _ = fs.Start(listener) }()
			t.Cleanup(func() { listener.Close(); fs.Shutdown(context.Background()) })
			tree := gitobj.NewObject(gitobj.ObjTree, nil)
			commit := gitobj.NewObject(gitobj.ObjCommit, gitobj.BuildCommit(tree.OID, nil, "A", "a@example.invalid", 1, "first"))
			line := gitobj.ZeroOID.Hex() + " " + commit.OID.Hex() + " refs/heads/main"
			if tc.caps != "" {
				line += "\x00" + tc.caps
			}
			body := gitobj.AppendPktString(nil, line+"\n")
			body = append(body, "0000"...)
			body = append(body, gitobj.BuildPack([]*gitobj.Object{tree, commit})...)
			url := "http://" + listener.Addr().String() + "/" + f.Viewer.Login + "/push-audit.git/git-receive-pack"
			req, err := http.NewRequest("POST", url, bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer audit-token")
			client := &http.Client{Timeout: 2 * time.Second}
			defer client.CloseIdleConnections()
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			received, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK || (tc.expect == "" && len(received) != 0) || (tc.expect != "" && !bytes.Contains(received, []byte(tc.expect))) {
				t.Fatalf("status %d, response %q", resp.StatusCode, received)
			}
			if snap := repo.Git(); !snap.HasHead || snap.HeadOID != commit.OID {
				t.Fatal("push did not update HEAD")
			}
		})
	}
}

func TestFastPushHandlesRejectedUpdateWithoutRedispatch(t *testing.T) {
	f := store.NewForge("audit-token")
	repo, err := f.CreateRepo("rejected-push", "", true, "main")
	if err != nil {
		t.Fatal(err)
	}
	tree := gitobj.NewObject(gitobj.ObjTree, nil)
	commit := gitobj.NewObject(gitobj.ObjCommit, gitobj.BuildCommit(tree.OID, nil, "A", "a@example.invalid", 1, "first"))
	var old gitobj.OID
	old[0] = 1
	line := old.Hex() + " " + commit.OID.Hex() + " refs/heads/main\x00report-status\n"
	body := gitobj.AppendPktString(nil, line)
	body = append(body, "0000"...)
	body = append(body, gitobj.BuildPack([]*gitobj.Object{tree, commit})...)
	frame, handled := tryFastReceivePack(repo, body, &connBuf{})
	if !handled {
		t.Fatal("rejected update was redispatched after ReceivePack")
	}
	if !bytes.Contains(frame, []byte("ng refs/heads/main non-fast-forward\n")) {
		t.Fatalf("bad failure report %q", frame)
	}
	if repo.Git().HasHead {
		t.Fatal("rejected update changed the ref")
	}
}

func TestFastPushReportsMalformedPackWithoutRedispatch(t *testing.T) {
	f := store.NewForge("audit-token")
	repo, err := f.CreateRepo("bad-pack", "", true, "main")
	if err != nil {
		t.Fatal(err)
	}
	var newOID gitobj.OID
	newOID[0] = 1
	line := gitobj.ZeroOID.Hex() + " " + newOID.Hex() + " refs/heads/main\x00report-status\n"
	body := gitobj.AppendPktString(nil, line)
	body = append(body, "0000broken pack"...)
	frame, handled := tryFastReceivePack(repo, body, &connBuf{})
	if !handled {
		t.Fatal("malformed pack was redispatched after ReceivePack")
	}
	if !bytes.Contains(frame, []byte("ng refs/heads/main unpacker error\n")) {
		t.Fatalf("bad failure report %q", frame)
	}
	if repo.Git().HasHead {
		t.Fatal("malformed pack changed the ref")
	}
}

func TestFastWireHealthHasRequestID(t *testing.T) {
	c := startFastWire(t)
	request := "GET /healthz HTTP/1.1\r\nHost: example\r\n\r\n"
	if _, err := io.WriteString(c, request+request); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(c)
	r, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, r.Body); err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	first := r.Header.Get("X-Github-Request-Id")
	if first == "" {
		t.Fatal("missing request ID")
	}
	r, err = http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if second := r.Header.Get("X-Github-Request-Id"); second == "" || second == first {
		t.Fatalf("request IDs %q and %q", first, second)
	}
}

func TestFastWireRejectsMalformedRequest(t *testing.T) {
	cases := []struct{ name, request string }{
		{"missing-host", "GET /healthz HTTP/1.1\r\n\r\n"},
		{"invalid-version", "GET /healthz HTTP/9.9\r\nHost: example\r\n\r\n"},
		{"invalid-length", "GET /healthz HTTP/1.1\r\nHost: example\r\nContent-Length: xyz\r\n\r\n"},
		{"signed-length", "GET /healthz HTTP/1.1\r\nHost: example\r\nContent-Length: +0\r\n\r\n"},
		{"space-in-name", "GET /healthz HTTP/1.1\r\nHost: example\r\nBad Header: x\r\n\r\n"},
		{"space-before-colon", "GET /healthz HTTP/1.1\r\nHost : example\r\n\r\n"},
		{"invalid-host", "GET /healthz HTTP/1.1\r\nHost: ex\x00ample\r\n\r\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := startFastWire(t)
			if _, err := io.WriteString(c, tc.request); err != nil {
				t.Fatal(err)
			}
			r, err := http.ReadResponse(bufio.NewReader(c), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Body.Close()
			if r.StatusCode < 400 {
				t.Fatalf("malformed request returned %d", r.StatusCode)
			}
		})
	}
}

func TestAuditFastLsRefsCacheKey(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want bool
	}{
		{"exact", []string{"peel", "symrefs", "unborn"}, true},
		{"missing-peel", []string{"symrefs", "unborn"}, false},
		{"missing-symrefs", []string{"peel", "unborn"}, false},
		{"missing-unborn", []string{"peel", "symrefs"}, false},
		{"prefix", []string{"peel", "symrefs", "unborn", "ref-prefix refs/heads/"}, false},
		{"unknown", []string{"peel", "symrefs", "unborn", "unexpected"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := gitobj.AppendPktString(nil, "command=ls-refs\n")
			b = append(b, "0001"...)
			for _, a := range tc.args {
				b = gitobj.AppendPktString(b, a+"\n")
			}
			b = append(b, "0000"...)
			if got := isDefaultLsRefsBody(b, &store.GitSnapshot{}); got != tc.want {
				t.Fatalf("cache match=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestAuditFastLsRefsPrefixesCoverSnapshot(t *testing.T) {
	b := gitobj.AppendPktString(nil, "command=ls-refs\n")
	b = gitobj.AppendPktString(b, "object-format=sha1\n")
	b = append(b, "0001"...)
	for _, line := range []string{"peel", "symrefs", "unborn", "ref-prefix HEAD", "ref-prefix refs/heads/", "ref-prefix refs/tags/"} {
		b = gitobj.AppendPktString(b, line+"\n")
	}
	b = append(b, "0000"...)
	snap := &store.GitSnapshot{SortedRefs: []store.RefEntry{{Name: "refs/heads/main"}, {Name: "refs/tags/v1"}}}
	if !isDefaultLsRefsBody(b, snap) {
		t.Fatal("prefixes cover every advertised ref")
	}
	snap.SortedRefs = append(snap.SortedRefs, store.RefEntry{Name: "refs/notes/hidden"})
	if isDefaultLsRefsBody(b, snap) {
		t.Fatal("cache exposed ref excluded by requested prefixes")
	}
}
