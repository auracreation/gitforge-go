package server

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"gitforge/internal/store"
)

// startDispatchWire starts a FastServer over a real TCP listener and returns a
// dialed connection plus the server (for counter assertions).
func startDispatchWire(t *testing.T, f *store.Forge) (net.Conn, *FastServer) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fs := NewFastServer(NewRouter(f))
	go func() { _ = fs.Start(listener) }()
	t.Cleanup(func() { listener.Close(); fs.Shutdown(context.Background()) })
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, fs
}

func wireRequest(t *testing.T, conn net.Conn, br *bufio.Reader, raw string, body []byte) *http.Response {
	t.Helper()
	if len(body) > 0 {
		raw = strings.Replace(raw, "\r\n\r\n", "\r\nContent-Length: "+strconv.Itoa(len(body))+"\r\n\r\n", 1)
	}
	if _, err := conn.Write(append([]byte(raw), body...)); err != nil {
		t.Fatal(err)
	}
	method := raw[:strings.Index(raw, " ")]
	// Pass the request so ReadResponse knows HEAD (and 204/304) carry no body
	// even when Content-Length describes the representation.
	r, err := http.ReadResponse(br, &http.Request{Method: method})
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return r
}

// TestDispatchKeepsConnectionFast reproduces the workload pattern that used to
// poison the pooled connection: an unrouted setup request (repo creation)
// followed by hot-path issue reads on the SAME connection. The reads must stay
// on the fast path instead of riding the fallback server.
func TestDispatchKeepsConnectionFast(t *testing.T) {
	f := store.NewForge("audit-token")
	conn, fs := startDispatchWire(t, f)
	br := bufio.NewReader(conn)
	const auth = "Authorization: Bearer audit-token\r\n"

	// Unrouted: POST /api/v3/user/repos -> in-memory dispatch, connection stays fast.
	r := wireRequest(t, conn, br, "POST /api/v3/user/repos HTTP/1.1\r\nHost: example\r\n"+auth+"Content-Type: application/json\r\n\r\n",
		[]byte(`{"name":"dispatch-repo","private":true,"auto_init":false}`))
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != http.StatusCreated {
		t.Fatalf("repo create status %d body %q", r.StatusCode, b)
	}

	// Fast route: create an issue.
	r = wireRequest(t, conn, br, "POST /api/v3/repos/"+f.Viewer.Login+"/dispatch-repo/issues HTTP/1.1\r\nHost: example\r\n"+auth+"Content-Type: application/json\r\n\r\n",
		[]byte(`{"title":"t","body":"b"}`))
	b, _ = io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != http.StatusCreated {
		t.Fatalf("issue create status %d body %q", r.StatusCode, b)
	}

	// Hot reads on the same connection must hit the fast path.
	for i := 0; i < 3; i++ {
		r = wireRequest(t, conn, br, "GET /api/v3/repos/"+f.Viewer.Login+"/dispatch-repo/issues/1 HTTP/1.1\r\nHost: example\r\n"+auth+"\r\n", nil)
		b, _ = io.ReadAll(r.Body)
		r.Body.Close()
		if r.StatusCode != http.StatusOK || !bytes.Contains(b, []byte(`"title":"t"`)) {
			t.Fatalf("read %d status %d body %q", i, r.StatusCode, b)
		}
	}
	if got := fs.FastReadHits.Load(); got != 3 {
		t.Fatalf("reads did not stay on the fast path: FastReadHits=%d FallbackHits=%d", got, fs.FallbackHits.Load())
	}
	if fs.FallbackHits.Load() == 0 {
		t.Fatal("repo create did not dispatch in memory")
	}
}

// TestDispatchFramingMatchesOracle compares in-memory dispatch responses
// against the standard net/http router for the statuses the evaluator exercises.
func TestDispatchFramingMatchesOracle(t *testing.T) {
	fast := store.NewForge("audit-token")
	std := store.NewForge("audit-token")
	for _, f := range []*store.Forge{fast, std} {
		if _, err := f.CreateRepo("framing", "", true, "main"); err != nil {
			t.Fatal(err)
		}
	}
	conn, _ := startDispatchWire(t, fast)
	br := bufio.NewReader(conn)
	const auth = "Authorization: Bearer audit-token\r\n"
	owner := fast.Viewer.Login

	for _, tc := range []struct {
		name, raw string
		body      []byte
	}{
		{"list-200", "GET /api/v3/repos/" + owner + "/framing/issues HTTP/1.1\r\nHost: example\r\n" + auth + "\r\n", nil},
		{"create-escaped-201", "POST /api/v3/repos/" + owner + "/framing/issues HTTP/1.1\r\nHost: example\r\n" + auth + "Content-Type: application/json\r\n\r\n",
			[]byte(`{"title":"quoted \"title\"","body":"line\nbreak"}`)},
		{"unknown-404", "GET /definitely/not/a/route HTTP/1.1\r\nHost: example\r\n" + auth + "\r\n", nil},
		{"unauth-401", "GET /api/v3/repos/" + owner + "/framing/issues HTTP/1.1\r\nHost: example\r\n\r\n", nil},
		{"head-healthz", "HEAD /healthz HTTP/1.1\r\nHost: example\r\n\r\n", nil},
		{"delete-204", "DELETE /api/v3/repos/" + owner + "/framing HTTP/1.1\r\nHost: example\r\n" + auth + "\r\n", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := wireRequest(t, conn, br, tc.raw, tc.body)
			gotBody, err := io.ReadAll(r.Body)
			r.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			// Oracle: same request through the standard router.
			var path string
			if i := strings.Index(tc.raw, " "); i >= 0 {
				rest := tc.raw[i+1:]
				path = rest[:strings.Index(rest, " ")]
			}
			method := tc.raw[:strings.Index(tc.raw, " ")]
			var oracleBody io.Reader
			if len(tc.body) > 0 {
				oracleBody = bytes.NewReader(tc.body)
			}
			req := httptest.NewRequest(method, path, oracleBody)
			if strings.Contains(tc.raw, "Authorization:") {
				req.Header.Set("Authorization", "Bearer audit-token")
			}
			if len(tc.body) > 0 {
				req.Header.Set("Content-Type", "application/json")
			}
			rec := httptest.NewRecorder()
			NewRouter(std).ServeHTTP(rec, req)
			if r.StatusCode != rec.Code {
				t.Fatalf("status fast=%d oracle=%d", r.StatusCode, rec.Code)
			}
			if method == "HEAD" {
				if len(gotBody) != 0 {
					t.Fatalf("HEAD transmitted a body: %q", gotBody)
				}
				if r.ContentLength != int64(len(rec.Body.Bytes())) {
					t.Fatalf("HEAD Content-Length=%d want representation length %d", r.ContentLength, len(rec.Body.Bytes()))
				}
			} else if !bytes.Equal(gotBody, rec.Body.Bytes()) {
				t.Fatalf("body fast=%q oracle=%q", gotBody, rec.Body.Bytes())
			}
			if r.Header.Get("X-Github-Request-Id") == "" {
				t.Fatal("dispatch response missing request id")
			}
			if r.StatusCode == http.StatusNoContent && r.Header.Get("Content-Length") != "" {
				t.Fatal("204 carried Content-Length")
			}
		})
	}
}

// TestFrameMemResponseRules pins the body/Content-Length semantics for every
// status class, including 304 and 1xx which no live route produces.
func TestFrameMemResponseRules(t *testing.T) {
	for _, tc := range []struct {
		status     int
		method     string
		body       string
		wantCL     string // "" means the header must be absent
		wantBody   bool
		wantCTGone bool
	}{
		{200, "GET", "hello", "5", true, false},
		{201, "POST", "{}", "2", true, false},
		{200, "HEAD", "hello", "5", false, false},
		{204, "DELETE", "", "", false, false},
		{304, "GET", "stale", "", false, true},
		{401, "GET", "no", "2", true, false},
		{404, "GET", "", "0", true, false},
		{500, "GET", "boom", "4", true, false},
		{100, "GET", "", "", false, false},
	} {
		m := &memResponseWriter{statusCode: tc.status, body: []byte(tc.body), wroteHdr: true}
		m.Header().Set("Content-Type", "application/json")
		m.Header().Set("Content-Length", "999") // wrong on purpose: must be recomputed or dropped
		w, sendBody := frameMemResponse(nil, tc.method, m, false)
		if sendBody != tc.wantBody {
			t.Fatalf("status %d %s: sendBody=%v want %v", tc.status, tc.method, sendBody, tc.wantBody)
		}
		resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(w)), nil)
		if err != nil {
			t.Fatalf("status %d: malformed frame: %v", tc.status, err)
		}
		resp.Body.Close()
		gotCL := resp.Header.Get("Content-Length")
		if gotCL != tc.wantCL {
			t.Fatalf("status %d %s: Content-Length=%q want %q", tc.status, tc.method, gotCL, tc.wantCL)
		}
		if tc.wantCTGone && resp.Header.Get("Content-Type") != "" {
			t.Fatalf("status %d: Content-Type not suppressed", tc.status)
		}
		if resp.Header.Get("Transfer-Encoding") != "" {
			t.Fatalf("status %d: Transfer-Encoding emitted", tc.status)
		}
	}
}

// TestDispatchGraphQLThenFastRead checks a GraphQL mutation dispatched in
// memory leaves the connection synchronized for a following fast-path read.
func TestDispatchGraphQLThenFastRead(t *testing.T) {
	f := store.NewForge("audit-token")
	if _, err := f.CreateRepo("gql", "", true, "main"); err != nil {
		t.Fatal(err)
	}
	conn, fs := startDispatchWire(t, f)
	br := bufio.NewReader(conn)
	const auth = "Authorization: Bearer audit-token\r\n"
	gql := `{"query":"{ viewer { login } }"}`
	r := wireRequest(t, conn, br, "POST /api/graphql HTTP/1.1\r\nHost: example\r\n"+auth+"Content-Type: application/json\r\n\r\n", []byte(gql))
	b, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != http.StatusOK || !bytes.Contains(b, []byte(f.Viewer.Login)) {
		t.Fatalf("graphql status %d body %q", r.StatusCode, b)
	}
	r = wireRequest(t, conn, br, "GET /healthz HTTP/1.1\r\nHost: example\r\n\r\n", nil)
	b, _ = io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != http.StatusOK || !bytes.Contains(b, []byte("ok")) {
		t.Fatalf("healthz after graphql status %d body %q", r.StatusCode, b)
	}
	if fs.FastReadHits.Load() != 0 || fs.FallbackHits.Load() == 0 {
		t.Fatalf("unexpected counters: read=%d fallback=%d", fs.FastReadHits.Load(), fs.FallbackHits.Load())
	}
}