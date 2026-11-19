package server

import (
	"bufio"
	"bytes"
	"gitforge/internal/gitobj"
	"gitforge/internal/store"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"
)

type legacyAuditConn struct{ bytes.Buffer }

func (c *legacyAuditConn) Close() error                     { return nil }
func (c *legacyAuditConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *legacyAuditConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *legacyAuditConn) SetDeadline(time.Time) error      { return nil }
func (c *legacyAuditConn) SetReadDeadline(time.Time) error  { return nil }
func (c *legacyAuditConn) SetWriteDeadline(time.Time) error { return nil }

func TestAuditFastFetchDepthExtended(t *testing.T) {
	var head gitobj.OID
	head[0] = 1
	// Depths: 1, 2, 5, 10, 50, 100, 9999
	for _, depth := range []int{1, 2, 5, 10, 50, 100, 9999} {
		body := gitobj.AppendPktString(nil, "command=fetch\n")
		body = append(body, "0001"...)
		body = gitobj.AppendPktString(body, "want "+head.Hex()+"\n")
		body = gitobj.AppendPktString(body, "deepen "+strconv.Itoa(depth)+"\n")
		body = gitobj.AppendPktString(body, "done\n")
		body = append(body, "0000"...)
		if got := isDepth1HeadFetch(body, &store.GitSnapshot{HeadOID: head, HasHead: true}); got != (depth == 1) {
			t.Errorf("depth=%d classified as depth-one fast path: got=%t expected=%t", depth, got, depth == 1)
		}
	}

	// Repeated requests with different depths
	for i := 0; i < 50; i++ {
		depth := (i % 10) + 1
		body := gitobj.AppendPktString(nil, "command=fetch\n")
		body = append(body, "0001"...)
		body = gitobj.AppendPktString(body, "want "+head.Hex()+"\n")
		body = gitobj.AppendPktString(body, "deepen "+strconv.Itoa(depth)+"\n")
		body = gitobj.AppendPktString(body, "done\n")
		body = append(body, "0000"...)
		if got := isDepth1HeadFetch(body, &store.GitSnapshot{HeadOID: head, HasHead: true}); got != (depth == 1) {
			t.Fatalf("iteration %d: depth=%d classified as depth-one: got=%t", i, depth, got)
		}
	}

	// Fetch with have commits should never use depth-1 fast path even if deepen 1
	bodyWithHave := gitobj.AppendPktString(nil, "command=fetch\n")
	bodyWithHave = append(bodyWithHave, "0001"...)
	bodyWithHave = gitobj.AppendPktString(bodyWithHave, "want "+head.Hex()+"\n")
	bodyWithHave = gitobj.AppendPktString(bodyWithHave, "have "+head.Hex()+"\n")
	bodyWithHave = gitobj.AppendPktString(bodyWithHave, "deepen 1\n")
	bodyWithHave = gitobj.AppendPktString(bodyWithHave, "done\n")
	bodyWithHave = append(bodyWithHave, "0000"...)
	if isDepth1HeadFetch(bodyWithHave, &store.GitSnapshot{HeadOID: head, HasHead: true}) {
		t.Errorf("fetch with 'have' incorrectly classified as depth-1 fast path")
	}

	// Fetch with different want OID
	var otherHead gitobj.OID
	otherHead[0] = 2
	bodyOtherWant := gitobj.AppendPktString(nil, "command=fetch\n")
	bodyOtherWant = append(bodyOtherWant, "0001"...)
	bodyOtherWant = gitobj.AppendPktString(bodyOtherWant, "want "+otherHead.Hex()+"\n")
	bodyOtherWant = gitobj.AppendPktString(bodyOtherWant, "deepen 1\n")
	bodyOtherWant = gitobj.AppendPktString(bodyOtherWant, "done\n")
	bodyOtherWant = append(bodyOtherWant, "0000"...)
	if isDepth1HeadFetch(bodyOtherWant, &store.GitSnapshot{HeadOID: head, HasHead: true}) {
		t.Errorf("fetch with mismatched want OID incorrectly classified as head depth-1 fast path")
	}
}

func TestAuditFastAuthResponseFramingExtended(t *testing.T) {
	f := store.NewForge("audit-token")
	fs := NewFastServer(NewRouter(f))

	// Test GET issue without auth
	c1 := &legacyAuditConn{}
	handled, _ := fs.tryFastPath(c1, &connBuf{}, []byte("GET"), []byte("/api/v3/repos/a/b/issues/1"), nil, nil, nil)
	if !handled {
		t.Fatal("request did not use fast path")
	}
	resp1, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(c1.Bytes())), nil)
	if err != nil {
		t.Fatal(err)
	}
	body1, err := io.ReadAll(resp1.Body)
	if err != nil {
		t.Fatalf("invalid HTTP framing: advertised=%d actual=%d error=%v", resp1.ContentLength, len(body1), err)
	}
	if int64(len(body1)) != resp1.ContentLength {
		t.Fatalf("mismatch content length: advertised=%d actual=%d", resp1.ContentLength, len(body1))
	}
	if resp1.StatusCode != 401 {
		t.Fatalf("expected 401, got %d", resp1.StatusCode)
	}

	// Test POST issue without auth
	c2 := &legacyAuditConn{}
	handled, _ = fs.tryFastPath(c2, &connBuf{}, []byte("POST"), []byte("/api/v3/repos/a/b/issues"), nil, nil, nil)
	if !handled {
		t.Fatal("request did not use fast path")
	}
	resp2, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(c2.Bytes())), nil)
	if err != nil {
		t.Fatal(err)
	}
	body2, err := io.ReadAll(resp2.Body)
	if err != nil {
		t.Fatalf("invalid HTTP framing: advertised=%d actual=%d error=%v", resp2.ContentLength, len(body2), err)
	}
	if int64(len(body2)) != resp2.ContentLength {
		t.Fatalf("mismatch content length: advertised=%d actual=%d", resp2.ContentLength, len(body2))
	}

	// Git auth is delegated so standard Bearer and Basic semantics stay aligned.
	c3 := startFastWire(t)
	if _, err := io.WriteString(c3, "GET /a/b.git/info/refs?service=git-upload-pack HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp3, err := http.ReadResponse(bufio.NewReader(c3), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp3.Body.Close()
	body3, err := io.ReadAll(resp3.Body)
	if err != nil || int64(len(body3)) != resp3.ContentLength || resp3.StatusCode != 401 {
		t.Fatalf("unexpected Git auth response: status=%d length=%d body=%q err=%v", resp3.StatusCode, resp3.ContentLength, body3, err)
	}

	// Test 404 response framing
	c4 := &legacyAuditConn{}
	handled, _ = fs.tryFastPath(c4, &connBuf{}, []byte("GET"), []byte("/api/v3/repos/adaiversen/nonexistent/issues/1"), []byte("Bearer audit-token"), nil, nil)
	if !handled {
		t.Fatal("request did not use fast path")
	}
	resp4, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(c4.Bytes())), nil)
	if err != nil {
		t.Fatal(err)
	}
	body4, err := io.ReadAll(resp4.Body)
	if err != nil {
		t.Fatalf("invalid 404 framing: advertised=%d actual=%d error=%v", resp4.ContentLength, len(body4), err)
	}
	if int64(len(body4)) != resp4.ContentLength {
		t.Fatalf("mismatch 404 content length: advertised=%d actual=%d", resp4.ContentLength, len(body4))
	}
	if resp4.StatusCode != 404 {
		t.Fatalf("expected 404, got %d", resp4.StatusCode)
	}
}

func TestAuditFastParserMandatory(t *testing.T) {
	// 1. Multiple requests on one keep-alive connection
	t.Run("MultipleRequestsKeepAlive", func(t *testing.T) {
		clientConn := startFastWire(t)

		reader := bufio.NewReader(clientConn)

		// Request 1: GET /healthz
		_, err := clientConn.Write([]byte("GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		if err != nil {
			t.Fatal(err)
		}
		resp1, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		b1, _ := io.ReadAll(resp1.Body)
		if resp1.StatusCode != 200 || !bytes.Contains(b1, []byte("ok")) {
			t.Fatalf("unexpected resp1: status=%d body=%s", resp1.StatusCode, string(b1))
		}

		// Request 2: GET /healthz (again on same connection)
		_, err = clientConn.Write([]byte("GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		if err != nil {
			t.Fatal(err)
		}
		resp2, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		b2, _ := io.ReadAll(resp2.Body)
		if resp2.StatusCode != 200 || !bytes.Contains(b2, []byte("ok")) {
			t.Fatalf("unexpected resp2: status=%d body=%s", resp2.StatusCode, string(b2))
		}

		// Request 3: GET with Connection: close
		_, err = clientConn.Write([]byte("GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
		if err != nil {
			t.Fatal(err)
		}
		resp3, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.ReadAll(resp3.Body)

		// Connection should now be closed by server
		oneByte := make([]byte, 1)
		_, readErr := clientConn.Read(oneByte)
		if readErr != io.EOF {
			t.Logf("expected EOF after Connection: close, got %v", readErr)
		}
	})

	// The live TCP premature EOF case is covered by TestFastWireMatchesStandardPrematureEOF.

	// 3. Fallback routing on non-fastpath method (DELETE /api/v3/repos/a/b)
	t.Run("FallbackNonFastPath", func(t *testing.T) {
		clientConn := startFastWire(t)

		reader := bufio.NewReader(clientConn)
		_, err := clientConn.Write([]byte("DELETE /api/v3/repos/a/b HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		// Should return 401 or 404 cleanly via fallback router
		if resp.StatusCode != 401 && resp.StatusCode != 404 {
			t.Fatalf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
		}
		if int64(len(body)) != resp.ContentLength {
			t.Fatalf("content-length mismatch: %d vs %d", resp.ContentLength, len(body))
		}
	})

	// 4. Non-ASCII payload (UTF-8 issue creation via fallback/fastpath)
	t.Run("NonASCIIUtf8Payload", func(t *testing.T) {
		clientConn := startFastWire(t)

		reader := bufio.NewReader(clientConn)
		utf8JSON := []byte(`{"title":"こんにちは世界 🚀","body":"Тест UTF-8"}`)
		reqStr := "POST /api/v3/repos/adaiversen/test-utf8/issues HTTP/1.1\r\n" +
			"Host: localhost\r\n" +
			"Authorization: Bearer audit-token\r\n" +
			"Content-Length: " + strconv.Itoa(len(utf8JSON)) + "\r\n" +
			"Content-Type: application/json\r\n\r\n"
		_, err := clientConn.Write(append([]byte(reqStr), utf8JSON...))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		// Expect 404 because test-utf8 repo doesn't exist, but framing must be valid
		if resp.StatusCode != 404 {
			t.Fatalf("expected 404, got %d", resp.StatusCode)
		}
		if int64(len(body)) != resp.ContentLength {
			t.Fatalf("framing mismatch on UTF-8 request: %d vs %d", resp.ContentLength, len(body))
		}
	})
}
