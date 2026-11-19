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
	"testing"
	"time"

	"gitforge/internal/gitobj"
	"gitforge/internal/store"
)

// buildPushBody frames a single-command receive-pack request with the given
// capability string and pack bytes.
func buildPushBody(oldOID, newOID gitobj.OID, ref, caps string, pack []byte) []byte {
	line := oldOID.Hex() + " " + newOID.Hex() + " " + ref
	if caps != "" {
		line += "\x00" + caps
	}
	body := gitobj.AppendPktString(nil, line+"\n")
	body = append(body, "0000"...)
	body = append(body, pack...)
	return body
}

func pushFixtureObjects() (*gitobj.Object, *gitobj.Object) {
	tree := gitobj.NewObject(gitobj.ObjTree, nil)
	commit := gitobj.NewObject(gitobj.ObjCommit, gitobj.BuildCommit(tree.OID, nil, "A", "a@example.invalid", 1, "first"))
	return tree, commit
}

// standardPushReport runs the same push through the net/http router (the
// behavioral oracle) and returns its raw response body.
func standardPushReport(t *testing.T, f *store.Forge, repoName string, body []byte) []byte {
	t.Helper()
	req := httptest.NewRequest("POST", "/"+f.Viewer.Login+"/"+repoName+".git/git-receive-pack", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer audit-token")
	req.Header.Set("Content-Type", "application/x-git-receive-pack-request")
	rec := httptest.NewRecorder()
	NewRouter(f).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("oracle status %d body %q", rec.Code, rec.Body.Bytes())
	}
	return rec.Body.Bytes()
}

// TestFastPushV2ReportMatchesOracle verifies the report-status-v2 fast path
// emits byte-identical reports to the standard handler for success, sideband,
// rejected updates and unpacker errors. The kit's push workload negotiates
// report-status-v2, so this path must stay on the fast path AND stay correct.
func TestFastPushV2ReportMatchesOracle(t *testing.T) {
	const kitCaps = "report-status-v2 side-band-64k ofs-delta agent=git/2.39.5"
	for _, tc := range []struct {
		name     string
		caps     string
		rejected bool
		badPack  bool
	}{
		{"v2-sideband-kit-caps", kitCaps, false, false},
		{"v2-no-sideband", "report-status-v2", false, false},
		{"v1-sideband", "report-status side-band-64k", false, false},
		{"v2-rejected", kitCaps, true, false},
		{"v2-unpacker-error", kitCaps, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tree, commit := pushFixtureObjects()
			newOID := commit.OID
			oldOID := gitobj.ZeroOID
			pack := gitobj.BuildPack([]*gitobj.Object{tree, commit})
			if tc.rejected {
				oldOID[0] = 1 // wrong old value: ref update must be refused
			}
			if tc.badPack {
				newOID[0] = 1
				pack = []byte("broken pack")
			}
			body := buildPushBody(oldOID, newOID, "refs/heads/main", tc.caps, pack)

			fFast := store.NewForge("audit-token")
			repoFast, err := fFast.CreateRepo("push-v2", "", true, "main")
			if err != nil {
				t.Fatal(err)
			}
			fStd := store.NewForge("audit-token")
			if _, err := fStd.CreateRepo("push-v2", "", true, "main"); err != nil {
				t.Fatal(err)
			}
			if tc.rejected {
				// Seed both repos with the same tip so the update is a
				// non-fast-forward against an existing ref.
				seed := buildPushBody(gitobj.ZeroOID, commit.OID, "refs/heads/main", "report-status", gitobj.BuildPack([]*gitobj.Object{tree, commit}))
				if _, ok := tryFastReceivePack(repoFast, seed, &connBuf{}); !ok {
					t.Fatal("fast seed push not handled")
				}
				standardPushReport(t, fStd, "push-v2", seed)
			}

			frame, handled := tryFastReceivePack(repoFast, body, &connBuf{})
			if !handled {
				t.Fatal("report-status-v2 push fell back to the standard handler")
			}
			sep := bytes.Index(frame, []byte("\r\n\r\n"))
			if sep < 0 {
				t.Fatalf("malformed fast frame %q", frame)
			}
			fastBody := frame[sep+4:]
			want := standardPushReport(t, fStd, "push-v2", body)
			if !bytes.Equal(fastBody, want) {
				t.Fatalf("v2 report differs from oracle:\nfast %q\nwant %q", fastBody, want)
			}
			// Resulting application state must match too.
			snapFast, snapStd := repoFast.Git(), fStd.GetRepoBytes([]byte(fStd.Viewer.Login), []byte("push-v2")).Git()
			if snapFast.HasHead != snapStd.HasHead || snapFast.HeadOID != snapStd.HeadOID {
				t.Fatalf("state diverged: fast %+v std %+v", snapFast, snapStd)
			}
		})
	}
}

// TestFastPushV2KeepAliveFraming drives a raw connection: a report-status-v2
// push followed by more requests on the same persistent connection. The byte
// stream must stay synchronized (no leftover report bytes, correct lengths).
func TestFastPushV2KeepAliveFraming(t *testing.T) {
	f := store.NewForge("audit-token")
	if _, err := f.CreateRepo("push-sync", "", true, "main"); err != nil {
		t.Fatal(err)
	}
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
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	tree, commit := pushFixtureObjects()
	body := buildPushBody(gitobj.ZeroOID, commit.OID, "refs/heads/main",
		"report-status-v2 side-band-64k ofs-delta agent=git/2.39.5",
		gitobj.BuildPack([]*gitobj.Object{tree, commit}))
	req := "POST /" + f.Viewer.Login + "/push-sync.git/git-receive-pack HTTP/1.1\r\n" +
		"Host: example\r\nAuthorization: Bearer audit-token\r\n" +
		"Content-Type: application/x-git-receive-pack-request\r\n" +
		"Content-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n"
	if _, err := conn.Write(append([]byte(req), body...)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	pushBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || !bytes.Contains(pushBody, []byte("option refname refs/heads/main")) {
		t.Fatalf("push response %d %q", resp.StatusCode, pushBody)
	}
	if fs.FastPushHits.Load() != 1 {
		t.Fatalf("push did not use the fast path (hits=%d fallback=%d)", fs.FastPushHits.Load(), fs.FallbackHits.Load())
	}

	// Two more requests on the same connection must frame cleanly.
	for i := 0; i < 2; i++ {
		if _, err := io.WriteString(conn, "GET /healthz HTTP/1.1\r\nHost: example\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		r, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("request %d after push desynchronized: %v", i, err)
		}
		b, err := io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil || r.StatusCode != http.StatusOK || !bytes.Contains(b, []byte("ok")) {
			t.Fatalf("request %d: status %d body %q err %v", i, r.StatusCode, b, err)
		}
	}
}

// TestWriteFrameVectoredLargeAndFreshID checks the zero-copy write path on a
// real TCP connection: a frame larger than the old 32 KiB staging buffer must
// arrive complete, and consecutive frames must carry fresh request IDs.
func TestWriteFrameVectoredLargeAndFreshID(t *testing.T) {
	f := store.NewForge("audit-token")
	fs := NewFastServer(NewRouter(f))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serverDone := make(chan struct{})
	var serverConn net.Conn
	go func() {
		defer close(serverDone)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		serverConn = c
	}()
	client, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	<-serverDone
	defer serverConn.Close()

	bigBody := bytes.Repeat([]byte("x"), 40*1024)
	frame := store.BuildHTTPResponse("HTTP/1.1 200 OK", "application/octet-stream", "", bigBody)
	// Wrap like the production path does to exercise wrapper unwrapping.
	wrapped := &readPendingConn{Conn: &trackedFastConn{Conn: serverConn}}
	if _, err := fs.writeFrame(wrapped, nil, frame); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	if _, err := fs.writeFrame(wrapped, nil, frame); err != nil {
		t.Fatal(err)
	}
	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(client)
	var ids []string
	for i := 0; i < 2; i++ {
		r, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("response %d: %v", i, err)
		}
		b, err := io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b, bigBody) {
			t.Fatalf("response %d body corrupted: %d bytes", i, len(b))
		}
		id := r.Header.Get("X-GitHub-Request-Id")
		if id == "" || id == "1" {
			t.Fatalf("response %d missing fresh request id: %q", i, id)
		}
		ids = append(ids, id)
	}
	if ids[0] == ids[1] {
		t.Fatalf("request IDs not fresh: %q", ids)
	}
}