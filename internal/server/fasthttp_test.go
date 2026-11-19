package server_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"gitforge/internal/gitobj"
	"gitforge/internal/server"
	"gitforge/internal/store"
)

func startFastTestServer(t *testing.T) (baseURL string, forge *store.Forge, stop func()) {
	t.Helper()
	f := store.NewForge(testToken)
	rt := server.NewRouter(f)
	fs := server.NewFastServer(rt)
	port, err := fs.BindPort(0)
	if err != nil {
		t.Fatalf("BindPort: %v", err)
	}
	go func() {
		_ = fs.ServeBound()
	}()
	baseURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	stop = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = fs.Shutdown(ctx)
	}
	return baseURL, f, stop
}

func TestFastServerHTTP11EdgeCasesAndKeepAlive(t *testing.T) {
	baseURL, f, stop := startFastTestServer(t)
	defer stop()

	owner := f.Viewer.Login
	repo, err := f.CreateRepo("edge-repo", "desc", false, "main")
	if err != nil {
		t.Fatalf("CreateRepo: %v", err)
	}
	_, err = repo.CreateIssue("Initial issue", "body text", nil)
	if err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	addr := strings.TrimPrefix(baseURL, "http://")

	// 1. Pipelined keep-alive GETs + Connection: close on final request
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	pipelined := fmt.Sprintf(
		"GET /healthz HTTP/1.1\r\nHost: %s\r\n\r\n"+
			"GET /api/v3/repos/%s/edge-repo/issues/1 HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\n\r\n"+
			"GET /api/v3/repos/%s/edge-repo/issues/1 HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nConnection: close\r\n\r\n",
		addr, owner, addr, testToken, owner, addr, testToken,
	)
	if _, err := conn.Write([]byte(pipelined)); err != nil {
		t.Fatalf("write pipelined: %v", err)
	}
	br := bufio.NewReader(conn)
	for i := 0; i < 3; i++ {
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("pipelined resp %d: %v", i, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("pipelined resp %d status=%d body=%s", i, resp.StatusCode, string(body))
		}
		if i > 0 && !strings.Contains(string(body), "Initial issue") {
			t.Fatalf("pipelined resp %d missing title: %s", i, string(body))
		}
	}

	// 2. Chunked Transfer-Encoding POST /issues + Expect: 100-continue
	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial2: %v", err)
	}
	defer conn2.Close()

	chunkPayload := `{"title":"Chunked Issue","body":"via chunked"}`
	reqHdr := fmt.Sprintf(
		"POST /api/v3/repos/%s/edge-repo/issues HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n",
		owner, addr, testToken,
	)
	if _, err := conn2.Write([]byte(reqHdr)); err != nil {
		t.Fatalf("write chunked hdr: %v", err)
	}
	br2 := bufio.NewReader(conn2)
	line100, err := br2.ReadString('\n')
	if err != nil || !strings.Contains(line100, "100 Continue") {
		t.Fatalf("expected 100 Continue, got %q err=%v", line100, err)
	}
	_, _ = br2.ReadString('\n') // blank line after 100 Continue

	chunkBody := fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(chunkPayload), chunkPayload)
	if _, err := conn2.Write([]byte(chunkBody)); err != nil {
		t.Fatalf("write chunked body: %v", err)
	}
	resp2, err := http.ReadResponse(br2, nil)
	if err != nil {
		t.Fatalf("read chunked resp: %v", err)
	}
	b2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != 201 || !strings.Contains(string(b2), "Chunked Issue") {
		t.Fatalf("chunked create failed: status=%d body=%s", resp2.StatusCode, string(b2))
	}
}

func TestFastServerConcurrentMixedClonePushIssueRW(t *testing.T) {
	baseURL, f, stop := startFastTestServer(t)
	defer stop()

	owner := f.Viewer.Login
	repo, err := f.CreateRepo("mixed-repo", "", false, "main")
	if err != nil {
		t.Fatalf("CreateRepo: %v", err)
	}
	c0, pk0 := buildSingleCommitPush(gitobj.ZeroOID, "f.txt", []byte("initial"), 1)
	uErr, sts := repo.ReceivePack([]store.PushCommand{{OldOID: gitobj.ZeroOID, NewOID: c0, Ref: "refs/heads/main"}}, pk0, false)
	if uErr != "" || len(sts) != 1 || !sts[0].OK {
		t.Fatalf("seed push failed: %q %+v", uErr, sts)
	}
	if _, err := repo.CreateIssue("Seed Issue", "v0", nil); err != nil {
		t.Fatalf("CreateIssue: %v", err)
	}

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
		},
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 128)

	// 25 concurrent shallow clones while pushes & issue writes/updates happen concurrently
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iter := 0; iter < 4; iter++ {
				req, _ := http.NewRequest("GET", fmt.Sprintf("%s/%s/mixed-repo.git/info/refs?service=git-upload-pack", baseURL, owner), nil)
				req.Header.Set("Authorization", "Bearer "+testToken)
				req.Header.Set("Git-Protocol", "version=2")
				resp, err := client.Do(req)
				if err != nil {
					errCh <- err
					return
				}
				_, _ = io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 {
					errCh <- fmt.Errorf("info/refs status=%d", resp.StatusCode)
					return
				}

				lsBody := append(append(gitobj.AppendPktString(nil, "command=ls-refs\n"), "0001"...),
					append(gitobj.AppendPktString(gitobj.AppendPktString(nil, "peel\n"), "symrefs\n"), "0000"...)...)
				req2, _ := http.NewRequest("POST", fmt.Sprintf("%s/%s/mixed-repo.git/git-upload-pack", baseURL, owner), bytes.NewReader(lsBody))
				req2.Header.Set("Authorization", "Bearer "+testToken)
				req2.Header.Set("Git-Protocol", "version=2")
				resp2, err := client.Do(req2)
				if err != nil {
					errCh <- err
					return
				}
				_, _ = io.ReadAll(resp2.Body)
				resp2.Body.Close()
				if resp2.StatusCode != 200 {
					errCh <- fmt.Errorf("ls-refs status=%d", resp2.StatusCode)
					return
				}
			}
		}()
	}

	// Concurrent issue writers + readers verifying fresh visibility
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				payload := fmt.Sprintf(`{"title":"Worker %d Issue %d","body":"hello"}`, id, j)
				req, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/v3/repos/%s/mixed-repo/issues", baseURL, owner), strings.NewReader(payload))
				req.Header.Set("Authorization", "Bearer "+testToken)
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				if err != nil {
					errCh <- err
					return
				}
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 201 {
					errCh <- fmt.Errorf("create issue status=%d body=%s", resp.StatusCode, string(b))
					return
				}
				var created struct {
					Number int    `json:"number"`
					Title  string `json:"title"`
				}
				if err := json.Unmarshal(b, &created); err != nil || created.Number <= 0 {
					errCh <- fmt.Errorf("bad created json: %s", string(b))
					return
				}
				// Immediate read-after-write check
				reqG, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v3/repos/%s/mixed-repo/issues/%d", baseURL, owner, created.Number), nil)
				reqG.Header.Set("Authorization", "Bearer "+testToken)
				respG, err := client.Do(reqG)
				if err != nil {
					errCh <- err
					return
				}
				bg, _ := io.ReadAll(respG.Body)
				respG.Body.Close()
				if respG.StatusCode != 200 || !strings.Contains(string(bg), created.Title) {
					errCh <- fmt.Errorf("read-after-write mismatch: status=%d body=%s", respG.StatusCode, string(bg))
					return
				}
			}
		}(w)
	}

	// Sequential fast-forward pushes running concurrently with clones
	wg.Add(1)
	go func() {
		defer wg.Done()
		prev := c0
		for p := 0; p < 20; p++ {
			nextOID, pk := buildSingleCommitPush(prev, "f.txt", []byte(fmt.Sprintf("rev-%d", p)), p+2)
			var buf []byte
			buf = gitobj.AppendPktString(buf, fmt.Sprintf("%s %s refs/heads/main\x00report-status side-band-64k\n", prev.Hex(), nextOID.Hex()))
			buf = append(buf, "0000"...)
			buf = append(buf, pk...)
			req, _ := http.NewRequest("POST", fmt.Sprintf("%s/%s/mixed-repo.git/git-receive-pack", baseURL, owner), bytes.NewReader(buf))
			req.Header.Set("Authorization", "Bearer "+testToken)
			resp, err := client.Do(req)
			if err != nil {
				errCh <- err
				return
			}
			rb, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 || !bytes.Contains(rb, []byte("ok refs/heads/main")) {
				errCh <- fmt.Errorf("push %d failed: status=%d body=%q", p, resp.StatusCode, string(rb))
				return
			}
			prev = nextOID
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	// Verify no lost issue updates (1 seed + 5*20 = 101 issues)
	issues, _ := repo.ListIssues("all", "asc", 1, 100)
	issues2, _ := repo.ListIssues("all", "asc", 2, 100)
	if len(issues)+len(issues2) != 101 {
		t.Fatalf("expected 101 issues (no lost updates), got %d", len(issues)+len(issues2))
	}
}
