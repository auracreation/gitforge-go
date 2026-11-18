package server_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"gitforge/internal/gitobj"
	"gitforge/internal/server"
	"gitforge/internal/store"
)

func BenchmarkHandlerPushEndToEnd(b *testing.B) {
	f := store.NewForge(testToken)
	_, _ = f.CreateRepo("bench-push", "", true, "main")
	rt := server.NewRouter(f)
	owner := f.Viewer.Login

	// Pre-build b.N sequential push requests so benchmark measures server push handling only
	reqs := make([][]byte, b.N)
	prev := gitobj.ZeroOID
	for i := 0; i < b.N; i++ {
		nextOID, pk := buildSingleCommitPush(prev, "bench-data.txt", bytes.Repeat([]byte("a"), 1024), i+1)
		var buf []byte
		line := fmt.Sprintf("%s %s refs/heads/main\x00report-status-v2 side-band-64k ofs-delta\n", prev.Hex(), nextOID.Hex())
		buf = gitobj.AppendPktString(buf, line)
		buf = append(buf, "0000"...)
		buf = append(buf, pk...)
		reqs[i] = buf
		prev = nextOID
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("POST", "/"+owner+"/bench-push.git/git-receive-pack", bytes.NewReader(reqs[i]))
		req.Header.Set("Authorization", "Bearer "+testToken)
		rw := dummyResponseWriter{hdr: make(http.Header, 4)}
		rt.ServeHTTP(&rw, req)
		if rw.status != 200 {
			b.Fatalf("push %d status=%d", i, rw.status)
		}
	}
}

func BenchmarkHandlerIssueWriteEndToEnd(b *testing.B) {
	f := store.NewForge(testToken)
	_, _ = f.CreateRepo("bench-write", "", true, "main")
	rt := server.NewRouter(f)
	owner := f.Viewer.Login
	bodyBytes := []byte(`{"title":"Slow issue pagination [bench 1]","body":"Steps to reproduce are below and the server answers with a JSON error."}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("POST", "/api/v3/repos/"+owner+"/bench-write/issues", bytes.NewReader(bodyBytes))
		req.Header.Set("Authorization", "Bearer "+testToken)
		rw := dummyResponseWriter{hdr: make(http.Header, 4)}
		rt.ServeHTTP(&rw, req)
		if rw.status != 201 {
			b.Fatalf("write %d status=%d", i, rw.status)
		}
	}
}

func BenchmarkHandlerLsRefsV2EndToEnd(b *testing.B) {
	f := store.NewForge(testToken)
	repo, _ := f.CreateRepo("bench-lsrefs", "", true, "main")
	c0, pk := buildSingleCommitPush(gitobj.ZeroOID, "a.txt", []byte("hi"), 1)
	repo.ReceivePack([]store.PushCommand{{OldOID: gitobj.ZeroOID, NewOID: c0, Ref: "refs/heads/main"}}, pk, false)
	rt := server.NewRouter(f)
	owner := f.Viewer.Login

	var lsBody []byte
	lsBody = gitobj.AppendPktString(lsBody, "command=ls-refs\n")
	lsBody = append(lsBody, "0001"...)
	lsBody = gitobj.AppendPktString(lsBody, "peel\n")
	lsBody = gitobj.AppendPktString(lsBody, "symrefs\n")
	lsBody = gitobj.AppendPktString(lsBody, "ref-prefix HEAD\n")
	lsBody = gitobj.AppendPktString(lsBody, "ref-prefix refs/heads/main\n")
	lsBody = append(lsBody, "0000"...)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("POST", "/"+owner+"/bench-lsrefs.git/git-upload-pack", bytes.NewReader(lsBody))
		req.Header.Set("Authorization", "Bearer "+testToken)
		req.Header.Set("Git-Protocol", "version=2")
		rw := dummyResponseWriter{hdr: make(http.Header, 4)}
		rt.ServeHTTP(&rw, req)
		if rw.status != 200 {
			b.Fatalf("ls-refs status=%d", rw.status)
		}
	}
}
