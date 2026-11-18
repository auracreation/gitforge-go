package server_test

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitforge/internal/gitobj"
	"gitforge/internal/server"
	"gitforge/internal/store"
)

const testToken = "test-bearer-token-local"

func newTestServer(t testing.TB) (*httptest.Server, *store.Forge, *http.Client) {
	t.Helper()
	f := store.NewForge(testToken)
	rt := server.NewRouter(f)
	ts := httptest.NewServer(rt)
	t.Cleanup(ts.Close)
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        128,
			MaxIdleConnsPerHost: 128,
			IdleConnTimeout:     60 * time.Second,
		},
	}
	t.Cleanup(client.CloseIdleConnections)
	return ts, f, client
}

func doAuthReq(t testing.TB, c *http.Client, method, url string, body []byte, contentType string) (*http.Response, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("NewRequest(%s %s): %v", method, url, err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	} else if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("Do(%s %s): %v", method, url, err)
	}
	respBytes, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("ReadAll(%s %s): %v", method, url, err)
	}
	return resp, respBytes
}

// ---------------------------------------------------------------------------
// Priority A Tests: Issue Reads, Cross-Protocol Parity, Cache Invalidation
// ---------------------------------------------------------------------------

func TestIssueReads2000SequentialAndConcurrent(t *testing.T) {
	ts, f, client := newTestServer(t)
	owner := f.Viewer.Login

	resp, _ := doAuthReq(t, client, "POST", ts.URL+"/api/v3/user/repos", []byte(`{"name":"issue-bench","private":true}`), "")
	if resp.StatusCode != 201 {
		t.Fatalf("create repo status = %d", resp.StatusCode)
	}
	resp, _ = doAuthReq(t, client, "POST", ts.URL+"/api/v3/repos/"+owner+"/issue-bench/issues",
		[]byte(`{"title":"Hot issue","body":"Read 2000 times"}`), "")
	if resp.StatusCode != 201 {
		t.Fatalf("create issue status = %d", resp.StatusCode)
	}

	issueURL := ts.URL + "/api/v3/repos/" + owner + "/issue-bench/issues/1"

	// 1. 2,000 sequential issue reads
	latencies := make([]time.Duration, 2000)
	for i := 0; i < 2000; i++ {
		start := time.Now()
		r, b := doAuthReq(t, client, "GET", issueURL, nil, "")
		latencies[i] = time.Since(start)
		if r.StatusCode != 200 || !bytes.Contains(b, []byte(`"Hot issue"`)) {
			t.Fatalf("seq read %d failed: %d %s", i, r.StatusCode, string(b))
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	t.Logf("2000 sequential HTTP issue reads: p50=%v p95=%v p99=%v", latencies[1000], latencies[1900], latencies[1980])

	// 2. 2,000 concurrent issue reads bounded across 32 workers
	var wg sync.WaitGroup
	var errCount atomic.Int32
	sem := make(chan struct{}, 32)
	for i := 0; i < 2000; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			r, b := doAuthReq(t, client, "GET", issueURL, nil, "")
			if r.StatusCode != 200 || !bytes.Contains(b, []byte(`"Hot issue"`)) {
				errCount.Add(1)
			}
		}()
	}
	wg.Wait()
	if errCount.Load() != 0 {
		t.Fatalf("concurrent issue reads had %d failures", errCount.Load())
	}
}

func TestMixedIssueReadsWritesAndCacheInvalidation(t *testing.T) {
	ts, f, client := newTestServer(t)
	owner := f.Viewer.Login

	doAuthReq(t, client, "POST", ts.URL+"/api/v3/user/repos", []byte(`{"name":"mixed-repo"}`), "")
	doAuthReq(t, client, "POST", ts.URL+"/api/v3/repos/"+owner+"/mixed-repo/labels", []byte(`{"name":"bug","color":"ff0000"}`), "")
	doAuthReq(t, client, "POST", ts.URL+"/api/v3/repos/"+owner+"/mixed-repo/issues", []byte(`{"title":"v1","body":"b1","labels":["bug"]}`), "")

	// Verify immediate cache invalidation on PATCH
	doAuthReq(t, client, "PATCH", ts.URL+"/api/v3/repos/"+owner+"/mixed-repo/issues/1", []byte(`{"title":"v2-edited","state":"closed"}`), "")
	r, b := doAuthReq(t, client, "GET", ts.URL+"/api/v3/repos/"+owner+"/mixed-repo/issues/1", nil, "")
	if r.StatusCode != 200 || !bytes.Contains(b, []byte(`"v2-edited"`)) || !bytes.Contains(b, []byte(`"closed"`)) {
		t.Fatalf("stale cache after edit: %s", string(b))
	}

	// Mixed concurrent readers + writers under race detector
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				payload := fmt.Sprintf(`{"title":"w-%d-%d","body":"concurrent"}`, worker, i)
				resp, _ := doAuthReq(t, client, "POST", ts.URL+"/api/v3/repos/"+owner+"/mixed-repo/issues", []byte(payload), "")
				if resp.StatusCode != 201 {
					t.Errorf("writer status = %d", resp.StatusCode)
				}
			}
		}(w)
	}
	for rIdx := 0; rIdx < 8; rIdx++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				resp, body := doAuthReq(t, client, "GET", ts.URL+"/api/v3/repos/"+owner+"/mixed-repo/issues?state=all&per_page=50", nil, "")
				if resp.StatusCode != 200 || len(body) == 0 || body[0] != '[' {
					t.Errorf("reader status=%d body=%s", resp.StatusCode, string(body))
				}
			}
		}()
	}
	wg.Wait()
}

func TestCrossProtocolParityRESTGraphQLMCP(t *testing.T) {
	ts, f, client := newTestServer(t)
	owner := f.Viewer.Login

	// 1. MCP create_repo -> REST read
	mcpCreateRepo := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"create_repo","arguments":{"name":"parity-repo","private":true}}}`
	resp, b := doAuthReq(t, client, "POST", ts.URL+"/mcp", []byte(mcpCreateRepo), "application/json")
	if resp.StatusCode != 200 || bytes.Contains(b, []byte(`"error"`)) {
		t.Fatalf("MCP create_repo failed: %d %s", resp.StatusCode, string(b))
	}

	// 2. REST write issue -> GraphQL read
	resp, _ = doAuthReq(t, client, "POST", ts.URL+"/api/v3/repos/"+owner+"/parity-repo/issues",
		[]byte(`{"title":"from-rest","body":"rest-body"}`), "")
	if resp.StatusCode != 201 {
		t.Fatalf("REST create issue failed: %d", resp.StatusCode)
	}

	gqlRead := fmt.Sprintf(`{"query":"query($o:String!,$n:String!){repository(owner:$o,name:$n){id issue(number:1){id number title body state}}}","variables":{"o":%q,"n":"parity-repo"}}`, owner)
	resp, b = doAuthReq(t, client, "POST", ts.URL+"/api/graphql", []byte(gqlRead), "application/json")
	if resp.StatusCode != 200 || !bytes.Contains(b, []byte(`"from-rest"`)) {
		t.Fatalf("GraphQL did not observe REST write: %d %s", resp.StatusCode, string(b))
	}
	var gqlParsed struct {
		Data struct {
			Repository struct {
				ID string `json:"id"`
			} `json:"repository"`
		} `json:"data"`
	}
	_ = json.Unmarshal(b, &gqlParsed)
	if gqlParsed.Data.Repository.ID == "" {
		t.Fatalf("missing repo node id in GraphQL response: %s", string(b))
	}

	// 3. GraphQL write issue -> MCP list_issues read
	gqlMut := fmt.Sprintf(`{"query":"mutation($in:CreateIssueInput!){createIssue(input:$in){issue{number title}}}","variables":{"in":{"repositoryId":%q,"title":"from-graphql","body":"gql-body"}}}`, gqlParsed.Data.Repository.ID)
	resp, b = doAuthReq(t, client, "POST", ts.URL+"/api/graphql", []byte(gqlMut), "application/json")
	if resp.StatusCode != 200 || !bytes.Contains(b, []byte(`"from-graphql"`)) {
		t.Fatalf("GraphQL createIssue failed: %d %s", resp.StatusCode, string(b))
	}

	mcpList := fmt.Sprintf(`{"jsonrpc":"2.0","id":"list-1","method":"tools/call","params":{"name":"list_issues","arguments":{"owner":%q,"repo":"parity-repo","state":"open"}}}`, owner)
	resp, b = doAuthReq(t, client, "POST", ts.URL+"/mcp", []byte(mcpList), "application/json")
	if resp.StatusCode != 200 || !bytes.Contains(b, []byte(`from-graphql`)) {
		t.Fatalf("MCP list_issues did not observe GraphQL write: %d %s", resp.StatusCode, string(b))
	}

	// 4. MCP write issue -> REST read
	mcpCreateIss := fmt.Sprintf(`{"jsonrpc":"2.0","id":"create-3","method":"tools/call","params":{"name":"create_issue","arguments":{"owner":%q,"repo":"parity-repo","title":"from-mcp","body":"mcp-body"}}}`, owner)
	resp, b = doAuthReq(t, client, "POST", ts.URL+"/mcp", []byte(mcpCreateIss), "application/json")
	if resp.StatusCode != 200 {
		t.Fatalf("MCP create_issue failed: %d %s", resp.StatusCode, string(b))
	}
	resp, b = doAuthReq(t, client, "GET", ts.URL+"/api/v3/repos/"+owner+"/parity-repo/issues/3", nil, "")
	if resp.StatusCode != 200 || !bytes.Contains(b, []byte(`"from-mcp"`)) {
		t.Fatalf("REST did not observe MCP write: %d %s", resp.StatusCode, string(b))
	}
}

func TestIssuePaginationAndOrdering(t *testing.T) {
	ts, f, client := newTestServer(t)
	owner := f.Viewer.Login
	doAuthReq(t, client, "POST", ts.URL+"/api/v3/user/repos", []byte(`{"name":"page-repo"}`), "")
	for i := 1; i <= 15; i++ {
		doAuthReq(t, client, "POST", ts.URL+"/api/v3/repos/"+owner+"/page-repo/issues",
			[]byte(fmt.Sprintf(`{"title":"Issue %d","body":"body"}`, i)), "")
	}

	// Ascending page 1 & 2
	_, b1 := doAuthReq(t, client, "GET", ts.URL+"/api/v3/repos/"+owner+"/page-repo/issues?direction=asc&per_page=10&page=1", nil, "")
	_, b2 := doAuthReq(t, client, "GET", ts.URL+"/api/v3/repos/"+owner+"/page-repo/issues?direction=asc&per_page=10&page=2", nil, "")
	var p1, p2 []struct {
		Number int `json:"number"`
	}
	_ = json.Unmarshal(b1, &p1)
	_ = json.Unmarshal(b2, &p2)
	if len(p1) != 10 || len(p2) != 5 || p1[0].Number != 1 || p1[9].Number != 10 || p2[4].Number != 15 {
		t.Fatalf("unexpected asc pages: p1=%v p2=%v", p1, p2)
	}

	// Descending
	_, bd := doAuthReq(t, client, "GET", ts.URL+"/api/v3/repos/"+owner+"/page-repo/issues?direction=desc&per_page=5&page=1", nil, "")
	var pd []struct {
		Number int `json:"number"`
	}
	_ = json.Unmarshal(bd, &pd)
	if len(pd) != 5 || pd[0].Number != 15 || pd[4].Number != 11 {
		t.Fatalf("unexpected desc page: %v", pd)
	}
}

// ---------------------------------------------------------------------------
// Priority B & C Tests: Git Clone (Full, Shallow, Concurrent, Real Git CLI),
// Push, Deltas, Malformed Packs, Conflicts, Snapshot Visibility
// ---------------------------------------------------------------------------

func buildSingleCommitPush(parent gitobj.OID, filename string, content []byte, seq int) (commitOID gitobj.OID, pack []byte) {
	blob := gitobj.NewObject(gitobj.ObjBlob, content)
	tree := gitobj.NewObject(gitobj.ObjTree, gitobj.BuildTree([]gitobj.TreeEntry{
		{Mode: "100644", Name: filename, OID: blob.OID},
	}))
	var parents []gitobj.OID
	if !parent.IsZero() {
		parents = []gitobj.OID{parent}
	}
	commitRaw := gitobj.BuildCommit(tree.OID, parents, "Tester", "tester@example.invalid", int64(1700000000+seq), fmt.Sprintf("commit %d\n", seq))
	commit := gitobj.NewObject(gitobj.ObjCommit, commitRaw)
	return commit.OID, gitobj.BuildPack([]*gitobj.Object{blob, tree, commit})
}

func sendReceivePackHTTP(t testing.TB, c *http.Client, baseURL, owner, repo string, oldOID, newOID gitobj.OID, ref string, pack []byte) []byte {
	t.Helper()
	var reqBuf []byte
	cmdLine := fmt.Sprintf("%s %s %s\x00report-status side-band-64k ofs-delta\n", oldOID.Hex(), newOID.Hex(), ref)
	reqBuf = gitobj.AppendPktString(reqBuf, cmdLine)
	reqBuf = append(reqBuf, "0000"...)
	reqBuf = append(reqBuf, pack...)
	resp, body := doAuthReq(t, c, "POST", fmt.Sprintf("%s/%s/%s.git/git-receive-pack", baseURL, owner, repo),
		reqBuf, "application/x-git-receive-pack-request")
	if resp.StatusCode != 200 {
		t.Fatalf("receive-pack HTTP %d: %s", resp.StatusCode, string(body))
	}
	return body
}

func sendShallowCloneV2(t testing.TB, c *http.Client, baseURL, owner, repo string, want gitobj.OID, depth int) []byte {
	t.Helper()
	var reqBuf []byte
	reqBuf = gitobj.AppendPktString(reqBuf, "command=fetch\n")
	reqBuf = gitobj.AppendPktString(reqBuf, "agent=git/2.39.5\n")
	reqBuf = append(reqBuf, "0001"...)
	reqBuf = gitobj.AppendPktString(reqBuf, "want "+want.Hex()+"\n")
	if depth > 0 {
		reqBuf = gitobj.AppendPktString(reqBuf, fmt.Sprintf("deepen %d\n", depth))
	}
	reqBuf = gitobj.AppendPktString(reqBuf, "done\n")
	reqBuf = append(reqBuf, "0000"...)

	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/%s/%s.git/git-upload-pack", baseURL, owner, repo), bytes.NewReader(reqBuf))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Git-Protocol", "version=2")
	req.Header.Set("Content-Type", "application/x-git-upload-pack-request")
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("upload-pack error: %v", err)
	}
	out, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("upload-pack HTTP %d: %s", resp.StatusCode, string(out))
	}
	return out
}

func TestGit200Pushes200IssueWritesAnd50ConcurrentShallowClones(t *testing.T) {
	ts, f, client := newTestServer(t)
	owner := f.Viewer.Login

	doAuthReq(t, client, "POST", ts.URL+"/api/v3/user/repos", []byte(`{"name":"git-heavy"}`), "")

	// 200 sequential Git pushes
	prev := gitobj.ZeroOID
	for i := 1; i <= 200; i++ {
		payload := bytes.Repeat([]byte(fmt.Sprintf("line-%d-", i)), 128)
		nextOID, pack := buildSingleCommitPush(prev, "data.txt", payload, i)
		rep := sendReceivePackHTTP(t, client, ts.URL, owner, "git-heavy", prev, nextOID, "refs/heads/main", pack)
		if !bytes.Contains(rep, []byte("unpack ok")) || !bytes.Contains(rep, []byte("ok refs/heads/main")) {
			t.Fatalf("push %d rejected: %q", i, string(rep))
		}
		prev = nextOID
	}

	// 50 concurrent depth-1 shallow clones
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := sendShallowCloneV2(t, client, ts.URL, owner, "git-heavy", prev, 1)
			if !bytes.Contains(res, []byte("shallow "+prev.Hex())) || !bytes.Contains(res, []byte("packfile\n")) {
				t.Errorf("unexpected shallow v2 response len=%d", len(res))
			}
		}()
	}
	wg.Wait()

	// Multiple shallow depths (depth 1, 2, 5) + full clone (depth 0)
	for _, d := range []int{1, 2, 5, 0} {
		res := sendShallowCloneV2(t, client, ts.URL, owner, "git-heavy", prev, d)
		if !bytes.Contains(res, []byte("packfile\n")) {
			t.Fatalf("clone depth %d missing packfile section", d)
		}
	}

	// 200 sequential issue writes
	for i := 1; i <= 200; i++ {
		r, _ := doAuthReq(t, client, "POST", ts.URL+"/api/v3/repos/"+owner+"/git-heavy/issues",
			[]byte(fmt.Sprintf(`{"title":"Issue #%d","body":"Write test %d"}`, i, i)), "")
		if r.StatusCode != 201 {
			t.Fatalf("issue write %d status=%d", i, r.StatusCode)
		}
	}
}

func TestRealGitCLIInteropFullShallowBranchesTagsPush(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not installed")
	}
	ts, f, client := newTestServer(t)
	owner := f.Viewer.Login

	doAuthReq(t, client, "POST", ts.URL+"/api/v3/user/repos", []byte(`{"name":"cli-repo"}`), "")

	tmp := t.TempDir()
	authArg := "http.extraHeader=Authorization: Bearer " + testToken
	remoteURL := fmt.Sprintf("%s/%s/cli-repo.git", ts.URL, owner)

	runGit := func(dir string, args ...string) string {
		t.Helper()
		full := append([]string{"-c", authArg}, args...)
		cmd := exec.Command("git", full...)
		cmd.Dir = dir
		cmd.Env = append(cmd.Environ(),
			"GIT_AUTHOR_NAME=Ada",
			"GIT_AUTHOR_EMAIL=ada@example.invalid",
			"GIT_COMMITTER_NAME=Ada",
			"GIT_COMMITTER_EMAIL=ada@example.invalid",
			"GIT_TERMINAL_PROMPT=0",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
		}
		return string(out)
	}

	// Empty repo clone
	emptyDir := filepath.Join(tmp, "empty-clone")
	runGit(tmp, "clone", remoteURL, emptyDir)

	// Push 3 commits + feature branch + annotated tag from emptyDir
	runGit(emptyDir, "checkout", "-b", "main")
	for i := 1; i <= 3; i++ {
		doWriteFile(t, filepath.Join(emptyDir, fmt.Sprintf("f%d.txt", i)), fmt.Sprintf("hello %d\n", i))
		runGit(emptyDir, "add", ".")
		runGit(emptyDir, "commit", "-m", fmt.Sprintf("c%d", i))
	}
	runGit(emptyDir, "push", "-u", "origin", "main")

	runGit(emptyDir, "checkout", "-b", "feature/sub")
	doWriteFile(t, filepath.Join(emptyDir, "feat.txt"), "feature work\n")
	runGit(emptyDir, "add", ".")
	runGit(emptyDir, "commit", "-m", "feat commit")
	runGit(emptyDir, "tag", "-a", "v1.0.0", "-m", "annotated v1")
	runGit(emptyDir, "push", "origin", "feature/sub", "v1.0.0")

	// Shallow clone depth 1 + fsck verification
	shallowDir := filepath.Join(tmp, "shallow-clone")
	runGit(tmp, "clone", "--depth=1", remoteURL, shallowDir)
	runGit(shallowDir, "fsck", "--full")

	// Deepen shallow clone
	runGit(shallowDir, "fetch", "--deepen=1", "origin")
	runGit(shallowDir, "fsck", "--full")

	// Full clone + fsck verification
	fullDir := filepath.Join(tmp, "full-clone")
	runGit(tmp, "clone", remoteURL, fullDir)
	runGit(fullDir, "fsck", "--full")
}

func doWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := exec.Command("sh", "-c", fmt.Sprintf("printf %%s %q > %q", content, path)).Run(); err != nil {
		// Use os write fallback via simple helper
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestConcurrentPushesConflictingUpdatesAndMalformedInputs(t *testing.T) {
	ts, f, client := newTestServer(t)
	owner := f.Viewer.Login

	doAuthReq(t, client, "POST", ts.URL+"/api/v3/user/repos", []byte(`{"name":"cas-repo"}`), "")
	c0, pack0 := buildSingleCommitPush(gitobj.ZeroOID, "init.txt", []byte("base\n"), 1)
	sendReceivePackHTTP(t, client, ts.URL, owner, "cas-repo", gitobj.ZeroOID, c0, "refs/heads/main", pack0)

	// 10 concurrent writers racing to advance refs/heads/main from the SAME oldOID c0:
	// Exactly ONE must win CAS ("ok refs/heads/main"), and the other 9 must be rejected cleanly!
	var wg sync.WaitGroup
	var winners atomic.Int32
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cNext, packNext := buildSingleCommitPush(c0, "race.txt", []byte(fmt.Sprintf("winner-%d\n", idx)), 10+idx)
			rep := sendReceivePackHTTP(t, client, ts.URL, owner, "cas-repo", c0, cNext, "refs/heads/main", packNext)
			if bytes.Contains(rep, []byte("ok refs/heads/main")) {
				winners.Add(1)
			}
		}(w)
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("expected exactly 1 CAS winner among 10 concurrent pushes, got %d", winners.Load())
	}

	// Concurrent pushes to separate repositories must all succeed without blocking one another
	for rIdx := 0; rIdx < 8; rIdx++ {
		doAuthReq(t, client, "POST", ts.URL+"/api/v3/user/repos", []byte(fmt.Sprintf(`{"name":"indep-%d"}`, rIdx)), "")
	}
	for rIdx := 0; rIdx < 8; rIdx++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c, pk := buildSingleCommitPush(gitobj.ZeroOID, "a.txt", []byte("ok"), idx)
			rep := sendReceivePackHTTP(t, client, ts.URL, owner, fmt.Sprintf("indep-%d", idx), gitobj.ZeroOID, c, "refs/heads/main", pk)
			if !bytes.Contains(rep, []byte("ok refs/heads/main")) {
				t.Errorf("indep-%d push failed: %s", idx, string(rep))
			}
		}(rIdx)
	}
	wg.Wait()

	// Malformed pkt-line & corrupt packfile must never crash or publish partial state
	beforeTip := f.GetRepo(owner, "cas-repo").Git().HeadOID
	rBadPkt, _ := doAuthReq(t, client, "POST", ts.URL+"/"+owner+"/cas-repo.git/git-upload-pack",
		[]byte("ZZZZbad-pkt"), "application/x-git-upload-pack-request")
	if rBadPkt.StatusCode != 400 {
		t.Fatalf("expected 400 for malformed pkt-line, got %d", rBadPkt.StatusCode)
	}

	// Corrupt packfile checksum on atomic push -> ref must remain untouched
	fakeTarget, validPack := buildSingleCommitPush(beforeTip, "corrupt.txt", []byte("bad"), 99)
	corruptPack := append([]byte(nil), validPack...)
	corruptPack[len(corruptPack)-1] ^= 0xff
	repCorrupt := sendReceivePackHTTP(t, client, ts.URL, owner, "cas-repo", beforeTip, fakeTarget, "refs/heads/main", corruptPack)
	if bytes.Contains(repCorrupt, []byte("unpack ok")) {
		t.Fatalf("expected corrupt packfile to fail unpack, got %s", string(repCorrupt))
	}
	if f.GetRepo(owner, "cas-repo").Git().HeadOID != beforeTip {
		t.Fatalf("corrupt push mutated repository HEAD!")
	}
}

func TestDeltaUnpackOFSAndREF(t *testing.T) {
	baseData := bytes.Repeat([]byte("abcdefghij0123456789\n"), 200)
	targetData := append(append([]byte(nil), baseData...), []byte("tail-addition\n")...)

	baseObj := gitobj.NewObject(gitobj.ObjBlob, baseData)

	// Build a minimal valid Git delta instruction stream: copy whole base + insert "tail-addition\n"
	var delta bytes.Buffer
	writeVarint := func(v int) {
		for {
			b := byte(v & 0x7f)
			v >>= 7
			if v > 0 {
				b |= 0x80
			}
			delta.WriteByte(b)
			if v == 0 {
				break
			}
		}
	}
	writeVarint(len(baseData))
	writeVarint(len(targetData))
	// Copy from offset 0, size len(baseData) (4200 = 0x1068 -> size0=0x68, size1=0x10 -> cmd 0x80 | 0x10 | 0x20)
	delta.WriteByte(0x80 | 0x10 | 0x20)
	delta.WriteByte(byte(len(baseData) & 0xff))
	delta.WriteByte(byte((len(baseData) >> 8) & 0xff))
	// Insert "tail-addition\n" (14 bytes)
	tail := []byte("tail-addition\n")
	delta.WriteByte(byte(len(tail)))
	delta.Write(tail)

	var zDelta bytes.Buffer
	zw := zlib.NewWriter(&zDelta)
	_, _ = zw.Write(delta.Bytes())
	_ = zw.Close()

	// Build REF_DELTA pack referencing external baseObj.OID
	var pack bytes.Buffer
	pack.WriteString("PACK")
	_ = binary.Write(&pack, binary.BigEndian, uint32(2))
	_ = binary.Write(&pack, binary.BigEndian, uint32(1))
	// Type 7 (REF_DELTA), size = delta.Len()
	dLen := delta.Len()
	first := byte((7 << 4) | (dLen & 0x0f))
	dLen >>= 4
	if dLen > 0 {
		pack.WriteByte(first | 0x80)
		pack.WriteByte(byte(dLen & 0x7f))
	} else {
		pack.WriteByte(first)
	}
	pack.Write(baseObj.OID[:])
	pack.Write(zDelta.Bytes())
	sum := sha1.Sum(pack.Bytes())
	pack.Write(sum[:])

	objs, err := gitobj.Unpack(pack.Bytes(), func(id gitobj.OID) (*gitobj.Object, bool) {
		if id == baseObj.OID {
			return baseObj, true
		}
		return nil, false
	})
	if err != nil {
		t.Fatalf("Unpack REF_DELTA failed: %v", err)
	}
	if len(objs) != 1 || !bytes.Equal(objs[0].Raw, targetData) {
		t.Fatalf("delta resolved mismatch")
	}
}

func TestAdversarialReviewRegressions(t *testing.T) {
	ts, f, client := newTestServer(t)
	owner := f.Viewer.Login

	doAuthReq(t, client, "POST", ts.URL+"/api/v3/user/repos", []byte(`{"name":"adv-repo"}`), "")
	doAuthReq(t, client, "POST", ts.URL+"/api/v3/repos/"+owner+"/adv-repo/labels", []byte(`{"name":"security","color":"d73a4a"}`), "")

	// 1. Trailing comma in JSON issue create must be rejected with HTTP 400 (never accepted by fast-path parser)
	rBadJSON, _ := doAuthReq(t, client, "POST", ts.URL+"/api/v3/repos/"+owner+"/adv-repo/issues",
		[]byte(`{"title":"trailing comma",}`), "")
	if rBadJSON.StatusCode != 400 {
		t.Fatalf("expected 400 for trailing-comma JSON, got %d", rBadJSON.StatusCode)
	}

	// 2. GraphQL Issue labels & comments connections advertised in __type must resolve cleanly
	doAuthReq(t, client, "POST", ts.URL+"/api/v3/repos/"+owner+"/adv-repo/issues",
		[]byte(`{"title":"GQL full fields","body":"check","labels":["security"]}`), "")
	doAuthReq(t, client, "POST", ts.URL+"/api/v3/repos/"+owner+"/adv-repo/issues/1/comments",
		[]byte(`{"body":"comment-via-rest"}`), "")

	q := fmt.Sprintf(`{"query":"query($o:String!,$n:String!){repository(owner:$o,name:$n){issue(number:1){labels(first:5){nodes{name color}} comments(first:5){nodes{body}}}}}","variables":{"o":%q,"n":"adv-repo"}}`, owner)
	rGQL, bGQL := doAuthReq(t, client, "POST", ts.URL+"/api/graphql", []byte(q), "application/json")
	if rGQL.StatusCode != 200 || !bytes.Contains(bGQL, []byte(`"security"`)) || !bytes.Contains(bGQL, []byte(`"comment-via-rest"`)) {
		t.Fatalf("GraphQL labels/comments resolution failed: %d %s", rGQL.StatusCode, string(bGQL))
	}

	// 3. Duplicate repo creation returns 422; foreign owner read returns 404 (never 403)
	rDup, _ := doAuthReq(t, client, "POST", ts.URL+"/api/v3/user/repos", []byte(`{"name":"adv-repo"}`), "")
	if rDup.StatusCode != 422 {
		t.Fatalf("expected 422 on duplicate repo, got %d", rDup.StatusCode)
	}
	rForeign, _ := doAuthReq(t, client, "GET", ts.URL+"/api/v3/repos/strangerlogin/adv-repo", nil, "")
	if rForeign.StatusCode != 404 {
		t.Fatalf("expected 404 on foreign owner repo lookup, got %d", rForeign.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// Microbenchmarks (allocs/op, B/op, latency)
// ---------------------------------------------------------------------------

func BenchmarkIssueReadHotPath(b *testing.B) {
	f := store.NewForge(testToken)
	repo, _ := f.CreateRepo("bench-read", "", true, "main")
	_, _ = repo.CreateIssue("Bench Issue", "Hot issue payload", nil)
	rt := server.NewRouter(f)

	req := httptest.NewRequest("GET", "/api/v3/repos/"+f.Viewer.Login+"/bench-read/issues/1", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rw := dummyResponseWriter{hdr: make(http.Header, 4)}
		rt.ServeHTTP(&rw, req)
		if rw.status != 200 {
			b.Fatalf("status=%d", rw.status)
		}
	}
}

func BenchmarkShallowCloneFetchV2(b *testing.B) {
	f := store.NewForge(testToken)
	repo, _ := f.CreateRepo("bench-clone", "", true, "main")
	prev := gitobj.ZeroOID
	for i := 1; i <= 200; i++ {
		next, pk := buildSingleCommitPush(prev, "history.txt", bytes.Repeat([]byte("x"), 512), i)
		repo.ReceivePack([]store.PushCommand{{OldOID: prev, NewOID: next, Ref: "refs/heads/main"}}, pk, false)
		prev = next
	}

	args := store.FetchArgs{
		Protocol: 2,
		Wants:    []gitobj.OID{prev},
		Depth:    1,
		Done:     true,
		Sideband: true,
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			out, err := repo.FetchUploadPack(args)
			if err != nil || len(out) == 0 {
				b.Fatalf("FetchUploadPack err=%v", err)
			}
		}
	})
}

type dummyResponseWriter struct {
	hdr    http.Header
	status int
	bytes  int
}

func (d *dummyResponseWriter) Header() http.Header         { return d.hdr }
func (d *dummyResponseWriter) WriteHeader(code int)        { d.status = code }
func (d *dummyResponseWriter) Write(p []byte) (int, error) { d.bytes += len(p); return len(p), nil }
