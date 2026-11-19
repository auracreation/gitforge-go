package server_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"gitforge/internal/store"
)

func TestLabelUpdateRefreshesAttachedIssuesAcrossProtocols(t *testing.T) {
	ts, f, client := newTestServer(t)
	repo, err := f.CreateRepo("label-update", "", true, "main")
	if err != nil {
		t.Fatal(err)
	}
	label, err := repo.CreateLabel("bug", "ff0000")
	if err != nil {
		t.Fatal(err)
	}
	// Span multiple chunks, retaining old issue pointers to check snapshot immutability.
	var original []*store.Issue
	for i := 0; i < 65; i++ {
		issue, err := repo.CreateIssue(fmt.Sprintf("issue-%d", i), "", []string{"bug"})
		if err != nil {
			t.Fatal(err)
		}
		_ = issue.HTTP200Frame()
		original = append(original, issue)
	}
	unaffected, err := repo.CreateIssue("no label", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = repo.ListIssues("all", "asc", 1, 100)
	base := ts.URL + "/api/v3/repos/" + f.Viewer.Login + "/label-update"
	resp, body := doAuthReq(t, client, "POST", base+"/labels", []byte(`{"name":"bug","color":"00ff00"}`), "")
	if resp.StatusCode != 201 {
		t.Fatalf("update label: %d %s", resp.StatusCode, body)
	}
	var updated store.Label
	if err := json.Unmarshal(body, &updated); err != nil {
		t.Fatal(err)
	}
	if updated.ID != label.ID {
		t.Errorf("label identity changed: %d -> %d", label.ID, updated.ID)
	}
	for _, old := range original {
		current := repo.GetIssue(old.Number)
		if current.Labels[0].Color != "00ff00" || current.Labels[0].ID != label.ID {
			t.Errorf("issue %d retains stale label: %+v", old.Number, current.Labels[0])
		}
		if !bytes.Contains(current.HTTP200Frame(), []byte(`"color":"00ff00"`)) || !bytes.Contains(old.HTTP200Frame(), []byte(`"color":"ff0000"`)) {
			t.Fatalf("issue %d has incorrect cached wire data", old.Number)
		}
		if old.Labels[0].Color != "ff0000" || !bytes.Contains(old.RestJSON, []byte(`"color":"ff0000"`)) {
			t.Fatalf("old issue %d was mutated", old.Number)
		}
	}
	if repo.GetIssue(unaffected.Number) != unaffected {
		t.Error("unaffected issue was copied")
	}
	t.Run("REST", func(t *testing.T) {
		for _, path := range []string{"/issues/1", "/issues?state=all&per_page=100"} {
			resp, body := doAuthReq(t, client, "GET", base+path, nil, "")
			if resp.StatusCode != 200 || !bytes.Contains(body, []byte(`"color":"00ff00"`)) || bytes.Contains(body, []byte(`"color":"ff0000"`)) {
				t.Fatalf("stale REST response: %d %.500s", resp.StatusCode, body)
			}
		}
	})
	t.Run("GraphQL", func(t *testing.T) {
		query := fmt.Sprintf(`{"query":"query($o:String!){repository(owner:$o,name:\"label-update\"){issue(number:65){labels(first:5){nodes{name color}}}}}","variables":{"o":%q}}`, f.Viewer.Login)
		resp, body := doAuthReq(t, client, "POST", ts.URL+"/api/graphql", []byte(query), "")
		if resp.StatusCode != 200 || !bytes.Contains(body, []byte(`"color":"00ff00"`)) || bytes.Contains(body, []byte(`"errors"`)) {
			t.Fatalf("stale GraphQL response: %d %s", resp.StatusCode, body)
		}
	})
	t.Run("MCP", func(t *testing.T) {
		call := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_issues","arguments":{"owner":%q,"repo":"label-update","state":"all","per_page":100}}}`, f.Viewer.Login)
		resp, body := doAuthReq(t, client, "POST", ts.URL+"/mcp", []byte(call), "")
		var result struct {
			Result struct {
				Content []struct{ Text string } `json:"content"`
			} `json:"result"`
		}
		if err := json.Unmarshal(body, &result); err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 200 || len(result.Result.Content) != 1 || !bytes.Contains([]byte(result.Result.Content[0].Text), []byte(`"color":"00ff00"`)) || bytes.Contains([]byte(result.Result.Content[0].Text), []byte(`"color":"ff0000"`)) {
			t.Fatalf("stale MCP response: %d %.500s", resp.StatusCode, body)
		}
	})
}
