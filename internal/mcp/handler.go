package mcp

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"gitforge/internal/store"
)

// Static, byte-identical tools/list result JSON so repeated calls are guaranteed byte-for-byte equal.
var toolsListResultRaw = json.RawMessage(`{
  "tools": [
    {
      "name": "create_repo",
      "description": "Create a new repository owned by the authenticated user.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "name": { "type": "string", "description": "Repository name" },
          "description": { "type": "string", "description": "Repository description" },
          "private": { "type": "boolean", "description": "Whether the repository is private" }
        },
        "required": ["name"]
      }
    },
    {
      "name": "get_repo",
      "description": "Fetch metadata for an existing repository by owner and name.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "owner": { "type": "string", "description": "Repository owner login" },
          "repo": { "type": "string", "description": "Repository name" }
        },
        "required": ["owner", "repo"]
      }
    },
    {
      "name": "create_issue",
      "description": "Create a new issue in a repository.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "owner": { "type": "string", "description": "Repository owner login" },
          "repo": { "type": "string", "description": "Repository name" },
          "title": { "type": "string", "description": "Issue title" },
          "body": { "type": "string", "description": "Issue body text" },
          "labels": { "type": "array", "items": { "type": "string" }, "description": "Optional label names" }
        },
        "required": ["owner", "repo", "title"]
      }
    },
    {
      "name": "list_issues",
      "description": "List issues in a repository filtered by state.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "owner": { "type": "string", "description": "Repository owner login" },
          "repo": { "type": "string", "description": "Repository name" },
          "state": { "type": "string", "description": "Issue state filter (open, closed, all)" }
        },
        "required": ["owner", "repo"]
      }
    },
    {
      "name": "create_pull_request",
      "description": "Open a new pull request from a head branch into a base branch.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "owner": { "type": "string", "description": "Repository owner login" },
          "repo": { "type": "string", "description": "Repository name" },
          "title": { "type": "string", "description": "Pull request title" },
          "body": { "type": "string", "description": "Pull request body" },
          "head": { "type": "string", "description": "Head branch name" },
          "base": { "type": "string", "description": "Base branch name" }
        },
        "required": ["owner", "repo", "title", "head", "base"]
      }
    },
    {
      "name": "merge_pull_request",
      "description": "Squash-merge an open pull request into its base branch.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "owner": { "type": "string", "description": "Repository owner login" },
          "repo": { "type": "string", "description": "Repository name" },
          "pull_number": { "type": "integer", "description": "Pull request number" }
        },
        "required": ["owner", "repo", "pull_number"]
      }
    }
  ]
}`)

var serverDiscoverResultRaw = json.RawMessage(`{
  "resultType": "discover",
  "supportedVersions": ["2026-07-28", "2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"],
  "capabilities": { "tools": { "listChanged": false } },
  "serverInfo": { "name": "gitforge", "version": "1.0.0" },
  "_meta": {
    "io.modelcontextprotocol/serverInfo": { "name": "gitforge", "version": "1.0.0" }
  }
}`)

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

// Handler serves MCP Streamable HTTP (2026-07-28 + legacy 2025-06-18/2025-11-25) at /mcp.
type Handler struct {
	Forge *store.Forge
}

func NewHandler(f *store.Forge) *Handler {
	return &Handler{Forge: f}
}

func writeRPC(w http.ResponseWriter, status int, id json.RawMessage, result any, rpcErr *rpcError) {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	resp := rpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
		Error:   rpcErr,
	}
	out, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(append(out, '\n'))
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-GitHub-Request-Id", h.Forge.NextRequestID())

	if !h.Forge.CheckAuth(r.Header.Get("Authorization")) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"message":"Requires authentication"}` + "\n"))
		return
	}

	if r.Method == http.MethodDelete {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = w.Write([]byte(`{"message":"Method Not Allowed"}` + "\n"))
		return
	}

	raw, err := io.ReadAll(io.LimitReader(r.Body, 4*1024*1024))
	if err != nil {
		writeRPC(w, http.StatusBadRequest, nil, nil, &rpcError{Code: -32700, Message: "Parse error"})
		return
	}

	var req rpcRequest
	if err := json.Unmarshal(bytes.TrimSpace(raw), &req); err != nil || req.Method == "" {
		writeRPC(w, http.StatusBadRequest, nil, nil, &rpcError{Code: -32700, Message: "Parse error"})
		return
	}

	// JSON-RPC notifications (no id)
	if len(req.ID) == 0 || strings.HasPrefix(req.Method, "notifications/") {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	switch req.Method {
	case "server/discover":
		writeRPC(w, http.StatusOK, req.ID, serverDiscoverResultRaw, nil)

	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &p)
		ver := p.ProtocolVersion
		if ver == "" {
			ver = "2025-11-25"
		}
		w.Header().Set("Mcp-Session-Id", "gitforge-session-1")
		writeRPC(w, http.StatusOK, req.ID, map[string]any{
			"protocolVersion": ver,
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": "gitforge", "version": "1.0.0"},
		}, nil)

	case "ping":
		writeRPC(w, http.StatusOK, req.ID, map[string]any{}, nil)

	case "tools/list":
		writeRPC(w, http.StatusOK, req.ID, toolsListResultRaw, nil)

	case "tools/call":
		h.handleToolsCall(w, req)

	default:
		writeRPC(w, http.StatusOK, req.ID, nil, &rpcError{Code: -32601, Message: "Method not found: " + req.Method})
	}
}

func argStr(args map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := args[k]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

func argOptionalStr(args map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := args[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}

func argInt(args map[string]any, keys ...string) (int, bool) {
	for _, k := range keys {
		v, ok := args[k]
		if !ok || v == nil {
			continue
		}
		switch x := v.(type) {
		case float64:
			return int(x), true
		case int:
			return x, true
		case string:
			if n, err := strconv.Atoi(strings.TrimSpace(x)); err == nil {
				return n, true
			}
		}
	}
	return 0, false
}

func writeToolSuccess(w http.ResponseWriter, id json.RawMessage, jsonPayload []byte) {
	var structured any
	_ = json.Unmarshal(jsonPayload, &structured)
	writeRPC(w, http.StatusOK, id, map[string]any{
		"content": []map[string]any{
			{
				"type": "text",
				"text": string(bytes.TrimSpace(jsonPayload)),
			},
		},
		"structuredContent": structured,
		"isError":           false,
	}, nil)
}

func writeToolExecError(w http.ResponseWriter, id json.RawMessage, msg string) {
	writeRPC(w, http.StatusOK, id, map[string]any{
		"content": []map[string]any{
			{
				"type": "text",
				"text": msg,
			},
		},
		"isError": true,
	}, nil)
}

func (h *Handler) handleToolsCall(w http.ResponseWriter, req rpcRequest) {
	var call struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
	}
	if err := json.Unmarshal(req.Params, &call); err != nil || call.Name == "" {
		writeRPC(w, http.StatusOK, req.ID, nil, &rpcError{Code: -32602, Message: "Invalid tools/call params"})
		return
	}
	args := call.Arguments
	if args == nil {
		args = map[string]any{}
	}

	switch call.Name {
	case "create_repo":
		name := argStr(args, "name", "repo", "repository")
		if name == "" {
			writeRPC(w, http.StatusOK, req.ID, nil, &rpcError{Code: -32602, Message: "Missing required parameter: name"})
			return
		}
		desc := argOptionalStr(args, "description")
		priv := false
		if b, ok := args["private"].(bool); ok {
			priv = b
		}
		repo, err := h.Forge.CreateRepo(name, desc, priv, "main")
		if err != nil {
			writeToolExecError(w, req.ID, err.Error())
			return
		}
		writeToolSuccess(w, req.ID, repo.Meta().RestJSON)

	case "get_repo":
		owner := argStr(args, "owner")
		repoName := argStr(args, "repo", "name", "repository")
		if owner == "" || repoName == "" {
			writeRPC(w, http.StatusOK, req.ID, nil, &rpcError{Code: -32602, Message: "Missing required parameters: owner, repo"})
			return
		}
		repo := h.Forge.GetRepo(owner, repoName)
		if repo == nil {
			writeToolExecError(w, req.ID, "Repository not found")
			return
		}
		writeToolSuccess(w, req.ID, repo.Meta().RestJSON)

	case "create_issue":
		owner := argStr(args, "owner")
		repoName := argStr(args, "repo", "repository")
		title := argStr(args, "title")
		if owner == "" || repoName == "" || title == "" {
			writeRPC(w, http.StatusOK, req.ID, nil, &rpcError{Code: -32602, Message: "Missing required parameters: owner, repo, title"})
			return
		}
		repo := h.Forge.GetRepo(owner, repoName)
		if repo == nil {
			writeToolExecError(w, req.ID, "Repository not found")
			return
		}
		var labels []string
		if rawLbls, ok := args["labels"].([]any); ok {
			for _, item := range rawLbls {
				if s, ok := item.(string); ok {
					labels = append(labels, s)
				}
			}
		}
		iss, err := repo.CreateIssue(title, argOptionalStr(args, "body"), labels)
		if err != nil {
			writeToolExecError(w, req.ID, err.Error())
			return
		}
		writeToolSuccess(w, req.ID, iss.RestJSON)

	case "list_issues":
		owner := argStr(args, "owner")
		repoName := argStr(args, "repo", "repository")
		if owner == "" || repoName == "" {
			writeRPC(w, http.StatusOK, req.ID, nil, &rpcError{Code: -32602, Message: "Missing required parameters: owner, repo"})
			return
		}
		repo := h.Forge.GetRepo(owner, repoName)
		if repo == nil {
			writeToolExecError(w, req.ID, "Repository not found")
			return
		}
		state := argStr(args, "state")
		if state == "" {
			state = "open"
		}
		issues, _ := repo.ListIssues(state, "desc", 1, 100)
		var out []byte
		out = append(out, '[')
		for i, iss := range issues {
			if i > 0 {
				out = append(out, ',')
			}
			out = append(out, iss.RestJSON...)
		}
		out = append(out, ']')
		writeRPC(w, http.StatusOK, req.ID, map[string]any{
			"content": []map[string]any{
				{
					"type": "text",
					"text": string(out),
				},
			},
			"isError": false,
		}, nil)

	case "create_pull_request":
		owner := argStr(args, "owner")
		repoName := argStr(args, "repo", "repository")
		title := argStr(args, "title")
		head := argStr(args, "head", "head_ref", "head_branch")
		base := argStr(args, "base", "base_ref", "base_branch")
		if owner == "" || repoName == "" || title == "" || head == "" || base == "" {
			writeRPC(w, http.StatusOK, req.ID, nil, &rpcError{Code: -32602, Message: "Missing required parameters: owner, repo, title, head, base"})
			return
		}
		repo := h.Forge.GetRepo(owner, repoName)
		if repo == nil {
			writeToolExecError(w, req.ID, "Repository not found")
			return
		}
		pr, err := repo.CreatePull(title, argOptionalStr(args, "body"), head, base)
		if err != nil {
			writeToolExecError(w, req.ID, err.Error())
			return
		}
		writeToolSuccess(w, req.ID, pr.RestJSON)

	case "merge_pull_request":
		owner := argStr(args, "owner")
		repoName := argStr(args, "repo", "repository")
		pullNum, okNum := argInt(args, "pull_number", "number", "pull_request_number")
		if owner == "" || repoName == "" || !okNum || pullNum <= 0 {
			writeRPC(w, http.StatusOK, req.ID, nil, &rpcError{Code: -32602, Message: "Missing required parameters: owner, repo, pull_number"})
			return
		}
		repo := h.Forge.GetRepo(owner, repoName)
		if repo == nil {
			writeToolExecError(w, req.ID, "Repository not found")
			return
		}
		mergeSHA, _, err := repo.MergePull(pullNum, argOptionalStr(args, "commit_title"))
		if err != nil {
			writeToolExecError(w, req.ID, err.Error())
			return
		}
		resBytes, _ := json.Marshal(map[string]any{
			"sha":     mergeSHA,
			"merged":  true,
			"message": "Pull Request successfully merged",
		})
		writeToolSuccess(w, req.ID, resBytes)

	default:
		writeRPC(w, http.StatusOK, req.ID, nil, &rpcError{Code: -32602, Message: "Unknown tool: " + call.Name})
	}
}
