package graphql

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"gitforge/internal/store"
)

type gqlRequest struct {
	Query         string         `json:"query"`
	OperationName string         `json:"operationName"`
	Variables     map[string]any `json:"variables"`
}

type gqlError struct {
	Message string `json:"message"`
}

type gqlResponse struct {
	Data   any        `json:"data"`
	Errors []gqlError `json:"errors,omitempty"`
}

// Schema introspection metadata matching GitHub GraphQL v4 probes in gh and github-mcp-server.
var (
	schemaTypes = map[string]struct {
		kind       string
		fields     []string
		enumValues []string
	}{
		"Query": {
			kind:   "OBJECT",
			fields: []string{"viewer", "repository", "node", "rateLimit"},
		},
		"Mutation": {
			kind:   "OBJECT",
			fields: []string{"createIssue", "closeIssue", "createPullRequest", "mergePullRequest", "addComment"},
		},
		"User": {
			kind:   "OBJECT",
			fields: []string{"id", "databaseId", "login", "name", "email"},
		},
		"Repository": {
			kind: "OBJECT",
			fields: []string{
				"id", "databaseId", "name", "nameWithOwner", "description", "isPrivate",
				"owner", "defaultBranchRef", "viewerPermission", "hasIssuesEnabled",
				"hasProjectsEnabled", "hasDiscussionsEnabled", "mergeCommitAllowed",
				"squashMergeAllowed", "rebaseMergeAllowed",
				"issue", "issues", "pullRequest", "pullRequests",
			},
		},
		"Issue": {
			kind: "OBJECT",
			fields: []string{
				"id", "databaseId", "number", "title", "body", "state", "stateReason",
				"createdAt", "updatedAt", "author", "labels", "comments", "repository",
			},
		},
		"PullRequest": {
			kind: "OBJECT",
			fields: []string{
				"id", "databaseId", "number", "title", "body", "state", "merged", "mergedAt",
				"mergeable", "headRefName", "baseRefName", "headRefOid", "baseRefOid",
				"isDraft", "files", "comments", "author", "repository",
			},
		},
		"SearchType": {
			kind:       "ENUM",
			enumValues: []string{"ISSUE", "REPOSITORY", "USER", "DISCUSSION"},
		},
		"IssueState": {
			kind:       "ENUM",
			enumValues: []string{"OPEN", "CLOSED"},
		},
		"PullRequestState": {
			kind:       "ENUM",
			enumValues: []string{"OPEN", "CLOSED", "MERGED"},
		},
		"PullRequestMergeMethod": {
			kind:       "ENUM",
			enumValues: []string{"MERGE", "SQUASH", "REBASE"},
		},
	}
)

// Handler serves GitHub GraphQL v4 at POST /api/graphql.
type Handler struct {
	Forge *store.Forge
}

func NewHandler(f *store.Forge) *Handler {
	return &Handler{Forge: f}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	hdr := w.Header()
	hdr.Set("Content-Type", "application/json; charset=utf-8")
	hdr.Set("X-GitHub-Request-Id", h.Forge.NextRequestID())

	if !h.Forge.CheckAuth(r.Header.Get("Authorization")) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"message":"Requires authentication"}` + "\n"))
		return
	}
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = w.Write([]byte(`{"message":"Method Not Allowed"}` + "\n"))
		return
	}

	raw, err := io.ReadAll(io.LimitReader(r.Body, 8*1024*1024))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"Failed to read request body"}` + "\n"))
		return
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"Problems parsing JSON"}` + "\n"))
		return
	}

	// GitHub operation batching: JSON array of operations -> JSON array of responses in order
	if trimmed[0] == '[' {
		var batch []gqlRequest
		if err := json.Unmarshal(trimmed, &batch); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"message":"Problems parsing JSON"}` + "\n"))
			return
		}
		results := make([]gqlResponse, len(batch))
		for i, op := range batch {
			results[i] = h.executeOne(op)
		}
		out, _ := json.Marshal(results)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(append(out, '\n'))
		return
	}

	var single gqlRequest
	if err := json.Unmarshal(trimmed, &single); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"Problems parsing JSON"}` + "\n"))
		return
	}
	res := h.executeOne(single)
	out, _ := json.Marshal(res)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(out, '\n'))
}

type execContext struct {
	forge  *store.Forge
	doc    *Document
	vars   map[string]any
	errors []gqlError
}

func (ctx *execContext) addErrorf(format string, args ...any) {
	ctx.errors = append(ctx.errors, gqlError{Message: fmt.Sprintf(format, args...)})
}

func (ctx *execContext) evalValue(v ValueNode) any {
	switch v.Kind {
	case ValVariable:
		if ctx.vars == nil {
			return nil
		}
		return ctx.vars[v.Str]
	case ValInt:
		return v.Int
	case ValFloat:
		f, _ := strconv.ParseFloat(v.Str, 64)
		return f
	case ValString, ValEnum:
		return v.Str
	case ValBool:
		return v.Bool
	case ValNull:
		return nil
	case ValList:
		out := make([]any, len(v.List))
		for i, elem := range v.List {
			out[i] = ctx.evalValue(elem)
		}
		return out
	case ValObject:
		out := make(map[string]any, len(v.Object))
		for k, elem := range v.Object {
			out[k] = ctx.evalValue(elem)
		}
		return out
	default:
		return nil
	}
}

func (ctx *execContext) argString(args map[string]ValueNode, name string) (string, bool) {
	vn, ok := args[name]
	if !ok {
		return "", false
	}
	val := ctx.evalValue(vn)
	if s, ok := val.(string); ok {
		return s, true
	}
	return "", false
}

func (ctx *execContext) argInt(args map[string]ValueNode, name string) (int, bool) {
	vn, ok := args[name]
	if !ok {
		return 0, false
	}
	val := ctx.evalValue(vn)
	switch x := val.(type) {
	case int64:
		return int(x), true
	case int:
		return x, true
	case float64:
		return int(x), true
	case json.Number:
		n, err := x.Int64()
		return int(n), err == nil
	}
	return 0, false
}

func (ctx *execContext) argMap(args map[string]ValueNode, name string) (map[string]any, bool) {
	vn, ok := args[name]
	if !ok {
		return nil, false
	}
	val := ctx.evalValue(vn)
	if m, ok := val.(map[string]any); ok {
		return m, true
	}
	return nil, false
}

func (ctx *execContext) argStringList(args map[string]ValueNode, name string) []string {
	vn, ok := args[name]
	if !ok {
		return nil
	}
	val := ctx.evalValue(vn)
	switch x := val.(type) {
	case []any:
		out := make([]string, 0, len(x))
		for _, item := range x {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case string:
		return []string{x}
	}
	return nil
}

func flattenSelections(doc *Document, runtimeType string, sels []Selection) []Selection {
	var flat []Selection
	for _, s := range sels {
		switch s.Kind {
		case "field":
			flat = append(flat, s)
		case "inline_fragment":
			if s.TypeCond == "" || s.TypeCond == runtimeType || s.TypeCond == "Node" {
				flat = append(flat, flattenSelections(doc, runtimeType, s.SelectionSet)...)
			}
		case "fragment_spread":
			if frag, ok := doc.Fragments[s.Name]; ok {
				if frag.TypeCond == "" || frag.TypeCond == runtimeType || frag.TypeCond == "Node" {
					flat = append(flat, flattenSelections(doc, runtimeType, frag.SelectionSet)...)
				}
			}
		}
	}
	return flat
}

func (h *Handler) executeOne(req gqlRequest) gqlResponse {
	doc, err := ParseDocument(req.Query)
	if err != nil {
		return gqlResponse{
			Data:   nil,
			Errors: []gqlError{{Message: err.Error()}},
		}
	}
	var op *OperationDef
	if req.OperationName != "" {
		for _, candidate := range doc.Operations {
			if candidate.Name == req.OperationName {
				op = candidate
				break
			}
		}
	}
	if op == nil {
		op = doc.Operations[0]
	}

	ctx := &execContext{
		forge: h.Forge,
		doc:   doc,
		vars:  req.Variables,
	}

	var data map[string]any
	if op.Kind == "mutation" {
		data = ctx.resolveMutation(op.SelectionSet)
	} else {
		data = ctx.resolveQuery(op.SelectionSet)
	}

	if len(ctx.errors) > 0 {
		return gqlResponse{
			Data:   nil,
			Errors: ctx.errors,
		}
	}
	return gqlResponse{Data: data}
}

func (ctx *execContext) resolveQuery(sels []Selection) map[string]any {
	out := make(map[string]any)
	for _, f := range flattenSelections(ctx.doc, "Query", sels) {
		key := f.ResponseKey()
		switch f.Name {
		case "__typename":
			out[key] = "Query"
		case "viewer":
			out[key] = ctx.resolveUser(ctx.forge.Viewer, f.SelectionSet)
		case "repository":
			owner, _ := ctx.argString(f.Args, "owner")
			name, _ := ctx.argString(f.Args, "name")
			repo := ctx.forge.GetRepo(owner, name)
			if repo == nil {
				out[key] = nil
			} else {
				out[key] = ctx.resolveRepository(repo, f.SelectionSet)
			}
		case "node":
			nodeID, _ := ctx.argString(f.Args, "id")
			out[key] = ctx.resolveNodeByID(nodeID, f.SelectionSet)
		case "__type":
			typeName, _ := ctx.argString(f.Args, "name")
			out[key] = ctx.resolveIntrospectionType(typeName, f.SelectionSet)
		case "__schema":
			out[key] = ctx.resolveIntrospectionSchema(f.SelectionSet)
		default:
			ctx.addErrorf("Field '%s' doesn't exist on type 'Query'", f.Name)
		}
	}
	return out
}

func (ctx *execContext) resolveNodeByID(nodeID string, sels []Selection) any {
	if rid, ok := store.DecodeRepoNodeID(nodeID); ok {
		repo := ctx.forge.GetRepoByID(rid)
		if repo == nil {
			return nil
		}
		return ctx.resolveRepository(repo, sels)
	}
	if rid, num, ok := store.DecodeIssueNodeID(nodeID); ok {
		repo := ctx.forge.GetRepoByID(rid)
		if repo == nil {
			return nil
		}
		iss := repo.GetIssue(num)
		if iss == nil {
			return nil
		}
		return ctx.resolveIssue(repo, iss, sels)
	}
	if rid, num, ok := store.DecodePullNodeID(nodeID); ok {
		repo := ctx.forge.GetRepoByID(rid)
		if repo == nil {
			return nil
		}
		pr, _, found := repo.GetPull(num)
		if !found {
			return nil
		}
		return ctx.resolvePullRequest(repo, pr, sels)
	}
	return nil
}

func (ctx *execContext) resolveIntrospectionSchema(sels []Selection) map[string]any {
	out := make(map[string]any)
	for _, f := range flattenSelections(ctx.doc, "__Schema", sels) {
		key := f.ResponseKey()
		switch f.Name {
		case "__typename":
			out[key] = "__Schema"
		case "queryType":
			out[key] = map[string]any{"name": "Query"}
		case "mutationType":
			out[key] = map[string]any{"name": "Mutation"}
		case "types":
			var list []map[string]any
			for name := range schemaTypes {
				list = append(list, ctx.resolveIntrospectionType(name, f.SelectionSet))
			}
			out[key] = list
		default:
			ctx.addErrorf("Field '%s' doesn't exist on type '__Schema'", f.Name)
		}
	}
	return out
}

func (ctx *execContext) resolveIntrospectionType(typeName string, sels []Selection) map[string]any {
	def, ok := schemaTypes[typeName]
	if !ok {
		return nil
	}
	out := make(map[string]any)
	for _, f := range flattenSelections(ctx.doc, "__Type", sels) {
		key := f.ResponseKey()
		switch f.Name {
		case "__typename":
			out[key] = "__Type"
		case "name":
			out[key] = typeName
		case "kind":
			out[key] = def.kind
		case "fields":
			if def.kind != "OBJECT" && def.kind != "INTERFACE" {
				out[key] = nil
				continue
			}
			fieldList := make([]map[string]any, len(def.fields))
			for i, fname := range def.fields {
				fObj := make(map[string]any)
				for _, sf := range flattenSelections(ctx.doc, "__Field", f.SelectionSet) {
					switch sf.Name {
					case "__typename":
						fObj[sf.ResponseKey()] = "__Field"
					case "name":
						fObj[sf.ResponseKey()] = fname
					case "isDeprecated":
						fObj[sf.ResponseKey()] = false
					default:
						ctx.addErrorf("Field '%s' doesn't exist on type '__Field'", sf.Name)
					}
				}
				fieldList[i] = fObj
			}
			out[key] = fieldList
		case "enumValues":
			if def.kind != "ENUM" {
				out[key] = nil
				continue
			}
			enumList := make([]map[string]any, len(def.enumValues))
			for i, ename := range def.enumValues {
				eObj := make(map[string]any)
				for _, sf := range flattenSelections(ctx.doc, "__EnumValue", f.SelectionSet) {
					switch sf.Name {
					case "__typename":
						eObj[sf.ResponseKey()] = "__EnumValue"
					case "name":
						eObj[sf.ResponseKey()] = ename
					case "isDeprecated":
						eObj[sf.ResponseKey()] = false
					default:
						ctx.addErrorf("Field '%s' doesn't exist on type '__EnumValue'", sf.Name)
					}
				}
				enumList[i] = eObj
			}
			out[key] = enumList
		default:
			ctx.addErrorf("Field '%s' doesn't exist on type '__Type'", f.Name)
		}
	}
	return out
}

func (ctx *execContext) resolveUser(u store.User, sels []Selection) map[string]any {
	out := make(map[string]any)
	for _, f := range flattenSelections(ctx.doc, "User", sels) {
		key := f.ResponseKey()
		switch f.Name {
		case "__typename":
			out[key] = "User"
		case "id":
			out[key] = fmt.Sprintf("U_kgDO_%d", u.ID)
		case "databaseId":
			out[key] = u.ID
		case "login":
			out[key] = u.Login
		case "name":
			out[key] = u.Name
		case "email":
			out[key] = u.Email
		default:
			ctx.addErrorf("Field '%s' doesn't exist on type 'User'", f.Name)
		}
	}
	return out
}

func encodeCursor(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte("cursor:" + strconv.Itoa(offset)))
}

func decodeCursor(c string) (int, bool) {
	if c == "" {
		return 0, true
	}
	raw, err := base64.StdEncoding.DecodeString(c)
	if err != nil {
		return 0, false
	}
	s := string(raw)
	if !strings.HasPrefix(s, "cursor:") {
		return 0, false
	}
	n, err := strconv.Atoi(s[len("cursor:"):])
	return n, err == nil && n >= 0
}

func (ctx *execContext) resolveRepository(repo *store.Repository, sels []Selection) map[string]any {
	out := make(map[string]any)
	meta := repo.Meta()
	for _, f := range flattenSelections(ctx.doc, "Repository", sels) {
		key := f.ResponseKey()
		switch f.Name {
		case "__typename":
			out[key] = "Repository"
		case "id":
			out[key] = repo.NodeID
		case "databaseId":
			out[key] = repo.ID
		case "name":
			out[key] = repo.Name
		case "nameWithOwner":
			out[key] = repo.FullName
		case "description":
			out[key] = meta.Description
		case "isPrivate":
			out[key] = meta.Private
		case "owner":
			out[key] = ctx.resolveUser(repo.Owner, f.SelectionSet)
		case "defaultBranchRef":
			refObj := make(map[string]any)
			for _, sf := range flattenSelections(ctx.doc, "Ref", f.SelectionSet) {
				switch sf.Name {
				case "__typename":
					refObj[sf.ResponseKey()] = "Ref"
				case "name":
					refObj[sf.ResponseKey()] = repo.DefaultBranch
				case "prefix":
					refObj[sf.ResponseKey()] = "refs/heads/"
				default:
					ctx.addErrorf("Field '%s' doesn't exist on type 'Ref'", sf.Name)
				}
			}
			out[key] = refObj
		case "viewerPermission":
			out[key] = "ADMIN"
		case "hasIssuesEnabled":
			out[key] = true
		case "hasProjectsEnabled", "hasDiscussionsEnabled":
			out[key] = false
		case "mergeCommitAllowed", "squashMergeAllowed", "rebaseMergeAllowed":
			out[key] = true
		case "issue":
			num, _ := ctx.argInt(f.Args, "number")
			iss := repo.GetIssue(num)
			if iss == nil {
				out[key] = nil
			} else {
				out[key] = ctx.resolveIssue(repo, iss, f.SelectionSet)
			}
		case "issues":
			out[key] = ctx.resolveIssueConnection(repo, f)
		case "pullRequest":
			num, _ := ctx.argInt(f.Args, "number")
			pr, _, ok := repo.GetPull(num)
			if !ok {
				out[key] = nil
			} else {
				out[key] = ctx.resolvePullRequest(repo, pr, f.SelectionSet)
			}
		case "pullRequests":
			out[key] = ctx.resolvePullRequestConnection(repo, f)
		default:
			ctx.addErrorf("Field '%s' doesn't exist on type 'Repository'", f.Name)
		}
	}
	return out
}

func (ctx *execContext) resolveIssueConnection(repo *store.Repository, f Selection) map[string]any {
	states := ctx.argStringList(f.Args, "states")
	stateFilter := "all"
	if len(states) == 1 {
		switch strings.ToUpper(states[0]) {
		case "OPEN":
			stateFilter = "open"
		case "CLOSED":
			stateFilter = "closed"
		}
	}

	direction := "asc"
	if ob, ok := ctx.argMap(f.Args, "orderBy"); ok {
		if dir, ok := ob["direction"].(string); ok && strings.EqualFold(dir, "DESC") {
			direction = "desc"
		}
	}

	// Materialize full ordered list from immutable snapshot, then apply Relay cursor + first
	allItems, _ := repo.ListIssues(stateFilter, direction, 1, 1_000_000)
	afterStr, _ := ctx.argString(f.Args, "after")
	startIdx, validCursor := decodeCursor(afterStr)
	if !validCursor {
		startIdx = 0
	}
	if startIdx > len(allItems) {
		startIdx = len(allItems)
	}
	first, hasFirst := ctx.argInt(f.Args, "first")
	if !hasFirst || first < 0 {
		first = 30
	}
	endIdx := startIdx + first
	hasNextPage := endIdx < len(allItems)
	if endIdx > len(allItems) {
		endIdx = len(allItems)
	}
	pageSlice := allItems[startIdx:endIdx]
	var endCursor any
	if len(pageSlice) > 0 {
		endCursor = encodeCursor(endIdx)
	}

	conn := make(map[string]any)
	for _, sf := range flattenSelections(ctx.doc, "IssueConnection", f.SelectionSet) {
		skey := sf.ResponseKey()
		switch sf.Name {
		case "__typename":
			conn[skey] = "IssueConnection"
		case "totalCount":
			conn[skey] = len(allItems)
		case "nodes":
			nodes := make([]map[string]any, len(pageSlice))
			for i, iss := range pageSlice {
				nodes[i] = ctx.resolveIssue(repo, iss, sf.SelectionSet)
			}
			conn[skey] = nodes
		case "edges":
			edges := make([]map[string]any, len(pageSlice))
			for i, iss := range pageSlice {
				eObj := make(map[string]any)
				for _, ef := range flattenSelections(ctx.doc, "IssueEdge", sf.SelectionSet) {
					switch ef.Name {
					case "cursor":
						eObj[ef.ResponseKey()] = encodeCursor(startIdx + i + 1)
					case "node":
						eObj[ef.ResponseKey()] = ctx.resolveIssue(repo, iss, ef.SelectionSet)
					}
				}
				edges[i] = eObj
			}
			conn[skey] = edges
		case "pageInfo":
			pi := make(map[string]any)
			for _, pf := range flattenSelections(ctx.doc, "PageInfo", sf.SelectionSet) {
				switch pf.Name {
				case "__typename":
					pi[pf.ResponseKey()] = "PageInfo"
				case "hasNextPage":
					pi[pf.ResponseKey()] = hasNextPage
				case "hasPreviousPage":
					pi[pf.ResponseKey()] = startIdx > 0
				case "endCursor":
					pi[pf.ResponseKey()] = endCursor
				case "startCursor":
					if len(pageSlice) > 0 {
						pi[pf.ResponseKey()] = encodeCursor(startIdx + 1)
					} else {
						pi[pf.ResponseKey()] = nil
					}
				default:
					ctx.addErrorf("Field '%s' doesn't exist on type 'PageInfo'", pf.Name)
				}
			}
			conn[skey] = pi
		default:
			ctx.addErrorf("Field '%s' doesn't exist on type 'IssueConnection'", sf.Name)
		}
	}
	return conn
}

func (ctx *execContext) resolveIssue(repo *store.Repository, iss *store.Issue, sels []Selection) map[string]any {
	out := make(map[string]any)
	for _, f := range flattenSelections(ctx.doc, "Issue", sels) {
		key := f.ResponseKey()
		switch f.Name {
		case "__typename":
			out[key] = "Issue"
		case "id":
			out[key] = iss.NodeID
		case "databaseId":
			out[key] = iss.ID
		case "number":
			out[key] = iss.Number
		case "title":
			out[key] = iss.Title
		case "body":
			out[key] = iss.Body
		case "state":
			out[key] = strings.ToUpper(iss.State)
		case "stateReason":
			if iss.State == "closed" {
				out[key] = "COMPLETED"
			} else {
				out[key] = nil
			}
		case "createdAt":
			out[key] = iss.CreatedAt
		case "updatedAt":
			out[key] = iss.UpdatedAt
		case "author":
			out[key] = ctx.resolveUser(repo.Owner, f.SelectionSet)
		case "repository":
			out[key] = ctx.resolveRepository(repo, f.SelectionSet)
		case "labels":
			out[key] = ctx.resolveLabelsConnection(iss.Labels, f)
		case "comments":
			out[key] = ctx.resolveCommentsConnection(repo, iss.Number, f)
		default:
			ctx.addErrorf("Field '%s' doesn't exist on type 'Issue'", f.Name)
		}
	}
	return out
}

func (ctx *execContext) resolveLabelsConnection(labels []store.Label, f Selection) map[string]any {
	first, hasFirst := ctx.argInt(f.Args, "first")
	slice := labels
	if hasFirst && first >= 0 && first < len(slice) {
		slice = slice[:first]
	}
	conn := make(map[string]any)
	for _, sf := range flattenSelections(ctx.doc, "LabelConnection", f.SelectionSet) {
		switch sf.Name {
		case "totalCount":
			conn[sf.ResponseKey()] = len(labels)
		case "nodes":
			nodes := make([]map[string]any, len(slice))
			for i, l := range slice {
				ln := make(map[string]any)
				for _, lf := range flattenSelections(ctx.doc, "Label", sf.SelectionSet) {
					switch lf.Name {
					case "id":
						ln[lf.ResponseKey()] = fmt.Sprintf("LA_kwDO_%d", l.ID)
					case "name":
						ln[lf.ResponseKey()] = l.Name
					case "color":
						ln[lf.ResponseKey()] = l.Color
					default:
						ctx.addErrorf("Field '%s' doesn't exist on type 'Label'", lf.Name)
					}
				}
				nodes[i] = ln
			}
			conn[sf.ResponseKey()] = nodes
		case "pageInfo":
			conn[sf.ResponseKey()] = map[string]any{"hasNextPage": false, "endCursor": nil}
		default:
			ctx.addErrorf("Field '%s' doesn't exist on type 'LabelConnection'", sf.Name)
		}
	}
	return conn
}

func (ctx *execContext) resolveCommentsConnection(repo *store.Repository, number int, f Selection) map[string]any {
	comments, _ := repo.ListComments(number)
	first, hasFirst := ctx.argInt(f.Args, "first")
	slice := comments
	if hasFirst && first >= 0 && first < len(slice) {
		slice = slice[:first]
	}
	conn := make(map[string]any)
	for _, sf := range flattenSelections(ctx.doc, "IssueCommentConnection", f.SelectionSet) {
		switch sf.Name {
		case "totalCount":
			conn[sf.ResponseKey()] = len(comments)
		case "nodes":
			nodes := make([]map[string]any, len(slice))
			for i, c := range slice {
				cn := make(map[string]any)
				for _, cf := range flattenSelections(ctx.doc, "IssueComment", sf.SelectionSet) {
					switch cf.Name {
					case "id":
						cn[cf.ResponseKey()] = c.NodeID
					case "databaseId":
						cn[cf.ResponseKey()] = c.ID
					case "body":
						cn[cf.ResponseKey()] = c.Body
					case "createdAt":
						cn[cf.ResponseKey()] = c.CreatedAt
					case "author":
						cn[cf.ResponseKey()] = ctx.resolveUser(repo.Owner, cf.SelectionSet)
					default:
						ctx.addErrorf("Field '%s' doesn't exist on type 'IssueComment'", cf.Name)
					}
				}
				nodes[i] = cn
			}
			conn[sf.ResponseKey()] = nodes
		case "pageInfo":
			conn[sf.ResponseKey()] = map[string]any{"hasNextPage": false, "endCursor": nil}
		default:
			ctx.addErrorf("Field '%s' doesn't exist on type 'IssueCommentConnection'", sf.Name)
		}
	}
	return conn
}

func (ctx *execContext) resolvePullRequestConnection(repo *store.Repository, f Selection) map[string]any {
	states := ctx.argStringList(f.Args, "states")
	stateFilter := "open"
	if len(states) > 0 {
		if len(states) > 1 {
			stateFilter = "all"
		} else {
			switch strings.ToUpper(states[0]) {
			case "OPEN":
				stateFilter = "open"
			case "CLOSED", "MERGED":
				stateFilter = "closed"
			}
		}
	}
	first, hasFirst := ctx.argInt(f.Args, "first")
	if !hasFirst || first <= 0 {
		first = 30
	}
	pulls := repo.ListPulls(stateFilter, 1, first)
	conn := make(map[string]any)
	for _, sf := range flattenSelections(ctx.doc, "PullRequestConnection", f.SelectionSet) {
		skey := sf.ResponseKey()
		switch sf.Name {
		case "__typename":
			conn[skey] = "PullRequestConnection"
		case "totalCount":
			conn[skey] = len(pulls)
		case "nodes":
			nodes := make([]map[string]any, len(pulls))
			for i, pr := range pulls {
				nodes[i] = ctx.resolvePullRequest(repo, pr, sf.SelectionSet)
			}
			conn[skey] = nodes
		case "pageInfo":
			conn[skey] = map[string]any{"hasNextPage": false, "endCursor": nil}
		default:
			ctx.addErrorf("Field '%s' doesn't exist on type 'PullRequestConnection'", sf.Name)
		}
	}
	return conn
}

func (ctx *execContext) resolvePullRequest(repo *store.Repository, pr *store.PullRequest, sels []Selection) map[string]any {
	out := make(map[string]any)
	for _, f := range flattenSelections(ctx.doc, "PullRequest", sels) {
		key := f.ResponseKey()
		switch f.Name {
		case "__typename":
			out[key] = "PullRequest"
		case "id":
			out[key] = pr.NodeID
		case "databaseId":
			out[key] = pr.ID
		case "number":
			out[key] = pr.Number
		case "title":
			out[key] = pr.Title
		case "body":
			out[key] = pr.Body
		case "state":
			if pr.Merged {
				out[key] = "MERGED"
			} else {
				out[key] = strings.ToUpper(pr.State)
			}
		case "merged":
			out[key] = pr.Merged
		case "mergedAt":
			if pr.MergedAt == "" {
				out[key] = nil
			} else {
				out[key] = pr.MergedAt
			}
		case "mergeable":
			out[key] = repo.PullMergeableState(pr)
		case "headRefName":
			out[key] = pr.HeadRef
		case "baseRefName":
			out[key] = pr.BaseRef
		case "headRefOid":
			if pr.State == "open" {
				if hOID, ok := repo.Git().LookupRef("refs/heads/" + pr.HeadRef); ok {
					out[key] = hOID.Hex()
					continue
				}
			}
			out[key] = pr.HeadSHA
		case "baseRefOid":
			if pr.State == "open" {
				if bOID, ok := repo.Git().LookupRef("refs/heads/" + pr.BaseRef); ok {
					out[key] = bOID.Hex()
					continue
				}
			}
			out[key] = pr.BaseSHA
		case "isDraft":
			out[key] = false
		case "author":
			out[key] = ctx.resolveUser(repo.Owner, f.SelectionSet)
		case "repository":
			out[key] = ctx.resolveRepository(repo, f.SelectionSet)
		case "comments":
			out[key] = ctx.resolveCommentsConnection(repo, pr.Number, f)
		case "files":
			first, hasFirst := ctx.argInt(f.Args, "first")
			files, _ := repo.GetPullFiles(pr.Number)
			if hasFirst && first >= 0 && first < len(files) {
				files = files[:first]
			}
			fConn := make(map[string]any)
			for _, sf := range flattenSelections(ctx.doc, "PullRequestChangedFileConnection", f.SelectionSet) {
				switch sf.Name {
				case "nodes":
					nodes := make([]map[string]any, len(files))
					for i, pf := range files {
						fn := make(map[string]any)
						for _, ff := range flattenSelections(ctx.doc, "PullRequestChangedFile", sf.SelectionSet) {
							switch ff.Name {
							case "path":
								fn[ff.ResponseKey()] = pf.Filename
							case "additions":
								fn[ff.ResponseKey()] = pf.Additions
							case "deletions":
								fn[ff.ResponseKey()] = pf.Deletions
							default:
								ctx.addErrorf("Field '%s' doesn't exist on type 'PullRequestChangedFile'", ff.Name)
							}
						}
						nodes[i] = fn
					}
					fConn[sf.ResponseKey()] = nodes
				case "totalCount":
					fConn[sf.ResponseKey()] = len(files)
				case "pageInfo":
					fConn[sf.ResponseKey()] = map[string]any{"hasNextPage": false, "endCursor": nil}
				default:
					ctx.addErrorf("Field '%s' doesn't exist on type 'PullRequestChangedFileConnection'", sf.Name)
				}
			}
			out[key] = fConn
		default:
			ctx.addErrorf("Field '%s' doesn't exist on type 'PullRequest'", f.Name)
		}
	}
	return out
}

func stringFromMap(m map[string]any, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}

func (ctx *execContext) resolveMutation(sels []Selection) map[string]any {
	out := make(map[string]any)
	for _, f := range flattenSelections(ctx.doc, "Mutation", sels) {
		key := f.ResponseKey()
		input, _ := ctx.argMap(f.Args, "input")

		switch f.Name {
		case "createIssue":
			repoID, ok := store.DecodeRepoNodeID(stringFromMap(input, "repositoryId"))
			if !ok {
				ctx.addErrorf("Invalid repositoryId")
				continue
			}
			repo := ctx.forge.GetRepoByID(repoID)
			if repo == nil {
				ctx.addErrorf("Repository not found")
				continue
			}
			iss, err := repo.CreateIssue(stringFromMap(input, "title"), stringFromMap(input, "body"), nil)
			if err != nil {
				ctx.addErrorf("%s", err.Error())
				continue
			}
			payload := make(map[string]any)
			for _, sf := range flattenSelections(ctx.doc, "CreateIssuePayload", f.SelectionSet) {
				if sf.Name == "issue" {
					payload[sf.ResponseKey()] = ctx.resolveIssue(repo, iss, sf.SelectionSet)
				} else if sf.Name == "clientMutationId" {
					payload[sf.ResponseKey()] = input["clientMutationId"]
				} else {
					ctx.addErrorf("Field '%s' doesn't exist on type 'CreateIssuePayload'", sf.Name)
				}
			}
			out[key] = payload

		case "closeIssue":
			repoID, num, ok := store.DecodeIssueNodeID(stringFromMap(input, "issueId"))
			if !ok {
				ctx.addErrorf("Invalid issueId")
				continue
			}
			repo := ctx.forge.GetRepoByID(repoID)
			if repo == nil {
				ctx.addErrorf("Repository not found")
				continue
			}
			closedStr := "closed"
			iss, err := repo.UpdateIssue(num, store.IssuePatch{State: &closedStr})
			if err != nil {
				ctx.addErrorf("%s", err.Error())
				continue
			}
			payload := make(map[string]any)
			for _, sf := range flattenSelections(ctx.doc, "CloseIssuePayload", f.SelectionSet) {
				if sf.Name == "issue" {
					payload[sf.ResponseKey()] = ctx.resolveIssue(repo, iss, sf.SelectionSet)
				} else if sf.Name == "clientMutationId" {
					payload[sf.ResponseKey()] = input["clientMutationId"]
				} else {
					ctx.addErrorf("Field '%s' doesn't exist on type 'CloseIssuePayload'", sf.Name)
				}
			}
			out[key] = payload

		case "createPullRequest":
			repoID, ok := store.DecodeRepoNodeID(stringFromMap(input, "repositoryId"))
			if !ok {
				ctx.addErrorf("Invalid repositoryId")
				continue
			}
			repo := ctx.forge.GetRepoByID(repoID)
			if repo == nil {
				ctx.addErrorf("Repository not found")
				continue
			}
			pr, err := repo.CreatePull(
				stringFromMap(input, "title"),
				stringFromMap(input, "body"),
				stringFromMap(input, "headRefName"),
				stringFromMap(input, "baseRefName"),
			)
			if err != nil {
				ctx.addErrorf("%s", err.Error())
				continue
			}
			payload := make(map[string]any)
			for _, sf := range flattenSelections(ctx.doc, "CreatePullRequestPayload", f.SelectionSet) {
				if sf.Name == "pullRequest" {
					payload[sf.ResponseKey()] = ctx.resolvePullRequest(repo, pr, sf.SelectionSet)
				} else if sf.Name == "clientMutationId" {
					payload[sf.ResponseKey()] = input["clientMutationId"]
				} else {
					ctx.addErrorf("Field '%s' doesn't exist on type 'CreatePullRequestPayload'", sf.Name)
				}
			}
			out[key] = payload

		case "mergePullRequest":
			repoID, num, ok := store.DecodePullNodeID(stringFromMap(input, "pullRequestId"))
			if !ok {
				ctx.addErrorf("Invalid pullRequestId")
				continue
			}
			repo := ctx.forge.GetRepoByID(repoID)
			if repo == nil {
				ctx.addErrorf("Repository not found")
				continue
			}
			_, mergedPR, err := repo.MergePull(num, stringFromMap(input, "commitHeadline"))
			if err != nil {
				ctx.addErrorf("%s", err.Error())
				continue
			}
			payload := make(map[string]any)
			for _, sf := range flattenSelections(ctx.doc, "MergePullRequestPayload", f.SelectionSet) {
				if sf.Name == "pullRequest" {
					payload[sf.ResponseKey()] = ctx.resolvePullRequest(repo, mergedPR, sf.SelectionSet)
				} else if sf.Name == "clientMutationId" {
					payload[sf.ResponseKey()] = input["clientMutationId"]
				} else {
					ctx.addErrorf("Field '%s' doesn't exist on type 'MergePullRequestPayload'", sf.Name)
				}
			}
			out[key] = payload

		case "addComment":
			subjectID := stringFromMap(input, "subjectId")
			var repoID int64
			var num int
			var ok bool
			if repoID, num, ok = store.DecodeIssueNodeID(subjectID); !ok {
				repoID, num, ok = store.DecodePullNodeID(subjectID)
			}
			if !ok {
				ctx.addErrorf("Invalid subjectId")
				continue
			}
			repo := ctx.forge.GetRepoByID(repoID)
			if repo == nil {
				ctx.addErrorf("Repository not found")
				continue
			}
			comment, err := repo.AddComment(num, stringFromMap(input, "body"))
			if err != nil {
				ctx.addErrorf("%s", err.Error())
				continue
			}
			payload := make(map[string]any)
			for _, sf := range flattenSelections(ctx.doc, "AddCommentPayload", f.SelectionSet) {
				if sf.Name == "commentEdge" {
					edge := make(map[string]any)
					for _, ef := range flattenSelections(ctx.doc, "IssueCommentEdge", sf.SelectionSet) {
						if ef.Name == "node" {
							nodeObj := make(map[string]any)
							for _, nf := range flattenSelections(ctx.doc, "IssueComment", ef.SelectionSet) {
								switch nf.Name {
								case "__typename":
									nodeObj[nf.ResponseKey()] = "IssueComment"
								case "id":
									nodeObj[nf.ResponseKey()] = comment.NodeID
								case "databaseId":
									nodeObj[nf.ResponseKey()] = comment.ID
								case "body":
									nodeObj[nf.ResponseKey()] = comment.Body
								case "createdAt":
									nodeObj[nf.ResponseKey()] = comment.CreatedAt
								default:
									ctx.addErrorf("Field '%s' doesn't exist on type 'IssueComment'", nf.Name)
								}
							}
							edge[ef.ResponseKey()] = nodeObj
						}
					}
					payload[sf.ResponseKey()] = edge
				} else if sf.Name == "clientMutationId" {
					payload[sf.ResponseKey()] = input["clientMutationId"]
				} else {
					ctx.addErrorf("Field '%s' doesn't exist on type 'AddCommentPayload'", sf.Name)
				}
			}
			out[key] = payload

		default:
			ctx.addErrorf("Field '%s' doesn't exist on type 'Mutation'", f.Name)
		}
	}
	return out
}
