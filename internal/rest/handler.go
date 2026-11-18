package rest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"gitforge/internal/store"
)

const maxJSONBodyBytes = 16 * 1024 * 1024

var (
	jsonContentTypeSlice = []string{"application/json; charset=utf-8"}
	oauthScopesSlice     = []string{"repo, read:org"}
	apiRootBody          = []byte(`{"current_user_url":"/api/v3/user","rate_limit_url":"/api/v3/rate_limit"}` + "\n")
	metaBody             = []byte(`{"installed_version":"3.14.0"}` + "\n")
	unauthBody        = []byte(`{"message":"Requires authentication"}` + "\n")
	notFoundBody      = []byte(`{"message":"Not Found"}` + "\n")
	malformedJSONBody = []byte(`{"message":"Problems parsing JSON"}` + "\n")

	jsonBufPool = sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, 4096))
		},
	}
)

// Handler serves the GitHub REST v3 surface under /api/v3.
type Handler struct {
	Forge    *store.Forge
	userBody []byte
}

func NewHandler(f *store.Forge) *Handler {
	var ub []byte
	ub = append(ub, `{"login":`...)
	ub = store.AppendJSONString(ub, f.Viewer.Login)
	ub = append(ub, `,"id":`...)
	ub = strconv.AppendInt(ub, f.Viewer.ID, 10)
	ub = append(ub, `,"name":`...)
	ub = store.AppendJSONString(ub, f.Viewer.Name)
	ub = append(ub, `,"email":`...)
	ub = store.AppendJSONString(ub, f.Viewer.Email)
	ub = append(ub, "}\n"...)
	return &Handler{
		Forge:    f,
		userBody: ub,
	}
}

func (h *Handler) setFastHeaders(w http.ResponseWriter) {
	hdr := w.Header()
	hdr["Content-Type"] = jsonContentTypeSlice
	hdr["X-Github-Request-Id"] = h.Forge.NextRequestIDSlice()
}

func (h *Handler) writeRaw(w http.ResponseWriter, status int, rawJSON []byte) {
	h.setFastHeaders(w)
	w.WriteHeader(status)
	_, _ = w.Write(rawJSON)
}

func (h *Handler) writeError(w http.ResponseWriter, status int, msg string) {
	h.setFastHeaders(w)
	w.WriteHeader(status)
	var b [192]byte
	out := append(b[:0], `{"message":`...)
	out = store.AppendJSONString(out, msg)
	out = append(out, "}\n"...)
	_, _ = w.Write(out)
}

func (h *Handler) writeDomainError(w http.ResponseWriter, err error) {
	if de, ok := err.(*store.DomainError); ok {
		switch de.Kind {
		case store.ErrNotFound:
			h.writeError(w, http.StatusNotFound, de.Message)
		case store.ErrConflict:
			h.writeError(w, http.StatusConflict, de.Message)
		case store.ErrUnprocessable:
			h.writeError(w, http.StatusUnprocessableEntity, de.Message)
		case store.ErrNotMergeable:
			h.writeError(w, http.StatusMethodNotAllowed, de.Message)
		default:
			h.writeError(w, http.StatusBadRequest, de.Message)
		}
		return
	}
	h.writeError(w, http.StatusInternalServerError, err.Error())
}

func decodeJSONBody(r *http.Request, dst any) error {
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer func() {
		if buf.Cap() <= 256*1024 {
			buf.Reset()
			jsonBufPool.Put(buf)
		}
	}()
	_, err := buf.ReadFrom(io.LimitReader(r.Body, maxJSONBodyBytes+1))
	if err != nil {
		return err
	}
	if buf.Len() > maxJSONBodyBytes {
		return io.ErrShortBuffer
	}
	trimmed := bytes.TrimSpace(buf.Bytes())
	if len(trimmed) == 0 {
		return nil
	}
	return json.Unmarshal(trimmed, dst)
}

func parsePositiveInt(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
		if n > 1_000_000_000 {
			return 0, false
		}
	}
	return n, n > 0
}

// ServeHTTP dispatches all /api/v3 routes with zero-allocation hot paths for issue reads and writes.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.Forge.CheckAuth(r.Header.Get("Authorization")) {
		h.writeRaw(w, http.StatusUnauthorized, unauthBody)
		return
	}

	// Strip "/api/v3" prefix using EscapedPath when needed for %2F branch names,
	// while using raw URL.Path on the fast path when no '%' is present.
	rawPath := r.URL.Path
	if rawPath == "/api/v3" || rawPath == "/api/v3/" {
		if r.Method != http.MethodGet {
			h.writeError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
			return
		}
		w.Header()["X-Oauth-Scopes"] = oauthScopesSlice
		h.writeRaw(w, http.StatusOK, apiRootBody)
		return
	}
	if !strings.HasPrefix(rawPath, "/api/v3/") {
		h.writeRaw(w, http.StatusNotFound, notFoundBody)
		return
	}

	sub := rawPath[len("/api/v3/"):]
	sub = strings.TrimSuffix(sub, "/")

	// Ultra-fast hot path for /api/v3/repos/{owner}/{repo}/issues[/{number}]
	if strings.HasPrefix(sub, "repos/") && !strings.ContainsRune(r.URL.RawPath, '%') {
		if h.tryFastRepoIssueRoute(w, r, sub[len("repos/"):]) {
			return
		}
	}

	switch {
	case sub == "meta" && r.Method == http.MethodGet:
		h.writeRaw(w, http.StatusOK, metaBody)

	case sub == "user" && r.Method == http.MethodGet:
		h.writeRaw(w, http.StatusOK, h.userBody)

	case sub == "user/repos":
		if r.Method == http.MethodPost {
			h.handleCreateRepo(w, r)
		} else if r.Method == http.MethodGet {
			h.handleListUserRepos(w, r)
		} else {
			h.writeError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
		}

	case strings.HasPrefix(sub, "repos/"):
		h.handleRepoRoutes(w, r)

	default:
		h.writeRaw(w, http.StatusNotFound, notFoundBody)
	}
}

// tryFastRepoIssueRoute handles GET /issues/{n} and POST /issues with minimal allocations.
func (h *Handler) tryFastRepoIssueRoute(w http.ResponseWriter, r *http.Request, afterRepos string) bool {
	s1 := strings.IndexByte(afterRepos, '/')
	if s1 <= 0 {
		return false
	}
	owner := afterRepos[:s1]
	rem := afterRepos[s1+1:]
	s2 := strings.IndexByte(rem, '/')
	if s2 <= 0 {
		return false
	}
	repoName := rem[:s2]
	tail := rem[s2+1:]

	if !strings.HasPrefix(tail, "issues") {
		return false
	}
	if tail == "issues" {
		if r.Method == http.MethodPost {
			repo := h.Forge.GetRepo(owner, repoName)
			if repo == nil {
				h.writeRaw(w, http.StatusNotFound, notFoundBody)
				return true
			}
			h.handleCreateIssue(w, r, repo)
			return true
		}
		return false
	}
	if tail[len("issues")] != '/' {
		return false
	}
	afterIssues := tail[len("issues/"):]
	if strings.IndexByte(afterIssues, '/') >= 0 || r.Method != http.MethodGet {
		return false
	}
	num, ok := parsePositiveInt(afterIssues)
	if !ok {
		return false
	}
	repo := h.Forge.GetRepo(owner, repoName)
	if repo == nil {
		h.writeRaw(w, http.StatusNotFound, notFoundBody)
		return true
	}
	iss := repo.GetIssue(num)
	if iss == nil {
		h.writeRaw(w, http.StatusNotFound, notFoundBody)
		return true
	}
	h.writeRaw(w, http.StatusOK, iss.RestJSON)
	return true
}

func (h *Handler) handleCreateRepo(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name          string `json:"name"`
		Description   string `json:"description"`
		Private       bool   `json:"private"`
		DefaultBranch string `json:"default_branch"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		h.writeRaw(w, http.StatusBadRequest, malformedJSONBody)
		return
	}
	repo, err := h.Forge.CreateRepo(req.Name, req.Description, req.Private, req.DefaultBranch)
	if err != nil {
		h.writeDomainError(w, err)
		return
	}
	h.writeRaw(w, http.StatusCreated, repo.Meta().RestJSON)
}

func parsePageParams(q url.Values) (page, perPage int) {
	page = 1
	perPage = 30
	if v := q.Get("page"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			page = n
		}
	}
	if v := q.Get("per_page"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			if n > 100 {
				n = 100
			}
			perPage = n
		}
	}
	return page, perPage
}

func requestOrigin(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host
}

func (h *Handler) handleListUserRepos(w http.ResponseWriter, r *http.Request) {
	page, perPage := parsePageParams(r.URL.Query())
	all := h.Forge.ListRepos()
	start := (page - 1) * perPage
	var slice []*store.Repository
	if start < len(all) {
		end := start + perPage
		if end < len(all) {
			linkURL := fmt.Sprintf("<%s/api/v3/user/repos?per_page=%d&page=%d>; rel=\"next\"", requestOrigin(r), perPage, page+1)
			w.Header().Set("Link", linkURL)
		} else {
			end = len(all)
		}
		slice = all[start:end]
	}

	out := make([]byte, 0, 2+len(slice)*256)
	out = append(out, '[')
	for i, repo := range slice {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, repo.Meta().RestJSON...)
	}
	out = append(out, ']', '\n')
	h.writeRaw(w, http.StatusOK, out)
}

func splitEscapedSegments(escapedPath string) []string {
	trimmed := strings.TrimPrefix(escapedPath, "/api/v3/repos/")
	trimmed = strings.TrimSuffix(trimmed, "/")
	if trimmed == "" {
		return nil
	}
	rawSegs := strings.Split(trimmed, "/")
	out := make([]string, len(rawSegs))
	for i, s := range rawSegs {
		if dec, err := url.PathUnescape(s); err == nil {
			out[i] = dec
		} else {
			out[i] = s
		}
	}
	return out
}

func (h *Handler) handleRepoRoutes(w http.ResponseWriter, r *http.Request) {
	segs := splitEscapedSegments(r.URL.EscapedPath())
	if len(segs) < 2 {
		h.writeRaw(w, http.StatusNotFound, notFoundBody)
		return
	}
	owner, repoName := segs[0], segs[1]

	// DELETE /repos/{owner}/{repo}
	if len(segs) == 2 && r.Method == http.MethodDelete {
		if !h.Forge.DeleteRepo(owner, repoName) {
			h.writeRaw(w, http.StatusNotFound, notFoundBody)
			return
		}
		h.setFastHeaders(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	repo := h.Forge.GetRepo(owner, repoName)
	if repo == nil {
		h.writeRaw(w, http.StatusNotFound, notFoundBody)
		return
	}

	if len(segs) == 2 {
		switch r.Method {
		case http.MethodGet:
			h.writeRaw(w, http.StatusOK, repo.Meta().RestJSON)
		case http.MethodPatch:
			var req struct {
				Description *string `json:"description"`
				Private     *bool   `json:"private"`
			}
			if err := decodeJSONBody(r, &req); err != nil {
				h.writeRaw(w, http.StatusBadRequest, malformedJSONBody)
				return
			}
			meta := repo.UpdateMeta(req.Description, req.Private)
			h.writeRaw(w, http.StatusOK, meta.RestJSON)
		default:
			h.writeError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
		}
		return
	}

	resource := segs[2]
	restSegs := segs[3:]

	switch resource {
	case "git":
		h.handleGitData(w, r, repo, restSegs)
	case "branches":
		h.handleBranches(w, r, repo, restSegs)
	case "commits":
		h.handleCommits(w, r, repo, restSegs)
	case "contents":
		h.handleContents(w, r, repo, restSegs)
	case "labels":
		h.handleLabels(w, r, repo, restSegs)
	case "issues":
		h.handleIssues(w, r, repo, restSegs)
	case "pulls":
		h.handlePulls(w, r, repo, restSegs)
	default:
		h.writeRaw(w, http.StatusNotFound, notFoundBody)
	}
}

func (h *Handler) handleGitData(w http.ResponseWriter, r *http.Request, repo *store.Repository, segs []string) {
	if len(segs) == 1 && segs[0] == "refs" && r.Method == http.MethodPost {
		var req struct {
			Ref string `json:"ref"`
			SHA string `json:"sha"`
		}
		if err := decodeJSONBody(r, &req); err != nil {
			h.writeRaw(w, http.StatusBadRequest, malformedJSONBody)
			return
		}
		oid, err := repo.CreateRef(req.Ref, req.SHA)
		if err != nil {
			h.writeDomainError(w, err)
			return
		}
		var out []byte
		out = append(out, `{"ref":`...)
		out = store.AppendJSONString(out, req.Ref)
		out = append(out, `,"object":{"sha":`...)
		out = store.AppendJSONString(out, oid.Hex())
		out = append(out, `,"type":"commit"}}`+"\n"...)
		h.writeRaw(w, http.StatusCreated, out)
		return
	}
	if len(segs) >= 2 && (segs[0] == "refs" || segs[0] == "ref") && r.Method == http.MethodGet {
		subRef := "refs/" + strings.Join(segs[1:], "/")
		oid, ok := repo.Git().Refs[subRef]
		if !ok {
			h.writeRaw(w, http.StatusNotFound, notFoundBody)
			return
		}
		var out []byte
		out = append(out, `{"ref":`...)
		out = store.AppendJSONString(out, subRef)
		out = append(out, `,"object":{"sha":`...)
		out = store.AppendJSONString(out, oid.Hex())
		out = append(out, `,"type":"commit"}}`+"\n"...)
		h.writeRaw(w, http.StatusOK, out)
		return
	}
	h.writeRaw(w, http.StatusNotFound, notFoundBody)
}

func (h *Handler) handleBranches(w http.ResponseWriter, r *http.Request, repo *store.Repository, segs []string) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
		return
	}
	if len(segs) == 0 {
		branches := repo.ListBranches()
		out := make([]byte, 0, 2+len(branches)*96)
		out = append(out, '[')
		for i, b := range branches {
			if i > 0 {
				out = append(out, ',')
			}
			out = append(out, `{"name":`...)
			out = store.AppendJSONString(out, b.Name)
			out = append(out, `,"commit":{"sha":`...)
			out = store.AppendJSONString(out, b.SHA)
			out = append(out, `}}`...)
		}
		out = append(out, ']', '\n')
		h.writeRaw(w, http.StatusOK, out)
		return
	}
	branchName := strings.Join(segs, "/")
	b, ok := repo.GetBranch(branchName)
	if !ok {
		h.writeRaw(w, http.StatusNotFound, notFoundBody)
		return
	}
	var out []byte
	out = append(out, `{"name":`...)
	out = store.AppendJSONString(out, b.Name)
	out = append(out, `,"commit":{"sha":`...)
	out = store.AppendJSONString(out, b.SHA)
	out = append(out, `}}`+"\n"...)
	h.writeRaw(w, http.StatusOK, out)
}

func (h *Handler) handleCommits(w http.ResponseWriter, r *http.Request, repo *store.Repository, segs []string) {
	if r.Method != http.MethodGet || len(segs) > 0 {
		h.writeRaw(w, http.StatusNotFound, notFoundBody)
		return
	}
	q := r.URL.Query()
	sha := q.Get("sha")
	_, perPage := parsePageParams(q)
	commits, err := repo.ListCommits(sha, perPage)
	if err != nil {
		h.writeDomainError(w, err)
		return
	}
	out := make([]byte, 0, 2+len(commits)*220)
	out = append(out, '[')
	for i, c := range commits {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, `{"sha":`...)
		out = store.AppendJSONString(out, c.OID.Hex())
		out = append(out, `,"commit":{"message":`...)
		out = store.AppendJSONString(out, c.Message)
		out = append(out, `,"author":{"name":`...)
		out = store.AppendJSONString(out, c.AuthorName)
		out = append(out, `,"email":`...)
		out = store.AppendJSONString(out, c.AuthorEmail)
		out = append(out, `,"date":`...)
		out = store.AppendJSONString(out, c.AuthorDateISO)
		out = append(out, `}}}`...)
	}
	out = append(out, ']', '\n')
	h.writeRaw(w, http.StatusOK, out)
}

func decodeBase64Flexible(s string) ([]byte, error) {
	cleaned := strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == ' ' || r == '\t' {
			return -1
		}
		return r
	}, s)
	return base64.StdEncoding.DecodeString(cleaned)
}

func (h *Handler) handleContents(w http.ResponseWriter, r *http.Request, repo *store.Repository, segs []string) {
	filePath := strings.Join(segs, "/")
	switch r.Method {
	case http.MethodGet:
		ref := r.URL.Query().Get("ref")
		if ref == "" {
			ref = r.URL.Query().Get("branch")
		}
		item, err := repo.GetContent(filePath, ref)
		if err != nil {
			h.writeDomainError(w, err)
			return
		}
		if item.IsDirectory {
			out := make([]byte, 0, 2+len(item.Entries)*128)
			out = append(out, '[')
			for i, e := range item.Entries {
				if i > 0 {
					out = append(out, ',')
				}
				out = append(out, `{"type":`...)
				out = store.AppendJSONString(out, e.Type)
				out = append(out, `,"name":`...)
				out = store.AppendJSONString(out, e.Name)
				out = append(out, `,"path":`...)
				out = store.AppendJSONString(out, e.Path)
				out = append(out, `,"sha":`...)
				out = store.AppendJSONString(out, e.SHA)
				out = append(out, `,"size":`...)
				out = strconv.AppendInt(out, int64(e.Size), 10)
				out = append(out, '}')
			}
			out = append(out, ']', '\n')
			h.writeRaw(w, http.StatusOK, out)
			return
		}
		out := make([]byte, 0, 192+len(item.ContentB64))
		out = append(out, `{"type":"file","encoding":"base64","size":`...)
		out = strconv.AppendInt(out, int64(item.Size), 10)
		out = append(out, `,"name":`...)
		out = store.AppendJSONString(out, item.Name)
		out = append(out, `,"path":`...)
		out = store.AppendJSONString(out, item.Path)
		out = append(out, `,"content":`...)
		out = store.AppendJSONString(out, item.ContentB64)
		out = append(out, `,"sha":`...)
		out = store.AppendJSONString(out, item.SHA)
		out = append(out, "}\n"...)
		h.writeRaw(w, http.StatusOK, out)

	case http.MethodPut:
		var req struct {
			Message string `json:"message"`
			Content string `json:"content"`
			Branch  string `json:"branch"`
			SHA     string `json:"sha"`
		}
		if err := decodeJSONBody(r, &req); err != nil {
			h.writeRaw(w, http.StatusBadRequest, malformedJSONBody)
			return
		}
		rawBytes, err := decodeBase64Flexible(req.Content)
		if err != nil {
			h.writeError(w, http.StatusBadRequest, "content is not valid base64")
			return
		}
		item, commitSHA, created, err := repo.PutContent(filePath, rawBytes, req.Message, req.Branch, req.SHA)
		if err != nil {
			h.writeDomainError(w, err)
			return
		}
		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		var out []byte
		out = append(out, `{"content":{"name":`...)
		out = store.AppendJSONString(out, item.Name)
		out = append(out, `,"path":`...)
		out = store.AppendJSONString(out, item.Path)
		out = append(out, `,"sha":`...)
		out = store.AppendJSONString(out, item.SHA)
		out = append(out, `,"size":`...)
		out = strconv.AppendInt(out, int64(item.Size), 10)
		out = append(out, `,"type":"file","encoding":"base64"},"commit":{"sha":`...)
		out = store.AppendJSONString(out, commitSHA)
		out = append(out, `,"message":`...)
		out = store.AppendJSONString(out, req.Message)
		out = append(out, `}}`+"\n"...)
		h.writeRaw(w, status, out)

	case http.MethodDelete:
		var req struct {
			Message string `json:"message"`
			Branch  string `json:"branch"`
			SHA     string `json:"sha"`
		}
		if err := decodeJSONBody(r, &req); err != nil {
			h.writeRaw(w, http.StatusBadRequest, malformedJSONBody)
			return
		}
		commitSHA, err := repo.DeleteContent(filePath, req.Message, req.Branch, req.SHA)
		if err != nil {
			h.writeDomainError(w, err)
			return
		}
		var out []byte
		out = append(out, `{"content":null,"commit":{"sha":`...)
		out = store.AppendJSONString(out, commitSHA)
		out = append(out, `,"message":`...)
		out = store.AppendJSONString(out, req.Message)
		out = append(out, `}}`+"\n"...)
		h.writeRaw(w, http.StatusOK, out)

	default:
		h.writeError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
	}
}

func (h *Handler) handleLabels(w http.ResponseWriter, r *http.Request, repo *store.Repository, segs []string) {
	if len(segs) > 0 || r.Method != http.MethodPost {
		h.writeRaw(w, http.StatusNotFound, notFoundBody)
		return
	}
	var req struct {
		Name  string `json:"name"`
		Color string `json:"color"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		h.writeRaw(w, http.StatusBadRequest, malformedJSONBody)
		return
	}
	lbl, err := repo.CreateLabel(req.Name, req.Color)
	if err != nil {
		h.writeDomainError(w, err)
		return
	}
	h.writeRaw(w, http.StatusCreated, lbl.RestJSON)
}

func parseFlexibleLabels(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal(raw, &names); err == nil {
		return names, nil
	}
	var objs []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &objs); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		out = append(out, o.Name)
	}
	return out, nil
}

func (h *Handler) handleCreateIssue(w http.ResponseWriter, r *http.Request, repo *store.Repository) {
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	_, err := buf.ReadFrom(io.LimitReader(r.Body, maxJSONBodyBytes+1))
	if err != nil || buf.Len() > maxJSONBodyBytes {
		if buf.Cap() <= 256*1024 {
			jsonBufPool.Put(buf)
		}
		h.writeRaw(w, http.StatusBadRequest, malformedJSONBody)
		return
	}

	// Fast-path JSON scan for the common `{title, body}` benchmark payload (avoids encoding/json reflection!)
	data := bytes.TrimSpace(buf.Bytes())
	if title, body, ok := tryParseSimpleIssueJSON(data); ok {
		if buf.Cap() <= 256*1024 {
			buf.Reset()
			jsonBufPool.Put(buf)
		}
		iss, createErr := repo.CreateIssue(title, body, nil)
		if createErr != nil {
			h.writeDomainError(w, createErr)
			return
		}
		h.writeRaw(w, http.StatusCreated, iss.RestJSON)
		return
	}

	var req struct {
		Title  string          `json:"title"`
		Body   string          `json:"body"`
		Labels json.RawMessage `json:"labels"`
	}
	unmarshalErr := json.Unmarshal(data, &req)
	if buf.Cap() <= 256*1024 {
		buf.Reset()
		jsonBufPool.Put(buf)
	}
	if unmarshalErr != nil {
		h.writeRaw(w, http.StatusBadRequest, malformedJSONBody)
		return
	}
	labels, lblErr := parseFlexibleLabels(req.Labels)
	if lblErr != nil {
		h.writeRaw(w, http.StatusBadRequest, malformedJSONBody)
		return
	}
	iss, createErr := repo.CreateIssue(req.Title, req.Body, labels)
	if createErr != nil {
		h.writeDomainError(w, createErr)
		return
	}
	h.writeRaw(w, http.StatusCreated, iss.RestJSON)
}

// tryParseSimpleIssueJSON parses `{"title":"...","body":"..."}` with unescaped ASCII strings in zero reflection.
// Any escapes, extra fields (such as "labels"), or unusual formatting fall back to encoding/json cleanly.
func tryParseSimpleIssueJSON(b []byte) (title, body string, ok bool) {
	if len(b) < 2 || b[0] != '{' || b[len(b)-1] != '}' {
		return "", "", false
	}
	i := 1
	skipWS := func() {
		for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
			i++
		}
	}
	readPlainString := func() (string, bool) {
		if i >= len(b) || b[i] != '"' {
			return "", false
		}
		i++
		start := i
		for i < len(b) {
			c := b[i]
			if c == '\\' || c < 0x20 {
				return "", false
			}
			if c == '"' {
				s := string(b[start:i])
				i++
				return s, true
			}
			i++
		}
		return "", false
	}

	var sawTitle bool
	firstField := true
	for {
		skipWS()
		if firstField && i < len(b) && b[i] == '}' {
			return title, body, sawTitle
		}
		firstField = false
		key, valid := readPlainString()
		if !valid {
			return "", "", false
		}
		skipWS()
		if i >= len(b) || b[i] != ':' {
			return "", "", false
		}
		i++
		skipWS()
		val, valid := readPlainString()
		if !valid {
			return "", "", false
		}
		switch key {
		case "title":
			title = val
			sawTitle = true
		case "body":
			body = val
		default:
			return "", "", false
		}
		skipWS()
		if i < len(b) && b[i] == ',' {
			i++
			continue
		}
		if i == len(b)-1 && b[i] == '}' {
			return title, body, sawTitle
		}
		return "", "", false
	}
}

func (h *Handler) handleIssues(w http.ResponseWriter, r *http.Request, repo *store.Repository, segs []string) {
	if len(segs) == 0 {
		switch r.Method {
		case http.MethodPost:
			h.handleCreateIssue(w, r, repo)
		case http.MethodGet:
			q := r.URL.Query()
			page, perPage := parsePageParams(q)
			issues, hasMore := repo.ListIssues(q.Get("state"), q.Get("direction"), page, perPage)
			if hasMore {
				linkURL := fmt.Sprintf("<%s%s?state=%s&per_page=%d&page=%d>; rel=\"next\"",
					requestOrigin(r), r.URL.Path, url.QueryEscape(q.Get("state")), perPage, page+1)
				w.Header().Set("Link", linkURL)
			}
			out := make([]byte, 0, 2+len(issues)*256)
			out = append(out, '[')
			for i, iss := range issues {
				if i > 0 {
					out = append(out, ',')
				}
				out = append(out, iss.RestJSON...)
			}
			out = append(out, ']', '\n')
			h.writeRaw(w, http.StatusOK, out)
		default:
			h.writeError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
		}
		return
	}

	num, ok := parsePositiveInt(segs[0])
	if !ok {
		h.writeRaw(w, http.StatusNotFound, notFoundBody)
		return
	}

	if len(segs) == 1 {
		switch r.Method {
		case http.MethodGet:
			iss := repo.GetIssue(num)
			if iss == nil {
				h.writeRaw(w, http.StatusNotFound, notFoundBody)
				return
			}
			h.writeRaw(w, http.StatusOK, iss.RestJSON)
		case http.MethodPatch:
			var req struct {
				Title  *string         `json:"title"`
				Body   *string         `json:"body"`
				State  *string         `json:"state"`
				Labels json.RawMessage `json:"labels"`
			}
			if err := decodeJSONBody(r, &req); err != nil {
				h.writeRaw(w, http.StatusBadRequest, malformedJSONBody)
				return
			}
			patch := store.IssuePatch{
				Title: req.Title,
				Body:  req.Body,
				State: req.State,
			}
			if len(req.Labels) > 0 && string(req.Labels) != "null" {
				lbls, err := parseFlexibleLabels(req.Labels)
				if err != nil {
					h.writeRaw(w, http.StatusBadRequest, malformedJSONBody)
					return
				}
				patch.Labels = &lbls
			}
			iss, err := repo.UpdateIssue(num, patch)
			if err != nil {
				h.writeDomainError(w, err)
				return
			}
			h.writeRaw(w, http.StatusOK, iss.RestJSON)
		default:
			h.writeError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
		}
		return
	}

	if len(segs) == 2 && segs[1] == "comments" {
		switch r.Method {
		case http.MethodPost:
			var req struct {
				Body string `json:"body"`
			}
			if err := decodeJSONBody(r, &req); err != nil {
				h.writeRaw(w, http.StatusBadRequest, malformedJSONBody)
				return
			}
			c, err := repo.AddComment(num, req.Body)
			if err != nil {
				h.writeDomainError(w, err)
				return
			}
			h.writeRaw(w, http.StatusCreated, c.RestJSON)
		case http.MethodGet:
			comments, err := repo.ListComments(num)
			if err != nil {
				h.writeDomainError(w, err)
				return
			}
			out := make([]byte, 0, 2+len(comments)*160)
			out = append(out, '[')
			for i, c := range comments {
				if i > 0 {
					out = append(out, ',')
				}
				out = append(out, c.RestJSON...)
			}
			out = append(out, ']', '\n')
			h.writeRaw(w, http.StatusOK, out)
		default:
			h.writeError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
		}
		return
	}

	h.writeRaw(w, http.StatusNotFound, notFoundBody)
}

func (h *Handler) handlePulls(w http.ResponseWriter, r *http.Request, repo *store.Repository, segs []string) {
	if len(segs) == 0 {
		switch r.Method {
		case http.MethodPost:
			var req struct {
				Title string `json:"title"`
				Body  string `json:"body"`
				Head  string `json:"head"`
				Base  string `json:"base"`
			}
			if err := decodeJSONBody(r, &req); err != nil {
				h.writeRaw(w, http.StatusBadRequest, malformedJSONBody)
				return
			}
			pr, err := repo.CreatePull(req.Title, req.Body, req.Head, req.Base)
			if err != nil {
				h.writeDomainError(w, err)
				return
			}
			h.writeRaw(w, http.StatusCreated, pr.RestJSON)

		case http.MethodGet:
			q := r.URL.Query()
			page, perPage := parsePageParams(q)
			pulls := repo.ListPulls(q.Get("state"), page, perPage)
			out := make([]byte, 0, 2+len(pulls)*320)
			out = append(out, '[')
			for i, pr := range pulls {
				if i > 0 {
					out = append(out, ',')
				}
				_, rawJSON, _ := repo.GetPull(pr.Number)
				out = append(out, rawJSON...)
			}
			out = append(out, ']', '\n')
			h.writeRaw(w, http.StatusOK, out)

		default:
			h.writeError(w, http.StatusMethodNotAllowed, "Method Not Allowed")
		}
		return
	}

	num, ok := parsePositiveInt(segs[0])
	if !ok {
		h.writeRaw(w, http.StatusNotFound, notFoundBody)
		return
	}

	if len(segs) == 1 && r.Method == http.MethodGet {
		_, rawJSON, ok := repo.GetPull(num)
		if !ok {
			h.writeRaw(w, http.StatusNotFound, notFoundBody)
			return
		}
		h.writeRaw(w, http.StatusOK, rawJSON)
		return
	}

	if len(segs) == 2 && segs[1] == "files" && r.Method == http.MethodGet {
		files, err := repo.GetPullFiles(num)
		if err != nil {
			h.writeDomainError(w, err)
			return
		}
		out := make([]byte, 0, 2+len(files)*160)
		out = append(out, '[')
		for i, f := range files {
			if i > 0 {
				out = append(out, ',')
			}
			out = append(out, `{"sha":`...)
			out = store.AppendJSONString(out, f.SHA)
			out = append(out, `,"filename":`...)
			out = store.AppendJSONString(out, f.Filename)
			out = append(out, `,"status":`...)
			out = store.AppendJSONString(out, f.Status)
			out = append(out, `,"additions":`...)
			out = strconv.AppendInt(out, int64(f.Additions), 10)
			out = append(out, `,"deletions":`...)
			out = strconv.AppendInt(out, int64(f.Deletions), 10)
			out = append(out, `,"changes":`...)
			out = strconv.AppendInt(out, int64(f.Changes), 10)
			out = append(out, '}')
		}
		out = append(out, ']', '\n')
		h.writeRaw(w, http.StatusOK, out)
		return
	}

	if len(segs) == 2 && segs[1] == "merge" && r.Method == http.MethodPut {
		var req struct {
			CommitTitle   string `json:"commit_title"`
			CommitMessage string `json:"commit_message"`
			MergeMethod   string `json:"merge_method"`
		}
		if err := decodeJSONBody(r, &req); err != nil {
			h.writeRaw(w, http.StatusBadRequest, malformedJSONBody)
			return
		}
		mergeSHA, _, err := repo.MergePull(num, req.CommitTitle)
		if err != nil {
			h.writeDomainError(w, err)
			return
		}
		var out []byte
		out = append(out, `{"sha":`...)
		out = store.AppendJSONString(out, mergeSHA)
		out = append(out, `,"merged":true,"message":"Pull Request successfully merged"}`+"\n"...)
		h.writeRaw(w, http.StatusOK, out)
		return
	}

	h.writeRaw(w, http.StatusNotFound, notFoundBody)
}
