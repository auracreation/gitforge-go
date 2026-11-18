package store

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitforge/internal/gitobj"
)

// Domain error sentinel codes mapped cleanly to HTTP/GraphQL/MCP errors.
type ErrorKind uint8

const (
	ErrNotFound ErrorKind = iota + 1
	ErrConflict
	ErrUnprocessable
	ErrNotMergeable
	ErrBadRequest
)

type DomainError struct {
	Kind    ErrorKind
	Message string
}

func (e *DomainError) Error() string { return e.Message }

// User represents a GitHub user identity.
type User struct {
	ID    int64
	Login string
	Name  string
	Email string
}

// Label is an immutable repository issue label.
type Label struct {
	ID       int64
	Name     string
	Color    string
	RestJSON []byte
}

// Comment is an immutable issue/PR comment.
type Comment struct {
	ID        int64
	NodeID    string
	Body      string
	CreatedAt string
	RestJSON  []byte
}

// Issue is an immutable snapshot version of an issue with pre-serialized REST JSON.
type Issue struct {
	ID        int64
	NodeID    string
	Number    int
	Title     string
	Body      string
	State     string // "open" | "closed"
	Labels    []Label
	CreatedAt string
	UpdatedAt string
	RestJSON  []byte
}

const issueChunkSize = 64

type issueChunk [issueChunkSize]*Issue

type precomputedIssueLists struct {
	openAsc  []*Issue
	openDesc []*Issue
	allAsc   []*Issue
	allDesc  []*Issue
}

// IssueSnapshot provides lock-free O(1) reads via 64-way chunked copy-on-write arrays
// and lazy versioned list indexes invalidated automatically on every mutation.
type IssueSnapshot struct {
	Version  uint64
	MaxNum   int
	Count    int
	chunks   []*issueChunk
	labels   map[string]Label
	comments map[int][]*Comment

	listsOnce sync.Once
	lists     precomputedIssueLists
}

// Get returns the Issue for 1-based number in O(1) with zero locks and zero allocations.
func (s *IssueSnapshot) Get(number int) *Issue {
	if number < 1 || number > s.MaxNum {
		return nil
	}
	cIdx := number >> 6
	if cIdx >= len(s.chunks) {
		return nil
	}
	ch := s.chunks[cIdx]
	if ch == nil {
		return nil
	}
	return ch[number&63]
}

// Lists builds (at most once per snapshot version) and returns pre-sorted issue lists.
func (s *IssueSnapshot) Lists() precomputedIssueLists {
	s.listsOnce.Do(func() {
		allAsc := make([]*Issue, 0, s.Count)
		openAsc := make([]*Issue, 0, s.Count)
		for num := 1; num <= s.MaxNum; num++ {
			iss := s.Get(num)
			if iss == nil {
				continue
			}
			allAsc = append(allAsc, iss)
			if iss.State == "open" {
				openAsc = append(openAsc, iss)
			}
		}
		allDesc := make([]*Issue, len(allAsc))
		for i, v := range allAsc {
			allDesc[len(allAsc)-1-i] = v
		}
		openDesc := make([]*Issue, len(openAsc))
		for i, v := range openAsc {
			openDesc[len(openAsc)-1-i] = v
		}
		s.lists = precomputedIssueLists{
			openAsc:  openAsc,
			openDesc: openDesc,
			allAsc:   allAsc,
			allDesc:  allDesc,
		}
	})
	return s.lists
}

// PullRequest is an immutable snapshot version of a pull request.
type PullRequest struct {
	ID             int64
	NodeID         string
	Number         int
	Title          string
	Body           string
	State          string // "open" | "closed"
	HeadRef        string
	BaseRef        string
	HeadSHA        string
	BaseSHA        string
	Merged         bool
	MergedAt       string
	MergeCommitSHA string
	RestJSON       []byte // pre-serialized when closed/merged
}

// PullFile represents a changed file in a PullRequest diff.
type PullFile struct {
	SHA       string
	Filename  string
	Status    string // "added" | "modified" | "removed"
	Additions int
	Deletions int
	Changes   int
}

// PullSnapshot holds immutable PR state for lock-free reads.
type PullSnapshot struct {
	ByNumber map[int]*PullRequest
	Ordered  []*PullRequest // creation order ascending
}

// RepoMeta holds immutable repository metadata and pre-serialized JSON.
type RepoMeta struct {
	Private     bool
	Description string
	RestJSON    []byte
}

// RefEntry is an advertised Git reference in lexicographical order.
type RefEntry struct {
	Name      string
	OID       gitobj.OID
	PeeledOID gitobj.OID
	HasPeeled bool
}

// GitSnapshot is an immutable publication of a repository's refs and cached advertisements.
// Any push or commit publishes a new GitSnapshot, automatically invalidating shallow caches.
type GitSnapshot struct {
	Repo       *Repository
	Version    uint64
	Refs       map[string]gitobj.OID
	SortedRefs []RefEntry
	HeadTarget string // e.g. "refs/heads/main"
	HeadOID    gitobj.OID
	HasHead    bool

	upAdvOnce   sync.Once
	advUploadV0 []byte

	recvAdvOnce  sync.Once
	advReceiveV0 []byte

	depth1Once   sync.Once
	depth1Framed []byte

	lsRefsSymPeelOnce sync.Once
	lsRefsSymPeel     []byte
}

type objShard struct {
	mu      sync.RWMutex
	objs    map[gitobj.OID]*gitobj.Object
	commits map[gitobj.OID]*gitobj.ParsedCommit
}

// Repository is a single unified repository shared across Git HTTP, REST, GraphQL, and MCP.
type Repository struct {
	Forge         *Forge
	ID            int64
	NodeID        string
	Owner         User
	Name          string
	FullName      string
	DefaultBranch string

	mu        sync.Mutex // repository-scoped write lock (held only during short commit/CAS sections)
	nextNum   int        // shared per-repo issue & PR sequence (1, 2, 3...)
	metaSnap  atomic.Pointer[RepoMeta]
	gitSnap   atomic.Pointer[GitSnapshot]
	issueSnap atomic.Pointer[IssueSnapshot]
	pullSnap  atomic.Pointer[PullSnapshot]

	shards [16]objShard
}

// RegistrySnapshot is an immutable snapshot of all active repositories in the Forge.
type RegistrySnapshot struct {
	ByName map[string]*Repository
	ByID   map[int64]*Repository
	List   []*Repository
}

// Forge is the top-level unified service backing all protocol endpoints.
type Forge struct {
	Viewer     User
	Token      string
	AuthHeader string // "Bearer " + Token
	FixedNow   string // deterministic ISO-8601 timestamp
	FixedEpoch int64  // deterministic Unix epoch seconds

	reposMu  sync.Mutex
	registry atomic.Pointer[RegistrySnapshot]

	nextRepoID    atomic.Int64
	nextIssueDBID atomic.Int64
	nextPullDBID  atomic.Int64
	nextCommentID atomic.Int64
	nextLabelID   atomic.Int64
	reqCounter    atomic.Uint64
}

// NewForge constructs a unified Forge configured from environment variables.
func NewForge(token string) *Forge {
	login := strings.TrimSpace(os.Getenv("FORGE_VIEWER_LOGIN"))
	if login == "" {
		// Seeded viewer from wire.ts SCENARIO_SEED = 0x6a09e667f3bcc909n
		login = "adaiversen"
	}
	name := strings.TrimSpace(os.Getenv("FORGE_VIEWER_NAME"))
	if name == "" {
		name = "ada iversen"
	}
	fixedNow := "2025-01-01T00:00:00Z"
	var fixedEpoch int64 = 1735689600
	if rawNow := strings.TrimSpace(os.Getenv("MAKEFUN_TRIAL_NOW")); rawNow != "" {
		if t, err := time.Parse(time.RFC3339, rawNow); err == nil {
			fixedNow = t.UTC().Format(time.RFC3339)
			fixedEpoch = t.Unix()
		} else if sec, err := strconv.ParseInt(rawNow, 10, 64); err == nil {
			fixedEpoch = sec
			fixedNow = time.Unix(sec, 0).UTC().Format(time.RFC3339)
		}
	}

	f := &Forge{
		Viewer: User{
			ID:    1000,
			Login: login,
			Name:  name,
			Email: login + "@example.invalid",
		},
		Token:      token,
		AuthHeader: "Bearer " + token,
		FixedNow:   fixedNow,
		FixedEpoch: fixedEpoch,
	}
	f.nextRepoID.Store(5000)
	f.nextIssueDBID.Store(10000)
	f.nextPullDBID.Store(50000)
	f.nextCommentID.Store(90000)
	f.nextLabelID.Store(80000)

	initReg := &RegistrySnapshot{
		ByName: make(map[string]*Repository),
		ByID:   make(map[int64]*Repository),
	}
	f.registry.Store(initReg)
	return f
}

// CheckAuth verifies the Authorization header against the configured Bearer token in constant-alloc style.
func (f *Forge) CheckAuth(authHeader string) bool {
	if f.Token == "" {
		return true
	}
	if authHeader == f.AuthHeader {
		return true
	}
	// Case-insensitive "Bearer " scheme check per RFC 6750
	if len(authHeader) == len(f.AuthHeader) && strings.EqualFold(authHeader[:7], "Bearer ") {
		return authHeader[7:] == f.Token
	}
	return false
}

var preallocReqIDSlices = func() [8192][]string {
	var tbl [8192][]string
	for i := range tbl {
		tbl[i] = []string{fmt.Sprintf("req-%04x", i+1)}
	}
	return tbl
}()

// NextSeq returns a monotonic uint64 request sequence number.
func (f *Forge) NextSeq() uint64 {
	return f.reqCounter.Add(1)
}

// NextRequestIDSlice returns a pre-allocated single-element header value slice with zero heap allocations.
func (f *Forge) NextRequestIDSlice() []string {
	seq := f.reqCounter.Add(1)
	return preallocReqIDSlices[(seq-1)&8191]
}

// NextRequestID returns a non-empty request ID string with zero heap allocations.
func (f *Forge) NextRequestID() string {
	return f.NextRequestIDSlice()[0]
}

// Global Relay ID helpers (opaque, non-numeric, self-indexing, deterministic).

func EncodeRepoNodeID(repoID int64) string {
	return fmt.Sprintf("R_kgDO_%d", repoID)
}

func DecodeRepoNodeID(nodeID string) (int64, bool) {
	if !strings.HasPrefix(nodeID, "R_kgDO_") {
		return 0, false
	}
	id, err := strconv.ParseInt(nodeID[len("R_kgDO_"):], 10, 64)
	return id, err == nil && id > 0
}

func EncodeIssueNodeID(repoID int64, number int) string {
	return fmt.Sprintf("I_kwDO_%d_%d", repoID, number)
}

func DecodeIssueNodeID(nodeID string) (int64, int, bool) {
	if !strings.HasPrefix(nodeID, "I_kwDO_") {
		return 0, 0, false
	}
	parts := strings.Split(nodeID[len("I_kwDO_"):], "_")
	if len(parts) != 2 {
		return 0, 0, false
	}
	rid, err1 := strconv.ParseInt(parts[0], 10, 64)
	num, err2 := strconv.Atoi(parts[1])
	return rid, num, err1 == nil && err2 == nil && rid > 0 && num > 0
}

func EncodePullNodeID(repoID int64, number int) string {
	return fmt.Sprintf("PR_kwDO_%d_%d", repoID, number)
}

func DecodePullNodeID(nodeID string) (int64, int, bool) {
	if !strings.HasPrefix(nodeID, "PR_kwDO_") {
		return 0, 0, false
	}
	parts := strings.Split(nodeID[len("PR_kwDO_"):], "_")
	if len(parts) != 2 {
		return 0, 0, false
	}
	rid, err1 := strconv.ParseInt(parts[0], 10, 64)
	num, err2 := strconv.Atoi(parts[1])
	return rid, num, err1 == nil && err2 == nil && rid > 0 && num > 0
}

func EncodeCommentNodeID(repoID int64, issueNum int, commentID int64) string {
	return fmt.Sprintf("IC_kwDO_%d_%d_%d", repoID, issueNum, commentID)
}

// CreateRepo creates a new viewer-owned repository, returning ErrUnprocessable (422) if it already exists.
func (f *Forge) CreateRepo(name, description string, private bool, defaultBranch string) (*Repository, error) {
	name = strings.TrimSpace(name)
	if name == "" || strings.Contains(name, "/") || name == "." || name == ".." {
		return nil, &DomainError{Kind: ErrUnprocessable, Message: "Repository name is invalid"}
	}
	if defaultBranch == "" {
		defaultBranch = "main"
	}

	f.reposMu.Lock()
	defer f.reposMu.Unlock()

	cur := f.registry.Load()
	if _, exists := cur.ByName[name]; exists {
		return nil, &DomainError{Kind: ErrUnprocessable, Message: "Repository already exists"}
	}

	id := f.nextRepoID.Add(1)
	repo := &Repository{
		Forge:         f,
		ID:            id,
		NodeID:        EncodeRepoNodeID(id),
		Owner:         f.Viewer,
		Name:          name,
		FullName:      f.Viewer.Login + "/" + name,
		DefaultBranch: defaultBranch,
	}
	for i := range repo.shards {
		repo.shards[i].objs = make(map[gitobj.OID]*gitobj.Object)
		repo.shards[i].commits = make(map[gitobj.OID]*gitobj.ParsedCommit)
	}

	meta := &RepoMeta{
		Private:     private,
		Description: description,
		RestJSON:    serializeRepoJSON(repo, private, description),
	}
	repo.metaSnap.Store(meta)
	repo.gitSnap.Store(repo.buildGitSnapshot(1, nil))
	repo.issueSnap.Store(&IssueSnapshot{
		Version:  1,
		labels:   make(map[string]Label),
		comments: make(map[int][]*Comment),
	})
	repo.pullSnap.Store(&PullSnapshot{
		ByNumber: make(map[int]*PullRequest),
	})

	nextByName := make(map[string]*Repository, len(cur.ByName)+1)
	for k, v := range cur.ByName {
		nextByName[k] = v
	}
	nextByName[name] = repo

	nextByID := make(map[int64]*Repository, len(cur.ByID)+1)
	for k, v := range cur.ByID {
		nextByID[k] = v
	}
	nextByID[id] = repo

	nextList := make([]*Repository, len(cur.List)+1)
	copy(nextList, cur.List)
	nextList[len(cur.List)] = repo

	f.registry.Store(&RegistrySnapshot{
		ByName: nextByName,
		ByID:   nextByID,
		List:   nextList,
	})
	return repo, nil
}

// GetRepo performs a lock-free O(1) lookup for (owner, name), enforcing viewer ownership (foreign owner -> nil/404).
func (f *Forge) GetRepo(owner, name string) *Repository {
	if owner != f.Viewer.Login {
		return nil
	}
	return f.registry.Load().ByName[name]
}

// GetRepoByID performs a lock-free O(1) lookup by database ID.
func (f *Forge) GetRepoByID(id int64) *Repository {
	return f.registry.Load().ByID[id]
}

// ListRepos returns the immutable slice of viewer repositories in creation order.
func (f *Forge) ListRepos() []*Repository {
	return f.registry.Load().List
}

// DeleteRepo removes a repository atomically; returns false if not found.
func (f *Forge) DeleteRepo(owner, name string) bool {
	if owner != f.Viewer.Login {
		return false
	}
	f.reposMu.Lock()
	defer f.reposMu.Unlock()

	cur := f.registry.Load()
	target, ok := cur.ByName[name]
	if !ok {
		return false
	}

	nextByName := make(map[string]*Repository, len(cur.ByName)-1)
	for k, v := range cur.ByName {
		if k != name {
			nextByName[k] = v
		}
	}
	nextByID := make(map[int64]*Repository, len(cur.ByID)-1)
	for k, v := range cur.ByID {
		if k != target.ID {
			nextByID[k] = v
		}
	}
	nextList := make([]*Repository, 0, len(cur.List)-1)
	for _, r := range cur.List {
		if r.ID != target.ID {
			nextList = append(nextList, r)
		}
	}
	f.registry.Store(&RegistrySnapshot{
		ByName: nextByName,
		ByID:   nextByID,
		List:   nextList,
	})
	return true
}

// UpdateRepoMeta updates description and/or private flag atomically on a repository.
func (r *Repository) UpdateMeta(description *string, private *bool) *RepoMeta {
	r.mu.Lock()
	defer r.mu.Unlock()

	cur := r.metaSnap.Load()
	desc := cur.Description
	if description != nil {
		desc = *description
	}
	priv := cur.Private
	if private != nil {
		priv = *private
	}
	next := &RepoMeta{
		Private:     priv,
		Description: desc,
		RestJSON:    serializeRepoJSON(r, priv, desc),
	}
	r.metaSnap.Store(next)
	return next
}

// Meta returns the current immutable RepoMeta snapshot.
func (r *Repository) Meta() *RepoMeta {
	return r.metaSnap.Load()
}
