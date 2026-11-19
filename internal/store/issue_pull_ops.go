package store

import (
	"fmt"
	"strings"

	"gitforge/internal/gitobj"
)

// CreateLabel adds or updates a repository label atomically.
func (r *Repository) CreateLabel(name, color string) (Label, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return Label{}, &DomainError{Kind: ErrUnprocessable, Message: "Label name is required"}
	}
	color = strings.TrimPrefix(strings.TrimSpace(color), "#")
	if color == "" {
		color = "ededed"
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	cur := r.issueMeta.Load()
	if existing, ok := cur.labels[name]; ok {
		if existing.Color == color {
			return existing, nil
		}
	}
	id := r.Forge.nextLabelID.Add(1)
	lbl := Label{
		ID:    id,
		Name:  name,
		Color: color,
	}
	lbl.RestJSON = serializeLabelJSON(lbl)

	nextLabels := make(map[string]Label, len(cur.labels)+1)
	for k, v := range cur.labels {
		nextLabels[k] = v
	}
	nextLabels[name] = lbl

	r.issueMeta.Store(&issueMetaSnap{
		labels:   nextLabels,
		comments: cur.comments,
	})
	return lbl, nil
}

// CreateIssue validates labels, pre-serializes HTTP/JSON, and publishes into lock-free atomic chunks in O(1).
func (r *Repository) CreateIssue(title, body string, labelNames []string) (*Issue, error) {
	title = strings.TrimSpace(title)
	if title == "" {
		return nil, &DomainError{Kind: ErrUnprocessable, Message: "Title is required"}
	}

	// Fast lock-free pre-check of labels when labelNames are supplied
	if len(labelNames) > 0 {
		meta := r.issueMeta.Load()
		for _, ln := range labelNames {
			if _, ok := meta.labels[ln]; !ok {
				return nil, &DomainError{Kind: ErrUnprocessable, Message: "Unknown label: " + ln}
			}
		}
	}

	dbID := r.Forge.nextIssueDBID.Add(1)
	now := r.Forge.FixedNow

	r.mu.Lock()
	var resolvedLabels []Label
	if len(labelNames) > 0 {
		meta := r.issueMeta.Load()
		resolvedLabels = make([]Label, 0, len(labelNames))
		for _, ln := range labelNames {
			lbl, ok := meta.labels[ln]
			if !ok {
				r.mu.Unlock()
				return nil, &DomainError{Kind: ErrUnprocessable, Message: "Unknown label: " + ln}
			}
			resolvedLabels = append(resolvedLabels, lbl)
		}
	}

	r.nextNum++
	num := r.nextNum
	iss := &Issue{
		ID:        dbID,
		NodeID:    EncodeIssueNodeID(r.ID, num),
		Number:    num,
		Title:     title,
		Body:      body,
		State:     "open",
		Labels:    resolvedLabels,
		CreatedAt: now,
		UpdatedAt: now,
	}
	iss.HTTP200, iss.RestJSON = serializeIssueHTTP(iss, r.Forge.OwnerJSON)

	cIdx := num >> 6
	tblPtr := r.issueChunks.Load()
	tbl := *tblPtr
	if cIdx >= len(tbl) {
		newLen := len(tbl) * 2
		if newLen <= cIdx {
			newLen = cIdx + 4
		}
		grown := make([]*issueAtomicChunk, newLen)
		copy(grown, tbl)
		ch := new(issueAtomicChunk)
		ch.slots[num&63].Store(iss)
		grown[cIdx] = ch
		r.issueChunks.Store(&grown)
	} else {
		ch := tbl[cIdx]
		if ch == nil {
			ch = new(issueAtomicChunk)
			ch.slots[num&63].Store(iss)
			// Copy small slice header only once per 64 issues so readers never race on slice element write
			grown := make([]*issueAtomicChunk, len(tbl))
			copy(grown, tbl)
			grown[cIdx] = ch
			r.issueChunks.Store(&grown)
		} else {
			ch.slots[num&63].Store(iss)
		}
	}
	r.issueMaxNum.Store(int64(num))
	r.issueCount.Add(1)
	r.issueVersion.Add(1)
	r.mu.Unlock()
	return iss, nil
}

// GetIssue performs a lock-free O(1) read from the atomic chunk table.
func (r *Repository) GetIssue(number int) *Issue {
	if number < 1 {
		return nil
	}
	tbl := *r.issueChunks.Load()
	cIdx := number >> 6
	if cIdx >= len(tbl) {
		return nil
	}
	ch := tbl[cIdx]
	if ch == nil {
		return nil
	}
	return ch.slots[number&63].Load()
}

// IssuePatch describes fields updated via PATCH /issues/{n} or GraphQL closeIssue.
type IssuePatch struct {
	Title  *string
	Body   *string
	State  *string
	Labels *[]string
}

// UpdateIssue updates an existing issue atomically and invalidates cached list views.
func (r *Repository) UpdateIssue(number int, patch IssuePatch) (*Issue, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	old := r.GetIssue(number)
	if old == nil {
		return nil, &DomainError{Kind: ErrNotFound, Message: "Not Found"}
	}

	title := old.Title
	if patch.Title != nil {
		t := strings.TrimSpace(*patch.Title)
		if t == "" {
			return nil, &DomainError{Kind: ErrUnprocessable, Message: "Title cannot be empty"}
		}
		title = t
	}
	body := old.Body
	if patch.Body != nil {
		body = *patch.Body
	}
	state := old.State
	if patch.State != nil {
		s := strings.ToLower(strings.TrimSpace(*patch.State))
		if s != "open" && s != "closed" {
			return nil, &DomainError{Kind: ErrUnprocessable, Message: "Invalid issue state"}
		}
		state = s
	}
	labels := old.Labels
	if patch.Labels != nil {
		meta := r.issueMeta.Load()
		labels = make([]Label, 0, len(*patch.Labels))
		for _, ln := range *patch.Labels {
			lbl, ok := meta.labels[ln]
			if !ok {
				return nil, &DomainError{Kind: ErrUnprocessable, Message: "Unknown label: " + ln}
			}
			labels = append(labels, lbl)
		}
	}

	updated := &Issue{
		ID:        old.ID,
		NodeID:    old.NodeID,
		Number:    old.Number,
		Title:     title,
		Body:      body,
		State:     state,
		Labels:    labels,
		CreatedAt: old.CreatedAt,
		UpdatedAt: r.Forge.FixedNow,
	}
	updated.HTTP200, updated.RestJSON = serializeIssueHTTP(updated, r.Forge.OwnerJSON)

	tbl := *r.issueChunks.Load()
	tbl[number>>6].slots[number&63].Store(updated)
	r.issueVersion.Add(1)
	return updated, nil
}

func (r *Repository) issueLists() precomputedIssueLists {
	ver := r.issueVersion.Load()
	if c := r.issueListCache.Load(); c != nil && c.version == ver {
		return c.lists
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	ver = r.issueVersion.Load()
	if c := r.issueListCache.Load(); c != nil && c.version == ver {
		return c.lists
	}

	maxNum := int(r.issueMaxNum.Load())
	count := int(r.issueCount.Load())
	tbl := *r.issueChunks.Load()
	allAsc := make([]*Issue, 0, count)
	openAsc := make([]*Issue, 0, count)
	for num := 1; num <= maxNum; num++ {
		cIdx := num >> 6
		if cIdx >= len(tbl) {
			break
		}
		ch := tbl[cIdx]
		if ch == nil {
			continue
		}
		iss := ch.slots[num&63].Load()
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
	lists := precomputedIssueLists{
		openAsc:  openAsc,
		openDesc: openDesc,
		allAsc:   allAsc,
		allDesc:  allDesc,
	}
	r.issueListCache.Store(&cachedIssueLists{
		version: ver,
		lists:   lists,
	})
	return lists
}

// ListIssues returns filtered, ordered, and paginated issues from the versioned list cache.
func (r *Repository) ListIssues(state, direction string, page, perPage int) ([]*Issue, bool) {
	lists := r.issueLists()

	state = strings.ToLower(strings.TrimSpace(state))
	if state == "" {
		state = "open"
	}
	direction = strings.ToLower(strings.TrimSpace(direction))
	if direction == "" {
		direction = "desc"
	}

	var base []*Issue
	switch state {
	case "all":
		if direction == "asc" {
			base = lists.allAsc
		} else {
			base = lists.allDesc
		}
	case "closed":
		all := lists.allDesc
		if direction == "asc" {
			all = lists.allAsc
		}
		base = make([]*Issue, 0, len(all))
		for _, iss := range all {
			if iss.State == "closed" {
				base = append(base, iss)
			}
		}
	default: // "open"
		if direction == "asc" {
			base = lists.openAsc
		} else {
			base = lists.openDesc
		}
	}

	if perPage <= 0 {
		perPage = 30
	}
	if perPage > 100 {
		perPage = 100
	}
	if page <= 0 {
		page = 1
	}
	start := (page - 1) * perPage
	if start >= len(base) {
		return []*Issue{}, false
	}
	end := start + perPage
	hasMore := end < len(base)
	if end > len(base) {
		end = len(base)
	}
	return base[start:end], hasMore
}

// AddComment appends a comment to an issue (or pull request) atomically.
func (r *Repository) AddComment(number int, body string) (*Comment, error) {
	if strings.TrimSpace(body) == "" {
		return nil, &DomainError{Kind: ErrUnprocessable, Message: "Comment body cannot be empty"}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	curMeta := r.issueMeta.Load()
	curPull := r.pullSnap.Load()
	if r.GetIssue(number) == nil && curPull.ByNumber[number] == nil {
		return nil, &DomainError{Kind: ErrNotFound, Message: "Not Found"}
	}

	cid := r.Forge.nextCommentID.Add(1)
	c := &Comment{
		ID:        cid,
		NodeID:    EncodeCommentNodeID(r.ID, number, cid),
		Body:      body,
		CreatedAt: r.Forge.FixedNow,
	}
	c.RestJSON = serializeCommentJSON(c, r.Owner)

	nextComments := make(map[int][]*Comment, len(curMeta.comments)+1)
	for k, v := range curMeta.comments {
		nextComments[k] = v
	}
	prevList := curMeta.comments[number]
	nextList := make([]*Comment, len(prevList)+1)
	copy(nextList, prevList)
	nextList[len(prevList)] = c
	nextComments[number] = nextList

	r.issueMeta.Store(&issueMetaSnap{
		labels:   curMeta.labels,
		comments: nextComments,
	})
	return c, nil
}

// ListComments returns the immutable comment slice for an issue or pull request.
func (r *Repository) ListComments(number int) ([]*Comment, error) {
	if r.GetIssue(number) == nil && r.pullSnap.Load().ByNumber[number] == nil {
		return nil, &DomainError{Kind: ErrNotFound, Message: "Not Found"}
	}
	list := r.issueMeta.Load().comments[number]
	if list == nil {
		return []*Comment{}, nil
	}
	return list, nil
}

func stripHeadsPrefix(ref string) string {
	return strings.TrimPrefix(strings.TrimSpace(ref), "refs/heads/")
}

// CreatePull validates head & base branches and publishes an open PullRequest.
func (r *Repository) CreatePull(title, body, head, base string) (*PullRequest, error) {
	title = strings.TrimSpace(title)
	head = stripHeadsPrefix(head)
	base = stripHeadsPrefix(base)
	if title == "" || head == "" || base == "" {
		return nil, &DomainError{Kind: ErrUnprocessable, Message: "title, head and base are required"}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	gitSnap := r.gitSnap.Load()
	headOID, okHead := gitSnap.LookupRef("refs/heads/" + head)
	baseOID, okBase := gitSnap.LookupRef("refs/heads/" + base)
	if !okHead || !okBase {
		return nil, &DomainError{Kind: ErrUnprocessable, Message: "Head or base branch does not exist"}
	}

	curPull := r.pullSnap.Load()
	for _, existing := range curPull.Ordered {
		if existing.State == "open" && existing.HeadRef == head && existing.BaseRef == base {
			return nil, &DomainError{Kind: ErrUnprocessable, Message: "A pull request already exists for " + head}
		}
	}

	r.nextNum++
	num := r.nextNum
	dbID := r.Forge.nextPullDBID.Add(1)
	pr := &PullRequest{
		ID:      dbID,
		NodeID:  EncodePullNodeID(r.ID, num),
		Number:  num,
		Title:   title,
		Body:    body,
		State:   "open",
		HeadRef: head,
		BaseRef: base,
		HeadSHA: headOID.Hex(),
		BaseSHA: baseOID.Hex(),
	}
	pr.RestJSON = serializePullJSON(pr, pr.HeadSHA, pr.BaseSHA, r.Owner)

	nextByNum := make(map[int]*PullRequest, len(curPull.ByNumber)+1)
	for k, v := range curPull.ByNumber {
		nextByNum[k] = v
	}
	nextByNum[num] = pr

	nextOrdered := make([]*PullRequest, len(curPull.Ordered)+1)
	copy(nextOrdered, curPull.Ordered)
	nextOrdered[len(curPull.Ordered)] = pr

	r.pullSnap.Store(&PullSnapshot{
		ByNumber: nextByNum,
		Ordered:  nextOrdered,
	})
	return pr, nil
}

// GetPull retrieves a PullRequest by number; for open PRs, reflects current head/base branch tips if advanced.
func (r *Repository) GetPull(number int) (*PullRequest, []byte, bool) {
	pr, ok := r.pullSnap.Load().ByNumber[number]
	if !ok {
		return nil, nil, false
	}
	if pr.State != "open" {
		return pr, pr.RestJSON, true
	}
	gitSnap := r.gitSnap.Load()
	headSHA := pr.HeadSHA
	if hOID, ok := gitSnap.LookupRef("refs/heads/" + pr.HeadRef); ok {
		headSHA = hOID.Hex()
	}
	baseSHA := pr.BaseSHA
	if bOID, ok := gitSnap.LookupRef("refs/heads/" + pr.BaseRef); ok {
		baseSHA = bOID.Hex()
	}
	if headSHA == pr.HeadSHA && baseSHA == pr.BaseSHA {
		return pr, pr.RestJSON, true
	}
	return pr, serializePullJSON(pr, headSHA, baseSHA, r.Owner), true
}

// ListPulls returns filtered pull requests ("open", "closed", or "all") in descending creation order.
func (r *Repository) ListPulls(state string, page, perPage int) []*PullRequest {
	state = strings.ToLower(strings.TrimSpace(state))
	if state == "" {
		state = "open"
	}
	ordered := r.pullSnap.Load().Ordered
	filtered := make([]*PullRequest, 0, len(ordered))
	for i := len(ordered) - 1; i >= 0; i-- {
		pr := ordered[i]
		if state == "all" || pr.State == state {
			filtered = append(filtered, pr)
		}
	}
	if perPage <= 0 {
		perPage = 30
	}
	if perPage > 100 {
		perPage = 100
	}
	if page <= 0 {
		page = 1
	}
	start := (page - 1) * perPage
	if start >= len(filtered) {
		return []*PullRequest{}
	}
	end := start + perPage
	if end > len(filtered) {
		end = len(filtered)
	}
	return filtered[start:end]
}

// GetPullFiles returns the changed files for a PullRequest.
func (r *Repository) GetPullFiles(number int) ([]PullFile, error) {
	pr, ok := r.pullSnap.Load().ByNumber[number]
	if !ok {
		return nil, &DomainError{Kind: ErrNotFound, Message: "Not Found"}
	}
	gitSnap := r.gitSnap.Load()
	var baseOID, headOID gitobj.OID
	if pr.State == "open" {
		if b, ok := gitSnap.LookupRef("refs/heads/" + pr.BaseRef); ok {
			baseOID = b
		} else {
			baseOID, _ = gitobj.ParseOID(pr.BaseSHA)
		}
		if h, ok := gitSnap.LookupRef("refs/heads/" + pr.HeadRef); ok {
			headOID = h
		} else {
			headOID, _ = gitobj.ParseOID(pr.HeadSHA)
		}
	} else {
		baseOID, _ = gitobj.ParseOID(pr.BaseSHA)
		headOID, _ = gitobj.ParseOID(pr.HeadSHA)
	}
	return r.DiffCommits(baseOID, headOID), nil
}

// IsPullMergeable checks 3-way mergeability without mutating state ("MERGEABLE" or "CONFLICTING").
func (r *Repository) PullMergeableState(pr *PullRequest) string {
	if pr.Merged {
		return "UNKNOWN"
	}
	gitSnap := r.gitSnap.Load()
	baseOID, okB := gitSnap.LookupRef("refs/heads/" + pr.BaseRef)
	headOID, okH := gitSnap.LookupRef("refs/heads/" + pr.HeadRef)
	if !okB || !okH {
		return "CONFLICTING"
	}
	if _, ok := r.merge3WayFiles(baseOID, headOID); !ok {
		return "CONFLICTING"
	}
	return "MERGEABLE"
}

// MergePull performs a 3-way conflict-checked squash merge onto the base branch,
// returning ErrNotMergeable (HTTP 405) if conflicting or already merged.
func (r *Repository) MergePull(number int, commitTitle string) (mergeSHA string, mergedPR *PullRequest, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	curPull := r.pullSnap.Load()
	pr, ok := curPull.ByNumber[number]
	if !ok {
		return "", nil, &DomainError{Kind: ErrNotFound, Message: "Not Found"}
	}
	if pr.Merged || pr.State != "open" {
		return "", nil, &DomainError{Kind: ErrNotMergeable, Message: "Pull Request is not mergeable"}
	}

	gitSnap := r.gitSnap.Load()
	baseRef := "refs/heads/" + pr.BaseRef
	headRef := "refs/heads/" + pr.HeadRef
	baseOID, okB := gitSnap.LookupRef(baseRef)
	headOID, okH := gitSnap.LookupRef(headRef)
	if !okB || !okH {
		return "", nil, &DomainError{Kind: ErrNotMergeable, Message: "Base or head branch no longer exists"}
	}

	mergedFiles, mergeOK := r.merge3WayFiles(baseOID, headOID)
	if !mergeOK {
		return "", nil, &DomainError{Kind: ErrNotMergeable, Message: "Pull Request is not mergeable"}
	}

	mergedTreeOID, treeObjs := r.buildTreeFromFlat(mergedFiles)
	msg := commitTitle
	if strings.TrimSpace(msg) == "" {
		msg = fmt.Sprintf("%s (#%d)", pr.Title, pr.Number)
	}
	commitBytes := gitobj.BuildCommit(mergedTreeOID, []gitobj.OID{baseOID}, r.Owner.Name, r.Owner.Email, r.Forge.FixedEpoch, msg)
	commitObj := gitobj.NewObject(gitobj.ObjCommit, commitBytes)

	allObjs := append(treeObjs, commitObj)
	if err := r.PutObjects(allObjs); err != nil {
		return "", nil, err
	}

	nextRefs := gitSnap.CopyRefs(0)
	nextRefs[baseRef] = commitObj.OID
	r.gitSnap.Store(r.buildGitSnapshot(gitSnap.Version+1, nextRefs))

	mergeHex := commitObj.OID.Hex()
	updatedPR := &PullRequest{
		ID:             pr.ID,
		NodeID:         pr.NodeID,
		Number:         pr.Number,
		Title:          pr.Title,
		Body:           pr.Body,
		State:          "closed",
		HeadRef:        pr.HeadRef,
		BaseRef:        pr.BaseRef,
		HeadSHA:        headOID.Hex(),
		BaseSHA:        baseOID.Hex(),
		Merged:         true,
		MergedAt:       r.Forge.FixedNow,
		MergeCommitSHA: mergeHex,
	}
	updatedPR.RestJSON = serializePullJSON(updatedPR, updatedPR.HeadSHA, updatedPR.BaseSHA, r.Owner)

	nextByNum := make(map[int]*PullRequest, len(curPull.ByNumber))
	for k, v := range curPull.ByNumber {
		nextByNum[k] = v
	}
	nextByNum[number] = updatedPR

	nextOrdered := make([]*PullRequest, len(curPull.Ordered))
	for i, item := range curPull.Ordered {
		if item.Number == number {
			nextOrdered[i] = updatedPR
		} else {
			nextOrdered[i] = item
		}
	}
	r.pullSnap.Store(&PullSnapshot{
		ByNumber: nextByNum,
		Ordered:  nextOrdered,
	})
	return mergeHex, updatedPR, nil
}
