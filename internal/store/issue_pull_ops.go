package store

import (
	"fmt"
	"strings"

	"gitforge/internal/gitobj"
)

// CreateLabel adds or updates a repository label and publishes a new IssueSnapshot.
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

	cur := r.issueSnap.Load()
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

	nextChunks := make([]*issueChunk, len(cur.chunks))
	copy(nextChunks, cur.chunks)

	r.issueSnap.Store(&IssueSnapshot{
		Version:  cur.Version + 1,
		MaxNum:   cur.MaxNum,
		Count:    cur.Count,
		chunks:   nextChunks,
		labels:   nextLabels,
		comments: cur.comments,
	})
	return lbl, nil
}

// CreateIssue validates labels, pre-serializes JSON, and publishes a new chunked IssueSnapshot in O(1).
func (r *Repository) CreateIssue(title, body string, labelNames []string) (*Issue, error) {
	title = strings.TrimSpace(title)
	if title == "" {
		return nil, &DomainError{Kind: ErrUnprocessable, Message: "Title is required"}
	}

	// Fast lock-free pre-check of labels when labelNames are supplied
	if len(labelNames) > 0 {
		snap := r.issueSnap.Load()
		for _, ln := range labelNames {
			if _, ok := snap.labels[ln]; !ok {
				return nil, &DomainError{Kind: ErrUnprocessable, Message: "Unknown label: " + ln}
			}
		}
	}

	dbID := r.Forge.nextIssueDBID.Add(1)
	now := r.Forge.FixedNow

	r.mu.Lock()
	cur := r.issueSnap.Load()

	var resolvedLabels []Label
	if len(labelNames) > 0 {
		resolvedLabels = make([]Label, 0, len(labelNames))
		for _, ln := range labelNames {
			lbl, ok := cur.labels[ln]
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
	iss.RestJSON = serializeIssueJSON(iss, r.Owner)

	cIdx := num >> 6
	neededLen := cIdx + 1
	if len(cur.chunks) > neededLen {
		neededLen = len(cur.chunks)
	}
	nextChunks := make([]*issueChunk, neededLen)
	copy(nextChunks, cur.chunks)

	var leaf issueChunk
	if cIdx < len(cur.chunks) && cur.chunks[cIdx] != nil {
		leaf = *cur.chunks[cIdx]
	}
	leaf[num&63] = iss
	nextChunks[cIdx] = &leaf

	r.issueSnap.Store(&IssueSnapshot{
		Version:  cur.Version + 1,
		MaxNum:   num,
		Count:    cur.Count + 1,
		chunks:   nextChunks,
		labels:   cur.labels,
		comments: cur.comments,
	})
	r.mu.Unlock()
	return iss, nil
}

// GetIssue performs a lock-free O(1) read from the current IssueSnapshot.
func (r *Repository) GetIssue(number int) *Issue {
	return r.issueSnap.Load().Get(number)
}

// IssuePatch describes fields updated via PATCH /issues/{n} or GraphQL closeIssue.
type IssuePatch struct {
	Title  *string
	Body   *string
	State  *string
	Labels *[]string
}

// UpdateIssue updates an existing issue and publishes a new IssueSnapshot invalidating cached views.
func (r *Repository) UpdateIssue(number int, patch IssuePatch) (*Issue, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	cur := r.issueSnap.Load()
	old := cur.Get(number)
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
		labels = make([]Label, 0, len(*patch.Labels))
		for _, ln := range *patch.Labels {
			lbl, ok := cur.labels[ln]
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
	updated.RestJSON = serializeIssueJSON(updated, r.Owner)

	cIdx := number >> 6
	nextChunks := make([]*issueChunk, len(cur.chunks))
	copy(nextChunks, cur.chunks)
	leaf := *cur.chunks[cIdx]
	leaf[number&63] = updated
	nextChunks[cIdx] = &leaf

	r.issueSnap.Store(&IssueSnapshot{
		Version:  cur.Version + 1,
		MaxNum:   cur.MaxNum,
		Count:    cur.Count,
		chunks:   nextChunks,
		labels:   cur.labels,
		comments: cur.comments,
	})
	return updated, nil
}

// ListIssues returns filtered, ordered, and paginated issues from the immutable IssueSnapshot.
func (r *Repository) ListIssues(state, direction string, page, perPage int) ([]*Issue, bool) {
	snap := r.issueSnap.Load()
	lists := snap.Lists()

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

// AddComment appends a comment to an issue (or pull request) and publishes a new IssueSnapshot.
func (r *Repository) AddComment(number int, body string) (*Comment, error) {
	if strings.TrimSpace(body) == "" {
		return nil, &DomainError{Kind: ErrUnprocessable, Message: "Comment body cannot be empty"}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	curIss := r.issueSnap.Load()
	curPull := r.pullSnap.Load()
	if curIss.Get(number) == nil && curPull.ByNumber[number] == nil {
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

	nextComments := make(map[int][]*Comment, len(curIss.comments)+1)
	for k, v := range curIss.comments {
		nextComments[k] = v
	}
	prevList := curIss.comments[number]
	nextList := make([]*Comment, len(prevList)+1)
	copy(nextList, prevList)
	nextList[len(prevList)] = c
	nextComments[number] = nextList

	r.issueSnap.Store(&IssueSnapshot{
		Version:  curIss.Version + 1,
		MaxNum:   curIss.MaxNum,
		Count:    curIss.Count,
		chunks:   curIss.chunks,
		labels:   curIss.labels,
		comments: nextComments,
	})
	return c, nil
}

// ListComments returns the immutable comment slice for an issue or pull request.
func (r *Repository) ListComments(number int) ([]*Comment, error) {
	curIss := r.issueSnap.Load()
	if curIss.Get(number) == nil && r.pullSnap.Load().ByNumber[number] == nil {
		return nil, &DomainError{Kind: ErrNotFound, Message: "Not Found"}
	}
	list := curIss.comments[number]
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
	headOID, okHead := gitSnap.Refs["refs/heads/"+head]
	baseOID, okBase := gitSnap.Refs["refs/heads/"+base]
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
	if hOID, ok := gitSnap.Refs["refs/heads/"+pr.HeadRef]; ok {
		headSHA = hOID.Hex()
	}
	baseSHA := pr.BaseSHA
	if bOID, ok := gitSnap.Refs["refs/heads/"+pr.BaseRef]; ok {
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
		if b, ok := gitSnap.Refs["refs/heads/"+pr.BaseRef]; ok {
			baseOID = b
		} else {
			baseOID, _ = gitobj.ParseOID(pr.BaseSHA)
		}
		if h, ok := gitSnap.Refs["refs/heads/"+pr.HeadRef]; ok {
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
	baseOID, okB := gitSnap.Refs["refs/heads/"+pr.BaseRef]
	headOID, okH := gitSnap.Refs["refs/heads/"+pr.HeadRef]
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
	baseOID, okB := gitSnap.Refs[baseRef]
	headOID, okH := gitSnap.Refs[headRef]
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

	nextRefs := make(map[string]gitobj.OID, len(gitSnap.Refs))
	for k, v := range gitSnap.Refs {
		nextRefs[k] = v
	}
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
