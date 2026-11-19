package store

import (
	"bytes"
	"encoding/base64"
	"path"
	"sort"
	"strings"

	"gitforge/internal/gitobj"
)

// UploadV2Capabilities is the pre-framed Smart HTTP v2 capability advertisement.
var UploadV2Capabilities = func() []byte {
	var b []byte
	b = gitobj.AppendPktString(b, "version 2\n")
	b = gitobj.AppendPktString(b, "agent=forge/1.0\n")
	b = gitobj.AppendPktString(b, "ls-refs=unborn\n")
	b = gitobj.AppendPktString(b, "fetch=shallow filter\n")
	b = gitobj.AppendPktString(b, "server-option\n")
	b = gitobj.AppendPktString(b, "object-format=sha1\n")
	b = append(b, "0000"...)
	return b
}()

// UploadV2HTTP200 is the pre-framed HTTP/1.1 200 OK response for v2 git-upload-pack info/refs.
var UploadV2HTTP200 = BuildHTTPResponse(
	"HTTP/1.1 200 OK",
	"application/x-git-upload-pack-advertisement",
	"Cache-Control: no-cache\r\n",
	UploadV2Capabilities,
)

func (r *Repository) shardFor(oid gitobj.OID) *objShard {
	return &r.shards[oid[0]&0x0f]
}

// GetObject retrieves an immutable Git object by OID with shard-level read locking.
func (r *Repository) GetObject(oid gitobj.OID) (*gitobj.Object, bool) {
	sh := r.shardFor(oid)
	sh.mu.RLock()
	obj, ok := sh.objs[oid]
	sh.mu.RUnlock()
	return obj, ok
}

// HasObject reports whether oid exists in the repository's object store.
func (r *Repository) HasObject(oid gitobj.OID) bool {
	_, ok := r.GetObject(oid)
	return ok
}

// GetCommit retrieves pre-parsed commit metadata, peeling annotated tags if needed.
func (r *Repository) GetCommit(oid gitobj.OID) (*gitobj.ParsedCommit, bool) {
	for hops := 0; hops < 8; hops++ {
		sh := r.shardFor(oid)
		sh.mu.RLock()
		pc, ok := sh.commits[oid]
		obj := sh.objs[oid]
		sh.mu.RUnlock()
		if ok {
			return pc, true
		}
		if obj == nil {
			return nil, false
		}
		if obj.Type == gitobj.ObjTag {
			target, _, valid := gitobj.ParseTag(obj.Raw)
			if !valid {
				return nil, false
			}
			oid = target
			continue
		}
		if obj.Type == gitobj.ObjCommit {
			parsed, err := gitobj.ParseCommit(oid, obj.Raw)
			if err != nil {
				return nil, false
			}
			sh.mu.Lock()
			sh.commits[oid] = parsed
			sh.mu.Unlock()
			return parsed, true
		}
		return nil, false
	}
	return nil, false
}

// PutObjects inserts immutable Git objects into the sharded store, parsing commits outside shard locks.
func (r *Repository) PutObjects(objs []*gitobj.Object) error {
	if len(objs) == 0 {
		return nil
	}
	type parsedPair struct {
		obj *gitobj.Object
		pc  *gitobj.ParsedCommit
	}
	// Stack-backed fast path for typical small pushes (e.g., 3 objects: blob + tree + commit)
	if len(objs) <= 8 {
		var stackPairs [8]parsedPair
		for i, obj := range objs {
			var pc *gitobj.ParsedCommit
			if obj.Type == gitobj.ObjCommit {
				var err error
				pc, err = gitobj.ParseCommit(obj.OID, obj.Raw)
				if err != nil {
					return err
				}
			}
			stackPairs[i] = parsedPair{obj: obj, pc: pc}
		}
		for i := 0; i < len(objs); i++ {
			item := stackPairs[i]
			sh := r.shardFor(item.obj.OID)
			sh.mu.Lock()
			if _, exists := sh.objs[item.obj.OID]; !exists {
				sh.objs[item.obj.OID] = item.obj
			}
			if item.pc != nil {
				sh.commits[item.obj.OID] = item.pc
			}
			sh.mu.Unlock()
		}
		return nil
	}

	var byShard [16][]parsedPair
	for _, obj := range objs {
		var pc *gitobj.ParsedCommit
		if obj.Type == gitobj.ObjCommit {
			var err error
			pc, err = gitobj.ParseCommit(obj.OID, obj.Raw)
			if err != nil {
				return err
			}
		}
		idx := obj.OID[0] & 0x0f
		byShard[idx] = append(byShard[idx], parsedPair{obj: obj, pc: pc})
	}
	for idx := range byShard {
		batch := byShard[idx]
		if len(batch) == 0 {
			continue
		}
		sh := &r.shards[idx]
		sh.mu.Lock()
		for _, item := range batch {
			if _, exists := sh.objs[item.obj.OID]; !exists {
				sh.objs[item.obj.OID] = item.obj
			}
			if item.pc != nil {
				sh.commits[item.obj.OID] = item.pc
			}
		}
		sh.mu.Unlock()
	}
	return nil
}

func (r *Repository) peelTag(oid gitobj.OID) (gitobj.OID, bool) {
	cur := oid
	peeled := false
	for hops := 0; hops < 8; hops++ {
		obj, ok := r.GetObject(cur)
		if !ok || obj.Type != gitobj.ObjTag {
			break
		}
		target, _, valid := gitobj.ParseTag(obj.Raw)
		if !valid {
			return gitobj.ZeroOID, false
		}
		cur = target
		peeled = true
	}
	if peeled && r.HasObject(cur) {
		return cur, true
	}
	return gitobj.ZeroOID, false
}

func (r *Repository) buildGitSnapshot(version uint64, refs map[string]gitobj.OID) *GitSnapshot {
	cpRefs := make(map[string]gitobj.OID, len(refs))
	sorted := make([]RefEntry, 0, len(refs))
	for k, v := range refs {
		if v.IsZero() {
			continue
		}
		cpRefs[k] = v
		ent := RefEntry{Name: k, OID: v}
		if strings.HasPrefix(k, "refs/tags/") {
			if pOID, ok := r.peelTag(v); ok {
				ent.HasPeeled = true
				ent.PeeledOID = pOID
			}
		}
		sorted = append(sorted, ent)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Name < sorted[j].Name
	})

	headTarget := "refs/heads/" + r.DefaultBranch
	headOID, hasHead := cpRefs[headTarget]

	return &GitSnapshot{
		Repo:       r,
		Version:    version,
		Refs:       cpRefs,
		SortedRefs: sorted,
		HeadTarget: headTarget,
		HeadOID:    headOID,
		HasHead:    hasHead,
	}
}

// UploadV0Advertisement lazily builds and caches the Protocol v0 upload-pack ref advertisement.
func (s *GitSnapshot) UploadV0Advertisement() []byte {
	s.upAdvOnce.Do(func() {
		uploadCaps := "multi_ack_detailed side-band-64k thin-pack ofs-delta shallow no-progress include-tag symref=HEAD:" + s.HeadTarget + " agent=forge/1.0 object-format=sha1"
		upAdv := make([]byte, 0, 256+len(s.SortedRefs)*64)
		upAdv = gitobj.AppendPktString(upAdv, "# service=git-upload-pack\n")
		upAdv = append(upAdv, "0000"...)
		if len(s.SortedRefs) == 0 {
			upAdv = gitobj.AppendPktString(upAdv, gitobj.ZeroHex+" capabilities^{}\x00"+uploadCaps+"\n")
		} else {
			var lineBuf [256]byte
			wroteCaps := false
			if s.HasHead {
				b := s.HeadOID.AppendHex(lineBuf[:0])
				b = append(b, " HEAD\x00"...)
				b = append(b, uploadCaps...)
				b = append(b, '\n')
				upAdv = gitobj.AppendPktLine(upAdv, b)
				wroteCaps = true
			}
			for _, ent := range s.SortedRefs {
				b := ent.OID.AppendHex(lineBuf[:0])
				b = append(b, ' ')
				b = append(b, ent.Name...)
				if !wroteCaps {
					b = append(b, 0)
					b = append(b, uploadCaps...)
					wroteCaps = true
				}
				b = append(b, '\n')
				upAdv = gitobj.AppendPktLine(upAdv, b)
				if ent.HasPeeled {
					pb := ent.PeeledOID.AppendHex(lineBuf[:0])
					pb = append(pb, ' ')
					pb = append(pb, ent.Name...)
					pb = append(pb, '^', '{', '}', '\n')
					upAdv = gitobj.AppendPktLine(upAdv, pb)
				}
			}
		}
		upAdv = append(upAdv, "0000"...)
		s.advUploadV0 = upAdv
	})
	return s.advUploadV0
}

// ReceiveV0Advertisement lazily builds and caches the Protocol v0 receive-pack ref advertisement.
func (s *GitSnapshot) ReceiveV0Advertisement() []byte {
	s.recvAdvOnce.Do(func() {
		const recvCaps = "report-status report-status-v2 delete-refs side-band-64k quiet atomic ofs-delta agent=forge/1.0 object-format=sha1"
		recvAdv := make([]byte, 0, 256+len(s.SortedRefs)*64)
		recvAdv = gitobj.AppendPktString(recvAdv, "# service=git-receive-pack\n")
		recvAdv = append(recvAdv, "0000"...)
		if len(s.SortedRefs) == 0 {
			recvAdv = gitobj.AppendPktString(recvAdv, gitobj.ZeroHex+" capabilities^{}\x00"+recvCaps+"\n")
		} else {
			var lineBuf [256]byte
			for i, ent := range s.SortedRefs {
				b := ent.OID.AppendHex(lineBuf[:0])
				b = append(b, ' ')
				b = append(b, ent.Name...)
				if i == 0 {
					b = append(b, 0)
					b = append(b, recvCaps...)
				}
				b = append(b, '\n')
				recvAdv = gitobj.AppendPktLine(recvAdv, b)
			}
		}
		recvAdv = append(recvAdv, "0000"...)
		s.advReceiveV0 = recvAdv
	})
	return s.advReceiveV0
}

// ReceiveV0HTTP200 returns the cached complete HTTP/1.1 200 OK frame for git-receive-pack info/refs.
func (s *GitSnapshot) ReceiveV0HTTP200() []byte {
	s.recvAdvHTTPOnce.Do(func() {
		s.advReceiveHTTP = BuildHTTPResponse(
			"HTTP/1.1 200 OK",
			"application/x-git-receive-pack-advertisement",
			"Cache-Control: no-cache\r\n",
			s.ReceiveV0Advertisement(),
		)
	})
	return s.advReceiveHTTP
}

// LsRefsHTTP200 returns the cached complete HTTP/1.1 200 OK frame for default v2 ls-refs.
func (s *GitSnapshot) LsRefsHTTP200() []byte {
	s.lsRefsHTTPOnce.Do(func() {
		body := s.Repo.formatLsRefs(s, LsRefsArgs{Peel: true, Symrefs: true, Unborn: true})
		s.lsRefsHTTP200 = BuildHTTPResponse(
			"HTTP/1.1 200 OK",
			"application/x-git-upload-pack-result",
			"Cache-Control: no-cache\r\n",
			body,
		)
	})
	return s.lsRefsHTTP200
}

// Depth1FetchHTTP200 returns the cached complete HTTP/1.1 200 OK frame for depth-1 shallow clone of HEAD.
func (s *GitSnapshot) Depth1FetchHTTP200() ([]byte, bool) {
	if !s.HasHead {
		return nil, false
	}
	s.depth1HTTPOnce.Do(func() {
		body, err := s.Repo.FetchUploadPack(FetchArgs{
			Protocol: 2,
			Wants:    []gitobj.OID{s.HeadOID},
			Depth:    1,
			Done:     true,
			Sideband: true,
		})
		if err == nil && len(body) > 0 {
			s.depth1HTTP200 = BuildHTTPResponse(
				"HTTP/1.1 200 OK",
				"application/x-git-upload-pack-result",
				"Cache-Control: no-cache\r\n",
				body,
			)
		}
	})
	return s.depth1HTTP200, len(s.depth1HTTP200) > 0
}

// Git returns the current lock-free GitSnapshot.
func (r *Repository) Git() *GitSnapshot {
	return r.gitSnap.Load()
}

// IsAncestor reports whether ancestorOID is reachable from descendantOID via parent links.
// Includes a zero-allocation linear fast path for single-parent chains up to 16 hops before allocating a BFS map.
func (r *Repository) IsAncestor(ancestorOID, descendantOID gitobj.OID) bool {
	if ancestorOID == descendantOID {
		return true
	}
	cur := descendantOID
	for hop := 0; hop < 16; hop++ {
		pc, ok := r.GetCommit(cur)
		if !ok || len(pc.Parents) == 0 {
			return false
		}
		for _, p := range pc.Parents {
			if p == ancestorOID {
				return true
			}
		}
		if len(pc.Parents) != 1 {
			break
		}
		cur = pc.Parents[0]
	}

	visited := make(map[gitobj.OID]struct{}, 32)
	queue := []gitobj.OID{cur}
	visited[cur] = struct{}{}
	for len(queue) > 0 {
		c := queue[0]
		queue = queue[1:]
		pc, ok := r.GetCommit(c)
		if !ok {
			return false
		}
		for _, p := range pc.Parents {
			if p == ancestorOID {
				return true
			}
			if _, seen := visited[p]; !seen {
				visited[p] = struct{}{}
				queue = append(queue, p)
			}
		}
	}
	return false
}

// verifyObjectClosure checks that rootOID and its newly introduced tree/commit graph exist in the store,
// stopping recursion at any object already referenced by knownCommits.
func (r *Repository) verifyObjectClosure(rootOID gitobj.OID, knownCommits map[gitobj.OID]struct{}) bool {
	obj, ok := r.GetObject(rootOID)
	if !ok {
		return false
	}

	// Zero-allocation fast path for the common single-commit push whose parents are already known
	if obj.Type == gitobj.ObjCommit {
		pc, ok := r.GetCommit(rootOID)
		if !ok {
			return false
		}
		allParentsKnown := true
		for _, p := range pc.Parents {
			if _, known := knownCommits[p]; !known {
				allParentsKnown = false
				break
			}
		}
		if allParentsKnown {
			return r.verifyTreeSimple(pc.Tree)
		}
	}

	seenTrees := make(map[gitobj.OID]struct{}, 16)
	var checkTree func(tOID gitobj.OID) bool
	checkTree = func(tOID gitobj.OID) bool {
		if _, ok := seenTrees[tOID]; ok {
			return true
		}
		tObj, ok := r.GetObject(tOID)
		if !ok || tObj.Type != gitobj.ObjTree {
			return false
		}
		entries, err := gitobj.ParseTree(tObj.Raw)
		if err != nil {
			return false
		}
		for _, e := range entries {
			if e.IsDir() {
				if !checkTree(e.OID) {
					return false
				}
			} else if !r.verifyTreeLeaf(e.Mode, e.OID) {
				return false
			}
		}
		seenTrees[tOID] = struct{}{}
		return true
	}

	switch obj.Type {
	case gitobj.ObjBlob:
		return true
	case gitobj.ObjTree:
		return checkTree(rootOID)
	case gitobj.ObjTag:
		target, targetType, valid := gitobj.ParseTag(obj.Raw)
		if !valid {
			return false
		}
		if targetObj, ok := r.GetObject(target); !ok || targetObj.Type != targetType {
			return false
		}
		return r.verifyObjectClosure(target, knownCommits)
	case gitobj.ObjCommit:
		visited := make(map[gitobj.OID]struct{}, 16)
		stack := []gitobj.OID{rootOID}
		for len(stack) > 0 {
			cOID := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if _, known := knownCommits[cOID]; known {
				continue
			}
			if _, v := visited[cOID]; v {
				continue
			}
			visited[cOID] = struct{}{}
			pc, ok := r.GetCommit(cOID)
			if !ok || pc.OID != cOID {
				return false
			}
			if !checkTree(pc.Tree) {
				return false
			}
			for _, p := range pc.Parents {
				if _, known := knownCommits[p]; !known {
					stack = append(stack, p)
				}
			}
		}
		return true
	default:
		return false
	}
}

func (r *Repository) verifyTreeLeaf(mode string, oid gitobj.OID) bool {
	typ := gitobj.ObjBlob
	if mode == "160000" {
		typ = gitobj.ObjCommit
	}
	obj, ok := r.GetObject(oid)
	return ok && obj.Type == typ
}

func (r *Repository) verifyTreeSimple(tOID gitobj.OID) bool {
	tObj, ok := r.GetObject(tOID)
	if !ok || tObj.Type != gitobj.ObjTree {
		return false
	}
	raw := tObj.Raw
	i := 0
	for i < len(raw) {
		sp := bytes.IndexByte(raw[i:], ' ')
		if sp <= 0 {
			return false
		}
		mode := string(raw[i : i+sp])
		isDir := mode == "40000" || mode == "040000"
		i += sp + 1
		nul := bytes.IndexByte(raw[i:], 0)
		if nul <= 0 {
			return false
		}
		i += nul + 1
		if i+20 > len(raw) {
			return false
		}
		var childOID gitobj.OID
		copy(childOID[:], raw[i:i+20])
		i += 20
		if isDir {
			if !r.verifyTreeSimple(childOID) {
				return false
			}
		} else if !r.verifyTreeLeaf(mode, childOID) {
			return false
		}
	}
	return true
}

// PushCommand describes one ref update inside a receive-pack push.
type PushCommand struct {
	OldOID gitobj.OID
	NewOID gitobj.OID
	Ref    string
}

// RefStatus records the outcome of one PushCommand.
type RefStatus struct {
	Ref    string
	OldOID gitobj.OID
	NewOID gitobj.OID
	OK     bool
	Reason string
}

func isValidGitRefName(ref string) bool {
	if !strings.HasPrefix(ref, "refs/") || len(ref) <= len("refs/") {
		return false
	}
	if strings.HasSuffix(ref, "/") || strings.HasSuffix(ref, ".lock") || strings.Contains(ref, "..") || strings.Contains(ref, "//") {
		return false
	}
	for i := 0; i < len(ref); i++ {
		c := ref[i]
		if c <= 0x20 || c == 0x7f || c == '~' || c == '^' || c == ':' || c == '?' || c == '*' || c == '[' || c == '\\' {
			return false
		}
	}
	return true
}

// ReceivePack unpacks incoming objects OUTSIDE the repository write lock, then acquires
// the short per-repo lock to validate CAS + fast-forward semantics and publish the new GitSnapshot.
func (r *Repository) ReceivePack(cmds []PushCommand, packBytes []byte, atomicPush bool) (unpackErr string, statuses []RefStatus) {
	// Phase 1 (outside repo.mu): unpack, verify SHA-1 checksum, resolve deltas, and store immutable objects.
	hasUpdates := false
	for _, c := range cmds {
		if !c.NewOID.IsZero() {
			hasUpdates = true
			break
		}
	}
	if len(packBytes) > 0 || hasUpdates {
		if len(packBytes) == 0 && hasUpdates {
			return "missing packfile", nil
		}
		if len(packBytes) > 0 {
			objs, err := gitobj.Unpack(packBytes, r.GetObject)
			if err != nil {
				return err.Error(), nil
			}
			if err := r.PutObjects(objs); err != nil {
				return err.Error(), nil
			}
		}
	}

	// Phase 2 (short critical section): validate CAS + ancestry and publish new GitSnapshot.
	r.mu.Lock()
	defer r.mu.Unlock()

	curSnap := r.gitSnap.Load()
	workingRefs := make(map[string]gitobj.OID, len(curSnap.Refs)+len(cmds))
	knownTips := make(map[gitobj.OID]struct{}, len(curSnap.Refs))
	for k, v := range curSnap.Refs {
		workingRefs[k] = v
		if obj, ok := r.GetObject(v); ok && obj.Type == gitobj.ObjCommit {
			knownTips[v] = struct{}{}
		}
	}

	statuses = make([]RefStatus, len(cmds))
	anyFailed := false
	anyChanged := false

	for i, cmd := range cmds {
		st := RefStatus{
			Ref:    cmd.Ref,
			OldOID: cmd.OldOID,
			NewOID: cmd.NewOID,
		}
		if !isValidGitRefName(cmd.Ref) {
			st.Reason = "invalid refname"
			statuses[i] = st
			anyFailed = true
			continue
		}

		curOID, exists := workingRefs[cmd.Ref]
		if !exists {
			curOID = gitobj.ZeroOID
		}

		switch {
		case cmd.NewOID.IsZero():
			// Delete ref
			if !exists || curOID != cmd.OldOID {
				st.Reason = "lock failed"
				anyFailed = true
			} else {
				delete(workingRefs, cmd.Ref)
				st.OK = true
				anyChanged = true
			}

		case cmd.OldOID.IsZero():
			// Create ref
			if exists {
				st.Reason = "reference already exists"
				anyFailed = true
			} else if !r.verifyRefTarget(cmd.Ref, cmd.NewOID, knownTips) {
				st.Reason = "missing necessary objects"
				anyFailed = true
			} else {
				workingRefs[cmd.Ref] = cmd.NewOID
				st.OK = true
				anyChanged = true
			}

		default:
			// Update existing ref: CAS + fast-forward check
			if !exists || curOID != cmd.OldOID {
				st.Reason = "non-fast-forward"
				anyFailed = true
			} else if !r.verifyRefTarget(cmd.Ref, cmd.NewOID, knownTips) {
				st.Reason = "missing necessary objects"
				anyFailed = true
			} else if strings.HasPrefix(cmd.Ref, "refs/heads/") && !r.IsAncestor(cmd.OldOID, cmd.NewOID) {
				st.Reason = "non-fast-forward"
				anyFailed = true
			} else if strings.HasPrefix(cmd.Ref, "refs/tags/") && cmd.OldOID != cmd.NewOID {
				// Git rejects moving an existing tag without force
				st.Reason = "already exists"
				anyFailed = true
			} else {
				workingRefs[cmd.Ref] = cmd.NewOID
				st.OK = true
				if cmd.OldOID != cmd.NewOID {
					anyChanged = true
				}
			}
		}
		if st.OK && !cmd.NewOID.IsZero() {
			if obj, ok := r.GetObject(cmd.NewOID); ok && obj.Type == gitobj.ObjCommit {
				knownTips[cmd.NewOID] = struct{}{}
			}
		}
		statuses[i] = st
	}

	if atomicPush && anyFailed {
		for i := range statuses {
			if statuses[i].OK {
				statuses[i].OK = false
				statuses[i].Reason = "atomic push failed"
			}
		}
		return "", statuses
	}

	if anyChanged {
		nextSnap := r.buildGitSnapshot(curSnap.Version+1, workingRefs)
		r.gitSnap.Store(nextSnap)
	}
	return "", statuses
}

func (r *Repository) verifyRefTarget(ref string, oid gitobj.OID, knownTips map[gitobj.OID]struct{}) bool {
	obj, ok := r.GetObject(oid)
	if !ok {
		return false
	}
	if strings.HasPrefix(ref, "refs/heads/") && obj.Type != gitobj.ObjCommit {
		return false
	}
	return r.verifyObjectClosure(oid, knownTips)
}

// LsRefsArgs holds parsed Protocol v2 ls-refs arguments.
type LsRefsArgs struct {
	Peel        bool
	Symrefs     bool
	Unborn      bool
	RefPrefixes []string
}

func matchesRefPrefixes(name string, prefixes []string) bool {
	if len(prefixes) == 0 {
		return true
	}
	for _, p := range prefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// LsRefs formats a Protocol v2 ls-refs response from the current immutable GitSnapshot without string concatenation allocs.
func (r *Repository) LsRefs(args LsRefsArgs) []byte {
	snap := r.gitSnap.Load()
	if args.Peel && args.Symrefs && len(args.RefPrefixes) == 0 && (snap.HasHead || !args.Unborn) {
		snap.lsRefsSymPeelOnce.Do(func() {
			snap.lsRefsSymPeel = r.formatLsRefs(snap, args)
		})
		return snap.lsRefsSymPeel
	}
	return r.formatLsRefs(snap, args)
}

func (r *Repository) formatLsRefs(snap *GitSnapshot, args LsRefsArgs) []byte {
	out := make([]byte, 0, 128+len(snap.SortedRefs)*64)
	var lineBuf [256]byte

	if matchesRefPrefixes("HEAD", args.RefPrefixes) {
		if snap.HasHead {
			b := snap.HeadOID.AppendHex(lineBuf[:0])
			b = append(b, " HEAD"...)
			if args.Symrefs {
				b = append(b, " symref-target:"...)
				b = append(b, snap.HeadTarget...)
			}
			b = append(b, '\n')
			out = gitobj.AppendPktLine(out, b)
		} else if args.Unborn {
			b := append(lineBuf[:0], "unborn HEAD"...)
			if args.Symrefs {
				b = append(b, " symref-target:"...)
				b = append(b, snap.HeadTarget...)
			}
			b = append(b, '\n')
			out = gitobj.AppendPktLine(out, b)
		}
	}

	for _, ent := range snap.SortedRefs {
		if !matchesRefPrefixes(ent.Name, args.RefPrefixes) {
			continue
		}
		b := ent.OID.AppendHex(lineBuf[:0])
		b = append(b, ' ')
		b = append(b, ent.Name...)
		if args.Peel && ent.HasPeeled {
			b = append(b, " peeled:"...)
			b = ent.PeeledOID.AppendHex(b)
		}
		b = append(b, '\n')
		out = gitobj.AppendPktLine(out, b)
	}
	out = append(out, "0000"...)
	return out
}

// FetchArgs holds parsed upload-pack fetch arguments for Protocol v2 or v0.
type FetchArgs struct {
	Protocol      int // 0 or 2
	Wants         []gitobj.OID
	Haves         []gitobj.OID
	ClientShallow []gitobj.OID
	Depth         int // 0 means full history, >= 1 means shallow depth
	Done          bool
	Sideband      bool
	IncludeTag    bool
	Negotiate     bool
}

// FetchUploadPack executes a lock-free Git upload-pack fetch against the current GitSnapshot.
// For depth-1 fresh shallow clones of HEAD, it reuses the snapshot-scoped pre-framed response.
func (r *Repository) FetchUploadPack(args FetchArgs) ([]byte, error) {
	snap := r.gitSnap.Load()

	// Validate all wants exist
	for _, w := range args.Wants {
		if !r.HasObject(w) {
			return nil, &DomainError{Kind: ErrBadRequest, Message: "not our ref " + w.Hex()}
		}
	}

	// Multi-round negotiation probe when client has not sent "done" yet
	if args.Protocol == 2 && !args.Done {
		var out []byte
		out = gitobj.AppendPktString(out, "acknowledgments\n")
		foundCommon := false
		for _, h := range args.Haves {
			if r.HasObject(h) {
				out = gitobj.AppendPktString(out, "ACK "+h.Hex()+" common\n")
				foundCommon = true
			}
		}
		if !foundCommon {
			out = gitobj.AppendPktString(out, "NAK\n")
		}
		out = append(out, "0000"...)
		return out, nil
	}

	// Snapshot-versioned fast path for concurrent depth-1 shallow clones of HEAD
	if args.Protocol == 2 && args.Done && args.Depth == 1 &&
		snap.HasHead && len(args.Wants) == 1 && args.Wants[0] == snap.HeadOID &&
		len(args.Haves) == 0 && len(args.ClientShallow) == 0 {
		if args.IncludeTag {
			for _, ref := range snap.SortedRefs {
				if strings.HasPrefix(ref.Name, "refs/tags/") && ref.HasPeeled {
					return r.buildFetchFramed(args, snap)
				}
			}
		}
		snap.depth1Once.Do(func() {
			framed, err := r.buildFetchFramed(args, snap)
			if err == nil {
				snap.depth1Framed = framed
			}
		})
		if len(snap.depth1Framed) > 0 {
			return snap.depth1Framed, nil
		}
	}

	return r.buildFetchFramed(args, snap)
}

func (r *Repository) buildFetchFramed(args FetchArgs, snap *GitSnapshot) ([]byte, error) {
	clientShallowSet := make(map[gitobj.OID]struct{}, len(args.ClientShallow))
	for _, s := range args.ClientShallow {
		clientShallowSet[s] = struct{}{}
	}

	// Step 1: Unwrap wanted tags and determine the wanted commit window + shallow boundary
	var extraObjs []*gitobj.Object
	seenObj := make(map[gitobj.OID]struct{}, 64)

	var wantCommits []gitobj.OID
	for _, w := range args.Wants {
		cur := w
		for hops := 0; hops < 8; hops++ {
			obj, ok := r.GetObject(cur)
			if !ok {
				break
			}
			if obj.Type == gitobj.ObjTag {
				if _, s := seenObj[cur]; !s {
					seenObj[cur] = struct{}{}
					extraObjs = append(extraObjs, obj)
				}
				target, _, valid := gitobj.ParseTag(obj.Raw)
				if !valid {
					break
				}
				cur = target
				continue
			}
			if obj.Type == gitobj.ObjCommit {
				wantCommits = append(wantCommits, cur)
			} else {
				if _, s := seenObj[cur]; !s {
					seenObj[cur] = struct{}{}
					extraObjs = append(extraObjs, obj)
				}
			}
			break
		}
	}

	includedCommits := make(map[gitobj.OID]int, 64) // commit -> min depth (1-based)
	var orderedCommits []gitobj.OID
	newShallowBoundary := make(map[gitobj.OID]struct{})

	if args.Depth > 0 {
		type bfsItem struct {
			oid   gitobj.OID
			depth int
		}
		queue := make([]bfsItem, 0, len(wantCommits)*2)
		for _, wc := range wantCommits {
			if _, seen := includedCommits[wc]; !seen {
				includedCommits[wc] = 1
				orderedCommits = append(orderedCommits, wc)
				queue = append(queue, bfsItem{oid: wc, depth: 1})
			}
		}
		for len(queue) > 0 {
			item := queue[0]
			queue = queue[1:]
			pc, ok := r.GetCommit(item.oid)
			if !ok {
				continue
			}
			if item.depth >= args.Depth {
				if len(pc.Parents) > 0 {
					newShallowBoundary[item.oid] = struct{}{}
				}
				continue
			}
			for _, p := range pc.Parents {
				if prevDepth, seen := includedCommits[p]; !seen || item.depth+1 < prevDepth {
					if !seen {
						orderedCommits = append(orderedCommits, p)
					}
					includedCommits[p] = item.depth + 1
					delete(newShallowBoundary, p)
					queue = append(queue, bfsItem{oid: p, depth: item.depth + 1})
				}
			}
		}
	} else {
		// Full reachability from wantCommits, pruning early at non-shallow client haves
		nonShallowHaves := make(map[gitobj.OID]struct{}, len(args.Haves))
		for _, h := range args.Haves {
			if _, isSh := clientShallowSet[h]; !isSh && r.HasObject(h) {
				nonShallowHaves[h] = struct{}{}
			}
		}
		stack := append([]gitobj.OID(nil), wantCommits...)
		for len(stack) > 0 {
			cOID := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if _, stop := nonShallowHaves[cOID]; stop {
				continue
			}
			if _, seen := includedCommits[cOID]; seen {
				continue
			}
			includedCommits[cOID] = 1
			orderedCommits = append(orderedCommits, cOID)
			if pc, ok := r.GetCommit(cOID); ok {
				for _, p := range pc.Parents {
					if _, stop := nonShallowHaves[p]; !stop {
						if _, seen := includedCommits[p]; !seen {
							stack = append(stack, p)
						}
					}
				}
			}
		}
	}

	// Step 2: Compute shallow / unshallow transitions
	var sendShallow []gitobj.OID
	var sendUnshallow []gitobj.OID
	if args.Depth > 0 {
		for _, cOID := range orderedCommits {
			if _, isBoundary := newShallowBoundary[cOID]; isBoundary {
				if _, wasShallow := clientShallowSet[cOID]; !wasShallow {
					sendShallow = append(sendShallow, cOID)
				}
			}
		}
		for _, cs := range args.ClientShallow {
			if _, included := includedCommits[cs]; included {
				if _, stillShallow := newShallowBoundary[cs]; !stillShallow {
					sendUnshallow = append(sendUnshallow, cs)
				}
			}
		}
	}

	// Step 3: Compute client's already-held objects from haves (respecting clientShallow!)
	haveObjs := make(map[gitobj.OID]struct{})
	var commonAcks []gitobj.OID
	if len(args.Haves) > 0 {
		var markTree func(tOID gitobj.OID)
		markTree = func(tOID gitobj.OID) {
			if _, ok := haveObjs[tOID]; ok {
				return
			}
			tObj, ok := r.GetObject(tOID)
			if !ok || tObj.Type != gitobj.ObjTree {
				return
			}
			haveObjs[tOID] = struct{}{}
			entries, err := gitobj.ParseTree(tObj.Raw)
			if err != nil {
				return
			}
			for _, e := range entries {
				if e.IsDir() {
					markTree(e.OID)
				} else {
					haveObjs[e.OID] = struct{}{}
				}
			}
		}

		haveQueue := make([]gitobj.OID, 0, len(args.Haves))
		visitedHaveCommits := make(map[gitobj.OID]struct{}, len(args.Haves))
		for _, h := range args.Haves {
			if !r.HasObject(h) {
				continue
			}
			commonAcks = append(commonAcks, h)
			haveObjs[h] = struct{}{}
			if _, ok := r.GetCommit(h); ok {
				if _, v := visitedHaveCommits[h]; !v {
					visitedHaveCommits[h] = struct{}{}
					haveQueue = append(haveQueue, h)
				}
			}
		}
		for len(haveQueue) > 0 {
			cOID := haveQueue[0]
			haveQueue = haveQueue[1:]
			haveObjs[cOID] = struct{}{}
			pc, ok := r.GetCommit(cOID)
			if !ok {
				continue
			}
			markTree(pc.Tree)
			// Do NOT walk ancestors past a commit that the client declared shallow!
			if _, isClientShallow := clientShallowSet[cOID]; isClientShallow {
				continue
			}
			for _, p := range pc.Parents {
				if _, v := visitedHaveCommits[p]; !v {
					visitedHaveCommits[p] = struct{}{}
					haveQueue = append(haveQueue, p)
				}
			}
		}
	}

	if args.Protocol == 0 && !args.Done {
		var out []byte
		for _, s := range sendShallow {
			out = gitobj.AppendPktString(out, "shallow "+s.Hex()+"\n")
		}
		for _, u := range sendUnshallow {
			out = gitobj.AppendPktString(out, "unshallow "+u.Hex()+"\n")
		}
		if args.Depth > 0 || len(sendShallow) > 0 || len(sendUnshallow) > 0 {
			out = append(out, "0000"...)
		}
		if args.Negotiate {
			if len(commonAcks) > 0 {
				out = gitobj.AppendPktString(out, "ACK "+commonAcks[len(commonAcks)-1].Hex()+"\n")
			} else {
				out = gitobj.AppendPktString(out, "NAK\n")
			}
		}
		return out, nil
	}

	// Step 4: Collect missing objects in clean commit -> tree -> blob order
	var toSend []*gitobj.Object
	for _, obj := range extraObjs {
		if _, held := haveObjs[obj.OID]; !held {
			toSend = append(toSend, obj)
		}
	}

	var collectTree func(tOID gitobj.OID)
	collectTree = func(tOID gitobj.OID) {
		if _, held := haveObjs[tOID]; held {
			return
		}
		if _, seen := seenObj[tOID]; seen {
			return
		}
		tObj, ok := r.GetObject(tOID)
		if !ok || tObj.Type != gitobj.ObjTree {
			return
		}
		seenObj[tOID] = struct{}{}
		toSend = append(toSend, tObj)
		entries, err := gitobj.ParseTree(tObj.Raw)
		if err != nil {
			return
		}
		for _, e := range entries {
			if e.IsDir() {
				collectTree(e.OID)
			} else {
				if _, held := haveObjs[e.OID]; held {
					continue
				}
				if _, seen := seenObj[e.OID]; seen {
					continue
				}
				if bObj, ok := r.GetObject(e.OID); ok {
					seenObj[e.OID] = struct{}{}
					toSend = append(toSend, bObj)
				}
			}
		}
	}

	for _, cOID := range orderedCommits {
		if _, held := haveObjs[cOID]; held {
			continue
		}
		if _, seen := seenObj[cOID]; !seen {
			if cObj, ok := r.GetObject(cOID); ok {
				seenObj[cOID] = struct{}{}
				toSend = append(toSend, cObj)
			}
		}
		if pc, ok := r.GetCommit(cOID); ok {
			collectTree(pc.Tree)
		}
	}

	if args.IncludeTag {
		packed := make(map[gitobj.OID]struct{}, len(toSend))
		for _, obj := range toSend {
			packed[obj.OID] = struct{}{}
		}
		for _, ref := range snap.SortedRefs {
			if !strings.HasPrefix(ref.Name, "refs/tags/") || !ref.HasPeeled {
				continue
			}
			if _, ok := packed[ref.PeeledOID]; !ok {
				continue
			}
			for oid, hops := ref.OID, 0; hops < 8; hops++ {
				obj, ok := r.GetObject(oid)
				if !ok || obj.Type != gitobj.ObjTag {
					break
				}
				if _, held := haveObjs[oid]; !held {
					if _, sent := packed[oid]; !sent {
						toSend = append(toSend, obj)
						packed[oid] = struct{}{}
					}
				}
				target, _, ok := gitobj.ParseTag(obj.Raw)
				if !ok {
					break
				}
				oid = target
			}
		}
	}
	pack := gitobj.BuildPack(toSend)

	// Step 5: Frame response according to Protocol v2 or v0.
	// Per gitprotocol-v2(5), when the client sent "done", the acknowledgments section MUST be omitted.
	out := make([]byte, 0, len(pack)+256)
	if args.Protocol == 2 {
		if len(sendShallow) > 0 || len(sendUnshallow) > 0 {
			out = gitobj.AppendPktString(out, "shallow-info\n")
			for _, s := range sendShallow {
				out = gitobj.AppendPktString(out, "shallow "+s.Hex()+"\n")
			}
			for _, u := range sendUnshallow {
				out = gitobj.AppendPktString(out, "unshallow "+u.Hex()+"\n")
			}
			out = append(out, "0001"...)
		}
		out = gitobj.AppendPktString(out, "packfile\n")
		out = gitobj.AppendSidebandData(out, 1, pack)
		out = append(out, "0000"...)
		return out, nil
	}

	// Protocol v0 framing
	for _, s := range sendShallow {
		out = gitobj.AppendPktString(out, "shallow "+s.Hex()+"\n")
	}
	for _, u := range sendUnshallow {
		out = gitobj.AppendPktString(out, "unshallow "+u.Hex()+"\n")
	}
	if args.Depth > 0 || len(sendShallow) > 0 || len(sendUnshallow) > 0 {
		out = append(out, "0000"...)
	}
	if len(commonAcks) > 0 {
		out = gitobj.AppendPktString(out, "ACK "+commonAcks[len(commonAcks)-1].Hex()+"\n")
	} else {
		out = gitobj.AppendPktString(out, "NAK\n")
	}
	if args.Sideband {
		out = gitobj.AppendSidebandData(out, 1, pack)
		out = append(out, "0000"...)
	} else {
		out = append(out, pack...)
	}
	return out, nil
}

// CreateRef creates a new Git ref via REST POST /git/refs.
func (r *Repository) CreateRef(ref string, sha string) (gitobj.OID, error) {
	oid, ok := gitobj.ParseOID(strings.TrimSpace(sha))
	if !ok || oid.IsZero() {
		return gitobj.ZeroOID, &DomainError{Kind: ErrUnprocessable, Message: "Object SHA is invalid"}
	}
	if !isValidGitRefName(ref) {
		return gitobj.ZeroOID, &DomainError{Kind: ErrUnprocessable, Message: "Reference name is invalid"}
	}
	if !r.verifyRefTarget(ref, oid, nil) {
		return gitobj.ZeroOID, &DomainError{Kind: ErrUnprocessable, Message: "Invalid reference target or missing necessary objects"}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	curSnap := r.gitSnap.Load()
	if _, exists := curSnap.Refs[ref]; exists {
		return gitobj.ZeroOID, &DomainError{Kind: ErrUnprocessable, Message: "Reference already exists"}
	}
	nextRefs := make(map[string]gitobj.OID, len(curSnap.Refs)+1)
	for k, v := range curSnap.Refs {
		nextRefs[k] = v
	}
	nextRefs[ref] = oid
	r.gitSnap.Store(r.buildGitSnapshot(curSnap.Version+1, nextRefs))
	return oid, nil
}

// BranchInfo represents a branch and its tip SHA.
type BranchInfo struct {
	Name string
	SHA  string
}

// ListBranches returns all heads in lexicographical order.
func (r *Repository) ListBranches() []BranchInfo {
	snap := r.gitSnap.Load()
	var out []BranchInfo
	for _, ent := range snap.SortedRefs {
		if strings.HasPrefix(ent.Name, "refs/heads/") {
			out = append(out, BranchInfo{
				Name: ent.Name[len("refs/heads/"):],
				SHA:  ent.OID.Hex(),
			})
		}
	}
	return out
}

// GetBranch looks up a single branch by name (including names containing '/').
func (r *Repository) GetBranch(branch string) (BranchInfo, bool) {
	snap := r.gitSnap.Load()
	oid, ok := snap.Refs["refs/heads/"+branch]
	if !ok {
		return BranchInfo{}, false
	}
	return BranchInfo{Name: branch, SHA: oid.Hex()}, true
}

// ResolveCommitish resolves a branch name, full ref, or 40-hex commit SHA.
func (r *Repository) ResolveCommitish(ref string) (gitobj.OID, bool) {
	snap := r.gitSnap.Load()
	if ref == "" || ref == "HEAD" {
		if snap.HasHead {
			return snap.HeadOID, true
		}
		return gitobj.ZeroOID, false
	}
	if oid, ok := snap.Refs["refs/heads/"+ref]; ok {
		return oid, true
	}
	if oid, ok := snap.Refs[ref]; ok {
		return oid, true
	}
	if oid, ok := snap.Refs["refs/tags/"+ref]; ok {
		if pc, ok := r.GetCommit(oid); ok {
			return pc.OID, true
		}
	}
	if oid, ok := gitobj.ParseOID(ref); ok {
		if pc, ok := r.GetCommit(oid); ok {
			return pc.OID, true
		}
	}
	return gitobj.ZeroOID, false
}

// ListCommits walks commit history starting from refOrSHA (defaulting to DefaultBranch).
func (r *Repository) ListCommits(refOrSHA string, limit int) ([]*gitobj.ParsedCommit, error) {
	if limit <= 0 {
		limit = 100
	}
	tip, ok := r.ResolveCommitish(refOrSHA)
	if !ok {
		if refOrSHA == "" {
			return []*gitobj.ParsedCommit{}, nil
		}
		return nil, &DomainError{Kind: ErrNotFound, Message: "Commit or branch not found"}
	}
	var out []*gitobj.ParsedCommit
	visited := make(map[gitobj.OID]struct{}, 32)
	queue := []gitobj.OID{tip}
	visited[tip] = struct{}{}
	for len(queue) > 0 && len(out) < limit {
		cur := queue[0]
		queue = queue[1:]
		pc, ok := r.GetCommit(cur)
		if !ok {
			break
		}
		out = append(out, pc)
		for _, p := range pc.Parents {
			if _, v := visited[p]; !v {
				visited[p] = struct{}{}
				queue = append(queue, p)
			}
		}
	}
	return out, nil
}

// ContentItem represents a file or directory returned by the REST Contents API.
type ContentItem struct {
	Type        string // "file" | "dir"
	Name        string
	Path        string
	SHA         string
	Size        int
	ContentB64  string
	IsDirectory bool
	Entries     []ContentItem
}

func cleanRepoPath(p string) (string, []string, error) {
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	if p == "" {
		return "", nil, nil
	}
	cleaned := path.Clean(p)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", nil, &DomainError{Kind: ErrBadRequest, Message: "Invalid repository path"}
	}
	segs := strings.Split(cleaned, "/")
	for _, seg := range segs {
		if seg == "" || seg == "." || seg == ".." {
			return "", nil, &DomainError{Kind: ErrBadRequest, Message: "Invalid path segment"}
		}
	}
	return cleaned, segs, nil
}

func (r *Repository) lookupTreeEntry(rootTree gitobj.OID, segs []string) (gitobj.TreeEntry, bool, error) {
	if rootTree.IsZero() {
		return gitobj.TreeEntry{}, false, nil
	}
	if len(segs) == 0 {
		return gitobj.TreeEntry{Mode: "40000", Name: "", OID: rootTree}, true, nil
	}
	curTree := rootTree
	for i, seg := range segs {
		tObj, ok := r.GetObject(curTree)
		if !ok || tObj.Type != gitobj.ObjTree {
			return gitobj.TreeEntry{}, false, nil
		}
		entries, err := gitobj.ParseTree(tObj.Raw)
		if err != nil {
			return gitobj.TreeEntry{}, false, err
		}
		var matched gitobj.TreeEntry
		found := false
		for _, e := range entries {
			if e.Name == seg {
				matched = e
				found = true
				break
			}
		}
		if !found {
			return gitobj.TreeEntry{}, false, nil
		}
		if i == len(segs)-1 {
			return matched, true, nil
		}
		if !matched.IsDir() {
			return gitobj.TreeEntry{}, false, nil
		}
		curTree = matched.OID
	}
	return gitobj.TreeEntry{}, false, nil
}

func (r *Repository) upsertTreeEntry(baseTree gitobj.OID, segs []string, blobOID gitobj.OID) (gitobj.OID, []*gitobj.Object, error) {
	var existing []gitobj.TreeEntry
	if !baseTree.IsZero() {
		tObj, ok := r.GetObject(baseTree)
		if ok && tObj.Type == gitobj.ObjTree {
			var err error
			existing, err = gitobj.ParseTree(tObj.Raw)
			if err != nil {
				return gitobj.ZeroOID, nil, err
			}
		}
	}
	name := segs[0]
	if len(segs) == 1 {
		replaced := false
		next := make([]gitobj.TreeEntry, 0, len(existing)+1)
		for _, e := range existing {
			if e.Name == name {
				next = append(next, gitobj.TreeEntry{Mode: "100644", Name: name, OID: blobOID})
				replaced = true
			} else {
				next = append(next, e)
			}
		}
		if !replaced {
			next = append(next, gitobj.TreeEntry{Mode: "100644", Name: name, OID: blobOID})
		}
		treeObj := gitobj.NewObject(gitobj.ObjTree, gitobj.BuildTree(next))
		return treeObj.OID, []*gitobj.Object{treeObj}, nil
	}

	var childBase gitobj.OID
	for _, e := range existing {
		if e.Name == name && e.IsDir() {
			childBase = e.OID
			break
		}
	}
	childOID, created, err := r.upsertTreeEntry(childBase, segs[1:], blobOID)
	if err != nil {
		return gitobj.ZeroOID, nil, err
	}
	replaced := false
	next := make([]gitobj.TreeEntry, 0, len(existing)+1)
	for _, e := range existing {
		if e.Name == name {
			next = append(next, gitobj.TreeEntry{Mode: "40000", Name: name, OID: childOID})
			replaced = true
		} else {
			next = append(next, e)
		}
	}
	if !replaced {
		next = append(next, gitobj.TreeEntry{Mode: "40000", Name: name, OID: childOID})
	}
	treeObj := gitobj.NewObject(gitobj.ObjTree, gitobj.BuildTree(next))
	created = append(created, treeObj)
	return treeObj.OID, created, nil
}

func (r *Repository) deleteTreeEntry(baseTree gitobj.OID, segs []string) (gitobj.OID, bool, []*gitobj.Object, error) {
	tObj, ok := r.GetObject(baseTree)
	if !ok || tObj.Type != gitobj.ObjTree {
		return gitobj.ZeroOID, false, nil, &DomainError{Kind: ErrNotFound, Message: "Not Found"}
	}
	existing, err := gitobj.ParseTree(tObj.Raw)
	if err != nil {
		return gitobj.ZeroOID, false, nil, err
	}
	name := segs[0]
	if len(segs) == 1 {
		found := false
		next := make([]gitobj.TreeEntry, 0, len(existing))
		for _, e := range existing {
			if e.Name == name && !e.IsDir() {
				found = true
				continue
			}
			next = append(next, e)
		}
		if !found {
			return gitobj.ZeroOID, false, nil, &DomainError{Kind: ErrNotFound, Message: "Not Found"}
		}
		if len(next) == 0 {
			emptyObj := gitobj.NewObject(gitobj.ObjTree, nil)
			return emptyObj.OID, false, []*gitobj.Object{emptyObj}, nil
		}
		treeObj := gitobj.NewObject(gitobj.ObjTree, gitobj.BuildTree(next))
		return treeObj.OID, true, []*gitobj.Object{treeObj}, nil
	}

	var childBase gitobj.OID
	foundDir := false
	for _, e := range existing {
		if e.Name == name && e.IsDir() {
			childBase = e.OID
			foundDir = true
			break
		}
	}
	if !foundDir {
		return gitobj.ZeroOID, false, nil, &DomainError{Kind: ErrNotFound, Message: "Not Found"}
	}
	childOID, childNonEmpty, created, err := r.deleteTreeEntry(childBase, segs[1:])
	if err != nil {
		return gitobj.ZeroOID, false, nil, err
	}
	next := make([]gitobj.TreeEntry, 0, len(existing))
	for _, e := range existing {
		if e.Name == name {
			if childNonEmpty {
				next = append(next, gitobj.TreeEntry{Mode: "40000", Name: name, OID: childOID})
			}
		} else {
			next = append(next, e)
		}
	}
	if len(next) == 0 {
		emptyObj := gitobj.NewObject(gitobj.ObjTree, nil)
		created = append(created, emptyObj)
		return emptyObj.OID, false, created, nil
	}
	treeObj := gitobj.NewObject(gitobj.ObjTree, gitobj.BuildTree(next))
	created = append(created, treeObj)
	return treeObj.OID, true, created, nil
}

// GetContent resolves a file or directory at repoPath on ref (defaulting to DefaultBranch).
func (r *Repository) GetContent(repoPath, ref string) (*ContentItem, error) {
	cleaned, segs, err := cleanRepoPath(repoPath)
	if err != nil {
		return nil, err
	}
	commitOID, ok := r.ResolveCommitish(ref)
	if !ok {
		return nil, &DomainError{Kind: ErrNotFound, Message: "Not Found"}
	}
	pc, ok := r.GetCommit(commitOID)
	if !ok {
		return nil, &DomainError{Kind: ErrNotFound, Message: "Not Found"}
	}
	ent, found, err := r.lookupTreeEntry(pc.Tree, segs)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, &DomainError{Kind: ErrNotFound, Message: "Not Found"}
	}
	if ent.IsDir() {
		tObj, ok := r.GetObject(ent.OID)
		if !ok || tObj.Type != gitobj.ObjTree {
			return nil, &DomainError{Kind: ErrNotFound, Message: "Not Found"}
		}
		entries, err := gitobj.ParseTree(tObj.Raw)
		if err != nil {
			return nil, err
		}
		items := make([]ContentItem, 0, len(entries))
		for _, child := range entries {
			childPath := child.Name
			if cleaned != "" {
				childPath = cleaned + "/" + child.Name
			}
			typ := "file"
			size := 0
			if child.IsDir() {
				typ = "dir"
			} else if bObj, ok := r.GetObject(child.OID); ok {
				size = len(bObj.Raw)
			}
			items = append(items, ContentItem{
				Type: typ,
				Name: child.Name,
				Path: childPath,
				SHA:  child.OID.Hex(),
				Size: size,
			})
		}
		return &ContentItem{
			IsDirectory: true,
			Path:        cleaned,
			SHA:         ent.OID.Hex(),
			Entries:     items,
		}, nil
	}

	bObj, ok := r.GetObject(ent.OID)
	if !ok || bObj.Type != gitobj.ObjBlob {
		return nil, &DomainError{Kind: ErrNotFound, Message: "Not Found"}
	}
	return &ContentItem{
		Type:       "file",
		Name:       ent.Name,
		Path:       cleaned,
		SHA:        ent.OID.Hex(),
		Size:       len(bObj.Raw),
		ContentB64: base64.StdEncoding.EncodeToString(bObj.Raw),
	}, nil
}

// PutContent creates or updates a file via the REST Contents API, committing directly to Git refs.
func (r *Repository) PutContent(repoPath string, rawContent []byte, message, branch, expectedSHA string) (contentItem ContentItem, commitSHA string, created bool, err error) {
	cleaned, segs, err := cleanRepoPath(repoPath)
	if err != nil || len(segs) == 0 {
		return ContentItem{}, "", false, &DomainError{Kind: ErrBadRequest, Message: "Invalid file path"}
	}
	if branch == "" {
		branch = r.DefaultBranch
	}
	refName := "refs/heads/" + branch
	if !isValidGitRefName(refName) {
		return ContentItem{}, "", false, &DomainError{Kind: ErrUnprocessable, Message: "Invalid branch name"}
	}

	blobObj := gitobj.NewObject(gitobj.ObjBlob, rawContent)

	r.mu.Lock()
	defer r.mu.Unlock()

	curSnap := r.gitSnap.Load()
	var parentOID gitobj.OID
	var hasParent bool
	var baseTree gitobj.OID

	if tip, ok := curSnap.Refs[refName]; ok {
		parentOID = tip
		hasParent = true
		if pc, ok := r.GetCommit(tip); ok {
			baseTree = pc.Tree
		}
	} else if curSnap.HasHead && branch != r.DefaultBranch {
		// Creating a new branch directly via PUT /contents?branch=... bases on DefaultBranch
		parentOID = curSnap.HeadOID
		hasParent = true
		if pc, ok := r.GetCommit(curSnap.HeadOID); ok {
			baseTree = pc.Tree
		}
	}

	existing, exists, err := r.lookupTreeEntry(baseTree, segs)
	if err != nil {
		return ContentItem{}, "", false, err
	}
	if exists && existing.IsDir() {
		return ContentItem{}, "", false, &DomainError{Kind: ErrConflict, Message: "Path is a directory"}
	}
	if exists {
		if expectedSHA == "" || !strings.EqualFold(expectedSHA, existing.OID.Hex()) {
			return ContentItem{}, "", false, &DomainError{Kind: ErrConflict, Message: "file sha does not match"}
		}
		created = false
	} else {
		if expectedSHA != "" {
			return ContentItem{}, "", false, &DomainError{Kind: ErrConflict, Message: "file does not exist"}
		}
		created = true
	}

	newTreeOID, treeObjs, err := r.upsertTreeEntry(baseTree, segs, blobObj.OID)
	if err != nil {
		return ContentItem{}, "", false, err
	}

	var parents []gitobj.OID
	if hasParent {
		parents = []gitobj.OID{parentOID}
	}
	commitBytes := gitobj.BuildCommit(newTreeOID, parents, r.Owner.Name, r.Owner.Email, r.Forge.FixedEpoch, message)
	commitObj := gitobj.NewObject(gitobj.ObjCommit, commitBytes)

	allObjs := make([]*gitobj.Object, 0, len(treeObjs)+2)
	allObjs = append(allObjs, blobObj)
	allObjs = append(allObjs, treeObjs...)
	allObjs = append(allObjs, commitObj)
	if err := r.PutObjects(allObjs); err != nil {
		return ContentItem{}, "", false, err
	}

	nextRefs := make(map[string]gitobj.OID, len(curSnap.Refs)+1)
	for k, v := range curSnap.Refs {
		nextRefs[k] = v
	}
	nextRefs[refName] = commitObj.OID
	r.gitSnap.Store(r.buildGitSnapshot(curSnap.Version+1, nextRefs))

	return ContentItem{
		Type: "file",
		Name: segs[len(segs)-1],
		Path: cleaned,
		SHA:  blobObj.OID.Hex(),
		Size: len(rawContent),
	}, commitObj.OID.Hex(), created, nil
}

// DeleteContent removes a file via the REST Contents API and commits the new tree.
func (r *Repository) DeleteContent(repoPath, message, branch, expectedSHA string) (commitSHA string, err error) {
	_, segs, err := cleanRepoPath(repoPath)
	if err != nil || len(segs) == 0 {
		return "", &DomainError{Kind: ErrBadRequest, Message: "Invalid file path"}
	}
	if branch == "" {
		branch = r.DefaultBranch
	}
	refName := "refs/heads/" + branch

	r.mu.Lock()
	defer r.mu.Unlock()

	curSnap := r.gitSnap.Load()
	tip, ok := curSnap.Refs[refName]
	if !ok {
		return "", &DomainError{Kind: ErrNotFound, Message: "Branch not found"}
	}
	pc, ok := r.GetCommit(tip)
	if !ok {
		return "", &DomainError{Kind: ErrNotFound, Message: "Commit not found"}
	}
	existing, exists, err := r.lookupTreeEntry(pc.Tree, segs)
	if err != nil {
		return "", err
	}
	if !exists || existing.IsDir() {
		return "", &DomainError{Kind: ErrNotFound, Message: "Not Found"}
	}
	if expectedSHA == "" || !strings.EqualFold(expectedSHA, existing.OID.Hex()) {
		return "", &DomainError{Kind: ErrConflict, Message: "file sha does not match"}
	}

	newTreeOID, _, treeObjs, err := r.deleteTreeEntry(pc.Tree, segs)
	if err != nil {
		return "", err
	}
	commitBytes := gitobj.BuildCommit(newTreeOID, []gitobj.OID{tip}, r.Owner.Name, r.Owner.Email, r.Forge.FixedEpoch, message)
	commitObj := gitobj.NewObject(gitobj.ObjCommit, commitBytes)

	allObjs := append(treeObjs, commitObj)
	if err := r.PutObjects(allObjs); err != nil {
		return "", err
	}

	nextRefs := make(map[string]gitobj.OID, len(curSnap.Refs))
	for k, v := range curSnap.Refs {
		nextRefs[k] = v
	}
	nextRefs[refName] = commitObj.OID
	r.gitSnap.Store(r.buildGitSnapshot(curSnap.Version+1, nextRefs))
	return commitObj.OID.Hex(), nil
}

type flatFile struct {
	Mode string
	OID  gitobj.OID
}

func (r *Repository) flattenTree(treeOID gitobj.OID) map[string]flatFile {
	out := make(map[string]flatFile)
	var walk func(tOID gitobj.OID, prefix string)
	walk = func(tOID gitobj.OID, prefix string) {
		if tOID.IsZero() {
			return
		}
		tObj, ok := r.GetObject(tOID)
		if !ok || tObj.Type != gitobj.ObjTree {
			return
		}
		entries, err := gitobj.ParseTree(tObj.Raw)
		if err != nil {
			return
		}
		for _, e := range entries {
			full := e.Name
			if prefix != "" {
				full = prefix + "/" + e.Name
			}
			if e.IsDir() {
				walk(e.OID, full)
			} else {
				out[full] = flatFile{Mode: e.Mode, OID: e.OID}
			}
		}
	}
	walk(treeOID, "")
	return out
}

func (r *Repository) buildTreeFromFlat(files map[string]flatFile) (gitobj.OID, []*gitobj.Object) {
	type dirNode struct {
		files   map[string]flatFile
		subdirs map[string]*dirNode
	}
	root := &dirNode{
		files:   make(map[string]flatFile),
		subdirs: make(map[string]*dirNode),
	}
	for p, f := range files {
		segs := strings.Split(p, "/")
		cur := root
		for i := 0; i < len(segs)-1; i++ {
			sd, ok := cur.subdirs[segs[i]]
			if !ok {
				sd = &dirNode{
					files:   make(map[string]flatFile),
					subdirs: make(map[string]*dirNode),
				}
				cur.subdirs[segs[i]] = sd
			}
			cur = sd
		}
		cur.files[segs[len(segs)-1]] = f
	}

	var newObjs []*gitobj.Object
	var encodeDir func(n *dirNode) gitobj.OID
	encodeDir = func(n *dirNode) gitobj.OID {
		entries := make([]gitobj.TreeEntry, 0, len(n.files)+len(n.subdirs))
		for name, f := range n.files {
			mode := f.Mode
			if mode == "" {
				mode = "100644"
			}
			entries = append(entries, gitobj.TreeEntry{Mode: mode, Name: name, OID: f.OID})
		}
		for name, sd := range n.subdirs {
			subOID := encodeDir(sd)
			entries = append(entries, gitobj.TreeEntry{Mode: "40000", Name: name, OID: subOID})
		}
		obj := gitobj.NewObject(gitobj.ObjTree, gitobj.BuildTree(entries))
		newObjs = append(newObjs, obj)
		return obj.OID
	}
	rootOID := encodeDir(root)
	return rootOID, newObjs
}

// findMergeBase computes the nearest common ancestor of commitA and commitB via dual BFS.
func (r *Repository) findMergeBase(a, b gitobj.OID) (gitobj.OID, bool) {
	if a == b {
		return a, true
	}
	ancA := make(map[gitobj.OID]int, 32)
	qA := []gitobj.OID{a}
	ancA[a] = 0
	for len(qA) > 0 {
		cur := qA[0]
		qA = qA[1:]
		d := ancA[cur]
		if pc, ok := r.GetCommit(cur); ok {
			for _, p := range pc.Parents {
				if _, seen := ancA[p]; !seen {
					ancA[p] = d + 1
					qA = append(qA, p)
				}
			}
		}
	}

	visitedB := make(map[gitobj.OID]struct{}, 32)
	qB := []gitobj.OID{b}
	visitedB[b] = struct{}{}
	bestDist := int(^uint(0) >> 1)
	var bestOID gitobj.OID
	found := false

	for len(qB) > 0 {
		cur := qB[0]
		qB = qB[1:]
		if dA, ok := ancA[cur]; ok {
			if !found || dA < bestDist {
				bestDist = dA
				bestOID = cur
				found = true
			}
			continue
		}
		if pc, ok := r.GetCommit(cur); ok {
			for _, p := range pc.Parents {
				if _, seen := visitedB[p]; !seen {
					visitedB[p] = struct{}{}
					qB = append(qB, p)
				}
			}
		}
	}
	return bestOID, found
}

func countLines(b []byte) int {
	if len(b) == 0 {
		return 0
	}
	n := bytes.Count(b, []byte{'\n'})
	if b[len(b)-1] != '\n' {
		n++
	}
	return n
}

func diffBlobLines(oldRaw, newRaw []byte) (additions, deletions int) {
	if len(oldRaw) == 0 {
		return countLines(newRaw), 0
	}
	if len(newRaw) == 0 {
		return 0, countLines(oldRaw)
	}
	oldLines := strings.Split(strings.TrimSuffix(string(oldRaw), "\n"), "\n")
	newLines := strings.Split(strings.TrimSuffix(string(newRaw), "\n"), "\n")
	prefix := 0
	for prefix < len(oldLines) && prefix < len(newLines) && oldLines[prefix] == newLines[prefix] {
		prefix++
	}
	oldEnd := len(oldLines)
	newEnd := len(newLines)
	for oldEnd > prefix && newEnd > prefix && oldLines[oldEnd-1] == newLines[newEnd-1] {
		oldEnd--
		newEnd--
	}
	deletions = oldEnd - prefix
	additions = newEnd - prefix
	if additions == 0 && deletions == 0 && !bytes.Equal(oldRaw, newRaw) {
		additions = 1
		deletions = 1
	}
	return additions, deletions
}

// DiffCommits computes the GitHub PR file diff of headCommit relative to mergeBase(baseCommit, headCommit).
func (r *Repository) DiffCommits(baseCommit, headCommit gitobj.OID) []PullFile {
	compareBase := baseCommit
	if mb, ok := r.findMergeBase(baseCommit, headCommit); ok {
		compareBase = mb
	}
	var baseTree, headTree gitobj.OID
	if pc, ok := r.GetCommit(compareBase); ok {
		baseTree = pc.Tree
	}
	if pc, ok := r.GetCommit(headCommit); ok {
		headTree = pc.Tree
	}
	baseFiles := r.flattenTree(baseTree)
	headFiles := r.flattenTree(headTree)

	pathSet := make(map[string]struct{}, len(baseFiles)+len(headFiles))
	for p := range baseFiles {
		pathSet[p] = struct{}{}
	}
	for p := range headFiles {
		pathSet[p] = struct{}{}
	}
	paths := make([]string, 0, len(pathSet))
	for p := range pathSet {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	var out []PullFile
	for _, p := range paths {
		bf, inBase := baseFiles[p]
		hf, inHead := headFiles[p]
		if inBase && inHead && bf.OID == hf.OID {
			continue
		}
		var oldBytes, newBytes []byte
		if inBase {
			if obj, ok := r.GetObject(bf.OID); ok {
				oldBytes = obj.Raw
			}
		}
		if inHead {
			if obj, ok := r.GetObject(hf.OID); ok {
				newBytes = obj.Raw
			}
		}
		adds, dels := diffBlobLines(oldBytes, newBytes)
		switch {
		case !inBase && inHead:
			if adds == 0 && len(newBytes) > 0 {
				adds = 1
			}
			out = append(out, PullFile{
				SHA:       hf.OID.Hex(),
				Filename:  p,
				Status:    "added",
				Additions: adds,
				Deletions: 0,
				Changes:   adds,
			})
		case inBase && !inHead:
			out = append(out, PullFile{
				SHA:       bf.OID.Hex(),
				Filename:  p,
				Status:    "removed",
				Additions: 0,
				Deletions: dels,
				Changes:   dels,
			})
		default:
			out = append(out, PullFile{
				SHA:       hf.OID.Hex(),
				Filename:  p,
				Status:    "modified",
				Additions: adds,
				Deletions: dels,
				Changes:   adds + dels,
			})
		}
	}
	return out
}

// merge3WayFiles performs a 3-way file-level tree merge between mergeBase, ours (baseTip), and theirs (headTip).
// Returns ok=false if any path was modified incompatibly on both branches since mergeBase.
func (r *Repository) merge3WayFiles(baseTip, headTip gitobj.OID) (merged map[string]flatFile, ok bool) {
	var ancTree, oursTree, theirsTree gitobj.OID
	if mb, found := r.findMergeBase(baseTip, headTip); found {
		if pc, ok := r.GetCommit(mb); ok {
			ancTree = pc.Tree
		}
	}
	if pc, ok := r.GetCommit(baseTip); ok {
		oursTree = pc.Tree
	}
	if pc, ok := r.GetCommit(headTip); ok {
		theirsTree = pc.Tree
	}

	ancMap := r.flattenTree(ancTree)
	oursMap := r.flattenTree(oursTree)
	theirsMap := r.flattenTree(theirsTree)

	allPaths := make(map[string]struct{}, len(ancMap)+len(oursMap)+len(theirsMap))
	for p := range ancMap {
		allPaths[p] = struct{}{}
	}
	for p := range oursMap {
		allPaths[p] = struct{}{}
	}
	for p := range theirsMap {
		allPaths[p] = struct{}{}
	}

	merged = make(map[string]flatFile, len(allPaths))
	for p := range allPaths {
		a := ancMap[p]
		o, inO := oursMap[p]
		t, inT := theirsMap[p]

		switch {
		case o.OID == t.OID && inO == inT:
			if inO {
				merged[p] = o
			}
		case o.OID == a.OID:
			// Only theirs changed (added, modified, or deleted)
			if inT {
				merged[p] = t
			}
		case t.OID == a.OID:
			// Only ours changed (added, modified, or deleted)
			if inO {
				merged[p] = o
			}
		default:
			// Both ours and theirs diverged from ancestor on the same path -> conflict!
			return nil, false
		}
	}
	return merged, true
}
