package store

import (
	"bytes"
	"gitforge/internal/gitobj"
	"testing"
)

func TestLsRefsHTTPFrameUsesOwningSnapshot(t *testing.T) {
	f := NewForge("audit-token")
	r, err := f.CreateRepo("snapshot-cache", "", true, "main")
	if err != nil {
		t.Fatal(err)
	}
	tree := gitobj.NewObject(gitobj.ObjTree, nil)
	first := gitobj.NewObject(gitobj.ObjCommit, gitobj.BuildCommit(tree.OID, nil, "A", "a@example.invalid", 1, "first"))
	second := gitobj.NewObject(gitobj.ObjCommit, gitobj.BuildCommit(tree.OID, []gitobj.OID{first.OID}, "A", "a@example.invalid", 2, "second"))
	_, st := r.ReceivePack([]PushCommand{{NewOID: first.OID, Ref: "refs/heads/main"}}, gitobj.BuildPack([]*gitobj.Object{tree, first}), true)
	if len(st) != 1 || !st[0].OK {
		t.Fatalf("first push: %+v", st)
	}
	old := r.Git()
	_, st = r.ReceivePack([]PushCommand{{OldOID: first.OID, NewOID: second.OID, Ref: "refs/heads/main"}}, gitobj.BuildPack([]*gitobj.Object{second}), true)
	if len(st) != 1 || !st[0].OK {
		t.Fatalf("second push: %+v", st)
	}
	frame := old.LsRefsHTTP200()
	if !bytes.Contains(frame, []byte(first.OID.Hex())) || bytes.Contains(frame, []byte(second.OID.Hex())) {
		t.Fatalf("old snapshot advertised wrong refs: %q", frame)
	}
}

func TestDepthOneIncludeTagReusesCacheWhenNoTagsExist(t *testing.T) {
	f := NewForge("audit-token")
	r, err := f.CreateRepo("depth-cache", "", true, "main")
	if err != nil {
		t.Fatal(err)
	}
	tree := gitobj.NewObject(gitobj.ObjTree, nil)
	commit := gitobj.NewObject(gitobj.ObjCommit, gitobj.BuildCommit(tree.OID, nil, "A", "a@example.invalid", 1, "first"))
	_, statuses := r.ReceivePack([]PushCommand{{NewOID: commit.OID, Ref: "refs/heads/main"}}, gitobj.BuildPack([]*gitobj.Object{tree, commit}), true)
	if len(statuses) != 1 || !statuses[0].OK {
		t.Fatalf("push: %+v", statuses)
	}
	args := FetchArgs{Protocol: 2, Wants: []gitobj.OID{commit.OID}, Depth: 1, Done: true, Sideband: true, IncludeTag: true}
	first, err := r.FetchUploadPack(args)
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.FetchUploadPack(args)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) == 0 || len(second) == 0 || &first[0] != &second[0] {
		t.Fatal("tag-free depth-one responses were rebuilt")
	}
}
