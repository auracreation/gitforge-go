package server_test

import (
	"fmt"
	"testing"

	"gitforge/internal/gitobj"
	"gitforge/internal/store"
)

func TestPushRejectsWrongTreeObjectType(t *testing.T) {
	for _, withParent := range []bool{false, true} {
		t.Run(fmt.Sprint(withParent), func(t *testing.T) {
			f := store.NewForge(testToken)
			r, e := f.CreateRepo("type-audit", "", true, "main")
			if e != nil {
				t.Fatal(e)
			}
			empty := gitobj.NewObject(gitobj.ObjTree, nil)
			parent := gitobj.NewObject(gitobj.ObjCommit, gitobj.BuildCommit(empty.OID, nil, "A", "a@x", 1, "parent"))
			badTree := gitobj.NewObject(gitobj.ObjTree, gitobj.BuildTree([]gitobj.TreeEntry{{Mode: "100644", Name: "file", OID: empty.OID}}))
			var parents []gitobj.OID
			if withParent {
				parents = []gitobj.OID{parent.OID}
			}
			child := gitobj.NewObject(gitobj.ObjCommit, gitobj.BuildCommit(badTree.OID, parents, "A", "a@x", 2, "bad file type"))
			pack := gitobj.BuildPack([]*gitobj.Object{empty, parent, badTree, child})
			before := r.Git()
			unpack, status := r.ReceivePack([]store.PushCommand{{NewOID: child.OID, Ref: "refs/heads/main"}}, pack, true)
			if unpack != "" {
				t.Fatal(unpack)
			}
			if len(status) != 1 || status[0].OK {
				t.Fatalf("accepted regular-file tree entry pointing at TREE: %+v", status)
			}
			if r.Git() != before {
				t.Fatal("invalid tree published refs")
			}
		})
	}
}

func TestPushRejectsWrongTagTargetType(t *testing.T) {
	f := store.NewForge(testToken)
	r, e := f.CreateRepo("tag-audit", "", true, "main")
	if e != nil {
		t.Fatal(e)
	}
	blob := gitobj.NewObject(gitobj.ObjBlob, []byte("blob"))
	tag := gitobj.NewObject(gitobj.ObjTag, []byte(fmt.Sprintf("object %s\ntype commit\ntag mismatch\n\ninvalid\n", blob.OID.Hex())))
	before := r.Git()
	unpack, status := r.ReceivePack([]store.PushCommand{{NewOID: tag.OID, Ref: "refs/tags/mismatch"}}, gitobj.BuildPack([]*gitobj.Object{blob, tag}), true)
	if unpack != "" {
		t.Fatal(unpack)
	}
	if len(status) != 1 || status[0].OK {
		t.Fatalf("accepted tag claiming COMMIT for BLOB target: %+v", status)
	}
	if r.Git() != before {
		t.Fatal("invalid tag published refs")
	}
	tag = gitobj.NewObject(gitobj.ObjTag, []byte(fmt.Sprintf("object %s\ntype blob\ntag valid\n\nvalid\n", blob.OID.Hex())))
	unpack, status = r.ReceivePack([]store.PushCommand{{NewOID: tag.OID, Ref: "refs/tags/valid"}}, gitobj.BuildPack([]*gitobj.Object{blob, tag}), true)
	if unpack != "" || len(status) != 1 || !status[0].OK {
		t.Fatalf("valid annotated blob tag rejected: %s %+v", unpack, status)
	}
}
