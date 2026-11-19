package server_test

import (
	"fmt"
	"testing"

	"gitforge/internal/gitobj"
	"gitforge/internal/store"
)

func TestPushRejectsNonCommitParent(t *testing.T) {
	for _, kind := range []string{"blob", "annotated-tag"} {
		for _, tagRef := range []string{"prior-push", "same-push", "unreferenced"} {
			t.Run(kind+"/"+tagRef, func(t *testing.T) {
				f := store.NewForge(testToken)
				repo, err := f.CreateRepo("invalid-parent", "", true, "main")
				if err != nil {
					t.Fatal(err)
				}
				root, rootPack := buildSingleCommitPush(gitobj.ZeroOID, "root.txt", []byte("root"), 1)
				objects, err := gitobj.Unpack(rootPack, nil)
				if err != nil {
					t.Fatal(err)
				}
				parent := gitobj.NewObject(gitobj.ObjBlob, []byte("not a commit"))
				if kind == "annotated-tag" {
					parent = gitobj.NewObject(gitobj.ObjTag, []byte(fmt.Sprintf("object %s\ntype commit\ntag parent\n\nnot a commit either\n", root.Hex())))
				}
				objects = append(objects, parent)
				tagCommand := store.PushCommand{NewOID: parent.OID, Ref: "refs/tags/parent"}
				if tagRef == "prior-push" {
					unpack, statuses := repo.ReceivePack([]store.PushCommand{tagCommand}, gitobj.BuildPack(objects), true)
					if unpack != "" || len(statuses) != 1 || !statuses[0].OK {
						t.Fatalf("valid tag setup failed: %s %+v", unpack, statuses)
					}
				}
				bad, badPack := buildSingleCommitPush(parent.OID, "child.txt", []byte("child"), 2)
				badObjects, err := gitobj.Unpack(badPack, nil)
				if err != nil {
					t.Fatal(err)
				}
				objects = append(objects, badObjects...)
				commands := []store.PushCommand{{NewOID: bad, Ref: "refs/heads/main"}}
				if tagRef == "same-push" {
					commands = append([]store.PushCommand{tagCommand}, commands...)
				}
				before := repo.Git()
				unpack, statuses := repo.ReceivePack(commands, gitobj.BuildPack(objects), true)
				if unpack != "" {
					t.Fatalf("fixture pack invalid: %s", unpack)
				}
				if len(statuses) != len(commands) || statuses[len(statuses)-1].OK {
					t.Fatalf("accepted %s as commit parent: %+v", kind, statuses)
				}
				if repo.Git() != before {
					t.Fatal("failed atomic push published refs")
				}
			})
		}
	}
}

func TestCreateRefRejectsInvalidCommitGraph(t *testing.T) {
	f := store.NewForge(testToken)
	repo, err := f.CreateRepo("invalid-rest-ref", "", true, "main")
	if err != nil {
		t.Fatal(err)
	}
	blob := gitobj.NewObject(gitobj.ObjBlob, []byte("not a commit"))
	bad, pack := buildSingleCommitPush(blob.OID, "child.txt", []byte("child"), 1)
	objects, err := gitobj.Unpack(pack, nil)
	if err != nil {
		t.Fatal(err)
	}
	objects = append(objects, blob)
	// A rejected CAS can leave immutable, unreferenced objects in the store.
	repo.ReceivePack([]store.PushCommand{{OldOID: bad, NewOID: bad, Ref: "refs/heads/absent"}}, gitobj.BuildPack(objects), true)
	before := repo.Git()
	for _, oid := range []gitobj.OID{blob.OID, bad} {
		if _, err := repo.CreateRef("refs/heads/main", oid.Hex()); err == nil {
			t.Fatalf("REST ref accepted invalid branch target %s", oid.Hex())
		}
		if repo.Git() != before {
			t.Fatal("rejected REST ref published a snapshot")
		}
	}
	if _, err := repo.CreateRef("refs/tags/blob", blob.OID.Hex()); err != nil {
		t.Fatalf("valid lightweight blob tag rejected: %v", err)
	}
}
