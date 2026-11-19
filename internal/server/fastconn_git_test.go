package server_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gitforge/internal/server"
	"gitforge/internal/store"
)

func TestFastServerRealGitMixedDepths(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	f := store.NewForge(testToken)
	if _, err := f.CreateRepo("depths", "", true, "main"); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fast := server.NewFastServer(server.NewRouter(f))
	standard := os.Getenv("AUDIT_STANDARD") == "1"
	if standard {
		srv := &http.Server{Handler: server.NewRouter(f)}
		go func() { _ = srv.Serve(listener) }()
		defer srv.Shutdown(context.Background())
	} else {
		go func() { _ = fast.Start(listener) }()
		defer fast.Shutdown(context.Background())
	}
	defer listener.Close()
	tmp := t.TempDir()
	remote := fmt.Sprintf("http://%s/%s/depths.git", listener.Addr(), f.Viewer.Login)
	run := func(dir string, args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		a := append([]string{"-c", "http.extraHeader=Authorization: Bearer " + testToken}, args...)
		cmd := exec.CommandContext(ctx, "git", a...)
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "GIT_AUTHOR_NAME=Audit", "GIT_AUTHOR_EMAIL=a@example.invalid", "GIT_COMMITTER_NAME=Audit", "GIT_COMMITTER_EMAIL=a@example.invalid")
		out, err := cmd.CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("git %v: %w: %s", args, err, out)
		}
		return strings.TrimSpace(string(out)), nil
	}
	must := func(dir string, args ...string) string {
		t.Helper()
		out, err := run(dir, args...)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	work := filepath.Join(tmp, "source")
	if err := os.Mkdir(work, 0700); err != nil {
		t.Fatal(err)
	}
	must(work, "init", "-b", "main")
	for i := 1; i <= 12; i++ {
		if err := os.WriteFile(filepath.Join(work, "data.txt"), []byte(strconv.Itoa(i)), 0600); err != nil {
			t.Fatal(err)
		}
		must(work, "add", "data.txt")
		must(work, "commit", "-m", fmt.Sprintf("commit %d", i))
	}
	must(work, "push", remote, "main")
	verify := func(depth int, name string) error {
		dest := filepath.Join(tmp, name)
		args := []string{"clone", "--depth", strconv.Itoa(depth)}
		if name == "seq-0" {
			args = append(args, "--no-tags")
		}
		args = append(args, remote, dest)
		cloneOutput, err := run(tmp, args...)
		if name == "seq-0" && os.Getenv("GIT_TRACE_PACKET") != "" {
			t.Log(cloneOutput)
		}
		if err != nil {
			return err
		}
		count, err := run(dest, "rev-list", "--count", "HEAD")
		if err != nil {
			return err
		}
		want := depth
		if want > 12 {
			want = 12
		}
		if count != strconv.Itoa(want) {
			return fmt.Errorf("depth %d: got %s commits; want %d", depth, count, want)
		}
		shallow, err := run(dest, "rev-parse", "--is-shallow-repository")
		if err != nil {
			return err
		}
		if (shallow == "true") != (depth < 12) {
			return fmt.Errorf("depth %d: shallow=%s", depth, shallow)
		}
		_, err = run(dest, "fsck", "--full")
		return err
	}
	depths := []int{1, 2, 5, 10, 25, 1}
	for i, d := range depths {
		before := fast.FastCloneHits.Load()
		if err := verify(d, fmt.Sprintf("seq-%d", i)); err != nil {
			t.Error(err)
		}
		if !standard && i == 0 && fast.FastCloneHits.Load()-before < 2 {
			t.Error("depth-one clone did not exercise both ls-refs and fetch caches")
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(depths))
	for i, d := range depths {
		wg.Add(1)
		go func(i, d int) { defer wg.Done(); errs <- verify(d, fmt.Sprintf("parallel-%d", i)) }(i, d)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	if hits := fast.FastCloneHits.Load(); !standard && hits == 0 {
		t.Error("real Git clones never exercised the fast clone path")
	}
	for _, clients := range []int{1, 10, 25, 50} {
		errs := make(chan error, clients)
		for i := 0; i < clients; i++ {
			go func(i int) { errs <- verify(depths[i%len(depths)], fmt.Sprintf("scale-%d-%d", clients, i)) }(i)
		}
		for i := 0; i < clients; i++ {
			if err := <-errs; err != nil {
				t.Error(err)
			}
		}
	}
	oldHead := must(work, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(work, "data.txt"), []byte("13"), 0600); err != nil {
		t.Fatal(err)
	}
	must(work, "add", "data.txt")
	must(work, "commit", "-m", "commit 13")
	newHead := must(work, "rev-parse", "HEAD")
	start := make(chan struct{})
	mixedErrors := make(chan error, 26)
	for i := 0; i < 25; i++ {
		go func(i int) {
			<-start
			depth := depths[i%len(depths)]
			dest := filepath.Join(tmp, fmt.Sprintf("mixed-%d", i))
			if _, err := run(tmp, "clone", "--depth", strconv.Itoa(depth), remote, dest); err != nil {
				mixedErrors <- err
				return
			}
			head, err := run(dest, "rev-parse", "HEAD")
			if err != nil {
				mixedErrors <- err
				return
			}
			history := 12
			if head == newHead {
				history = 13
			} else if head != oldHead {
				mixedErrors <- fmt.Errorf("unexpected mixed clone HEAD %s", head)
				return
			}
			count, err := run(dest, "rev-list", "--count", "HEAD")
			if err != nil {
				mixedErrors <- err
				return
			}
			want := depth
			if want > history {
				want = history
			}
			if count != strconv.Itoa(want) {
				mixedErrors <- fmt.Errorf("mixed depth %d: got %s commits, want %d", depth, count, want)
				return
			}
			shallow, err := run(dest, "rev-parse", "--is-shallow-repository")
			if err != nil {
				mixedErrors <- err
				return
			}
			if (shallow == "true") != (depth < history) {
				mixedErrors <- fmt.Errorf("mixed depth %d: shallow=%s", depth, shallow)
				return
			}
			_, err = run(dest, "fsck", "--full")
			mixedErrors <- err
		}(i)
	}
	go func() { <-start; _, err := run(work, "push", remote, "main"); mixedErrors <- err }()
	close(start)
	for i := 0; i < 26; i++ {
		if err := <-mixedErrors; err != nil {
			t.Error(err)
		}
	}
	must(work, "tag", "-a", "v1", "-m", "annotated")
	must(work, "tag", "-a", "v-outer", "v1", "-m", "nested annotated")
	must(work, "tag", "-a", "v-old", "HEAD~11", "-m", "old annotated")
	must(work, "push", remote, "refs/tags/v1", "refs/tags/v-outer", "refs/tags/v-old")
	tagClone := filepath.Join(tmp, "annotated-tag")
	before := fast.FastCloneHits.Load()
	cloneOutput := must(tmp, "clone", "--depth", "1", remote, tagClone)
	if os.Getenv("GIT_TRACE_PACKET") != "" {
		t.Log(cloneOutput)
	}
	if kind := must(tagClone, "cat-file", "-t", "refs/tags/v1"); kind != "tag" {
		t.Errorf("tag object has type %q", kind)
	}
	if tags := must(tagClone, "tag", "--list"); tags != "v-outer\nv1" {
		t.Errorf("shallow clone tags %q, want the two tags at HEAD", tags)
	}
	if got, want := must(tagClone, "rev-parse", "refs/tags/v1^{commit}"), must(work, "rev-parse", "HEAD"); got != want {
		t.Errorf("tag commit %s, want %s", got, want)
	}
	if hits := fast.FastCloneHits.Load() - before; !standard && hits != 1 {
		t.Errorf("tagged clone had %d cache hits; want ls-refs only", hits)
	}
	v0Clone := filepath.Join(tmp, "annotated-tag-v0")
	must(tmp, "-c", "protocol.version=0", "clone", "--depth", "1", remote, v0Clone)
	if tags := must(v0Clone, "tag", "--list"); tags != "v-outer\nv1" {
		t.Errorf("v0 clone tags %q", tags)
	}
	v0Full := filepath.Join(tmp, "annotated-tag-v0-full")
	must(tmp, "-c", "protocol.version=0", "clone", "--depth", "25", remote, v0Full)
	if shallow := must(v0Full, "rev-parse", "--is-shallow-repository"); shallow != "false" {
		t.Errorf("v0 deep clone shallow=%s", shallow)
	}
	if tags := must(v0Full, "tag", "--list"); tags != "v-old\nv-outer\nv1" {
		t.Errorf("v0 deep clone tags %q", tags)
	}
}
