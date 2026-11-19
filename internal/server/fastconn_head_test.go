package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
)

func TestFastWireHeadSequences(t *testing.T) {
	const missing = "/missing"
	const auth = "/api/v3/repos/a/b/issues/1"
	for _, tc := range []struct {
		name    string
		methods []string
		paths   []string
	}{
		{"head-get", []string{"HEAD", "GET"}, []string{missing, "/healthz"}},
		{"head-head", []string{"HEAD", "HEAD"}, []string{missing, auth}},
		{"get-head-get", []string{"GET", "HEAD", "GET"}, []string{"/healthz", auth, "/healthz"}},
		{"mixed", []string{"HEAD", "GET", "HEAD", "HEAD", "GET"}, []string{"/healthz", "/healthz", auth, missing, "/healthz"}},
		{"close", []string{"HEAD"}, []string{auth}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type value struct {
				status            int
				length            int64
				contentType, body string
			}
			read := func(c net.Conn) []value {
				t.Helper()
				var request strings.Builder
				for i, method := range tc.methods {
					fmt.Fprintf(&request, "%s %s HTTP/1.1\r\nHost: localhost\r\n", method, tc.paths[i])
					if i == len(tc.methods)-1 {
						request.WriteString("Connection: close\r\n")
					}
					request.WriteString("\r\n")
				}
				if _, err := io.WriteString(c, request.String()); err != nil {
					t.Fatal(err)
				}
				r := bufio.NewReader(c)
				var out []value
				for _, method := range tc.methods {
					prefix, err := r.Peek(len("HTTP/1.1 "))
					if err != nil || string(prefix) != "HTTP/1.1 " {
						t.Fatalf("unexpected wire prefix %q: %v", prefix, err)
					}
					resp, err := http.ReadResponse(r, &http.Request{Method: method})
					if err != nil {
						t.Fatal(err)
					}
					body, err := io.ReadAll(resp.Body)
					resp.Body.Close()
					if err != nil {
						t.Fatal(err)
					}
					out = append(out, value{resp.StatusCode, resp.ContentLength, resp.Header.Get("Content-Type"), string(body)})
				}
				if b, err := r.ReadByte(); err != io.EOF {
					t.Fatalf("expected EOF after final response, got %q, %v", b, err)
				}
				return out
			}
			want, got := read(startStandardWire(t)), read(startFastWire(t))
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("response %d: fast %+v, standard %+v", i, got[i], want[i])
				}
				if tc.methods[i] == "HEAD" && got[i].length <= 0 {
					t.Fatalf("HEAD lost representation length: %+v", got[i])
				}
			}
		})
	}
}
