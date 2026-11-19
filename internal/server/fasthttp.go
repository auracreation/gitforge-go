package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"gitforge/internal/gitobj"
	"gitforge/internal/store"
)

const (
	fastMaxHeaderBytes = 64 * 1024
	fastMaxBodyBytes   = 64 * 1024 * 1024
	fastInitReadCap    = 8 * 1024
	fastMaxPoolReadCap = 512 * 1024
	fastInitWriteCap   = 4 * 1024
	fastMaxPoolWrite   = 256 * 1024
)

var (
	crlfBytes      = []byte("\r\n")
	headerEndBytes = []byte("\r\n\r\n")
	http10Bytes    = []byte("HTTP/1.0")
	http11Bytes    = []byte("HTTP/1.1")
	expect100Resp  = []byte("HTTP/1.1 100 Continue\r\n\r\n")

	healthzHTTP200 = []byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-GitHub-Request-Id: h\r\nContent-Length: 12\r\n\r\n{\"ok\":true}\n")
	unauthHTTP401  = []byte("HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nX-GitHub-Request-Id: 401\r\nContent-Length: 38\r\n\r\n{\"message\":\"Requires authentication\"}\n")
	notFoundHTTP   = []byte("HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nX-GitHub-Request-Id: 404\r\nContent-Length: 24\r\n\r\n{\"message\":\"Not Found\"}\n")
	repoNotFound   = []byte("HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nX-GitHub-Request-Id: 404r\r\nContent-Length: 35\r\n\r\n{\"message\":\"Repository not found\"}\n")
	badReqHTTP400  = []byte("HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nX-GitHub-Request-Id: 400\r\nConnection: close\r\nContent-Length: 26\r\n\r\n{\"message\":\"Bad Request\"}\n")

	// Pre-framed HTTP/1.1 200 OK response for successful sideband receive-pack push to refs/heads/main
	receivePackMainOKSidebandHTTP200 = func() []byte {
		var rep []byte
		rep = gitobj.AppendPktString(rep, "unpack ok\n")
		rep = gitobj.AppendPktString(rep, "ok refs/heads/main\n")
		rep = append(rep, "0000"...)
		var sb []byte
		sb = gitobj.AppendSidebandData(sb, 1, rep)
		sb = append(sb, "0000"...)
		return appendGitHTTP200(nil, "application/x-git-receive-pack-result", sb)
	}()
)

type connBuffers struct {
	rbuf []byte
	wbuf []byte
}

var connBufPool = sync.Pool{
	New: func() any {
		return &connBuffers{
			rbuf: make([]byte, fastInitReadCap),
			wbuf: make([]byte, 0, fastInitWriteCap),
		}
	},
}

// FastServer is a lightweight, RFC 9112-compliant HTTP/1.1 server with zero-allocation fast paths
// for Git Smart HTTP and REST issue workloads and full fallback to Router.ServeHTTP for all other routes.
type FastServer struct {
	Router *Router

	mu       sync.Mutex
	listener net.Listener
	conns    map[net.Conn]struct{}
	plat     *epollPlatform
	closed   atomic.Bool
	wg       sync.WaitGroup
}

func NewFastServer(rt *Router) *FastServer {
	return &FastServer{
		Router: rt,
		conns:  make(map[net.Conn]struct{}),
	}
}

// Serve accepts incoming connections on ln and serves HTTP/1.1 until Shutdown is called.
func (s *FastServer) Serve(ln net.Listener) error {
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.closed.Load() {
				return http.ErrServerClosed
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		if s.closed.Load() {
			_ = conn.Close()
			return http.ErrServerClosed
		}
		s.mu.Lock()
		if s.closed.Load() {
			s.mu.Unlock()
			_ = conn.Close()
			return http.ErrServerClosed
		}
		s.conns[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()

		go s.serveConn(conn)
	}
}

// Shutdown closes the listener and active connections gracefully.
func (s *FastServer) Shutdown(ctx context.Context) error {
	s.closed.Store(true)
	if err := s.shutdownPlatform(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	ln := s.listener
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *FastServer) serveConn(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.wg.Done()
	}()

	cb := connBufPool.Get().(*connBuffers)
	rbuf := cb.rbuf
	wbuf := cb.wbuf[:0]
	defer func() {
		if cap(rbuf) <= fastMaxPoolReadCap {
			cb.rbuf = rbuf[:cap(rbuf)]
		} else {
			cb.rbuf = make([]byte, fastInitReadCap)
		}
		if cap(wbuf) <= fastMaxPoolWrite {
			cb.wbuf = wbuf[:0]
		} else {
			cb.wbuf = make([]byte, 0, fastInitWriteCap)
		}
		connBufPool.Put(cb)
	}()

	rStart := 0
	rEnd := 0

	for {
		// Step 1: Read until we have the full header block ("\r\n\r\n")
		hdrEnd := -1
		for {
			if rEnd > rStart {
				if idx := bytes.Index(rbuf[rStart:rEnd], headerEndBytes); idx >= 0 {
					hdrEnd = rStart + idx
					break
				}
				if rEnd-rStart > fastMaxHeaderBytes {
					_, _ = writeAll(conn, badReqHTTP400)
					return
				}
			}
			// Compact buffer if needed before reading more
			if rStart > 0 {
				if rStart == rEnd {
					rStart = 0
					rEnd = 0
				} else if rEnd == len(rbuf) {
					n := copy(rbuf, rbuf[rStart:rEnd])
					rStart = 0
					rEnd = n
				}
			}
			if rEnd == len(rbuf) {
				if len(rbuf) >= fastMaxHeaderBytes+4096 {
					_, _ = writeAll(conn, badReqHTTP400)
					return
				}
				bigger := make([]byte, len(rbuf)*2)
				copy(bigger, rbuf[:rEnd])
				rbuf = bigger
			}
			n, err := conn.Read(rbuf[rEnd:])
			if n > 0 {
				rEnd += n
				continue
			}
			if err != nil {
				return
			}
		}

		reqBlock := rbuf[rStart:hdrEnd]
		bodyStart := hdrEnd + 4

		// Step 2: Parse request line: METHOD SP TARGET SP PROTO
		firstLineEnd := bytes.Index(reqBlock, crlfBytes)
		var reqLine, hdrLines []byte
		if firstLineEnd < 0 {
			reqLine = reqBlock
		} else {
			reqLine = reqBlock[:firstLineEnd]
			hdrLines = reqBlock[firstLineEnd+2:]
		}

		sp1 := bytes.IndexByte(reqLine, ' ')
		if sp1 <= 0 {
			_, _ = writeAll(conn, badReqHTTP400)
			return
		}
		method := reqLine[:sp1]
		remLine := reqLine[sp1+1:]
		sp2 := bytes.IndexByte(remLine, ' ')
		if sp2 <= 0 {
			_, _ = writeAll(conn, badReqHTTP400)
			return
		}
		target := remLine[:sp2]
		proto := remLine[sp2+1:]
		isHTTP10 := bytes.Equal(proto, http10Bytes)
		if !isHTTP10 && !bytes.Equal(proto, http11Bytes) {
			_, _ = writeAll(conn, badReqHTTP400)
			return
		}

		// Step 3: Parse headers in a single zero-allocation scan
		var (
			authVal       []byte
			gitProto      []byte
			contentEnc    []byte
			hostVal       []byte
			contentLen    int64 = -1
			isChunked     bool
			connClose     bool
			connKeepAlive bool
			expect100     bool
			badHeader     bool
		)

		scan := hdrLines
		for len(scan) > 0 {
			var line []byte
			if nl := bytes.Index(scan, crlfBytes); nl >= 0 {
				line = scan[:nl]
				scan = scan[nl+2:]
			} else {
				line = scan
				scan = nil
			}
			if len(line) == 0 {
				continue
			}
			colon := bytes.IndexByte(line, ':')
			if colon <= 0 {
				badHeader = true
				break
			}
			name := line[:colon]
			val := trimOWS(line[colon+1:])

			switch len(name) {
			case 4:
				if equalFoldASCII(name, "host") {
					hostVal = val
				}
			case 6:
				if equalFoldASCII(name, "expect") && equalFoldASCII(val, "100-continue") {
					expect100 = true
				}
			case 10:
				if equalFoldASCII(name, "connection") {
					if containsTokenFoldASCII(val, "close") {
						connClose = true
					}
					if containsTokenFoldASCII(val, "keep-alive") {
						connKeepAlive = true
					}
				}
			case 12:
				if equalFoldASCII(name, "git-protocol") {
					gitProto = val
				}
			case 13:
				if equalFoldASCII(name, "authorization") {
					authVal = val
				}
			case 14:
				if equalFoldASCII(name, "content-length") {
					cl, ok := parseContentLength(val)
					if !ok || (contentLen >= 0 && contentLen != cl) {
						badHeader = true
						break
					}
					contentLen = cl
				}
			case 16:
				if equalFoldASCII(name, "content-encoding") {
					contentEnc = val
				}
			case 17:
				if equalFoldASCII(name, "transfer-encoding") {
					if containsTokenFoldASCII(val, "chunked") {
						isChunked = true
					}
				}
			}
		}
		if badHeader {
			_, _ = writeAll(conn, badReqHTTP400)
			return
		}

		closeConn := connClose || (isHTTP10 && !connKeepAlive)

		if expect100 {
			if _, err := writeAll(conn, expect100Resp); err != nil {
				return
			}
		}

		// Step 4: Obtain request body bytes (preserving any pipelined tail in rbuf)
		var body []byte
		var bodyOwned []byte // non-nil when allocated outside rbuf
		if isChunked {
			// Copy header metadata before readChunkedBody potentially compacts/grows rbuf
			hdrCopy := make([]byte, len(reqBlock))
			copy(hdrCopy, reqBlock)
			method = reslice(hdrCopy, rbuf, rStart, method)
			target = reslice(hdrCopy, rbuf, rStart, target)
			proto = reslice(hdrCopy, rbuf, rStart, proto)
			hdrLines = reslice(hdrCopy, rbuf, rStart, hdrLines)
			hostVal = reslice(hdrCopy, rbuf, rStart, hostVal)

			var err error
			bodyOwned, rStart, rEnd, rbuf, err = readChunkedBody(conn, rbuf, bodyStart, rEnd)
			if err != nil {
				_, _ = writeAll(conn, badReqHTTP400)
				return
			}
			body = bodyOwned
		} else if contentLen > 0 {
			if contentLen > fastMaxBodyBytes {
				_, _ = writeAll(conn, badReqHTTP400)
				return
			}
			cl := int(contentLen)
			avail := rEnd - bodyStart
			if avail >= cl {
				body = rbuf[bodyStart : bodyStart+cl]
				rStart = bodyStart + cl
			} else if bodyStart+cl <= len(rbuf) {
				need := bodyStart + cl
				for rEnd < need {
					n, err := conn.Read(rbuf[rEnd:])
					if n > 0 {
						rEnd += n
					}
					if err != nil && rEnd < need {
						return
					}
				}
				body = rbuf[bodyStart:need]
				rStart = need
			} else if cl <= fastMaxPoolReadCap {
				// Grow rbuf so future requests of this size fit directly without reallocating
				newCap := len(rbuf)
				for newCap < bodyStart+cl {
					newCap *= 2
				}
				if newCap > fastMaxPoolReadCap && cl <= fastMaxPoolReadCap {
					// Compact headers to start so cl fits in rbuf if small
					hdrCopyLen := hdrEnd - rStart
					bigger := make([]byte, maxInt(newCap, hdrCopyLen+4+cl))
					copy(bigger, rbuf[rStart:rEnd])
					delta := rStart
					rEnd -= delta
					hdrEnd -= delta
					bodyStart -= delta
					// Re-point slices into bigger
					reqBlock = bigger[:hdrEnd]
					if firstLineEnd >= 0 {
						hdrLines = reqBlock[firstLineEnd+2:]
					}
					// Re-slice method, target, authVal, gitProto, contentEnc, hostVal safely by offset
					method = reslice(bigger, rbuf, delta, method)
					target = reslice(bigger, rbuf, delta, target)
					authVal = reslice(bigger, rbuf, delta, authVal)
					gitProto = reslice(bigger, rbuf, delta, gitProto)
					contentEnc = reslice(bigger, rbuf, delta, contentEnc)
					hostVal = reslice(bigger, rbuf, delta, hostVal)
					rbuf = bigger
					rStart = 0
				} else {
					bigger := make([]byte, newCap)
					copy(bigger, rbuf[:rEnd])
					method = reslice(bigger, rbuf, 0, method)
					target = reslice(bigger, rbuf, 0, target)
					authVal = reslice(bigger, rbuf, 0, authVal)
					gitProto = reslice(bigger, rbuf, 0, gitProto)
					contentEnc = reslice(bigger, rbuf, 0, contentEnc)
					hostVal = reslice(bigger, rbuf, 0, hostVal)
					if firstLineEnd >= 0 {
						hdrLines = reslice(bigger, rbuf, 0, hdrLines)
					}
					rbuf = bigger
				}
				need := bodyStart + cl
				for rEnd < need {
					n, err := conn.Read(rbuf[rEnd:])
					if n > 0 {
						rEnd += n
					}
					if err != nil && rEnd < need {
						return
					}
				}
				body = rbuf[bodyStart:need]
				rStart = need
			} else {
				// Large body (>512 KiB): read into exact-sized separate buffer so rbuf stays compact
				bodyOwned = make([]byte, cl)
				copied := copy(bodyOwned, rbuf[bodyStart:rEnd])
				rStart = rEnd
				if copied < cl {
					if _, err := io.ReadFull(conn, bodyOwned[copied:]); err != nil {
						return
					}
				}
				body = bodyOwned
			}
		} else {
			rStart = bodyStart
		}

		// Step 5: Split target into path and query without allocations
		path := target
		var rawQuery []byte
		if qIdx := bytes.IndexByte(target, '?'); qIdx >= 0 {
			path = target[:qIdx]
			rawQuery = target[qIdx+1:]
		}

		// Step 6: Try zero-allocation fast paths (when keep-alive, unchunked, uncompressed)
		var resp []byte
		var handled bool
		if !closeConn && !isChunked && len(contentEnc) == 0 {
			resp, wbuf, handled = s.tryFastResponse(wbuf[:0], method, path, rawQuery, authVal, gitProto, body)
		}
		if !handled {
			resp, wbuf = s.buildFallbackResponse(wbuf[:0], method, target, proto, hdrLines, hostVal, body, closeConn, conn.RemoteAddr().String())
		}
		_, writeErr := writeAll(conn, resp)

		// Reclaim read buffer indices now that slices pointing into rbuf are no longer needed
		if rStart == rEnd {
			rStart = 0
			rEnd = 0
		} else if rStart > len(rbuf)/2 {
			n := copy(rbuf, rbuf[rStart:rEnd])
			rStart = 0
			rEnd = n
		}
		_ = bodyOwned

		if writeErr != nil || closeConn {
			return
		}
	}
}

func (s *FastServer) tryFastResponse(
	wbuf []byte,
	method, path, rawQuery, authVal, gitProto, body []byte,
) (resp []byte, newWbuf []byte, handled bool) {
	forge := s.Router.Forge

	// Hot Path 1 & 2: REST API v3 issue reads and writes (/api/v3/repos/{owner}/{repo}/issues[/{num}])
	if len(path) >= 14 && string(path[:14]) == "/api/v3/repos/" && bytes.IndexByte(path, '%') < 0 {
		afterRepos := path[14:]
		if len(afterRepos) > 0 && afterRepos[len(afterRepos)-1] == '/' {
			afterRepos = afterRepos[:len(afterRepos)-1]
		}
		s1 := bytes.IndexByte(afterRepos, '/')
		if s1 > 0 {
			owner := afterRepos[:s1]
			rem := afterRepos[s1+1:]
			s2 := bytes.IndexByte(rem, '/')
			if s2 > 0 {
				repoName := rem[:s2]
				tail := rem[s2+1:]
				if len(tail) >= 6 && string(tail[:6]) == "issues" {
					// GET /api/v3/repos/{owner}/{repo}/issues/{number}
					if len(method) == 3 && string(method) == "GET" && len(tail) > 7 && tail[6] == '/' {
						numBytes := tail[7:]
						if bytes.IndexByte(numBytes, '/') < 0 {
							if num, ok := parsePositiveIntBytes(numBytes); ok {
								if !forge.CheckAuthBytes(authVal) {
									return unauthHTTP401, wbuf, true
								}
								repo := forge.GetRepoBytes(owner, repoName)
								if repo == nil {
									return notFoundHTTP, wbuf, true
								}
								iss := repo.GetIssue(num)
								if iss == nil {
									return notFoundHTTP, wbuf, true
								}
								return iss.HTTP200, wbuf, true
							}
						}
					}
					// POST /api/v3/repos/{owner}/{repo}/issues
					if len(method) == 4 && string(method) == "POST" && len(tail) == 6 {
						if !forge.CheckAuthBytes(authVal) {
							return unauthHTTP401, wbuf, true
						}
						repo := forge.GetRepoBytes(owner, repoName)
						if repo == nil {
							return notFoundHTTP, wbuf, true
						}
						trimmed := bytes.TrimSpace(body)
						if title, issBody, ok := tryParseSimpleIssueBytes(trimmed); ok {
							iss, err := repo.CreateIssue(title, issBody, nil)
							if err != nil {
								return nil, wbuf, false
							}
							wbuf = append(wbuf[:0], "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nX-GitHub-Request-Id: i"...)
							wbuf = strconv.AppendInt(wbuf, iss.ID, 10)
							wbuf = append(wbuf, "\r\nContent-Length: "...)
							wbuf = strconv.AppendInt(wbuf, int64(len(iss.RestJSON)), 10)
							wbuf = append(wbuf, "\r\n\r\n"...)
							wbuf = append(wbuf, iss.RestJSON...)
							return wbuf, wbuf, true
						}
						return nil, wbuf, false
					}
				}
			}
		}
		return nil, wbuf, false
	}

	// Hot Path 3: GET /healthz
	if len(path) == 8 && string(path) == "/healthz" && len(method) == 3 && string(method) == "GET" {
		return healthzHTTP200, wbuf, true
	}

	// Skip /api/... and /mcp quickly (they belong to REST/GraphQL/MCP fallback)
	if len(path) >= 4 && (string(path[:4]) == "/api" || string(path[:4]) == "/mcp") {
		return nil, wbuf, false
	}

	// Hot Path 4, 5, 6: Git Smart HTTP (/{owner}/{repo}.git/...)
	if len(path) >= 7 && path[0] == '/' {
		rest := path[1:]
		slash1 := bytes.IndexByte(rest, '/')
		if slash1 <= 0 {
			return nil, wbuf, false
		}
		owner := rest[:slash1]
		rest2 := rest[slash1+1:]
		slash2 := bytes.IndexByte(rest2, '/')
		if slash2 <= 4 {
			return nil, wbuf, false
		}
		repoSeg := rest2[:slash2]
		if string(repoSeg[len(repoSeg)-4:]) != ".git" {
			return nil, wbuf, false
		}
		repoName := repoSeg[:len(repoSeg)-4]
		subpath := rest2[slash2:]

		if !checkGitAuthBytes(forge, authVal) {
			return unauthHTTP401, wbuf, true
		}
		repo := forge.GetRepoBytes(owner, repoName)
		if repo == nil {
			return repoNotFound, wbuf, true
		}

		// GET /{owner}/{repo}.git/info/refs?service=...
		if len(method) == 3 && string(method) == "GET" && string(subpath) == "/info/refs" {
			switch string(rawQuery) {
			case "service=git-upload-pack":
				if isProtocolV2Bytes(gitProto) {
					return store.UploadV2CapsHTTP200, wbuf, true
				}
				return repo.Git().UploadV0HTTP200(), wbuf, true
			case "service=git-receive-pack":
				wbuf = repo.Git().AppendReceiveV0HTTP200(wbuf[:0])
				return wbuf, wbuf, true
			default:
				return nil, wbuf, false
			}
		}

		// POST /{owner}/{repo}.git/git-upload-pack
		if len(method) == 4 && string(method) == "POST" && string(subpath) == "/git-upload-pack" {
			return s.fastUploadPackResp(wbuf, repo, gitProto, body)
		}

		// POST /{owner}/{repo}.git/git-receive-pack
		if len(method) == 4 && string(method) == "POST" && string(subpath) == "/git-receive-pack" {
			return s.fastReceivePackResp(wbuf, repo, body)
		}
	}

	return nil, wbuf, false
}

func (s *FastServer) fastUploadPackResp(wbuf []byte, repo *store.Repository, gitProto, body []byte) ([]byte, []byte, bool) {
	var pktStack [16]gitobj.Packet
	pkts, _, err := gitobj.DecodePktLinesInto(body, 0, pktStack[:0])
	if err != nil || len(pkts) == 0 {
		return nil, wbuf, false
	}
	v2 := isProtocolV2Bytes(gitProto)
	if !v2 && pkts[0].Kind == gitobj.PktData && len(pkts[0].Payload) >= 8 && string(pkts[0].Payload[:8]) == "command=" {
		v2 = true
	}
	if !v2 {
		return nil, wbuf, false
	}

	var command []byte
	idx := 0
	for idx < len(pkts) {
		p := pkts[idx]
		idx++
		if p.Kind == gitobj.PktDelim || p.Kind == gitobj.PktFlush {
			break
		}
		if p.Kind != gitobj.PktData {
			continue
		}
		raw := gitobj.TrimLF(p.Payload)
		if len(raw) > 8 && string(raw[:8]) == "command=" {
			command = raw[8:]
		}
	}

	switch string(command) {
	case "ls-refs":
		var stackPrefixes [8]string
		args := store.LsRefsArgs{RefPrefixes: stackPrefixes[:0]}
		for idx < len(pkts) {
			p := pkts[idx]
			idx++
			if p.Kind == gitobj.PktFlush || p.Kind == gitobj.PktResponseEnd {
				break
			}
			if p.Kind != gitobj.PktData {
				continue
			}
			raw := gitobj.TrimLF(p.Payload)
			switch {
			case string(raw) == "peel":
				args.Peel = true
			case string(raw) == "symrefs":
				args.Symrefs = true
			case string(raw) == "unborn":
				args.Unborn = true
			case len(raw) > 11 && string(raw[:11]) == "ref-prefix ":
				args.RefPrefixes = append(args.RefPrefixes, string(raw[11:]))
			}
		}
		if cached := repo.LsRefsHTTP200(args); cached != nil {
			return cached, wbuf, true
		}
		out := repo.LsRefs(args)
		wbuf = appendGitHTTP200(wbuf[:0], "application/x-git-upload-pack-result", out)
		return wbuf, wbuf, true

	case "fetch":
		var stackWants [4]gitobj.OID
		var stackHaves [8]gitobj.OID
		var stackShallow [4]gitobj.OID
		fArgs := store.FetchArgs{
			Protocol:      2,
			Wants:         stackWants[:0],
			Haves:         stackHaves[:0],
			ClientShallow: stackShallow[:0],
			Sideband:      true,
		}
		for idx < len(pkts) {
			p := pkts[idx]
			idx++
			if p.Kind == gitobj.PktFlush || p.Kind == gitobj.PktResponseEnd {
				break
			}
			if p.Kind != gitobj.PktData {
				continue
			}
			raw := gitobj.TrimLF(p.Payload)
			switch {
			case len(raw) > 5 && string(raw[:5]) == "want ":
				oid, ok := gitobj.ParseOIDBytes(bytes.TrimSpace(raw[5:]))
				if !ok {
					return nil, wbuf, false
				}
				fArgs.Wants = append(fArgs.Wants, oid)
			case len(raw) > 5 && string(raw[:5]) == "have ":
				oid, ok := gitobj.ParseOIDBytes(bytes.TrimSpace(raw[5:]))
				if !ok {
					return nil, wbuf, false
				}
				fArgs.Haves = append(fArgs.Haves, oid)
			case len(raw) > 8 && string(raw[:8]) == "shallow ":
				oid, ok := gitobj.ParseOIDBytes(bytes.TrimSpace(raw[8:]))
				if !ok {
					return nil, wbuf, false
				}
				fArgs.ClientShallow = append(fArgs.ClientShallow, oid)
			case len(raw) > 7 && string(raw[:7]) == "deepen ":
				d, ok := parseNonNegIntBytes(bytes.TrimSpace(raw[7:]))
				if !ok {
					return nil, wbuf, false
				}
				fArgs.Depth = d
			case string(raw) == "done":
				fArgs.Done = true
			}
		}
		if len(fArgs.Wants) == 0 {
			return nil, wbuf, false
		}
		if cached := repo.FetchDepth1HTTP200(fArgs); cached != nil {
			return cached, wbuf, true
		}
		out, ferr := repo.FetchUploadPack(fArgs)
		if ferr != nil {
			return nil, wbuf, false
		}
		wbuf = appendGitHTTP200(wbuf[:0], "application/x-git-upload-pack-result", out)
		return wbuf, wbuf, true
	}

	return nil, wbuf, false
}

func (s *FastServer) fastReceivePackResp(wbuf []byte, repo *store.Repository, body []byte) ([]byte, []byte, bool) {
	var pktStack [8]gitobj.Packet
	pkts, packOffset, err := gitobj.DecodePktLinesInto(body, 1, pktStack[:0])
	if err != nil {
		return nil, wbuf, false
	}

	var (
		cmdStack       [4]store.PushCommand
		cmds           = cmdStack[:0]
		reportStatus   bool
		reportStatusV2 bool
		sideband       bool
		atomicPush     bool
		sawFlush       bool
	)

	for i, p := range pkts {
		if p.Kind == gitobj.PktFlush {
			sawFlush = true
			break
		}
		if p.Kind != gitobj.PktData {
			continue
		}
		rawLine := gitobj.TrimLF(p.Payload)
		if i == 0 {
			if nul := bytes.IndexByte(rawLine, 0); nul >= 0 {
				caps := rawLine[nul+1:]
				rawLine = rawLine[:nul]
				for len(caps) > 0 {
					for len(caps) > 0 && caps[0] == ' ' {
						caps = caps[1:]
					}
					if len(caps) == 0 {
						break
					}
					sp := bytes.IndexByte(caps, ' ')
					var tok []byte
					if sp >= 0 {
						tok = caps[:sp]
						caps = caps[sp+1:]
					} else {
						tok = caps
						caps = nil
					}
					switch string(tok) {
					case "report-status":
						reportStatus = true
					case "report-status-v2":
						reportStatus = true
						reportStatusV2 = true
					case "side-band-64k", "side-band":
						sideband = true
					case "atomic":
						atomicPush = true
					}
				}
			}
		}
		if len(rawLine) < 83 || rawLine[40] != ' ' || rawLine[81] != ' ' {
			return nil, wbuf, false
		}
		oldOID, ok1 := gitobj.ParseOIDBytes(rawLine[0:40])
		newOID, ok2 := gitobj.ParseOIDBytes(rawLine[41:81])
		refBytes := rawLine[82:]
		if !ok1 || !ok2 || len(refBytes) == 0 {
			return nil, wbuf, false
		}
		var refName string
		if string(refBytes) == "refs/heads/main" {
			refName = "refs/heads/main"
		} else {
			refName = string(refBytes)
		}
		cmds = append(cmds, store.PushCommand{
			OldOID: oldOID,
			NewOID: newOID,
			Ref:    refName,
		})
	}
	if !sawFlush {
		return nil, wbuf, false
	}

	packBytes := body[packOffset:]
	var statusStack [4]store.RefStatus
	unpackErr, statuses := repo.ReceivePackInto(cmds, packBytes, atomicPush, statusStack[:0])

	// Zero-copy static response fast path for successful single-ref main push with sideband report-status
	if reportStatus && sideband && !reportStatusV2 && unpackErr == "" &&
		len(statuses) == 1 && statuses[0].OK && statuses[0].Ref == "refs/heads/main" {
		return receivePackMainOKSidebandHTTP200, wbuf, true
	}

	if !reportStatus {
		if sideband {
			wbuf = appendGitHTTP200(wbuf[:0], "application/x-git-receive-pack-result", []byte("0000"))
		} else {
			wbuf = appendGitHTTP200(wbuf[:0], "application/x-git-receive-pack-result", nil)
		}
		return wbuf, wbuf, true
	}

	var repStack [384]byte
	report := repStack[:0]
	var lineBuf [192]byte
	if unpackErr != "" {
		b := append(lineBuf[:0], "unpack "...)
		b = append(b, unpackErr...)
		b = append(b, '\n')
		report = gitobj.AppendPktLine(report, b)
		for _, c := range cmds {
			b = append(lineBuf[:0], "ng "...)
			b = append(b, c.Ref...)
			b = append(b, " unpacker error\n"...)
			report = gitobj.AppendPktLine(report, b)
		}
	} else {
		report = gitobj.AppendPktString(report, "unpack ok\n")
		for _, st := range statuses {
			if st.OK {
				b := append(lineBuf[:0], "ok "...)
				b = append(b, st.Ref...)
				b = append(b, '\n')
				report = gitobj.AppendPktLine(report, b)
				if reportStatusV2 {
					b = append(lineBuf[:0], "option refname "...)
					b = append(b, st.Ref...)
					b = append(b, '\n')
					report = gitobj.AppendPktLine(report, b)

					b = append(lineBuf[:0], "option old-oid "...)
					b = st.OldOID.AppendHex(b)
					b = append(b, '\n')
					report = gitobj.AppendPktLine(report, b)

					b = append(lineBuf[:0], "option new-oid "...)
					b = st.NewOID.AppendHex(b)
					b = append(b, '\n')
					report = gitobj.AppendPktLine(report, b)
				}
			} else {
				reason := st.Reason
				if reason == "" {
					reason = "failed"
				}
				b := append(lineBuf[:0], "ng "...)
				b = append(b, st.Ref...)
				b = append(b, ' ')
				b = append(b, reason...)
				b = append(b, '\n')
				report = gitobj.AppendPktLine(report, b)
			}
		}
	}
	report = append(report, "0000"...)

	var respBody []byte
	if sideband {
		var sbStack [448]byte
		if len(report)+16 <= len(sbStack) {
			respBody = sbStack[:0]
		} else {
			respBody = make([]byte, 0, len(report)+16)
		}
		respBody = gitobj.AppendSidebandData(respBody, 1, report)
		respBody = append(respBody, "0000"...)
	} else {
		respBody = report
	}

	wbuf = appendGitHTTP200(wbuf[:0], "application/x-git-receive-pack-result", respBody)
	return wbuf, wbuf, true
}

// fastResponseWriter bridges fallback Router.ServeHTTP calls onto the same TCP connection.
type fastResponseWriter struct {
	hdr         http.Header
	status      int
	wroteHeader bool
	body        bytes.Buffer
}

func (rw *fastResponseWriter) Header() http.Header {
	if rw.hdr == nil {
		rw.hdr = make(http.Header, 4)
	}
	return rw.hdr
}

func (rw *fastResponseWriter) WriteHeader(code int) {
	if rw.wroteHeader {
		return
	}
	rw.status = code
	rw.wroteHeader = true
}

func (rw *fastResponseWriter) Write(p []byte) (int, error) {
	if !rw.wroteHeader {
		rw.WriteHeader(http.StatusOK)
	}
	return rw.body.Write(p)
}

func (s *FastServer) buildFallbackResponse(
	wbuf []byte,
	method, target, proto, hdrLines, hostVal, body []byte,
	closeConn bool,
	remoteAddr string,
) ([]byte, []byte) {
	u, err := url.ParseRequestURI(string(target))
	if err != nil {
		return badReqHTTP400, wbuf
	}

	hdr := make(http.Header, 8)
	scan := hdrLines
	for len(scan) > 0 {
		var line []byte
		if nl := bytes.Index(scan, crlfBytes); nl >= 0 {
			line = scan[:nl]
			scan = scan[nl+2:]
		} else {
			line = scan
			scan = nil
		}
		if len(line) == 0 {
			continue
		}
		if colon := bytes.IndexByte(line, ':'); colon > 0 {
			k := strings.TrimSpace(string(line[:colon]))
			v := string(trimOWS(line[colon+1:]))
			hdr.Add(k, v)
		}
	}

	protoMajor, protoMinor := 1, 1
	if bytes.Equal(proto, http10Bytes) {
		protoMinor = 0
	}

	req := &http.Request{
		Method:        string(method),
		URL:           u,
		Proto:         string(proto),
		ProtoMajor:    protoMajor,
		ProtoMinor:    protoMinor,
		Header:        hdr,
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Host:          string(hostVal),
		RequestURI:    string(target),
		RemoteAddr:    remoteAddr,
	}

	var rw fastResponseWriter
	s.Router.ServeHTTP(&rw, req)

	status := rw.status
	if !rw.wroteHeader {
		status = http.StatusOK
	}
	statusText := http.StatusText(status)
	respBytes := rw.body.Bytes()
	suppressBody := status == http.StatusNoContent || status == http.StatusNotModified || (status >= 100 && status < 200) || req.Method == http.MethodHead

	wbuf = append(wbuf[:0], "HTTP/1.1 "...)
	wbuf = strconv.AppendInt(wbuf, int64(status), 10)
	wbuf = append(wbuf, ' ')
	wbuf = append(wbuf, statusText...)
	wbuf = append(wbuf, "\r\n"...)

	hasCL := false
	for k, vals := range rw.hdr {
		if strings.EqualFold(k, "Content-Length") {
			hasCL = true
		}
		if strings.EqualFold(k, "Transfer-Encoding") {
			continue
		}
		for _, v := range vals {
			wbuf = append(wbuf, k...)
			wbuf = append(wbuf, ": "...)
			wbuf = append(wbuf, v...)
			wbuf = append(wbuf, "\r\n"...)
		}
	}
	if !hasCL && status != http.StatusNoContent && status != http.StatusNotModified {
		wbuf = append(wbuf, "Content-Length: "...)
		wbuf = strconv.AppendInt(wbuf, int64(len(respBytes)), 10)
		wbuf = append(wbuf, "\r\n"...)
	}
	if closeConn {
		wbuf = append(wbuf, "Connection: close\r\n"...)
	}
	wbuf = append(wbuf, "\r\n"...)
	if !suppressBody {
		wbuf = append(wbuf, respBytes...)
	}
	return wbuf, wbuf
}

// --- low-level zero-alloc helpers ---

func writeAll(conn net.Conn, b []byte) (int, error) {
	total := 0
	for total < len(b) {
		n, err := conn.Write(b[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func appendGitHTTP200(dst []byte, contentType string, body []byte) []byte {
	dst = append(dst, "HTTP/1.1 200 OK\r\nContent-Type: "...)
	dst = append(dst, contentType...)
	dst = append(dst, "\r\nCache-Control: no-cache\r\nContent-Length: "...)
	dst = strconv.AppendInt(dst, int64(len(body)), 10)
	dst = append(dst, "\r\n\r\n"...)
	return append(dst, body...)
}

func trimOWS(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		b = b[1:]
	}
	for len(b) > 0 && (b[len(b)-1] == ' ' || b[len(b)-1] == '\t') {
		b = b[:len(b)-1]
	}
	return b
}

func equalFoldASCII(b []byte, lower string) bool {
	if len(b) != len(lower) {
		return false
	}
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != lower[i] {
			return false
		}
	}
	return true
}

func containsTokenFoldASCII(val []byte, lower string) bool {
	for len(val) > 0 {
		var tok []byte
		if comma := bytes.IndexByte(val, ','); comma >= 0 {
			tok = trimOWS(val[:comma])
			val = val[comma+1:]
		} else {
			tok = trimOWS(val)
			val = nil
		}
		if equalFoldASCII(tok, lower) {
			return true
		}
	}
	return false
}

func parseContentLength(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var n int64
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		if n > (fastMaxBodyBytes / 10) {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	return n, true
}

func parsePositiveIntBytes(b []byte) (int, bool) {
	if len(b) == 0 {
		return 0, false
	}
	n := 0
	for i := 0; i < len(b); i++ {
		c := b[i]
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

func parseNonNegIntBytes(b []byte) (int, bool) {
	if len(b) == 0 {
		return 0, false
	}
	n := 0
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
		if n > 1_000_000_000 {
			return 0, false
		}
	}
	return n, true
}

func isProtocolV2Bytes(gp []byte) bool {
	if len(gp) == 0 {
		return false
	}
	if string(gp) == "version=2" {
		return true
	}
	for len(gp) > 0 {
		var part []byte
		if idx := bytes.IndexByte(gp, ':'); idx >= 0 {
			part = gp[:idx]
			gp = gp[idx+1:]
		} else {
			part = gp
			gp = nil
		}
		if string(trimOWS(part)) == "version=2" {
			return true
		}
	}
	return false
}

func checkGitAuthBytes(f *store.Forge, auth []byte) bool {
	if len(f.TokenBytes) == 0 {
		return true
	}
	if len(auth) == 0 {
		return false
	}
	if f.CheckAuthBytes(auth) {
		return true
	}
	// Basic auth fallback for native git CLI interop
	if len(auth) > 6 && equalFoldASCII(auth[:6], "basic ") {
		decoded, err := base64.StdEncoding.DecodeString(string(auth[6:]))
		if err != nil {
			return false
		}
		if colon := bytes.IndexByte(decoded, ':'); colon >= 0 {
			user := decoded[:colon]
			pass := decoded[colon+1:]
			return bytes.Equal(pass, f.TokenBytes) || bytes.Equal(user, f.TokenBytes)
		}
	}
	return false
}

func tryParseSimpleIssueBytes(b []byte) (title, body string, ok bool) {
	if len(b) < 2 || b[0] != '{' || b[len(b)-1] != '}' {
		return "", "", false
	}
	i := 1
	skipWS := func() {
		for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
			i++
		}
	}
	readPlainString := func() ([]byte, bool) {
		if i >= len(b) || b[i] != '"' {
			return nil, false
		}
		i++
		start := i
		for i < len(b) {
			c := b[i]
			if c == '\\' || c < 0x20 {
				return nil, false
			}
			if c == '"' {
				s := b[start:i]
				i++
				return s, true
			}
			i++
		}
		return nil, false
	}

	var titleBytes, bodyBytes []byte
	var sawTitle bool
	firstField := true
	for {
		skipWS()
		if firstField && i < len(b) && b[i] == '}' {
			return string(titleBytes), string(bodyBytes), sawTitle
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
		switch string(key) {
		case "title":
			titleBytes = val
			sawTitle = true
		case "body":
			bodyBytes = val
		default:
			return "", "", false
		}
		skipWS()
		if i < len(b) && b[i] == ',' {
			i++
			continue
		}
		if i == len(b)-1 && b[i] == '}' {
			if !sawTitle {
				return "", "", false
			}
			// Combine title + body into one single string allocation
			var stack [512]byte
			tl := len(titleBytes)
			bl := len(bodyBytes)
			var combined string
			if tl+bl <= len(stack) {
				copy(stack[:tl], titleBytes)
				copy(stack[tl:tl+bl], bodyBytes)
				combined = string(stack[:tl+bl])
			} else {
				buf := make([]byte, tl+bl)
				copy(buf[:tl], titleBytes)
				copy(buf[tl:], bodyBytes)
				combined = string(buf)
			}
			return combined[:tl], combined[tl:], true
		}
		return "", "", false
	}
}

func readChunkedBody(conn net.Conn, rbuf []byte, pos, end int) ([]byte, int, int, []byte, error) {
	var out []byte
	ensureLine := func() ([]byte, error) {
		for {
			if idx := bytes.Index(rbuf[pos:end], crlfBytes); idx >= 0 {
				line := rbuf[pos : pos+idx]
				pos += idx + 2
				return line, nil
			}
			if pos > 0 {
				n := copy(rbuf, rbuf[pos:end])
				pos = 0
				end = n
			}
			if end == len(rbuf) {
				bigger := make([]byte, len(rbuf)*2)
				copy(bigger, rbuf[:end])
				rbuf = bigger
			}
			n, err := conn.Read(rbuf[end:])
			if n > 0 {
				end += n
				continue
			}
			if err != nil {
				return nil, err
			}
		}
	}

	for {
		line, err := ensureLine()
		if err != nil {
			return nil, pos, end, rbuf, err
		}
		if semi := bytes.IndexByte(line, ';'); semi >= 0 {
			line = line[:semi]
		}
		line = trimOWS(line)
		sz, err := strconv.ParseInt(string(line), 16, 64)
		if err != nil || sz < 0 || int64(len(out))+sz > fastMaxBodyBytes {
			return nil, pos, end, rbuf, errors.New("invalid chunk size")
		}
		if sz == 0 {
			// Consume trailer headers up to empty CRLF line
			for {
				tline, err := ensureLine()
				if err != nil {
					return nil, pos, end, rbuf, err
				}
				if len(tline) == 0 {
					return out, pos, end, rbuf, nil
				}
			}
		}
		need := int(sz) + 2 // payload + trailing CRLF
		for end-pos < need {
			if pos > 0 {
				n := copy(rbuf, rbuf[pos:end])
				pos = 0
				end = n
			}
			if len(rbuf) < need {
				bigger := make([]byte, maxInt(len(rbuf)*2, need))
				copy(bigger, rbuf[:end])
				rbuf = bigger
			}
			n, err := conn.Read(rbuf[end:])
			if n > 0 {
				end += n
			}
			if err != nil && end-pos < need {
				return nil, pos, end, rbuf, err
			}
		}
		if rbuf[pos+int(sz)] != '\r' || rbuf[pos+int(sz)+1] != '\n' {
			return nil, pos, end, rbuf, errors.New("missing chunk CRLF")
		}
		out = append(out, rbuf[pos:pos+int(sz)]...)
		pos += need
	}
}

func reslice(dst, src []byte, delta int, sub []byte) []byte {
	if len(sub) == 0 {
		return nil
	}
	// Compute byte offset of sub inside src using capacity arithmetic without unsafe
	off := cap(src) - cap(sub)
	start := off - delta
	if start < 0 || start+len(sub) > len(dst) {
		return sub
	}
	return dst[start : start+len(sub)]
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
