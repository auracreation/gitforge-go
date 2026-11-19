package githttp

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"gitforge/internal/gitobj"
	"gitforge/internal/store"
)

const maxGitBodyBytes = 64 * 1024 * 1024

var bodyBufPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 16*1024))
	},
}

func readRequestBody(r *http.Request) ([]byte, *bytes.Buffer, error) {
	var reader io.Reader = io.LimitReader(r.Body, maxGitBodyBytes+1)
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(reader)
		if err != nil {
			return nil, nil, err
		}
		defer gz.Close()
		reader = io.LimitReader(gz, maxGitBodyBytes+1)
	}
	buf := bodyBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	if _, err := buf.ReadFrom(reader); err != nil {
		if buf.Cap() <= 512*1024 {
			bodyBufPool.Put(buf)
		}
		return nil, nil, err
	}
	if buf.Len() > maxGitBodyBytes {
		return nil, nil, io.ErrShortBuffer
	}
	return buf.Bytes(), buf, nil
}

func releaseBodyBuf(buf *bytes.Buffer) {
	if buf != nil && buf.Cap() <= 512*1024 {
		buf.Reset()
		bodyBufPool.Put(buf)
	}
}

// Handler serves Git Smart HTTP v0 and v2 endpoints at /{owner}/{repo}.git/...
type Handler struct {
	Forge *store.Forge
}

func NewHandler(f *store.Forge) *Handler {
	return &Handler{Forge: f}
}

func checkGitAuth(f *store.Forge, r *http.Request) bool {
	if f.Token == "" {
		return true
	}
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return false
	}
	if f.CheckAuth(auth) {
		return true
	}
	// Also accept Basic auth where username or password equals token (for native git CLI testing)
	if user, pass, ok := r.BasicAuth(); ok {
		return pass == f.Token || user == f.Token
	}
	return false
}

func writeGitJSONError(f *store.Forge, w http.ResponseWriter, status int, msg string) {
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-GitHub-Request-Id", f.NextRequestID())
	w.WriteHeader(status)
	var b [128]byte
	out := append(b[:0], `{"message":`...)
	out = store.AppendJSONString(out, msg)
	out = append(out, "}\n"...)
	_, _ = w.Write(out)
}

var (
	ctUploadAdv     = []string{"application/x-git-upload-pack-advertisement"}
	ctReceiveAdv    = []string{"application/x-git-receive-pack-advertisement"}
	ctUploadResult  = []string{"application/x-git-upload-pack-result"}
	ctReceiveResult = []string{"application/x-git-receive-pack-result"}
	ctJSON          = []string{"application/json; charset=utf-8"}
	ccNoCache       = []string{"no-cache"}
)

func isProtocolV2(r *http.Request) bool {
	gp := r.Header.Get("Git-Protocol")
	if gp == "" {
		return false
	}
	if gp == "version=2" {
		return true
	}
	for len(gp) > 0 {
		var part string
		if idx := strings.IndexByte(gp, ':'); idx >= 0 {
			part = gp[:idx]
			gp = gp[idx+1:]
		} else {
			part = gp
			gp = ""
		}
		if strings.TrimSpace(part) == "version=2" {
			return true
		}
	}
	return false
}

// TryServe checks whether r.URL.Path matches /{owner}/{repo}.git/... and handles it if so.
func (h *Handler) TryServe(w http.ResponseWriter, r *http.Request) bool {
	p := r.URL.Path
	if len(p) < 6 || p[0] != '/' {
		return false
	}
	rest := p[1:]
	slash1 := strings.IndexByte(rest, '/')
	if slash1 <= 0 {
		return false
	}
	owner := rest[:slash1]
	rest2 := rest[slash1+1:]
	slash2 := strings.IndexByte(rest2, '/')
	var repoSeg, subpath string
	if slash2 < 0 {
		repoSeg = rest2
		subpath = ""
	} else {
		repoSeg = rest2[:slash2]
		subpath = rest2[slash2:]
	}
	if !strings.HasSuffix(repoSeg, ".git") || len(repoSeg) <= 4 {
		return false
	}
	repoName := repoSeg[:len(repoSeg)-4]

	if !checkGitAuth(h.Forge, r) {
		writeGitJSONError(h.Forge, w, http.StatusUnauthorized, "Requires authentication")
		return true
	}

	repo := h.Forge.GetRepo(owner, repoName)
	if repo == nil {
		writeGitJSONError(h.Forge, w, http.StatusNotFound, "Repository not found")
		return true
	}

	switch {
	case r.Method == http.MethodGet && subpath == "/info/refs":
		h.handleInfoRefs(w, r, repo)
	case r.Method == http.MethodPost && subpath == "/git-upload-pack":
		h.handleUploadPack(w, r, repo)
	case r.Method == http.MethodPost && subpath == "/git-receive-pack":
		h.handleReceivePack(w, r, repo)
	default:
		writeGitJSONError(h.Forge, w, http.StatusNotFound, "Not Found")
	}
	return true
}

func (h *Handler) handleInfoRefs(w http.ResponseWriter, r *http.Request, repo *store.Repository) {
	// Zero-allocation fast path for standard ?service=git-upload-pack / ?service=git-receive-pack queries
	var service string
	switch r.URL.RawQuery {
	case "service=git-upload-pack":
		service = "git-upload-pack"
	case "service=git-receive-pack":
		service = "git-receive-pack"
	default:
		service = r.URL.Query().Get("service")
	}

	hdr := w.Header()
	hdr["Cache-Control"] = ccNoCache
	switch service {
	case "git-upload-pack":
		hdr["Content-Type"] = ctUploadAdv
		w.WriteHeader(http.StatusOK)
		if isProtocolV2(r) {
			_, _ = w.Write(store.UploadV2Capabilities)
		} else {
			_, _ = w.Write(repo.Git().UploadV0Advertisement())
		}
	case "git-receive-pack":
		hdr["Content-Type"] = ctReceiveAdv
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(repo.Git().ReceiveV0Advertisement())
	default:
		writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Unsupported git service")
	}
}

func (h *Handler) handleUploadPack(w http.ResponseWriter, r *http.Request, repo *store.Repository) {
	body, buf, err := readRequestBody(r)
	defer releaseBodyBuf(buf)
	if err != nil {
		writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Failed to read request body")
		return
	}

	var packets [16]gitobj.Packet
	pkts, _, err := gitobj.DecodePktLines(packets[:0], body, 0)
	if err != nil {
		writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Malformed pkt-line stream")
		return
	}
	if len(pkts) == 0 {
		writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Empty upload-pack request")
		return
	}

	// Detect Protocol v2 either by Git-Protocol header or "command=" first packet
	v2 := isProtocolV2(r)
	if !v2 && pkts[0].Kind == gitobj.PktData && bytes.HasPrefix(pkts[0].Payload, []byte("command=")) {
		v2 = true
	}

	if v2 {
		h.serveUploadPackV2(w, repo, pkts)
		return
	}
	h.serveUploadPackV0(w, repo, pkts)
}

func (h *Handler) serveUploadPackV2(w http.ResponseWriter, repo *store.Repository, pkts []gitobj.Packet) {
	var command string
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
			command = string(raw[8:])
		}
	}

	var stackArgLines [16][]byte
	argLines := stackArgLines[:0]
	for idx < len(pkts) {
		p := pkts[idx]
		idx++
		if p.Kind == gitobj.PktFlush || p.Kind == gitobj.PktResponseEnd {
			break
		}
		if p.Kind == gitobj.PktData {
			argLines = append(argLines, gitobj.TrimLF(p.Payload))
		}
	}

	switch command {
	case "ls-refs":
		var stackPrefixes [8]string
		args := store.LsRefsArgs{
			RefPrefixes: stackPrefixes[:0],
		}
		for _, raw := range argLines {
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
		out := repo.LsRefs(args)
		hdr := w.Header()
		hdr["Content-Type"] = ctUploadResult
		hdr["Cache-Control"] = ccNoCache
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(out)

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
		for _, raw := range argLines {
			switch {
			case len(raw) > 5 && string(raw[:5]) == "want ":
				oid, ok := gitobj.ParseOIDBytes(bytes.TrimSpace(raw[5:]))
				if !ok {
					writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Invalid want OID")
					return
				}
				fArgs.Wants = append(fArgs.Wants, oid)
			case len(raw) > 5 && string(raw[:5]) == "have ":
				oid, ok := gitobj.ParseOIDBytes(bytes.TrimSpace(raw[5:]))
				if !ok {
					writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Invalid have OID")
					return
				}
				fArgs.Haves = append(fArgs.Haves, oid)
			case len(raw) > 8 && string(raw[:8]) == "shallow ":
				oid, ok := gitobj.ParseOIDBytes(bytes.TrimSpace(raw[8:]))
				if !ok {
					writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Invalid shallow OID")
					return
				}
				fArgs.ClientShallow = append(fArgs.ClientShallow, oid)
			case len(raw) > 7 && string(raw[:7]) == "deepen ":
				d, err := strconv.Atoi(string(bytes.TrimSpace(raw[7:])))
				if err != nil || d < 0 {
					writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Invalid deepen value")
					return
				}
				fArgs.Depth = d
			case string(raw) == "done":
				fArgs.Done = true
			case string(raw) == "include-tag":
				fArgs.IncludeTag = true
			}
		}
		if len(fArgs.Wants) == 0 {
			writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Missing want lines")
			return
		}
		out, err := repo.FetchUploadPack(fArgs)
		if err != nil {
			writeGitJSONError(h.Forge, w, http.StatusBadRequest, err.Error())
			return
		}
		hdr := w.Header()
		hdr["Content-Type"] = ctUploadResult
		hdr["Cache-Control"] = ccNoCache
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(out)

	default:
		writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Unknown upload-pack v2 command")
	}
}

func (h *Handler) serveUploadPackV0(w http.ResponseWriter, repo *store.Repository, pkts []gitobj.Packet) {
	fArgs := store.FetchArgs{
		Protocol: 0,
	}
	parsedCaps := false
	firstFlush := false
	for _, p := range pkts {
		if p.Kind == gitobj.PktFlush {
			if firstFlush {
				fArgs.Negotiate = true
			}
			firstFlush = true
			continue
		}
		if p.Kind != gitobj.PktData {
			continue
		}
		if firstFlush {
			fArgs.Negotiate = true
		}
		line := string(gitobj.TrimLF(p.Payload))
		if !parsedCaps {
			parsedCaps = true
			var capStr string
			if nul := strings.IndexByte(line, 0); nul >= 0 {
				capStr = line[nul+1:]
				line = line[:nul]
			} else if strings.HasPrefix(line, "want ") && len(line) > 46 && line[45] == ' ' {
				capStr = line[46:]
				line = line[:45]
			}
			for _, capTok := range strings.Fields(capStr) {
				if capTok == "side-band-64k" || capTok == "side-band" {
					fArgs.Sideband = true
				} else if capTok == "include-tag" {
					fArgs.IncludeTag = true
				}
			}
		}
		switch {
		case strings.HasPrefix(line, "want "):
			oid, ok := gitobj.ParseOID(strings.TrimSpace(line[5:]))
			if !ok {
				writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Invalid want OID")
				return
			}
			fArgs.Wants = append(fArgs.Wants, oid)
		case strings.HasPrefix(line, "shallow "):
			oid, ok := gitobj.ParseOID(strings.TrimSpace(line[8:]))
			if !ok {
				writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Invalid shallow OID")
				return
			}
			fArgs.ClientShallow = append(fArgs.ClientShallow, oid)
		case strings.HasPrefix(line, "deepen "):
			if d, err := strconv.Atoi(strings.TrimSpace(line[7:])); err == nil && d > 0 {
				fArgs.Depth = d
			}
		case strings.HasPrefix(line, "have "):
			if oid, ok := gitobj.ParseOID(strings.TrimSpace(line[5:])); ok {
				fArgs.Haves = append(fArgs.Haves, oid)
			}
		case line == "done":
			fArgs.Done = true
		}
	}

	if len(fArgs.Wants) == 0 {
		// Empty negotiation probe
		w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("0000"))
		return
	}
	out, err := repo.FetchUploadPack(fArgs)
	if err != nil {
		writeGitJSONError(h.Forge, w, http.StatusBadRequest, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

func (h *Handler) handleReceivePack(w http.ResponseWriter, r *http.Request, repo *store.Repository) {
	body, buf, err := readRequestBody(r)
	defer releaseBodyBuf(buf)
	if err != nil {
		writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Failed to read receive-pack body")
		return
	}

	var packets [16]gitobj.Packet
	pkts, packOffset, err := gitobj.DecodePktLines(packets[:0], body, 1)
	if err != nil {
		writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Malformed receive-pack command stream")
		return
	}

	var (
		stackCmds      [4]store.PushCommand
		cmds           = stackCmds[:0]
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
		// Expected format: "<40-hex old> <40-hex new> <refname>"
		if len(rawLine) < 83 || rawLine[40] != ' ' || rawLine[81] != ' ' {
			writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Malformed receive-pack command line")
			return
		}
		oldOID, ok1 := gitobj.ParseOIDBytes(rawLine[0:40])
		newOID, ok2 := gitobj.ParseOIDBytes(rawLine[41:81])
		refName := string(rawLine[82:])
		if !ok1 || !ok2 || refName == "" {
			writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Invalid OID or ref in receive-pack command")
			return
		}
		cmds = append(cmds, store.PushCommand{
			OldOID: oldOID,
			NewOID: newOID,
			Ref:    refName,
		})
	}

	if !sawFlush {
		writeGitJSONError(h.Forge, w, http.StatusBadRequest, "Missing flush after receive-pack commands")
		return
	}

	packBytes := body[packOffset:]
	unpackErr, statuses := repo.ReceivePack(cmds, packBytes, atomicPush)

	hdr := w.Header()
	hdr["Content-Type"] = ctReceiveResult
	hdr["Cache-Control"] = ccNoCache
	w.WriteHeader(http.StatusOK)

	if !reportStatus {
		if sideband {
			_, _ = w.Write([]byte("0000"))
		}
		return
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

	if sideband {
		var outStack [448]byte
		var out []byte
		if len(report)+16 <= len(outStack) {
			out = outStack[:0]
		} else {
			out = make([]byte, 0, len(report)+16)
		}
		out = gitobj.AppendSidebandData(out, 1, report)
		out = append(out, "0000"...)
		_, _ = w.Write(out)
	} else {
		_, _ = w.Write(report)
	}
}
