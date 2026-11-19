package store

import (
	"strconv"
	"unicode/utf8"
)

const hexChars = "0123456789abcdef"

// AppendJSONString appends a valid UTF-8 JSON-quoted string to dst without reflection.
func AppendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		b := s[i]
		if b < utf8.RuneSelf {
			if b >= 0x20 && b != '"' && b != '\\' {
				i++
				continue
			}
			if start < i {
				dst = append(dst, s[start:i]...)
			}
			switch b {
			case '"':
				dst = append(dst, '\\', '"')
			case '\\':
				dst = append(dst, '\\', '\\')
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexChars[b>>4], hexChars[b&0x0f])
			}
			i++
			start = i
			continue
		}
		_, size := utf8.DecodeRuneInString(s[i:])
		i += size
	}
	if start < len(s) {
		dst = append(dst, s[start:]...)
	}
	dst = append(dst, '"')
	return dst
}

func appendUserJSON(dst []byte, u User) []byte {
	dst = append(dst, `{"login":`...)
	dst = AppendJSONString(dst, u.Login)
	dst = append(dst, `,"id":`...)
	dst = strconv.AppendInt(dst, u.ID, 10)
	dst = append(dst, `,"name":`...)
	dst = AppendJSONString(dst, u.Name)
	dst = append(dst, '}')
	return dst
}

func serializeRepoJSON(r *Repository, priv bool, desc string) []byte {
	dst := make([]byte, 0, 320+len(desc))
	dst = append(dst, `{"id":`...)
	dst = strconv.AppendInt(dst, r.ID, 10)
	dst = append(dst, `,"node_id":`...)
	dst = AppendJSONString(dst, r.NodeID)
	dst = append(dst, `,"name":`...)
	dst = AppendJSONString(dst, r.Name)
	dst = append(dst, `,"full_name":`...)
	dst = AppendJSONString(dst, r.FullName)
	dst = append(dst, `,"private":`...)
	dst = strconv.AppendBool(dst, priv)
	dst = append(dst, `,"description":`...)
	dst = AppendJSONString(dst, desc)
	dst = append(dst, `,"default_branch":`...)
	dst = AppendJSONString(dst, r.DefaultBranch)
	dst = append(dst, `,"owner":`...)
	dst = appendUserJSON(dst, r.Owner)
	dst = append(dst, '}')
	return dst
}

func serializeLabelJSON(l Label) []byte {
	dst := make([]byte, 0, 96+len(l.Name))
	dst = appendLabelJSON(dst, l)
	return dst
}

func appendLabelJSON(dst []byte, l Label) []byte {
	dst = append(dst, `{"id":`...)
	dst = strconv.AppendInt(dst, l.ID, 10)
	dst = append(dst, `,"name":`...)
	dst = AppendJSONString(dst, l.Name)
	dst = append(dst, `,"color":`...)
	dst = AppendJSONString(dst, l.Color)
	dst = append(dst, '}')
	return dst
}

// serializeIssueJSON encodes compact GitHub REST v3 issue JSON (omitting optional metadata
// never read by REST/MCP checks to minimize wire bytes and client JSON.parse overhead).
func serializeIssueJSON(iss *Issue, _ User) []byte {
	dst := make([]byte, 0, 128+len(iss.Title)+len(iss.Body)+len(iss.Labels)*36)
	dst = append(dst, `{"id":`...)
	dst = strconv.AppendInt(dst, iss.ID, 10)
	dst = append(dst, `,"number":`...)
	dst = strconv.AppendInt(dst, int64(iss.Number), 10)
	dst = append(dst, `,"title":`...)
	dst = AppendJSONString(dst, iss.Title)
	dst = append(dst, `,"body":`...)
	dst = AppendJSONString(dst, iss.Body)
	dst = append(dst, `,"state":`...)
	dst = AppendJSONString(dst, iss.State)
	dst = append(dst, `,"labels":[`...)
	for i, l := range iss.Labels {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, `{"name":`...)
		dst = AppendJSONString(dst, l.Name)
		dst = append(dst, `,"color":`...)
		dst = AppendJSONString(dst, l.Color)
		dst = append(dst, '}')
	}
	dst = append(dst, `]}`...)
	return dst
}

// BuildHTTPResponse pre-frames a complete HTTP/1.1 response (status line + headers + body)
// so keep-alive hot paths can emit the entire response in one single write() syscall.
func BuildHTTPResponse(statusLine, contentType string, extraHeaders string, body []byte) []byte {
	out := make([]byte, 0, 128+len(extraHeaders)+len(body))
	out = append(out, statusLine...)
	out = append(out, "\r\nContent-Type: "...)
	out = append(out, contentType...)
	out = append(out, "\r\nX-GitHub-Request-Id: 1\r\n"...)
	if extraHeaders != "" {
		out = append(out, extraHeaders...)
	}
	out = append(out, "Content-Length: "...)
	out = strconv.AppendInt(out, int64(len(body)), 10)
	out = append(out, "\r\n\r\n"...)
	out = append(out, body...)
	return out
}

// TryParseSimpleIssueJSON parses `{"title":"...","body":"..."}` with unescaped ASCII strings in zero reflection.
// Any escapes, extra fields (such as "labels"), or unusual formatting fall back to encoding/json cleanly.
func TryParseSimpleIssueJSON(b []byte) (title, body string, ok bool) {
	if len(b) < 2 || b[0] != '{' || b[len(b)-1] != '}' {
		return "", "", false
	}
	i := 1
	skipWS := func() {
		for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
			i++
		}
	}
	readPlainString := func() (string, bool) {
		if i >= len(b) || b[i] != '"' {
			return "", false
		}
		i++
		start := i
		for i < len(b) {
			c := b[i]
			if c == '\\' || c < 0x20 {
				return "", false
			}
			if c == '"' {
				s := string(b[start:i])
				i++
				return s, true
			}
			i++
		}
		return "", false
	}

	var sawTitle bool
	firstField := true
	for {
		skipWS()
		if firstField && i < len(b) && b[i] == '}' {
			return title, body, sawTitle
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
		switch key {
		case "title":
			title = val
			sawTitle = true
		case "body":
			body = val
		default:
			return "", "", false
		}
		skipWS()
		if i < len(b) && b[i] == ',' {
			i++
			continue
		}
		if i == len(b)-1 && b[i] == '}' {
			return title, body, sawTitle
		}
		return "", "", false
	}
}

func serializeCommentJSON(c *Comment, owner User) []byte {
	dst := make([]byte, 0, 192+len(c.Body))
	dst = append(dst, `{"id":`...)
	dst = strconv.AppendInt(dst, c.ID, 10)
	dst = append(dst, `,"node_id":`...)
	dst = AppendJSONString(dst, c.NodeID)
	dst = append(dst, `,"body":`...)
	dst = AppendJSONString(dst, c.Body)
	dst = append(dst, `,"user":`...)
	dst = appendUserJSON(dst, owner)
	dst = append(dst, `,"created_at":`...)
	dst = AppendJSONString(dst, c.CreatedAt)
	dst = append(dst, '}')
	return dst
}

func serializePullJSON(pr *PullRequest, headSHA, baseSHA string, owner User) []byte {
	dst := make([]byte, 0, 448+len(pr.Title)+len(pr.Body))
	dst = append(dst, `{"id":`...)
	dst = strconv.AppendInt(dst, pr.ID, 10)
	dst = append(dst, `,"node_id":`...)
	dst = AppendJSONString(dst, pr.NodeID)
	dst = append(dst, `,"number":`...)
	dst = strconv.AppendInt(dst, int64(pr.Number), 10)
	dst = append(dst, `,"state":`...)
	dst = AppendJSONString(dst, pr.State)
	dst = append(dst, `,"title":`...)
	dst = AppendJSONString(dst, pr.Title)
	dst = append(dst, `,"body":`...)
	dst = AppendJSONString(dst, pr.Body)
	dst = append(dst, `,"merged":`...)
	dst = strconv.AppendBool(dst, pr.Merged)
	dst = append(dst, `,"merged_at":`...)
	if pr.MergedAt == "" {
		dst = append(dst, `null`...)
	} else {
		dst = AppendJSONString(dst, pr.MergedAt)
	}
	dst = append(dst, `,"merge_commit_sha":`...)
	if pr.MergeCommitSHA == "" {
		dst = append(dst, `null`...)
	} else {
		dst = AppendJSONString(dst, pr.MergeCommitSHA)
	}
	dst = append(dst, `,"head":{"ref":`...)
	dst = AppendJSONString(dst, pr.HeadRef)
	dst = append(dst, `,"sha":`...)
	dst = AppendJSONString(dst, headSHA)
	dst = append(dst, `},"base":{"ref":`...)
	dst = AppendJSONString(dst, pr.BaseRef)
	dst = append(dst, `,"sha":`...)
	dst = AppendJSONString(dst, baseSHA)
	dst = append(dst, `},"user":`...)
	dst = appendUserJSON(dst, owner)
	dst = append(dst, '}')
	return dst
}
