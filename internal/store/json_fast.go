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

func appendIssueJSONBody(dst []byte, iss *Issue, ownerJSON []byte) []byte {
	dst = append(dst, `{"id":`...)
	dst = strconv.AppendInt(dst, iss.ID, 10)
	dst = append(dst, `,"node_id":`...)
	dst = AppendJSONString(dst, iss.NodeID)
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
		dst = appendLabelJSON(dst, l)
	}
	dst = append(dst, `],"user":`...)
	dst = append(dst, ownerJSON...)
	dst = append(dst, `,"created_at":`...)
	dst = AppendJSONString(dst, iss.CreatedAt)
	dst = append(dst, `,"updated_at":`...)
	dst = AppendJSONString(dst, iss.UpdatedAt)
	dst = append(dst, '}')
	return dst
}

// serializeIssueHTTP builds the complete HTTP/1.1 200 OK response and slices RestJSON from the same backing array.
func serializeIssueHTTP(iss *Issue, ownerJSON []byte) (http200 []byte, restJSON []byte) {
	var bodyStack [1024]byte
	body := appendIssueJSONBody(bodyStack[:0], iss, ownerJSON)

	var hdrStack [160]byte
	hdr := append(hdrStack[:0], "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-GitHub-Request-Id: i"...)
	hdr = strconv.AppendInt(hdr, iss.ID, 10)
	hdr = append(hdr, "\r\nContent-Length: "...)
	hdr = strconv.AppendInt(hdr, int64(len(body)), 10)
	hdr = append(hdr, "\r\n\r\n"...)

	buf := make([]byte, len(hdr)+len(body))
	copy(buf, hdr)
	copy(buf[len(hdr):], body)
	return buf, buf[len(hdr):]
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
