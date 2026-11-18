package gitobj

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Object is an immutable Git object in the repository store.
// It caches its packfile representation ([packVarintHeader + zlibStream]) so
// clones and fetches never re-deflate unchanged objects.
type Object struct {
	OID       OID
	Type      ObjectType
	Raw       []byte
	packEntry atomic.Pointer[[]byte]
}

// NewObject constructs an immutable Object, computing its OID.
func NewObject(typ ObjectType, raw []byte) *Object {
	cp := make([]byte, len(raw))
	copy(cp, raw)
	return &Object{
		OID:  HashObject(typ, cp),
		Type: typ,
		Raw:  cp,
	}
}

// NewObjectWithPackEntry constructs an Object whose pack entry bytes
// (pack varint header + verified zlib stream) are already known from receive-pack.
func NewObjectWithPackEntry(oid OID, typ ObjectType, raw []byte, entryBytes []byte) *Object {
	obj := &Object{
		OID:  oid,
		Type: typ,
		Raw:  raw,
	}
	if len(entryBytes) > 0 {
		e := entryBytes
		obj.packEntry.Store(&e)
	}
	return obj
}

// PackEntry returns the immutable [packObjectHeader + zlibCompressedPayload] slice,
// computing and caching it once if the object did not arrive from a plain pack entry.
func (o *Object) PackEntry() []byte {
	if p := o.packEntry.Load(); p != nil {
		return *p
	}
	var hdr [10]byte
	hdrLen := encodePackObjHeader(hdr[:], o.Type, len(o.Raw))

	buf := getBuf()
	buf.Write(hdr[:hdrLen])

	zw := getZlibWriter(buf)
	_, _ = zw.Write(o.Raw)
	_ = zw.Close()
	putZlibWriter(zw)

	encoded := make([]byte, buf.Len())
	copy(encoded, buf.Bytes())
	putBuf(buf)

	o.packEntry.Store(&encoded)
	return encoded
}

func encodePackObjHeader(dst []byte, typ ObjectType, size int) int {
	rem := uint64(size)
	first := byte((uint8(typ)&0x07)<<4) | byte(rem&0x0f)
	rem >>= 4
	n := 0
	for rem > 0 {
		dst[n] = first | 0x80
		n++
		first = byte(rem & 0x7f)
		rem >>= 7
	}
	dst[n] = first
	return n + 1
}

// TreeEntry represents a single entry inside a Git tree object.
// Directory entries use Mode "40000" (no leading zero, per Git canonical tree format).
type TreeEntry struct {
	Mode string
	Name string
	OID  OID
}

// IsDir reports whether the entry is a subtree ("40000" or "040000").
func (e TreeEntry) IsDir() bool {
	return e.Mode == "40000" || e.Mode == "040000"
}

// gitTreeEntryLess implements Git's exact base_name_compare:
// directory entries behave as if they have a trailing '/' during byte comparison.
func gitTreeEntryLess(a, b TreeEntry) bool {
	na, nb := a.Name, b.Name
	la, lb := len(na), len(nb)
	minLen := la
	if lb < minLen {
		minLen = lb
	}
	for i := 0; i < minLen; i++ {
		if na[i] != nb[i] {
			return na[i] < nb[i]
		}
	}
	if la == lb {
		return false
	}
	var ca, cb int
	if minLen < la {
		ca = int(na[minLen])
	} else if a.IsDir() {
		ca = '/'
	} else {
		ca = -1
	}
	if minLen < lb {
		cb = int(nb[minLen])
	} else if b.IsDir() {
		cb = '/'
	} else {
		cb = -1
	}
	return ca < cb
}

// BuildTree encodes entries into canonical sorted Git tree object bytes.
func BuildTree(entries []TreeEntry) []byte {
	sorted := make([]TreeEntry, len(entries))
	copy(sorted, entries)
	for i := range sorted {
		if sorted[i].Mode == "040000" {
			sorted[i].Mode = "40000"
		}
	}
	sort.Slice(sorted, func(i, j int) bool {
		return gitTreeEntryLess(sorted[i], sorted[j])
	})
	total := 0
	for _, e := range sorted {
		total += len(e.Mode) + 1 + len(e.Name) + 1 + 20
	}
	out := make([]byte, 0, total)
	for _, e := range sorted {
		out = append(out, e.Mode...)
		out = append(out, ' ')
		out = append(out, e.Name...)
		out = append(out, 0)
		out = append(out, e.OID[:]...)
	}
	return out
}

// ParseTree parses raw Git tree object bytes into TreeEntry slices.
func ParseTree(raw []byte) ([]TreeEntry, error) {
	var entries []TreeEntry
	i := 0
	for i < len(raw) {
		sp := bytes.IndexByte(raw[i:], ' ')
		if sp <= 0 {
			return nil, errors.New("malformed tree: missing mode space")
		}
		mode := string(raw[i : i+sp])
		if mode == "040000" {
			mode = "40000"
		}
		i += sp + 1
		nul := bytes.IndexByte(raw[i:], 0)
		if nul <= 0 {
			return nil, errors.New("malformed tree: missing name NUL")
		}
		name := string(raw[i : i+nul])
		i += nul + 1
		if i+20 > len(raw) {
			return nil, errors.New("malformed tree: truncated entry OID")
		}
		var oid OID
		copy(oid[:], raw[i:i+20])
		i += 20
		entries = append(entries, TreeEntry{Mode: mode, Name: name, OID: oid})
	}
	return entries, nil
}

// ParsedCommit holds pre-parsed metadata for fast commit listing, merge-base, and ancestry walks.
type ParsedCommit struct {
	OID           OID
	Tree          OID
	Parents       []OID
	AuthorName    string
	AuthorEmail   string
	AuthorDateISO string
	Message       string
}

// BuildCommit formats a canonical Git commit payload.
func BuildCommit(tree OID, parents []OID, authorName, authorEmail string, epochSec int64, message string) []byte {
	var b bytes.Buffer
	b.Grow(256 + len(message))
	b.WriteString("tree ")
	b.WriteString(tree.Hex())
	b.WriteByte('\n')
	for _, p := range parents {
		b.WriteString("parent ")
		b.WriteString(p.Hex())
		b.WriteByte('\n')
	}
	ident := fmt.Sprintf("%s <%s> %d +0000\n", authorName, authorEmail, epochSec)
	b.WriteString("author ")
	b.WriteString(ident)
	b.WriteString("committer ")
	b.WriteString(ident)
	b.WriteByte('\n')
	b.WriteString(message)
	return b.Bytes()
}

// ParseCommit parses raw Git commit payload bytes without intermediate []string header split allocations.
func ParseCommit(oid OID, raw []byte) (*ParsedCommit, error) {
	pc := &ParsedCommit{
		OID:           oid,
		AuthorDateISO: "2025-01-01T00:00:00Z",
	}
	hdrEnd := bytes.Index(raw, []byte("\n\n"))
	var hdrBytes []byte
	if hdrEnd >= 0 {
		hdrBytes = raw[:hdrEnd]
		pc.Message = string(raw[hdrEnd+2:])
	} else {
		hdrBytes = raw
	}

	var hasTree bool
	rem := hdrBytes
	for len(rem) > 0 {
		var line []byte
		if nl := bytes.IndexByte(rem, '\n'); nl >= 0 {
			line = rem[:nl]
			rem = rem[nl+1:]
		} else {
			line = rem
			rem = nil
		}
		switch {
		case len(line) >= 45 && string(line[:5]) == "tree ":
			t, ok := ParseOIDBytes(bytes.TrimSpace(line[5:]))
			if !ok {
				return nil, errors.New("invalid commit tree OID")
			}
			pc.Tree = t
			hasTree = true
		case len(line) >= 47 && string(line[:7]) == "parent ":
			p, ok := ParseOIDBytes(bytes.TrimSpace(line[7:]))
			if !ok {
				return nil, errors.New("invalid commit parent OID")
			}
			pc.Parents = append(pc.Parents, p)
		case len(line) > 7 && string(line[:7]) == "author ":
			name, email, iso := parseIdentityLine(string(line[7:]))
			pc.AuthorName = name
			pc.AuthorEmail = email
			if iso != "" {
				pc.AuthorDateISO = iso
			}
		}
	}
	if !hasTree {
		return nil, errors.New("commit missing tree header")
	}
	return pc, nil
}

func parseIdentityLine(s string) (name, email, iso string) {
	lt := strings.IndexByte(s, '<')
	gt := strings.IndexByte(s, '>')
	if lt < 0 || gt < lt {
		return strings.TrimSpace(s), "", ""
	}
	name = strings.TrimSpace(s[:lt])
	email = s[lt+1 : gt]
	rest := strings.Fields(s[gt+1:])
	if len(rest) >= 1 {
		if sec, err := strconv.ParseInt(rest[0], 10, 64); err == nil {
			iso = time.Unix(sec, 0).UTC().Format(time.RFC3339)
		}
	}
	return name, email, iso
}

// ParseTag extracts the target OID and target type from an annotated tag object.
func ParseTag(raw []byte) (OID, ObjectType, bool) {
	hdrEnd := bytes.Index(raw, []byte("\n\n"))
	hdr := raw
	if hdrEnd >= 0 {
		hdr = raw[:hdrEnd]
	}
	var target OID
	var hasTarget bool
	var typ ObjectType
	var hasType bool
	for _, line := range strings.Split(string(hdr), "\n") {
		if strings.HasPrefix(line, "object ") {
			if o, ok := ParseOID(strings.TrimSpace(line[7:])); ok {
				target = o
				hasTarget = true
			}
		} else if strings.HasPrefix(line, "type ") {
			if t, ok := ParseObjectType(strings.TrimSpace(line[5:])); ok {
				typ = t
				hasType = true
			}
		}
	}
	return target, typ, hasTarget && hasType
}
