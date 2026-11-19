package gitobj

import (
	"crypto/sha1"
	"hash"
	"strconv"
	"sync"
)

// OID is a 20-byte raw SHA-1 Git object identifier.
type OID [20]byte

// ZeroOID is the all-zero OID ("0000000000000000000000000000000000000000").
var ZeroOID OID

const ZeroHex = "0000000000000000000000000000000000000000"

const hexDigits = "0123456789abcdef"

var hexValTable = func() [256]byte {
	var t [256]byte
	for i := range t {
		t[i] = 0xff
	}
	for c := byte('0'); c <= '9'; c++ {
		t[c] = c - '0'
	}
	for c := byte('a'); c <= 'f'; c++ {
		t[c] = c - 'a' + 10
	}
	for c := byte('A'); c <= 'F'; c++ {
		t[c] = c - 'A' + 10
	}
	return t
}()

// IsZero reports whether o equals ZeroOID.
func (o OID) IsZero() bool {
	return o == ZeroOID
}

// Hex returns the 40-character lowercase hexadecimal encoding of o.
func (o OID) Hex() string {
	var buf [40]byte
	for i, b := range o {
		buf[i*2] = hexDigits[b>>4]
		buf[i*2+1] = hexDigits[b&0x0f]
	}
	return string(buf[:])
}

// AppendHex appends the 40-byte lowercase hex of o to dst.
func (o OID) AppendHex(dst []byte) []byte {
	var buf [40]byte
	for i, b := range o {
		buf[i*2] = hexDigits[b>>4]
		buf[i*2+1] = hexDigits[b&0x0f]
	}
	return append(dst, buf[:]...)
}

func fromHexNibble(c byte) (byte, bool) {
	v := hexValTable[c]
	return v, v != 0xff
}

// ParseOID parses a 40-character hexadecimal SHA-1 string into an OID.
func ParseOID(s string) (OID, bool) {
	var out OID
	if len(s) != 40 {
		return out, false
	}
	for i := 0; i < 20; i++ {
		hi := hexValTable[s[i*2]]
		lo := hexValTable[s[i*2+1]]
		if (hi | lo) == 0xff {
			return ZeroOID, false
		}
		out[i] = (hi << 4) | lo
	}
	return out, true
}

// ParseOIDBytes parses 40 ASCII hex bytes into an OID without string conversion.
func ParseOIDBytes(b []byte) (OID, bool) {
	var out OID
	if len(b) != 40 {
		return out, false
	}
	for i := 0; i < 20; i++ {
		hi := hexValTable[b[i*2]]
		lo := hexValTable[b[i*2+1]]
		if (hi | lo) == 0xff {
			return ZeroOID, false
		}
		out[i] = (hi << 4) | lo
	}
	return out, true
}

// ObjectType is the Git packfile type code (1..4, plus 6/7 for deltas).
type ObjectType uint8

const (
	ObjCommit   ObjectType = 1
	ObjTree     ObjectType = 2
	ObjBlob     ObjectType = 3
	ObjTag      ObjectType = 4
	ObjOfsDelta ObjectType = 6
	ObjRefDelta ObjectType = 7
)

func (t ObjectType) String() string {
	switch t {
	case ObjCommit:
		return "commit"
	case ObjTree:
		return "tree"
	case ObjBlob:
		return "blob"
	case ObjTag:
		return "tag"
	default:
		return "unknown"
	}
}

func ParseObjectType(name string) (ObjectType, bool) {
	switch name {
	case "commit":
		return ObjCommit, true
	case "tree":
		return ObjTree, true
	case "blob":
		return ObjBlob, true
	case "tag":
		return ObjTag, true
	default:
		return 0, false
	}
}

var sha1Pool = sync.Pool{
	New: func() any {
		return sha1.New()
	},
}

// HashObject computes the canonical Git SHA-1 over "<type> <len>\x00" + raw with zero heap allocations.
func HashObject(typ ObjectType, raw []byte) OID {
	if len(raw) <= 2016 {
		var buf [2048]byte
		b := buf[:0]
		b = append(b, typ.String()...)
		b = append(b, ' ')
		b = strconv.AppendInt(b, int64(len(raw)), 10)
		b = append(b, 0)
		b = append(b, raw...)
		return OID(sha1.Sum(b))
	}
	h := sha1Pool.Get().(hash.Hash)
	h.Reset()
	var hdr [32]byte
	b := hdr[:0]
	b = append(b, typ.String()...)
	b = append(b, ' ')
	b = strconv.AppendInt(b, int64(len(raw)), 10)
	b = append(b, 0)
	h.Write(b)
	h.Write(raw)
	var out OID
	h.Sum(out[:0])
	sha1Pool.Put(h)
	return out
}
