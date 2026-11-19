package gitobj

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

const (
	maxPooledBufCap = 256 * 1024
	maxPktPayload   = 65516 // 65520 - 4
	maxSidebandData = 65515 // 65520 - 4 - 1
)

var (
	bufPool = sync.Pool{
		New: func() any {
			return bytes.NewBuffer(make([]byte, 0, 8192))
		},
	}
	zlibWriterPool = sync.Pool{
		New: func() any {
			w, _ := zlib.NewWriterLevel(io.Discard, zlib.BestSpeed)
			return w
		},
	}
	zlibReaderPool sync.Pool
)

func getBuf() *bytes.Buffer {
	b := bufPool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func putBuf(b *bytes.Buffer) {
	if b.Cap() <= maxPooledBufCap {
		b.Reset()
		bufPool.Put(b)
	}
}

func getZlibWriter(dst io.Writer) *zlib.Writer {
	w := zlibWriterPool.Get().(*zlib.Writer)
	w.Reset(dst)
	return w
}

func putZlibWriter(w *zlib.Writer) {
	w.Reset(io.Discard)
	zlibWriterPool.Put(w)
}

// sliceByteReader implements io.Reader + io.ByteReader over a []byte so compress/zlib
// NEVER wraps the input in a 4 KiB bufio.Reader and stops at the exact end of the zlib stream!
type sliceByteReader struct {
	b   []byte
	pos int
}

func (r *sliceByteReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.pos:])
	r.pos += n
	return n, nil
}

func (r *sliceByteReader) ReadByte() (byte, error) {
	if r.pos >= len(r.b) {
		return 0, io.EOF
	}
	c := r.b[r.pos]
	r.pos++
	return c, nil
}

var scratchPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 8192)
		return &b
	},
}

func inflateExact(src []byte, expectedSize int) (raw []byte, consumed int, err error) {
	out := make([]byte, expectedSize)
	consumed, err = inflateZlibInto(src, out)
	if err != nil {
		return nil, 0, err
	}
	return out, consumed, nil
}

type rawPackEntry struct {
	offset     int
	typ        ObjectType
	baseOffset int
	baseOID    OID
	data       []byte // raw data for 1..4, delta instructions for 6..7
	packStart  int    // start offset in pack for plain 1..4 entries
	packLen    int    // length in pack for plain 1..4 entries
	rawOff     int    // offset in scratch buffer for plain 1..4 entries
	rawLen     int    // uncompressed length for plain 1..4 entries
	oid        OID
	resolved   *Object
}

// Unpack verifies the SHA-1 trailer of a Git v2/v3 packfile and inflates all objects,
// resolving OFS_DELTA and REF_DELTA entries against earlier pack entries or lookupExternal.
func Unpack(pack []byte, lookupExternal func(OID) (*Object, bool)) ([]*Object, error) {
	return UnpackInto(pack, lookupExternal, nil)
}

// UnpackInto unpacks pack into dst (reusing dst's backing array when large enough)
// and packs all plain Object structs and their inflated + pack-entry bytes into contiguous slabs.
func UnpackInto(pack []byte, lookupExternal func(OID) (*Object, bool), dst []*Object) ([]*Object, error) {
	if len(pack) < 32 {
		return nil, errors.New("packfile shorter than 32-byte minimum")
	}
	if string(pack[0:4]) != "PACK" {
		return nil, errors.New("invalid packfile magic")
	}
	ver := binary.BigEndian.Uint32(pack[4:8])
	if ver != 2 && ver != 3 {
		return nil, fmt.Errorf("unsupported packfile version %d", ver)
	}
	count := int(binary.BigEndian.Uint32(pack[8:12]))
	if count < 0 || count > 500_000 {
		return nil, fmt.Errorf("unreasonable pack object count %d", count)
	}

	bodyLen := len(pack) - 20
	expectedSum := sha1.Sum(pack[:bodyLen])
	if !bytes.Equal(expectedSum[:], pack[bodyLen:]) {
		return nil, errors.New("packfile SHA-1 checksum mismatch")
	}
	if count == 0 {
		if bodyLen != 12 {
			return nil, errors.New("trailing unparsed bytes before packfile trailer")
		}
		return dst[:0], nil
	}

	var stackEntries [8]rawPackEntry
	var entries []rawPackEntry
	if count <= len(stackEntries) {
		entries = stackEntries[:count]
	} else {
		entries = make([]rawPackEntry, count)
	}
	var unresolved []int

	// Scratch buffer accumulates all plain inflated bytes during unpack so we can commit them
	// together with all plain packEntry slices into ONE contiguous slab allocation at the end.
	scratchPtr := scratchPool.Get().(*[]byte)
	scratch := (*scratchPtr)[:0]
	defer func() {
		if cap(scratch) <= maxPooledBufCap {
			*scratchPtr = scratch[:0]
			scratchPool.Put(scratchPtr)
		}
	}()

	totalPlainPackBytes := 0
	plainCount := 0

	pos := 12
	for i := 0; i < count; i++ {
		if pos >= bodyLen {
			return nil, errors.New("truncated packfile before declared object count")
		}
		entryStart := pos
		c := pack[pos]
		pos++
		typ := ObjectType((c >> 4) & 0x07)
		size := uint64(c & 0x0f)
		shift := uint(4)
		for (c & 0x80) != 0 {
			if pos >= bodyLen || shift > 56 {
				return nil, errors.New("malformed pack object varint")
			}
			c = pack[pos]
			pos++
			size |= uint64(c&0x7f) << shift
			shift += 7
		}
		if size > 64*1024*1024 {
			return nil, errors.New("pack object exceeds 64 MiB limit")
		}

		ent := rawPackEntry{
			offset: entryStart,
			typ:    typ,
		}
		switch typ {
		case ObjCommit, ObjTree, ObjBlob, ObjTag:
			sz := int(size)
			rawOff := len(scratch)
			if cap(scratch)-rawOff < sz {
				newCap := cap(scratch) * 2
				if newCap < rawOff+sz {
					newCap = rawOff + sz
				}
				grown := make([]byte, rawOff+sz, newCap)
				copy(grown, scratch)
				scratch = grown
			} else {
				scratch = scratch[:rawOff+sz]
			}
			rawSlice := scratch[rawOff : rawOff+sz]
			consumed, err := inflateZlibInto(pack[pos:bodyLen], rawSlice)
			if err != nil {
				return nil, err
			}
			rawEntryLen := (pos - entryStart) + consumed
			pos += consumed

			ent.oid = HashObject(typ, rawSlice)
			ent.rawOff = rawOff
			ent.rawLen = sz
			ent.packStart = entryStart
			ent.packLen = rawEntryLen
			totalPlainPackBytes += rawEntryLen
			plainCount++

		case ObjOfsDelta:
			if pos >= bodyLen {
				return nil, errors.New("truncated OFS_DELTA offset")
			}
			b := pack[pos]
			pos++
			negOff := int(b & 0x7f)
			for (b & 0x80) != 0 {
				if pos >= bodyLen {
					return nil, errors.New("truncated OFS_DELTA varint")
				}
				b = pack[pos]
				pos++
				negOff = ((negOff + 1) << 7) | int(b&0x7f)
			}
			baseOff := entryStart - negOff
			if baseOff < 12 || baseOff >= entryStart {
				return nil, errors.New("invalid OFS_DELTA base offset")
			}
			deltaBytes, consumed, err := inflateExact(pack[pos:bodyLen], int(size))
			if err != nil {
				return nil, err
			}
			pos += consumed
			ent.baseOffset = baseOff
			ent.data = deltaBytes
			unresolved = append(unresolved, i)

		case ObjRefDelta:
			if pos+20 > bodyLen {
				return nil, errors.New("truncated REF_DELTA base OID")
			}
			copy(ent.baseOID[:], pack[pos:pos+20])
			pos += 20
			deltaBytes, consumed, err := inflateExact(pack[pos:bodyLen], int(size))
			if err != nil {
				return nil, err
			}
			pos += consumed
			ent.data = deltaBytes
			unresolved = append(unresolved, i)

		default:
			return nil, fmt.Errorf("invalid pack object type %d", typ)
		}

		entries[i] = ent
	}

	if pos != bodyLen {
		return nil, errors.New("trailing unparsed bytes before packfile trailer")
	}

	// Commit all plain Objects and their bytes into ONE Object slab + ONE byte slab!
	objSlab := make([]Object, count)
	if plainCount > 0 {
		totalSlabBytes := len(scratch) + totalPlainPackBytes
		byteSlab := make([]byte, totalSlabBytes)
		copy(byteSlab, scratch)
		packDst := len(scratch)
		if plainCount == count {
			// All pack entries are contiguous in pack[12:bodyLen]: single bulk copy!
			copy(byteSlab[packDst:], pack[12:bodyLen])
			for i := range entries {
				ent := &entries[i]
				rawCopy := byteSlab[ent.rawOff : ent.rawOff+ent.rawLen : ent.rawOff+ent.rawLen]
				peOff := packDst + (ent.packStart - 12)
				peCopy := byteSlab[peOff : peOff+ent.packLen : peOff+ent.packLen]
				objSlab[i] = Object{
					OID:            ent.oid,
					Type:           ent.typ,
					Raw:            rawCopy,
					packEntryBytes: peCopy,
				}
				ent.resolved = &objSlab[i]
			}
		} else {
			for i := range entries {
				ent := &entries[i]
				if ent.typ >= ObjCommit && ent.typ <= ObjTag {
					rawCopy := byteSlab[ent.rawOff : ent.rawOff+ent.rawLen : ent.rawOff+ent.rawLen]
					peCopy := byteSlab[packDst : packDst+ent.packLen : packDst+ent.packLen]
					copy(peCopy, pack[ent.packStart:ent.packStart+ent.packLen])
					packDst += ent.packLen
					objSlab[i] = Object{
						OID:            ent.oid,
						Type:           ent.typ,
						Raw:            rawCopy,
						packEntryBytes: peCopy,
					}
					ent.resolved = &objSlab[i]
				}
			}
		}
	}

	// Lazily build offset/OID maps ONLY when the packfile actually contains OFS_DELTA or REF_DELTA entries
	if len(unresolved) > 0 {
		byOffset := make(map[int]int, count)
		byOID := make(map[OID]*Object, count)
		for i := range entries {
			byOffset[entries[i].offset] = i
			if entries[i].resolved != nil {
				byOID[entries[i].resolved.OID] = entries[i].resolved
			}
		}
		for len(unresolved) > 0 {
			progressed := false
			nextPending := unresolved[:0]
			for _, idx := range unresolved {
				ent := &entries[idx]
				var base *Object
				if ent.typ == ObjOfsDelta {
					baseIdx, ok := byOffset[ent.baseOffset]
					if !ok {
						return nil, errors.New("OFS_DELTA base offset does not match any pack entry")
					}
					base = entries[baseIdx].resolved
				} else {
					if b, ok := byOID[ent.baseOID]; ok {
						base = b
					} else if lookupExternal != nil {
						if b, ok := lookupExternal(ent.baseOID); ok {
							base = b
						}
					}
				}
				if base == nil {
					nextPending = append(nextPending, idx)
					continue
				}
				targetRaw, err := applyDelta(base.Raw, ent.data)
				if err != nil {
					return nil, err
				}
				oid := HashObject(base.Type, targetRaw)
				objSlab[idx] = Object{
					OID:  oid,
					Type: base.Type,
					Raw:  targetRaw,
				}
				obj := &objSlab[idx]
				ent.resolved = obj
				ent.data = nil
				byOID[oid] = obj
				progressed = true
			}
			if !progressed {
				return nil, errors.New("unresolvable delta base in packfile")
			}
			unresolved = nextPending
		}
	}

	var out []*Object
	if cap(dst) >= count {
		out = dst[:count]
	} else {
		out = make([]*Object, count)
	}
	for i := range entries {
		out[i] = entries[i].resolved
	}
	return out, nil
}

func readDeltaSize(delta []byte, pos int) (int, int, error) {
	var size uint64
	var shift uint
	for {
		if pos >= len(delta) || shift > 56 {
			return 0, pos, errors.New("truncated delta size varint")
		}
		b := delta[pos]
		pos++
		size |= uint64(b&0x7f) << shift
		if (b & 0x80) == 0 {
			break
		}
		shift += 7
	}
	return int(size), pos, nil
}

func applyDelta(base, delta []byte) ([]byte, error) {
	srcSize, pos, err := readDeltaSize(delta, 0)
	if err != nil {
		return nil, err
	}
	if srcSize != len(base) {
		return nil, fmt.Errorf("delta base size mismatch (%d != %d)", srcSize, len(base))
	}
	dstSize, pos, err := readDeltaSize(delta, pos)
	if err != nil || dstSize < 0 || dstSize > 64*1024*1024 {
		return nil, errors.New("invalid delta target size")
	}
	out := make([]byte, 0, dstSize)
	for pos < len(delta) {
		cmd := delta[pos]
		pos++
		if (cmd & 0x80) != 0 {
			var cpOff, cpSize int
			if (cmd & 0x01) != 0 {
				if pos >= len(delta) {
					return nil, errors.New("truncated delta copy offset")
				}
				cpOff |= int(delta[pos])
				pos++
			}
			if (cmd & 0x02) != 0 {
				if pos >= len(delta) {
					return nil, errors.New("truncated delta copy offset")
				}
				cpOff |= int(delta[pos]) << 8
				pos++
			}
			if (cmd & 0x04) != 0 {
				if pos >= len(delta) {
					return nil, errors.New("truncated delta copy offset")
				}
				cpOff |= int(delta[pos]) << 16
				pos++
			}
			if (cmd & 0x08) != 0 {
				if pos >= len(delta) {
					return nil, errors.New("truncated delta copy offset")
				}
				cpOff |= int(delta[pos]) << 24
				pos++
			}
			if (cmd & 0x10) != 0 {
				if pos >= len(delta) {
					return nil, errors.New("truncated delta copy size")
				}
				cpSize |= int(delta[pos])
				pos++
			}
			if (cmd & 0x20) != 0 {
				if pos >= len(delta) {
					return nil, errors.New("truncated delta copy size")
				}
				cpSize |= int(delta[pos]) << 8
				pos++
			}
			if (cmd & 0x40) != 0 {
				if pos >= len(delta) {
					return nil, errors.New("truncated delta copy size")
				}
				cpSize |= int(delta[pos]) << 16
				pos++
			}
			if cpSize == 0 {
				cpSize = 0x10000
			}
			if cpOff < 0 || cpOff+cpSize > len(base) {
				return nil, errors.New("delta copy out of bounds")
			}
			out = append(out, base[cpOff:cpOff+cpSize]...)
		} else if cmd > 0 {
			n := int(cmd)
			if pos+n > len(delta) {
				return nil, errors.New("delta insert out of bounds")
			}
			out = append(out, delta[pos:pos+n]...)
			pos += n
		} else {
			return nil, errors.New("reserved delta opcode 0")
		}
	}
	if len(out) != dstSize {
		return nil, fmt.Errorf("delta reconstructed size %d != %d", len(out), dstSize)
	}
	return out, nil
}

// EmptyPackBytes is the precomputed 32-byte empty packfile ("PACK" v2, 0 objects + SHA-1).
var EmptyPackBytes = func() []byte {
	var b [32]byte
	copy(b[0:4], "PACK")
	binary.BigEndian.PutUint32(b[4:8], 2)
	binary.BigEndian.PutUint32(b[8:12], 0)
	sum := sha1.Sum(b[:12])
	copy(b[12:32], sum[:])
	return b[:]
}()

// BuildPack assembles a valid v2 Git packfile from pre-compressed Object entries
// in a single exact-sized allocation with zero re-deflation of pushed objects.
func BuildPack(objs []*Object) []byte {
	if len(objs) == 0 {
		return EmptyPackBytes
	}
	entries := make([][]byte, len(objs))
	totalBody := 12
	for i, obj := range objs {
		pe := obj.PackEntry()
		entries[i] = pe
		totalBody += len(pe)
	}
	pack := make([]byte, totalBody+20)
	copy(pack[0:4], "PACK")
	binary.BigEndian.PutUint32(pack[4:8], 2)
	binary.BigEndian.PutUint32(pack[8:12], uint32(len(objs)))
	off := 12
	for _, pe := range entries {
		copy(pack[off:], pe)
		off += len(pe)
	}
	sum := sha1.Sum(pack[:totalBody])
	copy(pack[totalBody:], sum[:])
	return pack
}

// AppendPktLine appends a single 4-hex-prefixed pkt-line containing payload to dst.
func AppendPktLine(dst []byte, payload []byte) []byte {
	length := len(payload) + 4
	dst = append(dst,
		hexDigits[(length>>12)&0xf],
		hexDigits[(length>>8)&0xf],
		hexDigits[(length>>4)&0xf],
		hexDigits[length&0xf],
	)
	return append(dst, payload...)
}

// AppendPktString appends a single 4-hex-prefixed pkt-line containing s to dst.
func AppendPktString(dst []byte, s string) []byte {
	length := len(s) + 4
	dst = append(dst,
		hexDigits[(length>>12)&0xf],
		hexDigits[(length>>8)&0xf],
		hexDigits[(length>>4)&0xf],
		hexDigits[length&0xf],
	)
	return append(dst, s...)
}

// AppendSidebandData frames pack or report bytes into sideband channel `band` (1, 2, or 3)
// in chunks of up to 65515 bytes (65520 max pkt-line).
func AppendSidebandData(dst []byte, band byte, data []byte) []byte {
	for len(data) > 0 {
		chunk := len(data)
		if chunk > maxSidebandData {
			chunk = maxSidebandData
		}
		pktLen := chunk + 5
		dst = append(dst,
			hexDigits[(pktLen>>12)&0xf],
			hexDigits[(pktLen>>8)&0xf],
			hexDigits[(pktLen>>4)&0xf],
			hexDigits[pktLen&0xf],
			band,
		)
		dst = append(dst, data[:chunk]...)
		data = data[chunk:]
	}
	return dst
}
