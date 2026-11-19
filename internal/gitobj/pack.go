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

func inflateExact(src []byte, expectedSize int) (raw []byte, consumed int, err error) {
	sr := &sliceByteReader{b: src}
	var zr io.ReadCloser
	if pooled := zlibReaderPool.Get(); pooled != nil {
		zr = pooled.(io.ReadCloser)
		if err := zr.(zlib.Resetter).Reset(sr, nil); err != nil {
			return nil, 0, err
		}
	} else {
		var err error
		zr, err = zlib.NewReader(sr)
		if err != nil {
			return nil, 0, err
		}
	}

	out := make([]byte, expectedSize)
	n, readErr := io.ReadFull(zr, out)
	if readErr != nil && !(readErr == io.EOF && expectedSize == 0) {
		_ = zr.Close()
		return nil, 0, fmt.Errorf("zlib inflate short read (%d/%d): %w", n, expectedSize, readErr)
	}
	// Drain trailing Adler-32 checksum to advance sr.pos to exact end of compressed stream
	var extra [1]byte
	m, eofErr := zr.Read(extra[:])
	_ = zr.Close()
	if m != 0 || eofErr != io.EOF {
		return nil, 0, errors.New("zlib stream has unexpected trailing uncompressed data")
	}
	zlibReaderPool.Put(zr)
	return out, sr.pos, nil
}

type rawPackEntry struct {
	offset     int
	typ        ObjectType
	baseOffset int
	baseOID    OID
	data       []byte // raw data for 1..4, delta instructions for 6..7
	packSlice  []byte // exact [varintHeader + zlibStream] for plain 1..4 entries
	resolved   *Object
}

// Unpack verifies the SHA-1 trailer of a Git v2/v3 packfile and inflates all objects,
// resolving OFS_DELTA and REF_DELTA entries against earlier pack entries or lookupExternal.
func Unpack(pack []byte, lookupExternal func(OID) (*Object, bool)) ([]*Object, error) {
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

	var stackEntries [8]rawPackEntry
	var entries []rawPackEntry
	if count <= len(stackEntries) {
		entries = stackEntries[:count]
	} else {
		entries = make([]rawPackEntry, count)
	}
	objSlab := make([]Object, count)
	var unresolved []int

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
			inflated, consumed, err := inflateExact(pack[pos:bodyLen], int(size))
			if err != nil {
				return nil, err
			}
			rawEntryLen := (pos - entryStart) + consumed
			entryCopy := make([]byte, rawEntryLen)
			copy(entryCopy, pack[entryStart:entryStart+rawEntryLen])
			pos += consumed

			oid := HashObject(typ, inflated)
			obj := &objSlab[i]
			obj.OID = oid
			obj.Type = typ
			obj.Raw = inflated
			obj.packEntry.Store(&entryCopy)
			ent.resolved = obj

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
				obj := &objSlab[idx]
				obj.OID = oid
				obj.Type = base.Type
				obj.Raw = targetRaw
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

	out := make([]*Object, count)
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
