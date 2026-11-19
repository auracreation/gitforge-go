package gitobj

import (
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

const (
	primaryBits = 9
	primarySize = 1 << primaryBits
	primaryMask = primarySize - 1
	maxCodeBits = 15
)

// huffTable is a two-level canonical Huffman lookup table (9-bit primary + flat secondary pool).
// Primary entry encoding:
//   - bit 15 == 0 (direct hit): bits [14:4] = symbol (0..287), bits [3:0] = code length (1..9)
//   - bit 15 == 1 (subtable):   bits [14:4] = subtable offset in sub[], bits [3:0] = subtable bit width (1..6)
//   - value == 0:               invalid code
type huffTable struct {
	primary [primarySize]uint16
	sub     [1024]uint16
	subUsed int
}

func (t *huffTable) build(lengths []uint8) error {
	var count [maxCodeBits + 1]uint16
	maxLen := 0
	for _, l := range lengths {
		if l > maxCodeBits {
			return errors.New("huffman code length exceeds 15")
		}
		if l > 0 {
			count[l]++
			if int(l) > maxLen {
				maxLen = int(l)
			}
		}
	}
	if maxLen == 0 {
		// Empty tree (valid for unused distance tree in some blocks)
		clear(t.primary[:])
		t.subUsed = 0
		return nil
	}

	// Verify tree is not over-subscribed
	left := 1
	for bits := 1; bits <= maxLen; bits++ {
		left <<= 1
		left -= int(count[bits])
		if left < 0 {
			return errors.New("oversubscribed huffman tree")
		}
	}

	var nextCode [maxCodeBits + 1]uint16
	code := uint16(0)
	for bits := 1; bits <= maxLen; bits++ {
		code = (code + count[bits-1]) << 1
		nextCode[bits] = code
	}

	clear(t.primary[:])
	t.subUsed = 0

	// Track per-prefix maximum length for codes longer than primaryBits
	var subMax [primarySize]uint8
	var symCode [320]uint16
	for sym, l := range lengths {
		if l == 0 {
			continue
		}
		c := nextCode[l]
		nextCode[l]++
		rev := reverseBits15(c, l)
		symCode[sym] = rev
		if l <= primaryBits {
			entry := uint16(sym<<4) | uint16(l)
			step := 1 << l
			for idx := int(rev); idx < primarySize; idx += step {
				t.primary[idx] = entry
			}
		} else {
			pref := rev & primaryMask
			if l > subMax[pref] {
				subMax[pref] = l
			}
		}
	}

	// Allocate subtables for prefixes that have long codes
	for pref := 0; pref < primarySize; pref++ {
		ml := subMax[pref]
		if ml == 0 {
			continue
		}
		subBits := int(ml) - primaryBits
		subSize := 1 << subBits
		if t.subUsed+subSize > len(t.sub) {
			return errors.New("huffman subtable overflow")
		}
		off := t.subUsed
		clear(t.sub[off : off+subSize])
		t.subUsed += subSize
		t.primary[pref] = 0x8000 | uint16(off<<4) | uint16(subBits)
	}

	// Fill subtables
	for sym, l := range lengths {
		if l <= primaryBits {
			continue
		}
		rev := symCode[sym]
		pref := rev & primaryMask
		pEntry := t.primary[pref]
		off := int((pEntry >> 4) & 0x7ff)
		subBits := int(pEntry & 0x0f)
		subSize := 1 << subBits
		remBits := int(l) - primaryBits
		subIdx := int(rev >> primaryBits)
		entry := uint16(sym<<4) | uint16(remBits)
		step := 1 << remBits
		for idx := subIdx; idx < subSize; idx += step {
			t.sub[off+idx] = entry
		}
	}
	return nil
}

var bitRevByte = func() [256]uint8 {
	var tbl [256]uint8
	for i := 0; i < 256; i++ {
		v := uint8(i)
		v = (v&0xf0)>>4 | (v&0x0f)<<4
		v = (v&0xcc)>>2 | (v&0x33)<<2
		v = (v&0xaa)>>1 | (v&0x55)<<1
		tbl[i] = v
	}
	return tbl
}()

func reverseBits15(code uint16, length uint8) uint16 {
	rev := (uint16(bitRevByte[code&0xff]) << 8) | uint16(bitRevByte[code>>8])
	return rev >> (16 - length)
}

var (
	fixedLitTable  huffTable
	fixedDistTable huffTable

	codeLenOrder = [19]uint8{16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15}
	lengthBase   = [29]uint16{3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131, 163, 195, 227, 258}
	lengthExtra  = [29]uint8{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0}
	distBase     = [30]uint16{1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537, 2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577}
	distExtra    = [30]uint8{0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13}
)

func init() {
	var litLens [288]uint8
	for i := 0; i <= 143; i++ {
		litLens[i] = 8
	}
	for i := 144; i <= 255; i++ {
		litLens[i] = 9
	}
	for i := 256; i <= 279; i++ {
		litLens[i] = 7
	}
	for i := 280; i <= 287; i++ {
		litLens[i] = 8
	}
	_ = fixedLitTable.build(litLens[:])

	var distLens [32]uint8
	for i := range distLens {
		distLens[i] = 5
	}
	_ = fixedDistTable.build(distLens[:])
}

type fastInflateState struct {
	litTable  huffTable
	distTable huffTable
	clTable   [128]uint16 // 7-bit direct lookup for code-length alphabet
	lens      [320]uint8
}

var fastInflatePool = sync.Pool{
	New: func() any {
		return new(fastInflateState)
	},
}

// buildCLTable builds a 7-bit direct lookup table for the RFC 1951 code-length alphabet.
func buildCLTable(dst *[128]uint16, lengths []uint8) error {
	var count [8]uint16
	maxLen := 0
	for _, l := range lengths {
		if l > 7 {
			return errors.New("code-length symbol exceeds 7 bits")
		}
		if l > 0 {
			count[l]++
			if int(l) > maxLen {
				maxLen = int(l)
			}
		}
	}
	if maxLen == 0 {
		return errors.New("empty code-length tree")
	}
	left := 1
	for bits := 1; bits <= maxLen; bits++ {
		left <<= 1
		left -= int(count[bits])
		if left < 0 {
			return errors.New("oversubscribed code-length tree")
		}
	}
	var nextCode [8]uint16
	code := uint16(0)
	for bits := 1; bits <= maxLen; bits++ {
		code = (code + count[bits-1]) << 1
		nextCode[bits] = code
	}
	clear(dst[:])
	for sym, l := range lengths {
		if l == 0 {
			continue
		}
		c := nextCode[l]
		nextCode[l]++
		rev := reverseBits15(c, l)
		entry := uint16(sym<<4) | uint16(l)
		step := 1 << l
		for idx := int(rev); idx < 128; idx += step {
			dst[idx] = entry
		}
	}
	return nil
}

// adler32Sum computes RFC 1950 Adler-32 checksum with zero allocations.
func adler32Sum(p []byte) uint32 {
	const mod = 65521
	const nmax = 5552
	s1 := uint32(1)
	s2 := uint32(0)
	for len(p) > 0 {
		var q []byte
		if len(p) > nmax {
			p, q = p[:nmax], p[nmax:]
		}
		for len(p) >= 8 {
			s1 += uint32(p[0])
			s2 += s1
			s1 += uint32(p[1])
			s2 += s1
			s1 += uint32(p[2])
			s2 += s1
			s1 += uint32(p[3])
			s2 += s1
			s1 += uint32(p[4])
			s2 += s1
			s1 += uint32(p[5])
			s2 += s1
			s1 += uint32(p[6])
			s2 += s1
			s1 += uint32(p[7])
			s2 += s1
			p = p[8:]
		}
		for _, b := range p {
			s1 += uint32(b)
			s2 += s1
		}
		s1 %= mod
		s2 %= mod
		p = q
	}
	return (s2 << 16) | s1
}

// inflateZlibInto inflates an RFC 1950 zlib stream from src directly into dst (whose len must equal expectedSize),
// verifies the exact uncompressed length and Adler-32 trailer, and returns exact consumed bytes of src.
func inflateZlibInto(src []byte, dst []byte) (int, error) {
	if len(src) < 6 {
		return 0, errors.New("zlib stream too short")
	}
	cmf := src[0]
	flg := src[1]
	if (cmf&0x0f) != 8 || (cmf>>4) > 7 || (uint16(cmf)<<8|uint16(flg))%31 != 0 {
		return 0, errors.New("invalid zlib header")
	}
	if (flg & 0x20) != 0 {
		// Preset dictionary: fall back to stdlib zlib
		return inflateStdlibInto(src, dst)
	}

	pos := 2
	srcLen := len(src)
	var bits uint64
	var nbits uint

	refill := func() {
		if pos+8 <= srcLen && nbits <= 56 {
			// Load up to remaining bit capacity using fast unaligned LE read
			avail := (64 - nbits) >> 3
			raw := binary.LittleEndian.Uint64(src[pos:])
			if avail < 8 {
				raw &= (uint64(1) << (avail << 3)) - 1
			}
			bits |= raw << nbits
			pos += int(avail)
			nbits += avail << 3
			return
		}
		for nbits <= 56 && pos < srcLen {
			bits |= uint64(src[pos]) << nbits
			pos++
			nbits += 8
		}
	}

	ensureBits := func(need uint) bool {
		if nbits >= need {
			return true
		}
		refill()
		return nbits >= need
	}

	var st *fastInflateState
	outPos := 0
	expectedSize := len(dst)

	for {
		if !ensureBits(3) {
			if st != nil {
				fastInflatePool.Put(st)
			}
			return 0, errors.New("truncated deflate block header")
		}
		bfinal := bits & 1
		btype := (bits >> 1) & 3
		bits >>= 3
		nbits -= 3

		switch btype {
		case 0: // Stored block
			// Align to byte boundary, returning whole prefetched bytes back to src
			pos -= int(nbits >> 3)
			bits = 0
			nbits = 0
			if pos+4 > srcLen {
				if st != nil {
					fastInflatePool.Put(st)
				}
				return 0, errors.New("truncated stored block header")
			}
			blen := binary.LittleEndian.Uint16(src[pos : pos+2])
			nlen := binary.LittleEndian.Uint16(src[pos+2 : pos+4])
			pos += 4
			if blen != ^nlen {
				if st != nil {
					fastInflatePool.Put(st)
				}
				return 0, errors.New("stored block length mismatch")
			}
			n := int(blen)
			if pos+n > srcLen || outPos+n > expectedSize {
				if st != nil {
					fastInflatePool.Put(st)
				}
				return 0, errors.New("stored block out of bounds")
			}
			copy(dst[outPos:outPos+n], src[pos:pos+n])
			pos += n
			outPos += n

		case 1, 2:
			var litTbl, distTbl *huffTable
			if btype == 1 {
				litTbl = &fixedLitTable
				distTbl = &fixedDistTable
			} else {
				if st == nil {
					st = fastInflatePool.Get().(*fastInflateState)
				}
				if !ensureBits(14) {
					fastInflatePool.Put(st)
					return 0, errors.New("truncated dynamic huffman header")
				}
				hlit := int(bits&0x1f) + 257
				hdist := int((bits>>5)&0x1f) + 1
				hclen := int((bits>>10)&0x0f) + 4
				bits >>= 14
				nbits -= 14
				if hlit > 286 || hdist > 30 {
					fastInflatePool.Put(st)
					return 0, errors.New("invalid dynamic huffman counts")
				}

				var clLens [19]uint8
				for i := 0; i < hclen; i++ {
					if !ensureBits(3) {
						fastInflatePool.Put(st)
						return 0, errors.New("truncated code-length table")
					}
					clLens[codeLenOrder[i]] = uint8(bits & 7)
					bits >>= 3
					nbits -= 3
				}
				if err := buildCLTable(&st.clTable, clLens[:]); err != nil {
					fastInflatePool.Put(st)
					return 0, err
				}

				totalLens := hlit + hdist
				i := 0
				for i < totalLens {
					if nbits < 15 {
						refill()
					}
					e := st.clTable[bits&0x7f]
					bl := uint(e & 0x0f)
					if bl == 0 || nbits < bl {
						fastInflatePool.Put(st)
						return 0, errors.New("invalid code-length huffman symbol")
					}
					sym := e >> 4
					bits >>= bl
					nbits -= bl

					if sym < 16 {
						st.lens[i] = uint8(sym)
						i++
					} else if sym == 16 {
						if i == 0 || !ensureBits(2) {
							fastInflatePool.Put(st)
							return 0, errors.New("invalid repeat-prev code")
						}
						rep := int(bits&3) + 3
						bits >>= 2
						nbits -= 2
						if i+rep > totalLens {
							fastInflatePool.Put(st)
							return 0, errors.New("code-length repeat overflow")
						}
						prev := st.lens[i-1]
						for r := 0; r < rep; r++ {
							st.lens[i+r] = prev
						}
						i += rep
					} else if sym == 17 {
						if !ensureBits(3) {
							fastInflatePool.Put(st)
							return 0, errors.New("truncated repeat-zero-3 code")
						}
						rep := int(bits&7) + 3
						bits >>= 3
						nbits -= 3
						if i+rep > totalLens {
							fastInflatePool.Put(st)
							return 0, errors.New("code-length zero repeat overflow")
						}
						for r := 0; r < rep; r++ {
							st.lens[i+r] = 0
						}
						i += rep
					} else { // sym == 18
						if !ensureBits(7) {
							fastInflatePool.Put(st)
							return 0, errors.New("truncated repeat-zero-11 code")
						}
						rep := int(bits&0x7f) + 11
						bits >>= 7
						nbits -= 7
						if i+rep > totalLens {
							fastInflatePool.Put(st)
							return 0, errors.New("code-length long zero repeat overflow")
						}
						for r := 0; r < rep; r++ {
							st.lens[i+r] = 0
						}
						i += rep
					}
				}
				if st.lens[256] == 0 {
					fastInflatePool.Put(st)
					return 0, errors.New("missing EOB symbol in dynamic huffman block")
				}
				if err := st.litTable.build(st.lens[:hlit]); err != nil {
					fastInflatePool.Put(st)
					return 0, err
				}
				if err := st.distTable.build(st.lens[hlit : hlit+hdist]); err != nil {
					fastInflatePool.Put(st)
					return 0, err
				}
				litTbl = &st.litTable
				distTbl = &st.distTable
			}

			// Tight hot decode loop
			for {
				if nbits < 30 {
					if pos+8 <= srcLen {
						avail := (64 - nbits) >> 3
						raw := binary.LittleEndian.Uint64(src[pos:])
						if avail < 8 {
							raw &= (uint64(1) << (avail << 3)) - 1
						}
						bits |= raw << nbits
						pos += int(avail)
						nbits += avail << 3
					} else {
						for nbits <= 56 && pos < srcLen {
							bits |= uint64(src[pos]) << nbits
							pos++
							nbits += 8
						}
					}
				}

				// Decode literal/length symbol
				e := litTbl.primary[bits&primaryMask]
				var sym uint16
				if (e & 0x8000) == 0 {
					bl := uint(e & 0x0f)
					if bl == 0 || nbits < bl {
						if st != nil {
							fastInflatePool.Put(st)
						}
						return 0, errors.New("invalid literal/length huffman code")
					}
					sym = e >> 4
					bits >>= bl
					nbits -= bl
				} else {
					if nbits < primaryBits {
						if st != nil {
							fastInflatePool.Put(st)
						}
						return 0, errors.New("truncated long literal/length code")
					}
					subOff := int((e >> 4) & 0x7ff)
					subMask := uint64((1 << (e & 0x0f)) - 1)
					se := litTbl.sub[subOff+int((bits>>primaryBits)&subMask)]
					sbl := uint(se & 0x0f)
					totalBits := primaryBits + sbl
					if sbl == 0 || nbits < totalBits {
						if st != nil {
							fastInflatePool.Put(st)
						}
						return 0, errors.New("invalid subtable literal/length code")
					}
					sym = se >> 4
					bits >>= totalBits
					nbits -= totalBits
				}

				if sym < 256 {
					if outPos >= expectedSize {
						if st != nil {
							fastInflatePool.Put(st)
						}
						return 0, errors.New("inflate output exceeds declared size")
					}
					dst[outPos] = byte(sym)
					outPos++
					continue
				}
				if sym == 256 {
					break
				}
				if sym > 285 {
					if st != nil {
						fastInflatePool.Put(st)
					}
					return 0, errors.New("invalid length symbol")
				}

				lenIdx := sym - 257
				length := int(lengthBase[lenIdx])
				lextra := uint(lengthExtra[lenIdx])
				if lextra > 0 {
					if nbits < lextra {
						refill()
						if nbits < lextra {
							if st != nil {
								fastInflatePool.Put(st)
							}
							return 0, errors.New("truncated length extra bits")
						}
					}
					length += int(bits & ((uint64(1) << lextra) - 1))
					bits >>= lextra
					nbits -= lextra
				}

				if nbits < 28 {
					refill()
				}

				// Decode distance symbol
				de := distTbl.primary[bits&primaryMask]
				var distSym uint16
				if (de & 0x8000) == 0 {
					bl := uint(de & 0x0f)
					if bl == 0 || nbits < bl {
						if st != nil {
							fastInflatePool.Put(st)
						}
						return 0, errors.New("invalid distance huffman code")
					}
					distSym = de >> 4
					bits >>= bl
					nbits -= bl
				} else {
					if nbits < primaryBits {
						if st != nil {
							fastInflatePool.Put(st)
						}
						return 0, errors.New("truncated long distance code")
					}
					subOff := int((de >> 4) & 0x7ff)
					subMask := uint64((1 << (de & 0x0f)) - 1)
					se := distTbl.sub[subOff+int((bits>>primaryBits)&subMask)]
					sbl := uint(se & 0x0f)
					totalBits := primaryBits + sbl
					if sbl == 0 || nbits < totalBits {
						if st != nil {
							fastInflatePool.Put(st)
						}
						return 0, errors.New("invalid subtable distance code")
					}
					distSym = se >> 4
					bits >>= totalBits
					nbits -= totalBits
				}

				if distSym >= 30 {
					if st != nil {
						fastInflatePool.Put(st)
					}
					return 0, errors.New("invalid distance symbol")
				}
				dist := int(distBase[distSym])
				dextra := uint(distExtra[distSym])
				if dextra > 0 {
					if nbits < dextra {
						refill()
						if nbits < dextra {
							if st != nil {
								fastInflatePool.Put(st)
							}
							return 0, errors.New("truncated distance extra bits")
						}
					}
					dist += int(bits & ((uint64(1) << dextra) - 1))
					bits >>= dextra
					nbits -= dextra
				}

				if dist > outPos || outPos+length > expectedSize {
					if st != nil {
						fastInflatePool.Put(st)
					}
					return 0, errors.New("lz77 match out of bounds")
				}

				// Fast LZ77 copy
				if dist >= length {
					copy(dst[outPos:outPos+length], dst[outPos-dist:outPos-dist+length])
				} else if dist == 1 {
					b := dst[outPos-1]
					for k := outPos; k < outPos+length; k++ {
						dst[k] = b
					}
				} else {
					srcOff := outPos - dist
					dstOff := outPos
					rem := length
					for rem > 0 {
						n := copy(dst[dstOff:dstOff+rem], dst[srcOff:dstOff])
						dstOff += n
						rem -= n
					}
				}
				outPos += length
			}

		default:
			if st != nil {
				fastInflatePool.Put(st)
			}
			return 0, errors.New("invalid deflate block type 3")
		}

		if bfinal != 0 {
			break
		}
	}

	if st != nil {
		fastInflatePool.Put(st)
	}

	// Align to byte boundary for the 4-byte Adler-32 trailer
	pos -= int(nbits >> 3)
	if outPos != expectedSize {
		return 0, fmt.Errorf("zlib inflate size mismatch (%d != %d)", outPos, expectedSize)
	}
	if pos+4 > srcLen {
		return 0, errors.New("truncated zlib adler32 trailer")
	}
	wantAdler := binary.BigEndian.Uint32(src[pos : pos+4])
	pos += 4
	if adler32Sum(dst) != wantAdler {
		return 0, errors.New("zlib adler32 checksum mismatch")
	}
	return pos, nil
}

func inflateStdlibInto(src []byte, dst []byte) (int, error) {
	sr := &sliceByteReader{b: src}
	zr, err := zlib.NewReader(sr)
	if err != nil {
		return 0, err
	}
	n, readErr := io.ReadFull(zr, dst)
	if readErr != nil && !(readErr == io.EOF && len(dst) == 0) {
		_ = zr.Close()
		return 0, fmt.Errorf("zlib inflate short read (%d/%d): %w", n, len(dst), readErr)
	}
	var extra [1]byte
	m, eofErr := zr.Read(extra[:])
	_ = zr.Close()
	if m != 0 || eofErr != io.EOF {
		return 0, errors.New("zlib stream has unexpected trailing uncompressed data")
	}
	return sr.pos, nil
}
