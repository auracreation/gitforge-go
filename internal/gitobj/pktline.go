package gitobj

import (
	"errors"
	"fmt"
)

// PktKind classifies a Git pkt-line frame.
type PktKind uint8

const (
	PktData        PktKind = 1
	PktFlush       PktKind = 2 // 0000
	PktDelim       PktKind = 3 // 0001
	PktResponseEnd PktKind = 4 // 0002
)

// Packet is a parsed Git pkt-line frame.
type Packet struct {
	Kind    PktKind
	Payload []byte
}

const maxPktLineLen = 65520

// DecodePktLines decodes pkt-line frames from src.
// When stopAfterFlush > 0, decoding stops after encountering that many flush (0000) packets
// and returns the byte offset immediately following the final flush (used by receive-pack to slice the trailing packfile).
func DecodePktLines(src []byte, stopAfterFlush int) ([]Packet, int, error) {
	pkts := make([]Packet, 0, 8)
	pos := 0
	flushes := 0
	for pos < len(src) {
		if pos+4 > len(src) {
			return nil, pos, errors.New("truncated pkt-line header")
		}
		n0, ok0 := fromHexNibble(src[pos])
		n1, ok1 := fromHexNibble(src[pos+1])
		n2, ok2 := fromHexNibble(src[pos+2])
		n3, ok3 := fromHexNibble(src[pos+3])
		if !ok0 || !ok1 || !ok2 || !ok3 {
			return nil, pos, errors.New("invalid non-hex pkt-line length")
		}
		pktLen := (int(n0) << 12) | (int(n1) << 8) | (int(n2) << 4) | int(n3)
		switch pktLen {
		case 0:
			pos += 4
			pkts = append(pkts, Packet{Kind: PktFlush})
			flushes++
			if stopAfterFlush > 0 && flushes >= stopAfterFlush {
				return pkts, pos, nil
			}
		case 1:
			pos += 4
			pkts = append(pkts, Packet{Kind: PktDelim})
		case 2:
			pos += 4
			pkts = append(pkts, Packet{Kind: PktResponseEnd})
		case 3:
			return nil, pos, errors.New("invalid pkt-line length 0003")
		default:
			if pktLen > maxPktLineLen {
				return nil, pos, fmt.Errorf("pkt-line length %d exceeds maximum %d", pktLen, maxPktLineLen)
			}
			if pos+pktLen > len(src) {
				return nil, pos, errors.New("truncated pkt-line payload")
			}
			pkts = append(pkts, Packet{
				Kind:    PktData,
				Payload: src[pos+4 : pos+pktLen],
			})
			pos += pktLen
		}
	}
	return pkts, pos, nil
}

// TrimLF strips a single trailing '\n' (and optional '\r') from a pkt-line payload.
func TrimLF(b []byte) []byte {
	if len(b) > 0 && b[len(b)-1] == '\n' {
		b = b[:len(b)-1]
	}
	if len(b) > 0 && b[len(b)-1] == '\r' {
		b = b[:len(b)-1]
	}
	return b
}
