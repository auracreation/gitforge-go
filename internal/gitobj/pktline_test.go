package gitobj

import (
	"strings"
	"testing"
)

func TestDecodeSmallRequestAllocation(t *testing.T) {
	src := []byte("0008abc\n000100020000")
	allocs := testing.AllocsPerRun(100, func() {
		var storage [16]Packet
		packets, _, err := DecodePktLines(storage[:0], src, 0)
		if err != nil || len(packets) != 4 {
			panic("invalid decoded request")
		}
	})
	if allocs != 0 {
		t.Fatalf("small packet decode allocates %g times", allocs)
	}
}

func TestDecodePktLines(t *testing.T) {
	for _, tt := range []struct {
		name, input  string
		stop, offset int
		kinds        []PktKind
		payloads     []string
		invalid      bool
	}{
		{name: "empty"},
		{name: "data and controls", input: "0008abc\n000100020000", offset: 20, kinds: []PktKind{PktData, PktDelim, PktResponseEnd, PktFlush}, payloads: []string{"abc\n", "", "", ""}},
		{name: "pack boundary", input: "0008abc\n0000PACK", stop: 1, offset: 12, kinds: []PktKind{PktData, PktFlush}, payloads: []string{"abc\n", ""}},
		{name: "second flush", input: "000000010000PACK", stop: 2, offset: 12, kinds: []PktKind{PktFlush, PktDelim, PktFlush}, payloads: []string{"", "", ""}},
		{name: "short header", input: "000", invalid: true},
		{name: "nonhex", input: "000g", invalid: true},
		{name: "invalid length", input: "0003", invalid: true},
		{name: "short payload", input: "0008ab", invalid: true},
		{name: "oversize", input: "fff1", invalid: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var storage [16]Packet
			packets, offset, err := DecodePktLines(storage[:0], []byte(tt.input), tt.stop)
			if tt.invalid {
				if err == nil {
					t.Fatal("accepted malformed packet")
				}
				return
			}
			if err != nil || offset != tt.offset || len(packets) != len(tt.kinds) {
				t.Fatalf("packets=%v offset=%d err=%v", packets, offset, err)
			}
			for i, packet := range packets {
				if packet.Kind != tt.kinds[i] || string(packet.Payload) != tt.payloads[i] {
					t.Fatalf("packet %d: %+v", i, packet)
				}
			}
		})
	}
	var storage [16]Packet
	packets, offset, err := DecodePktLines(storage[:0], []byte(strings.Repeat("0005x", 40)+"0000"), 1)
	if err != nil || offset != 204 || len(packets) != 41 {
		t.Fatalf("large request: len=%d offset=%d err=%v", len(packets), offset, err)
	}
	for _, packet := range packets[:40] {
		if string(packet.Payload) != "x" {
			t.Fatalf("lost payload: %q", packet.Payload)
		}
	}
}
