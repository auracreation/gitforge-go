package gitobj

import (
	"bytes"
	"compress/zlib"
	"math/rand"
	"testing"
)

func compressZlib(t testing.TB, level int, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := zlib.NewWriterLevel(&buf, level)
	if err != nil {
		t.Fatalf("NewWriterLevel(%d): %v", level, err)
	}
	if _, err := w.Write(raw); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

func TestInflateZlibIntoDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	levels := []int{zlib.NoCompression, zlib.BestSpeed, zlib.DefaultCompression, zlib.BestCompression}

	inputs := [][]byte{
		{},
		[]byte("a"),
		bytes.Repeat([]byte("x"), 1024),
		bytes.Repeat([]byte("abc"), 1000),
		bytes.Repeat([]byte("The quick brown fox jumps over the lazy dog. "), 100),
		[]byte("tree 4b825dc642cb6eb9a060e54bf8d69288fbee4904\nauthor Ada <ada@example.invalid> 1700000000 +0000\ncommitter Ada <ada@example.invalid> 1700000000 +0000\n\nbench: push 1\n"),
	}
	// Add random & mixed compressible blobs
	for _, sz := range []int{17, 38, 64, 200, 1024, 4096, 16384} {
		raw := make([]byte, sz)
		for i := range raw {
			if i%5 == 0 {
				raw[i] = byte(rng.Intn(256))
			} else {
				raw[i] = byte('a' + rng.Intn(26))
			}
		}
		inputs = append(inputs, raw)

		rnd := make([]byte, sz)
		rng.Read(rnd)
		inputs = append(inputs, rnd)
	}

	trailing := []byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04}

	for idx, raw := range inputs {
		for _, lvl := range levels {
			z := compressZlib(t, lvl, raw)
			withTrail := append(append([]byte(nil), z...), trailing...)
			dst := make([]byte, len(raw))
			consumed, err := inflateZlibInto(withTrail, dst)
			if err != nil {
				t.Fatalf("input %d (len=%d) level %d: inflateZlibInto err: %v", idx, len(raw), lvl, err)
			}
			if consumed != len(z) {
				t.Fatalf("input %d (len=%d) level %d: consumed=%d want=%d", idx, len(raw), lvl, consumed, len(z))
			}
			if !bytes.Equal(dst, raw) {
				t.Fatalf("input %d (len=%d) level %d: output mismatch", idx, len(raw), lvl)
			}
		}
	}

	// Verify error detection: bad checksum, wrong size, truncated
	sample := compressZlib(t, zlib.DefaultCompression, bytes.Repeat([]byte("hello git forge! "), 80))
	badSum := append([]byte(nil), sample...)
	badSum[len(badSum)-1] ^= 0xff
	if _, err := inflateZlibInto(badSum, make([]byte, 80*17)); err == nil {
		t.Fatal("expected error on bad adler32 checksum")
	}
	if _, err := inflateZlibInto(sample, make([]byte, 80*17-1)); err == nil {
		t.Fatal("expected error on undersized dst")
	}
	if _, err := inflateZlibInto(sample, make([]byte, 80*17+1)); err == nil {
		t.Fatal("expected error on oversized dst")
	}
	if _, err := inflateZlibInto(sample[:len(sample)-5], make([]byte, 80*17)); err == nil {
		t.Fatal("expected error on truncated stream")
	}
}

func BenchmarkInflateFast1KiB(b *testing.B) {
	rng := rand.New(rand.NewSource(7))
	raw := make([]byte, 1024)
	for i := range raw {
		raw[i] = byte('a' + rng.Intn(26))
	}
	z := compressZlib(b, zlib.DefaultCompression, raw)
	dst := make([]byte, len(raw))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := inflateZlibInto(z, dst); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInflateStdlib1KiB(b *testing.B) {
	rng := rand.New(rand.NewSource(7))
	raw := make([]byte, 1024)
	for i := range raw {
		raw[i] = byte('a' + rng.Intn(26))
	}
	z := compressZlib(b, zlib.DefaultCompression, raw)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := inflateExact(z, len(raw)); err != nil {
			b.Fatal(err)
		}
	}
}
