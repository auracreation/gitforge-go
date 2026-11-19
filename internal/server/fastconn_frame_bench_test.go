package server

import (
	"bytes"
	"net"
	"testing"
	"time"

	"gitforge/internal/store"
)

type frameSink struct{ net.Conn }

func (frameSink) Write(p []byte) (int, error)      { return len(p), nil }
func (frameSink) SetWriteDeadline(time.Time) error { return nil }

func BenchmarkFastFrame(b *testing.B) {
	srv := NewFastServer(NewRouter(store.NewForge("")))
	for _, entry := range []struct {
		name string
		size int
	}{{"issue", 512}, {"pack", 16384}} {
		b.Run(entry.name, func(b *testing.B) {
			frame := store.BuildHTTPResponse("HTTP/1.1 200 OK", "application/octet-stream", "", bytes.Repeat([]byte("x"), entry.size))
			conn := frameSink{}
			b.ReportAllocs()
			b.SetBytes(int64(entry.size))
			for i := 0; i < b.N; i++ {
				if _, err := srv.writeFrame(conn, &connBuf{}, frame); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
