//go:build !linux || noepoll

package server

import (
	"context"
	"errors"
	"fmt"
	"net"
)

type epollPlatform struct {
	ln net.Listener
}

// BindPort binds a standard TCP listener on non-Linux platforms (e.g. macOS development).
func (s *FastServer) BindPort(port int) (int, error) {
	ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		return 0, err
	}
	boundPort := ln.Addr().(*net.TCPAddr).Port
	s.mu.Lock()
	s.plat = &epollPlatform{ln: ln}
	s.mu.Unlock()
	return boundPort, nil
}

// ServeBound serves the bound listener using the goroutine-per-connection FastServer loop.
func (s *FastServer) ServeBound() error {
	s.mu.Lock()
	p := s.plat
	s.mu.Unlock()
	if p == nil || p.ln == nil {
		return errors.New("BindPort must be called before ServeBound")
	}
	return s.Serve(p.ln)
}

func (s *FastServer) shutdownPlatform(ctx context.Context) error {
	return nil
}
