//go:build linux && !noepoll

package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"runtime"
	"strconv"
	"syscall"
	"unsafe"
)

// rawRead performs a non-blocking read(2) without runtime entersyscall/exitsyscall overhead.
// Safe exclusively on O_NONBLOCK file descriptors.
func rawRead(fd int, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	r1, _, e1 := syscall.RawSyscall(syscall.SYS_READ, uintptr(fd), uintptr(unsafe.Pointer(unsafe.SliceData(p))), uintptr(len(p)))
	if e1 != 0 {
		return -1, e1
	}
	return int(r1), nil
}

// rawWrite performs a non-blocking write(2) without runtime entersyscall/exitsyscall overhead.
// Safe exclusively on O_NONBLOCK file descriptors.
func rawWrite(fd int, p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	r1, _, e1 := syscall.RawSyscall(syscall.SYS_WRITE, uintptr(fd), uintptr(unsafe.Pointer(unsafe.SliceData(p))), uintptr(len(p)))
	if e1 != 0 {
		return -1, e1
	}
	return int(r1), nil
}

type epollConn struct {
	fd              int
	rbuf            []byte
	rStart          int
	rEnd            int
	wPending        []byte
	wantOut         bool
	closeAfterWrite bool
	sent100         bool
	remoteAddr      string
}

type epollPlatform struct {
	lfd  int
	epfd int
	evfd int
	done chan struct{}
}

// BindPort binds a non-blocking TCP listening socket directly on Linux for the epoll event loop.
func (s *FastServer) BindPort(port int) (int, error) {
	lfd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, 0)
	if err != nil {
		return 0, err
	}
	_ = syscall.SetsockoptInt(lfd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	_ = syscall.SetsockoptInt(lfd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	if err := syscall.Bind(lfd, &syscall.SockaddrInet4{Port: port}); err != nil {
		_ = syscall.Close(lfd)
		return 0, err
	}
	if err := syscall.Listen(lfd, 512); err != nil {
		_ = syscall.Close(lfd)
		return 0, err
	}
	sa, err := syscall.Getsockname(lfd)
	if err != nil {
		_ = syscall.Close(lfd)
		return 0, err
	}
	boundPort := sa.(*syscall.SockaddrInet4).Port

	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		_ = syscall.Close(lfd)
		return 0, err
	}
	r1, _, errno := syscall.Syscall(syscall.SYS_EVENTFD2, 0, uintptr(syscall.O_NONBLOCK|syscall.O_CLOEXEC), 0)
	if errno != 0 {
		_ = syscall.Close(epfd)
		_ = syscall.Close(lfd)
		return 0, errno
	}
	evfd := int(r1)

	if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, lfd, &syscall.EpollEvent{
		Events: syscall.EPOLLIN,
		Fd:     int32(lfd),
	}); err != nil {
		_ = syscall.Close(evfd)
		_ = syscall.Close(epfd)
		_ = syscall.Close(lfd)
		return 0, err
	}
	if err := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, evfd, &syscall.EpollEvent{
		Events: syscall.EPOLLIN,
		Fd:     int32(evfd),
	}); err != nil {
		_ = syscall.Close(evfd)
		_ = syscall.Close(epfd)
		_ = syscall.Close(lfd)
		return 0, err
	}

	s.mu.Lock()
	s.plat = &epollPlatform{
		lfd:  lfd,
		epfd: epfd,
		evfd: evfd,
		done: make(chan struct{}),
	}
	s.mu.Unlock()
	return boundPort, nil
}

// ServeBound runs the single-threaded pinned Linux epoll event loop with pre-warmed connection buffers.
func (s *FastServer) ServeBound() error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	s.mu.Lock()
	p := s.plat
	s.mu.Unlock()
	if p == nil {
		return errors.New("BindPort must be called before ServeBound")
	}
	defer close(p.done)

	lfd := p.lfd
	epfd := p.epfd
	evfd := p.evfd

	conns := make([]*epollConn, 256)
	// Pre-allocate 64 connection structs + 4 KiB read buffers in one contiguous slab
	// so 50 concurrent shallow clones incur zero heap allocations on accept!
	const prewarmN = 64
	const prewarmBuf = 4096
	slab := make([]byte, prewarmN*prewarmBuf)
	preConns := make([]epollConn, prewarmN)
	freeConns := make([]*epollConn, prewarmN)
	for i := 0; i < prewarmN; i++ {
		preConns[i].rbuf = slab[i*prewarmBuf : (i+1)*prewarmBuf : (i+1)*prewarmBuf]
		freeConns[i] = &preConns[i]
	}

	wbuf := make([]byte, 0, 16*1024)
	var events [128]syscall.EpollEvent

	getConn := func(fd int) *epollConn {
		var c *epollConn
		if n := len(freeConns); n > 0 {
			c = freeConns[n-1]
			freeConns = freeConns[:n-1]
		} else {
			c = &epollConn{
				rbuf: make([]byte, fastInitReadCap),
			}
		}
		c.fd = fd
		c.rStart = 0
		c.rEnd = 0
		c.wPending = c.wPending[:0]
		c.wantOut = false
		c.closeAfterWrite = false
		c.sent100 = false
		c.remoteAddr = "127.0.0.1:0"
		if fd >= len(conns) {
			grown := make([]*epollConn, fd+256)
			copy(grown, conns)
			conns = grown
		}
		conns[fd] = c
		return c
	}

	closeConn := func(c *epollConn) {
		fd := c.fd
		if fd >= 0 {
			_ = syscall.EpollCtl(epfd, syscall.EPOLL_CTL_DEL, fd, nil)
			_ = syscall.Close(fd)
			if fd < len(conns) {
				conns[fd] = nil
			}
			c.fd = -1
		}
		if cap(c.rbuf) > fastMaxPoolReadCap {
			c.rbuf = make([]byte, fastInitReadCap)
		}
		if cap(c.wPending) > fastMaxPoolWrite {
			c.wPending = nil
		} else {
			c.wPending = c.wPending[:0]
		}
		if len(freeConns) < 128 {
			freeConns = append(freeConns, c)
		}
	}

	setWantOut := func(c *epollConn, want bool) {
		if c.wantOut == want {
			return
		}
		ev := uint32(syscall.EPOLLIN)
		if want {
			ev |= syscall.EPOLLOUT
		}
		_ = syscall.EpollCtl(epfd, syscall.EPOLL_CTL_MOD, c.fd, &syscall.EpollEvent{
			Events: ev,
			Fd:     int32(c.fd),
		})
		c.wantOut = want
	}

	writeResp := func(c *epollConn, data []byte, closeAfter bool) bool {
		if len(data) == 0 {
			if closeAfter {
				closeConn(c)
				return false
			}
			return true
		}
		n, err := rawWrite(c.fd, data)
		if n < 0 {
			n = 0
		}
		if n == len(data) {
			if closeAfter {
				closeConn(c)
				return false
			}
			return true
		}
		if err != nil && err != syscall.EAGAIN && err != syscall.EWOULDBLOCK && err != syscall.EINTR {
			closeConn(c)
			return false
		}
		c.wPending = append(c.wPending[:0], data[n:]...)
		c.closeAfterWrite = closeAfter
		setWantOut(c, true)
		return false
	}

	processRequests := func(c *epollConn) {
		for len(c.wPending) == 0 && c.rEnd > c.rStart {
			hdrIdx := bytes.Index(c.rbuf[c.rStart:c.rEnd], headerEndBytes)
			if hdrIdx < 0 {
				if c.rEnd-c.rStart > fastMaxHeaderBytes {
					writeResp(c, badReqHTTP400, true)
				}
				return
			}
			hdrEnd := c.rStart + hdrIdx
			reqBlock := c.rbuf[c.rStart:hdrEnd]
			bodyStart := hdrEnd + 4

			firstLineEnd := bytes.Index(reqBlock, crlfBytes)
			var reqLine, hdrLines []byte
			if firstLineEnd < 0 {
				reqLine = reqBlock
			} else {
				reqLine = reqBlock[:firstLineEnd]
				hdrLines = reqBlock[firstLineEnd+2:]
			}

			sp1 := bytes.IndexByte(reqLine, ' ')
			if sp1 <= 0 {
				writeResp(c, badReqHTTP400, true)
				return
			}
			method := reqLine[:sp1]
			remLine := reqLine[sp1+1:]
			sp2 := bytes.IndexByte(remLine, ' ')
			if sp2 <= 0 {
				writeResp(c, badReqHTTP400, true)
				return
			}
			target := remLine[:sp2]
			proto := remLine[sp2+1:]
			isHTTP10 := bytes.Equal(proto, http10Bytes)
			if !isHTTP10 && !bytes.Equal(proto, http11Bytes) {
				writeResp(c, badReqHTTP400, true)
				return
			}

			var (
				authVal       []byte
				gitProto      []byte
				contentEnc    []byte
				hostVal       []byte
				contentLen    int64 = -1
				isChunked     bool
				connClose     bool
				connKeepAlive bool
				expect100     bool
				badHeader     bool
			)

			scan := hdrLines
			for len(scan) > 0 {
				var line []byte
				if nl := bytes.Index(scan, crlfBytes); nl >= 0 {
					line = scan[:nl]
					scan = scan[nl+2:]
				} else {
					line = scan
					scan = nil
				}
				if len(line) == 0 {
					continue
				}
				colon := bytes.IndexByte(line, ':')
				if colon <= 0 {
					badHeader = true
					break
				}
				name := line[:colon]
				val := trimOWS(line[colon+1:])

				switch len(name) {
				case 4:
					if equalFoldASCII(name, "host") {
						hostVal = val
					}
				case 6:
					if equalFoldASCII(name, "expect") && equalFoldASCII(val, "100-continue") {
						expect100 = true
					}
				case 10:
					if equalFoldASCII(name, "connection") {
						if containsTokenFoldASCII(val, "close") {
							connClose = true
						}
						if containsTokenFoldASCII(val, "keep-alive") {
							connKeepAlive = true
						}
					}
				case 12:
					if equalFoldASCII(name, "git-protocol") {
						gitProto = val
					}
				case 13:
					if equalFoldASCII(name, "authorization") {
						authVal = val
					}
				case 14:
					if equalFoldASCII(name, "content-length") {
						cl, ok := parseContentLength(val)
						if !ok || (contentLen >= 0 && contentLen != cl) {
							badHeader = true
							break
						}
						contentLen = cl
					}
				case 16:
					if equalFoldASCII(name, "content-encoding") {
						contentEnc = val
					}
				case 17:
					if equalFoldASCII(name, "transfer-encoding") {
						if containsTokenFoldASCII(val, "chunked") {
							isChunked = true
						}
					}
				}
			}
			if badHeader {
				writeResp(c, badReqHTTP400, true)
				return
			}

			closeReq := connClose || (isHTTP10 && !connKeepAlive)

			var body []byte
			nextStart := bodyStart
			if isChunked {
				if expect100 && !c.sent100 {
					c.sent100 = true
					if _, err := rawWrite(c.fd, expect100Resp); err != nil && err != syscall.EAGAIN {
						closeConn(c)
						return
					}
				}
				decoded, consumed, complete, cerr := tryDecodeChunkedNonBlocking(c.rbuf[bodyStart:c.rEnd])
				if cerr != nil {
					writeResp(c, badReqHTTP400, true)
					return
				}
				if !complete {
					return
				}
				body = decoded
				nextStart = bodyStart + consumed
			} else if contentLen > 0 {
				if contentLen > fastMaxBodyBytes {
					writeResp(c, badReqHTTP400, true)
					return
				}
				cl := int(contentLen)
				if c.rEnd-bodyStart < cl {
					if expect100 && !c.sent100 {
						c.sent100 = true
						if _, err := rawWrite(c.fd, expect100Resp); err != nil && err != syscall.EAGAIN {
							closeConn(c)
							return
						}
					}
					if c.rStart > 0 {
						n := copy(c.rbuf, c.rbuf[c.rStart:c.rEnd])
						bodyStart -= c.rStart
						c.rStart = 0
						c.rEnd = n
					}
					need := bodyStart + cl
					if len(c.rbuf) < need {
						newCap := len(c.rbuf) * 2
						if newCap < need {
							newCap = need
						}
						bigger := make([]byte, newCap)
						copy(bigger, c.rbuf[:c.rEnd])
						c.rbuf = bigger
					}
					return
				}
				body = c.rbuf[bodyStart : bodyStart+cl]
				nextStart = bodyStart + cl
			}

			c.sent100 = false

			path := target
			var rawQuery []byte
			if qIdx := bytes.IndexByte(target, '?'); qIdx >= 0 {
				path = target[:qIdx]
				rawQuery = target[qIdx+1:]
			}

			var resp []byte
			var handled bool
			if !closeReq && !isChunked && len(contentEnc) == 0 {
				resp, wbuf, handled = s.tryFastResponse(wbuf[:0], method, path, rawQuery, authVal, gitProto, body)
			}
			if !handled {
				resp, wbuf = s.buildFallbackResponse(wbuf[:0], method, target, proto, hdrLines, hostVal, body, closeReq, c.remoteAddr)
			}

			c.rStart = nextStart
			if c.rStart == c.rEnd {
				c.rStart = 0
				c.rEnd = 0
			} else if c.rStart > len(c.rbuf)/2 {
				n := copy(c.rbuf, c.rbuf[c.rStart:c.rEnd])
				c.rStart = 0
				c.rEnd = n
			}

			if !writeResp(c, resp, closeReq) {
				return
			}
		}
	}

	defer func() {
		for _, c := range conns {
			if c != nil {
				closeConn(c)
			}
		}
		_ = syscall.Close(evfd)
		_ = syscall.Close(epfd)
		_ = syscall.Close(lfd)
	}()

	for {
		nEvents, err := syscall.EpollWait(epfd, events[:], -1)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			if s.closed.Load() {
				return http.ErrServerClosed
			}
			return err
		}

		for i := 0; i < nEvents; i++ {
			fd := int(events[i].Fd)
			ev := events[i].Events

			if fd == evfd {
				var drain [8]byte
				_, _ = syscall.Read(evfd, drain[:])
				if s.closed.Load() {
					return http.ErrServerClosed
				}
				continue
			}

			if fd == lfd {
				for {
					cfd, _, aerr := syscall.Accept4(lfd, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
					if aerr != nil {
						if aerr == syscall.EAGAIN || aerr == syscall.EWOULDBLOCK {
							break
						}
						if aerr == syscall.EINTR {
							continue
						}
						break
					}
					_ = syscall.SetsockoptInt(cfd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
					c := getConn(cfd)
					if ctlErr := syscall.EpollCtl(epfd, syscall.EPOLL_CTL_ADD, cfd, &syscall.EpollEvent{
						Events: syscall.EPOLLIN,
						Fd:     int32(cfd),
					}); ctlErr != nil {
						closeConn(c)
					}
				}
				continue
			}

			if fd < 0 || fd >= len(conns) {
				continue
			}
			c := conns[fd]
			if c == nil {
				continue
			}

			if ev&(syscall.EPOLLERR|syscall.EPOLLHUP) != 0 && ev&syscall.EPOLLIN == 0 {
				closeConn(c)
				continue
			}

			if ev&syscall.EPOLLOUT != 0 && len(c.wPending) > 0 {
				n, werr := rawWrite(c.fd, c.wPending)
				if n > 0 {
					if n == len(c.wPending) {
						c.wPending = c.wPending[:0]
						if c.closeAfterWrite {
							closeConn(c)
							continue
						}
						setWantOut(c, false)
						if c.rEnd > c.rStart {
							processRequests(c)
						}
					} else {
						remaining := copy(c.wPending, c.wPending[n:])
						c.wPending = c.wPending[:remaining]
					}
				} else if werr != nil && werr != syscall.EAGAIN && werr != syscall.EWOULDBLOCK && werr != syscall.EINTR {
					closeConn(c)
					continue
				}
			}

			if ev&syscall.EPOLLIN != 0 && c.fd >= 0 {
				for {
					if c.rEnd == len(c.rbuf) {
						if c.rStart > 0 {
							n := copy(c.rbuf, c.rbuf[c.rStart:c.rEnd])
							c.rStart = 0
							c.rEnd = n
						} else {
							if len(c.rbuf) >= fastMaxBodyBytes+fastMaxHeaderBytes {
								writeResp(c, badReqHTTP400, true)
								break
							}
							bigger := make([]byte, len(c.rbuf)*2)
							copy(bigger, c.rbuf[:c.rEnd])
							c.rbuf = bigger
						}
					}
					avail := len(c.rbuf) - c.rEnd
					n, rerr := rawRead(c.fd, c.rbuf[c.rEnd:])
					if n > 0 {
						c.rEnd += n
						if len(c.wPending) == 0 {
							processRequests(c)
							if c.fd < 0 {
								break
							}
						}
						if n < avail {
							break
						}
						continue
					}
					if n == 0 {
						closeConn(c)
						break
					}
					if rerr == syscall.EAGAIN || rerr == syscall.EWOULDBLOCK {
						break
					}
					if rerr == syscall.EINTR {
						continue
					}
					closeConn(c)
					break
				}
			}
		}
	}
}

func (s *FastServer) shutdownPlatform(ctx context.Context) error {
	s.mu.Lock()
	p := s.plat
	s.mu.Unlock()
	if p == nil {
		return nil
	}
	var one [8]byte
	binary.LittleEndian.PutUint64(one[:], 1)
	_, _ = syscall.Write(p.evfd, one[:])
	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// tryDecodeChunkedNonBlocking decodes an HTTP/1.1 chunked body from src without blocking.
// Returns complete=false when more bytes are needed from the socket.
func tryDecodeChunkedNonBlocking(src []byte) (body []byte, consumed int, complete bool, err error) {
	pos := 0
	var out []byte
	for {
		lineEnd := bytes.Index(src[pos:], crlfBytes)
		if lineEnd < 0 {
			return nil, 0, false, nil
		}
		line := src[pos : pos+lineEnd]
		pos += lineEnd + 2
		if semi := bytes.IndexByte(line, ';'); semi >= 0 {
			line = line[:semi]
		}
		line = trimOWS(line)
		sz, perr := strconv.ParseInt(string(line), 16, 64)
		if perr != nil || sz < 0 || int64(len(out))+sz > fastMaxBodyBytes {
			return nil, 0, false, errors.New("invalid chunk size")
		}
		if sz == 0 {
			for {
				tEnd := bytes.Index(src[pos:], crlfBytes)
				if tEnd < 0 {
					return nil, 0, false, nil
				}
				pos += tEnd + 2
				if tEnd == 0 {
					return out, pos, true, nil
				}
			}
		}
		need := int(sz) + 2
		if pos+need > len(src) {
			return nil, 0, false, nil
		}
		if src[pos+int(sz)] != '\r' || src[pos+int(sz)+1] != '\n' {
			return nil, 0, false, errors.New("missing chunk CRLF")
		}
		out = append(out, src[pos:pos+int(sz)]...)
		pos += need
	}
}
