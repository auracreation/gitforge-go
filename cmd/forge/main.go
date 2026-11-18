package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"gitforge/internal/server"
	"gitforge/internal/store"
)

// tcpNoDelayListener wraps a *net.TCPListener so every accepted connection sets TCP_NODELAY
// and keep-alive immediately (avoiding Nagle delays on loopback / gVisor Netstack).
type tcpNoDelayListener struct {
	*net.TCPListener
}

func (ln tcpNoDelayListener) Accept() (net.Conn, error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return nil, err
	}
	_ = tc.SetNoDelay(true)
	_ = tc.SetKeepAlive(true)
	_ = tc.SetKeepAlivePeriod(60 * time.Second)
	return tc, nil
}

func main() {
	// Respect the 1024 MiB sandbox RAM ceiling while avoiding excessive GC thrashing on transient HTTP buffers
	debug.SetMemoryLimit(750 * 1024 * 1024)
	debug.SetGCPercent(400)

	portFlag := flag.Int("port", 0, "TCP port to listen on (0 picks an ephemeral port)")
	tokenFlag := flag.String("token", "", "override MAKEFUN_SHOP_TOKEN for standalone benchmarking")
	pprofAddr := flag.String("pprof", "", "optional pprof listen address (e.g. 127.0.0.1:6060) for local profiling")
	standalone := flag.Bool("standalone", false, "run standalone without MAKEFUN_ENV_URL")
	flag.Parse()

	startPprof(*pprofAddr)

	envURL := strings.TrimRight(strings.TrimSpace(os.Getenv("MAKEFUN_ENV_URL")), "/")
	if !*standalone && envURL == "" {
		fmt.Fprintln(os.Stderr, "[entry] MAKEFUN_ENV_URL is not set: there is no env to register with")
		os.Exit(1)
	}

	token := *tokenFlag
	if token == "" {
		token = os.Getenv("MAKEFUN_SHOP_TOKEN")
	}

	forge := store.NewForge(token)
	router := server.NewRouter(forge)

	rawLn, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *portFlag))
	if err != nil {
		fmt.Fprintf(os.Stderr, "[entry] failed to bind TCP port: %v\n", err)
		os.Exit(1)
	}
	tcpLn := tcpNoDelayListener{rawLn.(*net.TCPListener)}
	boundPort := tcpLn.Addr().(*net.TCPAddr).Port

	srv := &http.Server{
		Handler:           router,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    64 * 1024,
	}

	go func() {
		if serveErr := srv.Serve(tcpLn); serveErr != nil && serveErr != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "[entry] http server error: %v\n", serveErr)
			os.Exit(1)
		}
	}()

	fmt.Fprintf(os.Stdout, "[entry] listening on port %d\n", boundPort)

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	if *standalone {
		<-sigCh
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		return
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        4,
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	if err := registerWithEnv(client, envURL, boundPort); err != nil {
		fmt.Fprintf(os.Stderr, "[entry] %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stdout, "[entry] registered; waiting for the env to say done")

	waitDoneCh := make(chan struct{})
	go func() {
		waitForDone(client, envURL)
		close(waitDoneCh)
	}()

	select {
	case <-waitDoneCh:
	case <-sigCh:
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	fmt.Fprintln(os.Stdout, "[entry] done")
}

func retryDelay(attempt int) time.Duration {
	if attempt > 3 {
		return 500 * time.Millisecond
	}
	d := 100 * time.Millisecond * time.Duration(1<<attempt)
	if d > 500*time.Millisecond {
		return 500 * time.Millisecond
	}
	return d
}

func registerWithEnv(client *http.Client, envURL string, port int) error {
	payload, _ := json.Marshal(map[string]int{"port": port})
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequest(http.MethodPost, envURL+"/register", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusConflict {
				return nil
			}
			if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusMethodNotAllowed {
				return fmt.Errorf("register answered HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
			}
		}
		time.Sleep(retryDelay(attempt))
	}
}

func waitForDone(client *http.Client, envURL string) {
	for attempt := 0; ; attempt++ {
		resp, err := client.Get(envURL + "/done")
		if err == nil {
			var status struct {
				Done bool `json:"done"`
			}
			decErr := json.NewDecoder(resp.Body).Decode(&status)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK && decErr == nil && status.Done {
				return
			}
			attempt = 0
		}
		time.Sleep(250 * time.Millisecond)
	}
}
