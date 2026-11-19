package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

func main() {
	// Hard ceiling below 1024 MiB pod limit; high GOGC prevents GC worker wakeups during timed rounds
	debug.SetMemoryLimit(750 * 1024 * 1024)
	debug.SetGCPercent(1000)

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

	// Go's net.Listen("tcp", ...) enables TCP_NODELAY and keep-alives by default without extra setsockopt syscalls per Accept.
	tcpLn, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *portFlag))
	if err != nil {
		fmt.Fprintf(os.Stderr, "[entry] failed to bind TCP port: %v\n", err)
		os.Exit(1)
	}
	boundPort := tcpLn.Addr().(*net.TCPAddr).Port

	serverVariant := strings.ToLower(strings.TrimSpace(os.Getenv("FORGE_SERVER")))

	var (
		srv     *http.Server
		fastSrv *server.FastServer
	)

	if serverVariant == "fastconn" || serverVariant == "fastconn-close" {
		fastSrv = server.NewFastServer(router)
		if serverVariant == "fastconn-close" {
			fastSrv.ForceClose = true
		}
		go func() {
			if serveErr := fastSrv.Start(tcpLn); serveErr != nil && !errors.Is(serveErr, net.ErrClosed) && serveErr != http.ErrServerClosed && !strings.Contains(serveErr.Error(), "closed network connection") {
				fmt.Fprintf(os.Stderr, "[entry] fast server error: %v\n", serveErr)
				os.Exit(1)
			}
		}()
		fmt.Fprintf(os.Stdout, "[entry] starting FastServer (variant=%s)\n", serverVariant)
	} else {
		srv = &http.Server{
			Handler:        router,
			MaxHeaderBytes: 64 * 1024,
		}
		go func() {
			if serveErr := srv.Serve(tcpLn); serveErr != nil && serveErr != http.ErrServerClosed {
				fmt.Fprintf(os.Stderr, "[entry] http server error: %v\n", serveErr)
				os.Exit(1)
			}
		}()
		fmt.Fprintf(os.Stdout, "[entry] starting standard net/http Server\n")
	}

	fmt.Fprintf(os.Stdout, "[entry] listening on port %d\n", boundPort)

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	shutdown := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if fastSrv != nil {
			_ = fastSrv.Shutdown(ctx)
		}
		if srv != nil {
			_ = srv.Shutdown(ctx)
		}
	}

	if *standalone {
		<-sigCh
		shutdown()
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

	shutdown()
	fmt.Fprintln(os.Stdout, "[entry] done")
}

func retryDelay(attempt int) time.Duration {
	if attempt >= 3 {
		return 1500 * time.Millisecond
	}
	return 250 * time.Millisecond * time.Duration(1<<attempt)
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

// waitForDone backs off exponentially up to 1.5s (matching starter/main.ts) so background polling
// does not contend with the single-threaded env server during timed benchmark rounds.
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
		}
		time.Sleep(retryDelay(attempt))
	}
}
