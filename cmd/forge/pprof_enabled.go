//go:build pprof

package main

import (
	"net/http"
	_ "net/http/pprof"
	"runtime"
)

func startPprof(addr string) {
	if addr != "" {
		runtime.SetMutexProfileFraction(5)
		runtime.SetBlockProfileRate(1000)
		go func() {
			_ = http.ListenAndServe(addr, nil)
		}()
	}
}
