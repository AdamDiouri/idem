package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Slice 2.3b (review R8): serve drains in-flight requests for the configured
// grace, not a fixed 10s.
func TestServe_DrainsWithinGrace(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	started := make(chan struct{})
	srv := &http.Server{Addr: addr, ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, srv, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Second) }()

	require.Eventually(t, func() bool { // listening
		c, err := net.Dial("tcp", addr)
		if err == nil {
			_ = c.Close()
		}
		return err == nil
	}, 3*time.Second, 10*time.Millisecond)
	got := make(chan int, 1)
	go func() {
		r, err := http.Get("http://" + addr + "/")
		if err != nil {
			got <- 0
			return
		}
		_ = r.Body.Close()
		got <- r.StatusCode
	}()
	<-started
	cancel() // SIGTERM mid-request
	require.NoError(t, <-done)
	require.Equal(t, http.StatusOK, <-got, "the in-flight request completes during the grace")
}
