// File: in_memory_server.go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

// WebCounter uses atomic.Int64 for maximum performance.
type WebCounter struct {
	count atomic.Int64
}

// IncHandler atomically increments the counter by 1.
func (wc *WebCounter) IncHandler(w http.ResponseWriter, r *http.Request) {
	wc.count.Add(1)
	w.WriteHeader(http.StatusOK)
}

// CountHandler atomically reads the current value of the counter.
func (wc *WebCounter) CountHandler(w http.ResponseWriter, r *http.Request) {
	currentCount := wc.count.Load()
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%d", currentCount)
}

// ResetHandler atomically resets the counter to 0.
func (wc *WebCounter) ResetHandler(w http.ResponseWriter, r *http.Request) {
	wc.count.Store(0)
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, "Counter reset to 0")
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	counter := &WebCounter{}

	mux := http.NewServeMux()
	mux.HandleFunc("/inc", counter.IncHandler)
	mux.HandleFunc("/count", counter.CountHandler)
	mux.HandleFunc("/reset", counter.ResetHandler)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Start the server in a separate goroutine
	go func() {
		slog.Info("Starting In-Memory server on", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Server failed to start", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful Shutdown implementation
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM) // Catch Ctrl+C and kill signals

	<-stopChan // Block until a signal is received

	slog.Info("Shutting down server gracefully...")

	// Allow 5 seconds for current requests to finish
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		slog.Error("Graceful shutdown failed", "error", err)
	} else {
		slog.Info("Server stopped gracefully")
	}
}
