// File: db_server.go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	// PostgreSQL driver. The _ means we are using it for its side effects (driver registration).
	_ "github.com/lib/pq"
)

// DBWrapper contains the database connection.
type DBWrapper struct {
	DB *sql.DB
}

// IncHandler uses ExecContext for a thread-safe increment in the DB.
func (dw *DBWrapper) IncHandler(w http.ResponseWriter, r *http.Request) {
	_, err := dw.DB.ExecContext(r.Context(), "UPDATE counter_table SET value = value + 1 WHERE id = 1")
	if err != nil {
		slog.Error("DB error during increment", "error", err)
		http.Error(w, "DB error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// CountHandler uses QueryRowContext to read the value.
func (dw *DBWrapper) CountHandler(w http.ResponseWriter, r *http.Request) {
	var count int64
	err := dw.DB.QueryRowContext(r.Context(), "SELECT value FROM counter_table WHERE id = 1").Scan(&count)
	if err != nil {
		slog.Error("DB error during read", "error", err)
		http.Error(w, "DB error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%d", count)
}

// ResetHandler resets the counter in the DB.
func (dw *DBWrapper) ResetHandler(w http.ResponseWriter, r *http.Request) {
	_, err := dw.DB.ExecContext(r.Context(), "UPDATE counter_table SET value = 0 WHERE id = 1")
	if err != nil {
		slog.Error("DB error during reset", "error", err)
		http.Error(w, "DB error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintln(w, "Counter reset to 0 in DB")
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// --- Get the connection string from an environment variable ---
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		// Default value for local development if the variable is not set.
		connStr = "user=postgres password=mysecretpassword dbname=web_counter_db sslmode=disable"
		slog.Warn("DATABASE_URL environment variable not set. Using default value.")
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		slog.Error("FATAL: Failed to open DB connection", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// Configure the connection pool for high performance.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err = db.Ping(); err != nil {
		slog.Error("FATAL: Failed to connect to DB", "error", err)
		os.Exit(1)
	}

	dbWrapper := &DBWrapper{DB: db}

	mux := http.NewServeMux()
	mux.HandleFunc("/inc", dbWrapper.IncHandler)
	mux.HandleFunc("/count", dbWrapper.CountHandler)
	mux.HandleFunc("/reset", dbWrapper.ResetHandler)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Start the server in a separate goroutine
	go func() {
		slog.Info("Starting DB server on", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Server failed to start", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful Shutdown
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	<-stopChan
	slog.Info("Shutting down server gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		slog.Error("Graceful shutdown failed", "error", err)
	} else {
		slog.Info("Server stopped gracefully")
	}
}
