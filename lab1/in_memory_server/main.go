// Файл: in_memory_server.go
package main

import (
	"fmt"
	"net/http"
	"sync"
)

// WebCounter - структура для каунтера
type WebCounter struct {
	count int64
	mu    sync.Mutex // Mutex для забезпечення потоко-безпеки
}

// IncHandler - обробник для /inc
func (wc *WebCounter) IncHandler(w http.ResponseWriter, r *http.Request) {
	wc.mu.Lock()
	defer wc.mu.Unlock()

	wc.count++
	w.WriteHeader(http.StatusOK)
}

// CountHandler - обробник для /count
func (wc *WebCounter) CountHandler(w http.ResponseWriter, r *http.Request) {
	wc.mu.Lock()
	defer wc.mu.Unlock()

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%d", wc.count)
}

func main() {
	counter := WebCounter{count: 0}

	http.HandleFunc("/inc", counter.IncHandler)
	http.HandleFunc("/count", counter.CountHandler)

	fmt.Println("Starting In-Memory server on :8080")
	// Обробка помилок сервера (рекомендовано)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("Server failed: %v\n", err)
	}
}
