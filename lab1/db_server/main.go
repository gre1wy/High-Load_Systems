// Файл: db_server.go
package main

import (
	"database/sql"

	"fmt" // <<< fmt знаходиться тут
	"net/http"
	"os"

	// Імпортуємо драйвер PostgreSQL. Зверніть увагу на _
	_ "github.com/lib/pq"
)

// DBWrapper містить з'єднання з БД
type DBWrapper struct {
	DB *sql.DB
}

// IncHandler - обробник для /inc. Інкрементує значення в БД.
func (dw *DBWrapper) IncHandler(w http.ResponseWriter, r *http.Request) {
	// Атомарна операція SQL: UPDATE.
	// PostgreSQL гарантує, що цей запит є потоко-безпечним,
	// запобігаючи lost-update на рівні бази даних.
	_, err := dw.DB.Exec("UPDATE counter_table SET value = value + 1 WHERE id = 1")

	if err != nil {
		http.Error(w, "DB error during increment", http.StatusInternalServerError)
		// Для налагодження:
		fmt.Printf("DB Exec error: %v\n", err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// CountHandler - обробник для /count. Зчитує значення з БД.
func (dw *DBWrapper) CountHandler(w http.ResponseWriter, r *http.Request) {
	var count int64

	// Використовуємо QueryRow для отримання одного рядка
	row := dw.DB.QueryRow("SELECT value FROM counter_table WHERE id = 1")

	// Скануємо результат у змінну 'count'
	err := row.Scan(&count)

	if err == sql.ErrNoRows {
		http.Error(w, "Counter not initialized", http.StatusInternalServerError)
		return
	}
	if err != nil {
		http.Error(w, "DB error during read", http.StatusInternalServerError)
		fmt.Printf("DB Query error: %v\n", err)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%d", count)
}

func main() {
	// !!! ЗМІНІТЬ ЦЕЙ РЯДОК НА ВАШІ ОБЛІКОВІ ДАНІ !!!
	connStr := "user=postgres password=qn48dkdjd74 dbname=web_counter_db sslmode=disable"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		fmt.Printf("FATAL: Failed to open DB connection: %v\n", err)
		os.Exit(1) // Завершення програми з кодом помилки

	}
	// Важливо: Налаштування Connection Pool.
	// Це гарантує, що Go не відкриває нове з'єднання для кожного HTTP-запиту.
	db.SetMaxOpenConns(25) // Приклад: до 25 активних з'єднань одночасно
	defer db.Close()

	if err = db.Ping(); err != nil {
		fmt.Printf("FATAL: Failed to connect to DB: %v\n", err)
		os.Exit(1) // Завершення програми з кодом помилки
	}
	fmt.Println("Successfully connected to PostgreSQL. Server starting on :8080")

	dbWrapper := &DBWrapper{DB: db}

	http.HandleFunc("/inc", dbWrapper.IncHandler)
	http.HandleFunc("/count", dbWrapper.CountHandler)

	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("Server failed: %v\n", err)
	}
}
