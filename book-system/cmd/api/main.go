package main

import (
	"log"
	"net/http"
	"os"

	"booksystem/internal/db"
	"booksystem/internal/handlers"

	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	// Initialize database
	dbManager, err := db.NewManager("books.db")
	if err != nil {
		log.Fatal(err)
	}
	defer dbManager.Close()

	// Initialize services
	userService := db.NewUserService(dbManager.DB())
	bookService := db.NewBookService(dbManager.DB())
	loanService := db.NewLoanService(dbManager.DB())
	categoryService := db.NewCategoryService(dbManager.DB())
	reviewService := db.NewReviewService(dbManager.DB())
	statsService := db.NewStatsService(dbManager.DB())

	// Initialize session store
	store := sessions.NewCookieStore([]byte("your-secret-key"))

	// Initialize handlers
	authHandler := handlers.NewAuthHandler(userService, []byte("your-secret-key"))
	bookHandler := handlers.NewBookHandler(bookService)
	loanHandler := handlers.NewLoanHandler(loanService, store)
	categoryHandler := handlers.NewCategoryHandler(categoryService)
	reviewHandler := handlers.NewReviewHandler(reviewService)
	statsHandler := handlers.NewStatsHandler(statsService)

	// Initialize router
	r := mux.NewRouter()

	// Static files
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir("web/static"))))

	// Web routes
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "web/templates/index.html")
	}).Methods("GET")

	r.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "web/templates/login.html")
	}).Methods("GET")

	r.HandleFunc("/books", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "web/templates/books.html")
	}).Methods("GET")

	r.HandleFunc("/loans", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "web/templates/loans.html")
	}).Methods("GET")

	// API routes
	r.HandleFunc("/api/login", authHandler.Login).Methods("POST")
	r.HandleFunc("/api/logout", authHandler.Logout).Methods("POST")

	// Book routes
	r.HandleFunc("/api/books", authHandler.RequireAuth(bookHandler.Create)).Methods("POST")
	r.HandleFunc("/api/books/{id}", bookHandler.Get).Methods("GET")
	r.HandleFunc("/api/books/search", bookHandler.Search).Methods("GET")
	r.HandleFunc("/api/books/{id}/stock", authHandler.RequireAdmin(bookHandler.UpdateStock)).Methods("PUT")

	// Loan routes
	r.HandleFunc("/api/loans", authHandler.RequireAuth(loanHandler.List)).Methods("GET")
	r.HandleFunc("/api/loans", authHandler.RequireAuth(loanHandler.Create)).Methods("POST")
	r.HandleFunc("/api/loans/{id}/return", authHandler.RequireAuth(loanHandler.Return)).Methods("POST")
	r.HandleFunc("/api/loans/overdue", authHandler.RequireAdmin(loanHandler.GetOverdue)).Methods("GET")

	// Category routes
	r.HandleFunc("/api/categories", categoryHandler.List).Methods("GET")
	r.HandleFunc("/api/categories", authHandler.RequireAdmin(categoryHandler.Create)).Methods("POST")

	// Review routes
	r.HandleFunc("/api/books/{id}/reviews", reviewHandler.ListByBook).Methods("GET")
	r.HandleFunc("/api/reviews", authHandler.RequireAuth(reviewHandler.Create)).Methods("POST")

	// Stats routes
	r.HandleFunc("/api/stats", authHandler.RequireAdmin(statsHandler.GetStats)).Methods("GET")
	r.HandleFunc("/api/stats/popular-books", statsHandler.GetPopularBooks).Methods("GET")

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Server starting on port %s...\n", port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		log.Fatal(err)
	}
}
