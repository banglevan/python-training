package handlers

import (
	"booksystem/internal/models"
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"text/template"
	"time"

	"github.com/gorilla/mux"
)

type BookHandler struct {
	bookService models.BookService
	db          *sql.DB
}

func NewBookHandler(bookService models.BookService, db *sql.DB) *BookHandler {
	return &BookHandler{
		bookService: bookService,
		db:          db,
	}
}

func (h *BookHandler) List(w http.ResponseWriter, r *http.Request) {
	// Query directly from the storage database
	rows, err := h.db.Query(`
		SELECT 
			id, title, author, description, 
			image_path, content_path, language,
			rights, source_url, source_id, 
			format, file_size
		FROM books 
		ORDER BY id DESC
	`)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	var books []models.Book
	for rows.Next() {
		var book models.Book
		err := rows.Scan(
			&book.ID, &book.Title, &book.Author, &book.Description,
			&book.ImagePath, &book.ContentPath, &book.Language,
			&book.Rights, &book.SourceURL, &book.SourceID,
			&book.Format, &book.FileSize,
		)
		if err != nil {
			respondWithError(w, http.StatusInternalServerError, err.Error())
			return
		}
		// Convert storage path to URL
		if book.ImagePath != "" {
			book.ImagePath = "/storage/" + book.ImagePath
		}
		books = append(books, book)
	}

	respondWithJSON(w, http.StatusOK, models.NewResponse("success", "Books retrieved", books))
}

func (h *BookHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid book ID")
		return
	}

	book, err := h.bookService.GetByID(id)
	if err != nil {
		respondWithError(w, http.StatusNotFound, "Book not found")
		return
	}

	// Convert storage path to URL for image
	if book.ImagePath != "" {
		book.ImagePath = "/storage/" + book.ImagePath
	}

	respondWithJSON(w, http.StatusOK, models.NewResponse("success", "Book retrieved", book))
}

func (h *BookHandler) Create(w http.ResponseWriter, r *http.Request) {
	var book models.Book
	if err := json.NewDecoder(r.Body).Decode(&book); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	if err := h.bookService.Create(&book); err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondWithJSON(w, http.StatusCreated, models.NewResponse("success", "Book created", book))
}

func (h *BookHandler) Update(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid book ID")
		return
	}

	// Get existing book first
	existingBook, err := h.bookService.GetByID(id)
	if err != nil {
		respondWithError(w, http.StatusNotFound, "Book not found")
		return
	}

	// Decode updates from request
	var updates models.Book
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	// Update only non-zero fields
	if updates.Title != "" {
		existingBook.Title = updates.Title
	}
	if updates.Author != "" {
		existingBook.Author = updates.Author
	}
	if updates.ISBN != "" {
		existingBook.ISBN = updates.ISBN
	}
	if updates.PublishedYear != 0 {
		existingBook.PublishedYear = updates.PublishedYear
	}
	if updates.Publisher != "" {
		existingBook.Publisher = updates.Publisher
	}
	if updates.Description != "" {
		existingBook.Description = updates.Description
	}
	if updates.Category != "" {
		existingBook.Category = updates.Category
	}
	if updates.Stock != 0 {
		existingBook.Stock = updates.Stock
	}

	// Update timestamp
	existingBook.UpdatedAt = time.Now()

	// Save the updated book
	if err := h.bookService.Update(existingBook); err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, models.NewResponse("success", "Book updated", existingBook))
}

func (h *BookHandler) Delete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid book ID")
		return
	}

	if err := h.bookService.Delete(id); err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, models.NewResponse("success", "Book deleted", nil))
}

func (h *BookHandler) Read(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		http.Error(w, "Invalid book ID", http.StatusBadRequest)
		return
	}

	// Get book details
	book, err := h.bookService.GetByID(id)
	if err != nil {
		http.Error(w, "Book not found", http.StatusNotFound)
		return
	}

	// Read content file
	content, err := os.ReadFile("storage/" + book.ContentPath)
	if err != nil {
		http.Error(w, "Error reading book content", http.StatusInternalServerError)
		return
	}

	// Prepare template data
	data := struct {
		Title   string
		Author  string
		Content string
	}{
		Title:   book.Title,
		Author:  book.Author,
		Content: string(content),
	}

	// Render template
	tmpl := template.Must(template.ParseFiles("web/templates/read.html"))
	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, "Error rendering template", http.StatusInternalServerError)
	}
}
