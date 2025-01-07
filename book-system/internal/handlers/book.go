package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"booksystem/internal/models"

	"github.com/gorilla/mux"
)

type BookHandler struct {
	bookService models.BookService
}

func NewBookHandler(bookService models.BookService) *BookHandler {
	return &BookHandler{bookService: bookService}
}

func (h *BookHandler) Create(w http.ResponseWriter, r *http.Request) {
	var book models.Book
	if err := json.NewDecoder(r.Body).Decode(&book); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	// Get user ID from session
	session, _ := getSession(r)
	book.CreatedBy = session.Values["user_id"].(int)

	if err := h.bookService.Create(&book); err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondWithJSON(w, http.StatusCreated, models.NewResponse("success", "Book created", book))
}

func (h *BookHandler) Get(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid book ID")
		return
	}

	book, err := h.bookService.GetByID(id)
	if err == models.ErrNotFound {
		respondWithError(w, http.StatusNotFound, "Book not found")
		return
	}
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, models.NewResponse("success", "Book retrieved", book))
}

func (h *BookHandler) Search(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		respondWithError(w, http.StatusBadRequest, "Search query is required")
		return
	}

	books, err := h.bookService.Search(query)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, models.NewResponse("success", "Search results", books))
}

func (h *BookHandler) UpdateStock(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid book ID")
		return
	}

	var input struct {
		Quantity int `json:"quantity"`
	}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	if err := h.bookService.UpdateStock(id, input.Quantity); err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, models.NewResponse("success", "Stock updated", nil))
}
