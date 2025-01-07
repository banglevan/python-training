package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"booksystem/internal/models"

	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
)

type LoanHandler struct {
	store       sessions.Store
	loanService models.LoanService
}

func NewLoanHandler(loanService models.LoanService, store sessions.Store) *LoanHandler {
	return &LoanHandler{
		loanService: loanService,
		store:       store,
	}
}

func (h *LoanHandler) Create(w http.ResponseWriter, r *http.Request) {
	var input struct {
		BookID  int       `json:"book_id"`
		DueDate time.Time `json:"due_date"`
	}

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	// Get user ID from session
	session, _ := getSession(r)
	userID := session.Values["user_id"].(int)

	loan := &models.Loan{
		BookID:   input.BookID,
		UserID:   userID,
		LoanDate: time.Now(),
		DueDate:  input.DueDate,
		Status:   string(models.LoanStatusBorrowed),
	}

	if err := h.loanService.Create(loan); err != nil {
		if err == models.ErrOutOfStock {
			respondWithError(w, http.StatusBadRequest, "Book is out of stock")
			return
		}
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondWithJSON(w, http.StatusCreated, models.NewResponse("success", "Loan created", loan))
}

func (h *LoanHandler) Return(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid loan ID")
		return
	}

	if err := h.loanService.Return(id); err != nil {
		if err == models.ErrNotFound {
			respondWithError(w, http.StatusNotFound, "Loan not found")
			return
		}
		if err == models.ErrInvalidInput {
			respondWithError(w, http.StatusBadRequest, "Book already returned")
			return
		}
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, models.NewResponse("success", "Book returned successfully", nil))
}

func (h *LoanHandler) GetOverdue(w http.ResponseWriter, r *http.Request) {
	loans, err := h.loanService.GetOverdue()
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, models.NewResponse("success", "Overdue loans retrieved", loans))
}

func (h *LoanHandler) List(w http.ResponseWriter, r *http.Request) {
	// Get user ID from session
	session, _ := h.store.Get(r, "session-name")
	userID := session.Values["user_id"].(int)

	loans, err := h.loanService.ListByUser(userID)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, models.NewResponse("success", "Loans retrieved", loans))
}
