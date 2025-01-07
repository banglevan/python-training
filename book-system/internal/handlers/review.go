package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"booksystem/internal/models"

	"github.com/gorilla/mux"
)

type ReviewHandler struct {
	reviewService models.ReviewService
}

func NewReviewHandler(reviewService models.ReviewService) *ReviewHandler {
	return &ReviewHandler{reviewService: reviewService}
}

func (h *ReviewHandler) Create(w http.ResponseWriter, r *http.Request) {
	var review models.Review
	if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	// Get user ID from session
	session, _ := getSession(r)
	review.UserID = session.Values["user_id"].(int)

	if err := h.reviewService.Create(&review); err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondWithJSON(w, http.StatusCreated, models.NewResponse("success", "Review created", review))
}

func (h *ReviewHandler) ListByBook(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bookID, err := strconv.Atoi(vars["id"])
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid book ID")
		return
	}

	reviews, err := h.reviewService.ListByBook(bookID)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, models.NewResponse("success", "Reviews retrieved", reviews))
}
