package handlers

import (
	"booksystem/internal/models"
	"net/http"
	"strconv"
)

type StatsHandler struct {
	statsService models.StatsService
}

func NewStatsHandler(statsService models.StatsService) *StatsHandler {
	return &StatsHandler{statsService: statsService}
}

func (h *StatsHandler) GetStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.statsService.GetBookStats()
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, models.NewResponse("success", "Statistics retrieved", stats))
}

func (h *StatsHandler) GetPopularBooks(w http.ResponseWriter, r *http.Request) {
	limit := 10 // default limit
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	books, err := h.statsService.GetPopularBooks(limit)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, err.Error())
		return
	}

	respondWithJSON(w, http.StatusOK, models.NewResponse("success", "Popular books retrieved", books))
}
