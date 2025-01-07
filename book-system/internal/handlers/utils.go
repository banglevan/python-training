package handlers

import (
	"booksystem/internal/models"
	"encoding/json"
	"net/http"

	"github.com/gorilla/sessions"
)

var store = sessions.NewCookieStore([]byte("your-secret-key"))

func getSession(r *http.Request) (*sessions.Session, error) {
	return store.Get(r, "session-name")
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, models.NewErrorResponse(message))
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}
