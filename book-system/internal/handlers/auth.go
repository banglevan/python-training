package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/sessions"

	"booksystem/internal/models"
	"log"
)

type AuthHandler struct {
	userService models.UserService
	store       *sessions.CookieStore
}

func NewAuthHandler(userService models.UserService, key []byte) *AuthHandler {
	return &AuthHandler{
		userService: userService,
		store:       sessions.NewCookieStore(key),
	}
}

func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		log.Printf("Error decoding credentials: %v", err)
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	log.Printf("Login attempt for user: %s", creds.Username)

	user, err := h.userService.Authenticate(creds.Username, creds.Password)
	if err != nil {
		log.Printf("Authentication failed: %v", err)
		respondWithError(w, http.StatusUnauthorized, "Invalid credentials")
		return
	}

	// Create session
	session, _ := h.store.Get(r, "session-name")
	session.Values["user_id"] = user.ID
	session.Values["username"] = user.Username
	session.Values["role"] = user.Role
	if err := session.Save(r, w); err != nil {
		log.Printf("Error saving session: %v", err)
		respondWithError(w, http.StatusInternalServerError, "Error creating session")
		return
	}

	log.Printf("User %s logged in successfully", user.Username)
	respondWithJSON(w, http.StatusOK, models.NewResponse("success", "Login successful", user))
}

func (h *AuthHandler) Logout(w http.ResponseWriter, r *http.Request) {
	session, _ := h.store.Get(r, "session-name")
	session.Options.MaxAge = -1
	session.Save(r, w)

	respondWithJSON(w, http.StatusOK, models.NewResponse("success", "Logout successful", nil))
}

// Middleware to check if user is authenticated
func (h *AuthHandler) RequireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		session, _ := h.store.Get(r, "session-name")
		if session.Values["user_id"] == nil {
			respondWithError(w, http.StatusUnauthorized, "Unauthorized")
			return
		}
		next(w, r)
	}
}

// Middleware to check if user has admin role
func (h *AuthHandler) RequireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		session, _ := h.store.Get(r, "session-name")
		if role, ok := session.Values["role"].(string); !ok || role != "admin" {
			respondWithError(w, http.StatusForbidden, "Admin access required")
			return
		}
		next(w, r)
	}
}
