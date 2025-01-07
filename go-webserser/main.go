package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"time"
)

// Struct cho template HTML
type PageData struct {
	Title   string
	Message string
}

// Struct cho API
type ApiResponse struct {
	Status  string      `json:"status"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func main() {
	// Static files
	fs := http.FileServer(http.Dir("static"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))

	// HTML routes
	http.HandleFunc("/", homeHandler)
	http.HandleFunc("/about", aboutHandler)
	http.HandleFunc("/contact", contactHandler)

	// API routes
	http.HandleFunc("/api/hello", apiHelloHandler)
	http.HandleFunc("/api/status", apiStatusHandler)

	fmt.Println("Server is running on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// HTML Handlers (giữ nguyên các handler cũ)
func homeHandler(w http.ResponseWriter, r *http.Request) {
	data := PageData{
		Title:   "Home Page",
		Message: "Welcome to our website!",
	}
	renderTemplate(w, "home", data)
}

func aboutHandler(w http.ResponseWriter, r *http.Request) {
	data := PageData{
		Title:   "About Us",
		Message: "Learn more about our company",
	}
	renderTemplate(w, "about", data)
}

func contactHandler(w http.ResponseWriter, r *http.Request) {
	data := PageData{
		Title:   "Contact Us",
		Message: "Get in touch with us",
	}
	renderTemplate(w, "contact", data)
}

// Template renderer
func renderTemplate(w http.ResponseWriter, tmpl string, data PageData) {
	t, err := template.ParseFiles("templates/" + tmpl + ".html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	t.Execute(w, data)
}

// API Handlers
func apiHelloHandler(w http.ResponseWriter, r *http.Request) {
	response := ApiResponse{
		Status:  "success",
		Message: "Hello from API!",
		Data: map[string]string{
			"server_time": fmt.Sprintf("%v", time.Now()),
		},
	}
	sendJSON(w, response)
}

func apiStatusHandler(w http.ResponseWriter, r *http.Request) {
	response := ApiResponse{
		Status:  "success",
		Message: "Server is running",
		Data: map[string]interface{}{
			"status": "healthy",
			"uptime": "1h 30m",
			"memory": "24MB",
		},
	}
	sendJSON(w, response)
}

// Helper function to send JSON response
func sendJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}
