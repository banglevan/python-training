package main

import (
	"fmt"
	"html/template"
	"log"
	"net/http"
)

// Struct để chứa data cho template
type PageData struct {
	Title   string
	Message string
}

func main() {
	// Serve static files
	fs := http.FileServer(http.Dir("static"))
	http.Handle("/static/", http.StripPrefix("/static/", fs))

	// Route handlers
	http.HandleFunc("/", homeHandler)
	http.HandleFunc("/about", aboutHandler)
	http.HandleFunc("/contact", contactHandler)

	// Start server
	fmt.Println("Server is running on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

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

func renderTemplate(w http.ResponseWriter, tmpl string, data PageData) {
	t, err := template.ParseFiles("templates/" + tmpl + ".html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	t.Execute(w, data)
}
