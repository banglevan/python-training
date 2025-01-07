package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

type Task struct {
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	Done      bool      `json:"done"`
	CreatedAt time.Time `json:"created_at"`
}

var tasks = []Task{
	{ID: 1, Title: "Learn Go", Done: false, CreatedAt: time.Now()},
	{ID: 2, Title: "Build API", Done: true, CreatedAt: time.Now()},
}

func main() {
	// API routes
	http.HandleFunc("/api/tasks", handleTasks)
	http.HandleFunc("/api/tasks/", handleTask) // For individual tasks

	fmt.Println("Server running on http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleTasks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case "GET":
		json.NewEncoder(w).Encode(tasks)
	case "POST":
		var task Task
		if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		task.ID = len(tasks) + 1
		task.CreatedAt = time.Now()
		tasks = append(tasks, task)
		json.NewEncoder(w).Encode(task)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleTask(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	id := r.URL.Path[len("/api/tasks/"):]

	// Convert id to int and find task
	// (simplified error handling for demo)
	var taskID int
	fmt.Sscanf(id, "%d", &taskID)

	for _, task := range tasks {
		if task.ID == taskID {
			json.NewEncoder(w).Encode(task)
			return
		}
	}
	http.Error(w, "Task not found", http.StatusNotFound)
}
