package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

const baseURL = "http://localhost:8080"

func main() {
	fmt.Println("=== API Test Client ===")

	// Test GET all tasks
	fmt.Println("\nTesting GET /api/tasks:")
	testGetTasks()

	// Test GET single task
	fmt.Println("\nTesting GET /api/tasks/1:")
	testGetTask(1)

	// Test POST new task
	fmt.Println("\nTesting POST /api/tasks:")
	testCreateTask()
}

func testGetTasks() {
	resp, err := http.Get(baseURL + "/api/tasks")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Printf("Status: %d\n", resp.StatusCode)
	fmt.Printf("Response: %s\n", body)
}

func testGetTask(id int) {
	resp, err := http.Get(fmt.Sprintf("%s/api/tasks/%d", baseURL, id))
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Printf("Status: %d\n", resp.StatusCode)
	fmt.Printf("Response: %s\n", body)
}

func testCreateTask() {
	newTask := map[string]interface{}{
		"title": "Test Task",
		"done":  false,
	}

	jsonData, _ := json.Marshal(newTask)
	resp, err := http.Post(
		baseURL+"/api/tasks",
		"application/json",
		bytes.NewBuffer(jsonData),
	)

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Printf("Status: %d\n", resp.StatusCode)
	fmt.Printf("Response: %s\n", body)
}
