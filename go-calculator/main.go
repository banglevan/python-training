package main

import (
    "html/template"
    "log"
    "net/http"
    "strconv"
)

func main() {
    // Serve static files
    http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))
    
    // Handle main page
    http.HandleFunc("/", handleCalculator)
    
    // Handle calculation
    http.HandleFunc("/calculate", handleCalculate)
    
    log.Println("Server starting at :8080")
    log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleCalculator(w http.ResponseWriter, r *http.Request) {
    tmpl := template.Must(template.ParseFiles("templates/calculator.html"))
    tmpl.Execute(w, nil)
}

func handleCalculate(w http.ResponseWriter, r *http.Request) {
    if r.Method != "POST" {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }

    // Get values from form
    num1, _ := strconv.ParseFloat(r.FormValue("num1"), 64)
    num2, _ := strconv.ParseFloat(r.FormValue("num2"), 64)
    operation := r.FormValue("operation")

    // Perform calculation
    var result float64
    switch operation {
    case "add":
        result = num1 + num2
    case "subtract":
        result = num1 - num2
    case "multiply":
        result = num1 * num2
    case "divide":
        if num2 != 0 {
            result = num1 / num2
        } else {
            http.Error(w, "Cannot divide by zero", http.StatusBadRequest)
            return
        }
    }

    // Return result
    w.Write([]byte(strconv.FormatFloat(result, 'f', 2, 64)))
}