# Golang Fundamentals Course

## Course Structure

```
golang-fundamentals/
├── 01-basics/
│   ├── 01-setup/
│   ├── 02-syntax/
│   └── 03-data-types/
├── 02-control-flow/
├── 03-functions/
├── 04-collections/
├── 05-structs-interfaces/
├── 06-concurrency/
├── 07-error-handling/
└── 08-practical-projects/
```

## 1. Getting Started with Go

### 1.1 First Program
```go
// hello.go
package main

import "fmt"

func main() {
    fmt.Println("Hello, Go!")
}
```

PROMPT để hiểu code:
```
"Explain this Go code:
1. Package declaration
2. Import statement
3. Main function
4. Print function
5. Basic syntax elements"
```

### 1.2 Variables & Types
```go
package main

import "fmt"

func main() {
    // Variable declarations
    var name string = "John"
    age := 25 // Type inference
    
    // Constants
    const pi = 3.14159
    
    // Multiple declarations
    var (
        isActive bool = true
        score    int  = 100
    )
    
    // Basic types
    var (
        intVal    int     = 42
        floatVal  float64 = 3.14
        boolVal   bool    = true
        stringVal string  = "text"
    )
    
    fmt.Printf("Types and values:\n")
    fmt.Printf("name: %v (%T)\n", name, name)
    fmt.Printf("age: %v (%T)\n", age, age)
}
```

## 2. Control Flow

### 2.1 Conditionals
```go
func checkNumber(x int) string {
    // If-else statement
    if x > 0 {
        return "positive"
    } else if x < 0 {
        return "negative"
    } else {
        return "zero"
    }
}

func checkGrade(score int) string {
    // Switch statement
    switch {
    case score >= 90:
        return "A"
    case score >= 80:
        return "B"
    case score >= 70:
        return "C"
    default:
        return "F"
    }
}
```

### 2.2 Loops
```go
func loopExamples() {
    // Basic for loop
    for i := 0; i < 5; i++ {
        fmt.Println(i)
    }
    
    // While-style loop
    count := 0
    for count < 5 {
        fmt.Println(count)
        count++
    }
    
    // Infinite loop with break
    sum := 0
    for {
        sum++
        if sum > 100 {
            break
        }
    }
    
    // Range loop
    numbers := []int{1, 2, 3, 4, 5}
    for index, value := range numbers {
        fmt.Printf("Index: %d, Value: %d\n", index, value)
    }
}
```

## 3. Functions

### 3.1 Basic Functions
```go
// Function with parameters and return value
func add(a, b int) int {
    return a + b
}

// Multiple return values
func divide(a, b float64) (float64, error) {
    if b == 0 {
        return 0, fmt.Errorf("division by zero")
    }
    return a / b, nil
}

// Named return values
func split(sum int) (x, y int) {
    x = sum * 4 / 9
    y = sum - x
    return // Naked return
}
```

### 3.2 Advanced Functions
```go
// Variadic function
func sum(nums ...int) int {
    total := 0
    for _, num := range nums {
        total += num
    }
    return total
}

// Function as parameter
func calculate(operation func(int, int) int, a, b int) int {
    return operation(a, b)
}

// Closure
func counter() func() int {
    count := 0
    return func() int {
        count++
        return count
    }
}
```

## 4. Collections

### 4.1 Arrays and Slices
```go
func arrayExamples() {
    // Array
    var numbers [5]int
    numbers = [5]int{1, 2, 3, 4, 5}
    
    // Slice
    slice := numbers[1:4]
    
    // Make slice
    dynamicSlice := make([]int, 3, 5)
    
    // Append
    slice = append(slice, 6, 7, 8)
    
    // Copy
    newSlice := make([]int, len(slice))
    copy(newSlice, slice)
}
```

### 4.2 Maps
```go
func mapExamples() {
    // Create map
    scores := make(map[string]int)
    
    // Add key-value pairs
    scores["Alice"] = 95
    scores["Bob"] = 80
    
    // Check existence
    if score, exists := scores["Alice"]; exists {
        fmt.Printf("Alice's score: %d\n", score)
    }
    
    // Delete
    delete(scores, "Bob")
    
    // Iterate
    for name, score := range scores {
        fmt.Printf("%s: %d\n", name, score)
    }
}
```

## 5. Structs and Interfaces

### 5.1 Structs
```go
// Define struct
type Person struct {
    Name    string
    Age     int
    Address string
}

// Method
func (p Person) Greet() string {
    return fmt.Sprintf("Hello, my name is %s", p.Name)
}

// Pointer receiver
func (p *Person) Birthday() {
    p.Age++
}
```

### 5.2 Interfaces
```go
// Define interface
type Shape interface {
    Area() float64
    Perimeter() float64
}

// Implement interface
type Rectangle struct {
    Width  float64
    Height float64
}

func (r Rectangle) Area() float64 {
    return r.Width * r.Height
}

func (r Rectangle) Perimeter() float64 {
    return 2 * (r.Width + r.Height)
}
```

## 6. Concurrency

### 6.1 Goroutines
```go
func printNumbers() {
    for i := 0; i < 5; i++ {
        time.Sleep(100 * time.Millisecond)
        fmt.Printf("%d ", i)
    }
}

func main() {
    // Start goroutine
    go printNumbers()
    
    // Wait
    time.Sleep(time.Second)
}
```

### 6.2 Channels
```go
func producer(ch chan<- int) {
    for i := 0; i < 5; i++ {
        ch <- i
    }
    close(ch)
}

func consumer(ch <-chan int) {
    for num := range ch {
        fmt.Printf("Received: %d\n", num)
    }
}

func main() {
    ch := make(chan int)
    go producer(ch)
    consumer(ch)
}
```

## 7. Error Handling

### 7.1 Basic Error Handling
```go
func divide(a, b float64) (float64, error) {
    if b == 0 {
        return 0, fmt.Errorf("division by zero")
    }
    return a / b, nil
}

func main() {
    result, err := divide(10, 0)
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }
    fmt.Printf("Result: %f\n", result)
}
```

### 7.2 Custom Errors
```go
type ValidationError struct {
    Field string
    Error string
}

func (e *ValidationError) Error() string {
    return fmt.Sprintf("%s: %s", e.Field, e.Error)
}

func validateAge(age int) error {
    if age < 0 {
        return &ValidationError{
            Field: "age",
            Error: "must be positive",
        }
    }
    return nil
}
``` 