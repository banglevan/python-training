# Golang Practical Projects

## Project 1: CLI Task Manager

### 1.1 Project Structure
```
task-manager/
├── cmd/
│   └── main.go
├── internal/
│   ├── task/
│   │   ├── task.go
│   │   └── repository.go
│   └── storage/
│       └── file.go
└── go.mod
```

### 1.2 Basic Implementation
```go
// internal/task/task.go
package task

type Task struct {
    ID          int       `json:"id"`
    Title       string    `json:"title"`
    Description string    `json:"description"`
    Done        bool      `json:"done"`
    CreatedAt   time.Time `json:"created_at"`
}

type Repository interface {
    Add(task Task) error
    Get(id int) (Task, error)
    List() ([]Task, error)
    Update(task Task) error
    Delete(id int) error
}
```

```go
// cmd/main.go
package main

import (
    "flag"
    "fmt"
    "log"
)

func main() {
    var (
        add    = flag.String("add", "", "Add new task")
        list   = flag.Bool("list", false, "List all tasks")
        done   = flag.Int("done", 0, "Mark task as done")
        delete = flag.Int("delete", 0, "Delete task")
    )
    flag.Parse()

    repo := storage.NewFileRepository("tasks.json")
    
    switch {
    case *add != "":
        task := task.Task{
            Title:     *add,
            CreatedAt: time.Now(),
        }
        if err := repo.Add(task); err != nil {
            log.Fatal(err)
        }
        fmt.Println("Task added successfully")
        
    case *list:
        tasks, err := repo.List()
        if err != nil {
            log.Fatal(err)
        }
        for _, t := range tasks {
            fmt.Printf("[%d] %s - %v\n", t.ID, t.Title, t.Done)
        }
    }
}
```

## Project 2: REST API Server

### 2.1 Project Structure
```
book-api/
├── api/
│   ├── handlers/
│   │   └── book.go
│   └── router.go
├── internal/
│   ├── model/
│   │   └── book.go
│   └── storage/
│       └── memory.go
├── cmd/
│   └── main.go
└── go.mod
```

### 2.2 Implementation
```go
// internal/model/book.go
package model

type Book struct {
    ID          string    `json:"id"`
    Title       string    `json:"title"`
    Author      string    `json:"author"`
    PublishedAt time.Time `json:"published_at"`
}

// api/handlers/book.go
package handlers

import (
    "encoding/json"
    "net/http"
    
    "github.com/gorilla/mux"
)

type BookHandler struct {
    store Storage
}

func (h *BookHandler) GetBook(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    id := vars["id"]
    
    book, err := h.store.Get(id)
    if err != nil {
        http.Error(w, err.Error(), http.StatusNotFound)
        return
    }
    
    json.NewEncoder(w).Encode(book)
}

func (h *BookHandler) CreateBook(w http.ResponseWriter, r *http.Request) {
    var book Book
    if err := json.NewDecoder(r.Body).Decode(&book); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }
    
    if err := h.store.Add(book); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    
    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(book)
}
```

## Project 3: Concurrent Web Crawler

### 3.1 Project Structure
```
web-crawler/
├── cmd/
│   └── main.go
├── internal/
│   ├── crawler/
│   │   └── crawler.go
│   ├── parser/
│   │   └── parser.go
│   └── storage/
│       └── memory.go
└── go.mod
```

### 3.2 Implementation
```go
// internal/crawler/crawler.go
package crawler

import (
    "sync"
)

type Crawler struct {
    visited sync.Map
    workers int
}

func (c *Crawler) Crawl(url string, depth int) error {
    // Create work queue
    queue := make(chan string, 100)
    done := make(chan bool)
    
    // Start workers
    var wg sync.WaitGroup
    for i := 0; i < c.workers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for url := range queue {
                c.processURL(url, depth, queue)
            }
        }()
    }
    
    // Add initial URL
    queue <- url
    
    // Wait for completion
    go func() {
        wg.Wait()
        close(done)
    }()
    
    select {
    case <-done:
        return nil
    case <-time.After(30 * time.Second):
        return errors.New("crawl timeout")
    }
}

func (c *Crawler) processURL(url string, depth int, queue chan<- string) {
    // Skip if already visited
    if _, visited := c.visited.LoadOrStore(url, true); visited {
        return
    }
    
    // Get page content
    links, err := c.fetchLinks(url)
    if err != nil {
        log.Printf("Error fetching %s: %v", url, err)
        return
    }
    
    // Queue new links
    if depth > 0 {
        for _, link := range links {
            queue <- link
        }
    }
}
```

## Project 4: Real-time Chat Server

### 4.1 Project Structure
```
chat-server/
├── cmd/
│   └── main.go
├── internal/
│   ├── chat/
│   │   ├── room.go
│   │   ├── client.go
│   │   └── message.go
│   └── websocket/
│       └── handler.go
└── go.mod
```

### 4.2 Implementation
```go
// internal/chat/room.go
package chat

type Room struct {
    clients    map[*Client]bool
    broadcast  chan []byte
    register   chan *Client
    unregister chan *Client
}

func (r *Room) Run() {
    for {
        select {
        case client := <-r.register:
            r.clients[client] = true
            
        case client := <-r.unregister:
            if _, ok := r.clients[client]; ok {
                delete(r.clients, client)
                close(client.send)
            }
            
        case message := <-r.broadcast:
            for client := range r.clients {
                select {
                case client.send <- message:
                default:
                    close(client.send)
                    delete(r.clients, client)
                }
            }
        }
    }
}

// internal/chat/client.go
type Client struct {
    room *Room
    conn *websocket.Conn
    send chan []byte
}

func (c *Client) readPump() {
    defer func() {
        c.room.unregister <- c
        c.conn.Close()
    }()
    
    for {
        _, message, err := c.conn.ReadMessage()
        if err != nil {
            break
        }
        c.room.broadcast <- message
    }
}

func (c *Client) writePump() {
    defer c.conn.Close()
    
    for {
        select {
        case message, ok := <-c.send:
            if !ok {
                c.conn.WriteMessage(websocket.CloseMessage, []byte{})
                return
            }
            
            w, err := c.conn.NextWriter(websocket.TextMessage)
            if err != nil {
                return
            }
            w.Write(message)
            
            if err := w.Close(); err != nil {
                return
            }
        }
    }
}
```

Mỗi project bao gồm:
1. **Cấu trúc project** rõ ràng
2. **Code mẫu** đầy đủ
3. **Error handling**
4. **Concurrency patterns**
5. **Best practices**
