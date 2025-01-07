package models

import "time"

type Book struct {
	ID            int       `json:"id"`
	Title         string    `json:"title"`
	Author        string    `json:"author"`
	ISBN          string    `json:"isbn"`
	PublishedYear int       `json:"published_year"`
	Publisher     string    `json:"publisher"`
	Description   string    `json:"description"`
	Category      string    `json:"category"`
	Stock         int       `json:"stock"`
	CreatedBy     int       `json:"created_by"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// type bookService interface {
// 	Create(book *Book) error
// 	GetByID(id int) (*Book, error)
// 	GetByISBN(isbn string) (*Book, error)
// 	Update(book *Book) error
// 	Delete(id int) error
// 	List() ([]Book, error)
// 	Search(query string) ([]Book, error)
// 	UpdateStock(id int, quantity int) error
// }
