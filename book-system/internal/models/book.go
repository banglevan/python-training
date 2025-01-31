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
	Language      string    `json:"language"`
	Rights        string    `json:"rights"`
	ContentPath   string    `json:"content_path"`
	ImagePath     string    `json:"image_path"`
	SourceURL     string    `json:"source_url"`
	SourceID      string    `json:"source_id"`
	Format        string    `json:"format"`
	FileSize      int64     `json:"file_size"`
	Stock         int       `json:"stock"`
	CreatedBy     int       `json:"created_by"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// type BookService interface {
// 	List() ([]Book, error)
// 	GetByID(id int) (*Book, error)
// 	Create(book *Book) error
// 	Update(book *Book) error
// 	Delete(id int) error
// }
