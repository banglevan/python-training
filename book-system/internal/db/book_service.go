package db

import (
	"booksystem/internal/models"
	"database/sql"
	"time"
)

type BookService struct {
	db *sql.DB
}

func NewBookService(db *sql.DB) *BookService {
	return &BookService{db: db}
}

func (s *BookService) Create(book *models.Book) error {
	query := `
        INSERT INTO books (
            title, author, isbn, published_year, publisher,
            description, category, stock, created_by, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `
	now := time.Now()
	result, err := s.db.Exec(query,
		book.Title,
		book.Author,
		book.ISBN,
		book.PublishedYear,
		book.Publisher,
		book.Description,
		book.Category,
		book.Stock,
		book.CreatedBy,
		now,
		now,
	)
	if err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	book.ID = int(id)
	book.CreatedAt = now
	book.UpdatedAt = now
	return nil
}

func (s *BookService) GetByID(id int) (*models.Book, error) {
	book := &models.Book{}
	query := `SELECT * FROM books WHERE id = ?`
	err := s.db.QueryRow(query, id).Scan(
		&book.ID,
		&book.Title,
		&book.Author,
		&book.ISBN,
		&book.PublishedYear,
		&book.Publisher,
		&book.Description,
		&book.Category,
		&book.Stock,
		&book.CreatedBy,
		&book.CreatedAt,
		&book.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, models.ErrNotFound
	}
	return book, err
}

func (s *BookService) UpdateStock(id int, quantity int) error {
	query := `
        UPDATE books 
        SET stock = stock + ?, updated_at = ?
        WHERE id = ?
    `
	result, err := s.db.Exec(query, quantity, time.Now(), id)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return models.ErrNotFound
	}
	return nil
}

func (s *BookService) Search(query string) ([]models.Book, error) {
	searchQuery := `
        SELECT * FROM books 
        WHERE title LIKE ? OR author LIKE ? OR isbn LIKE ?
        ORDER BY title
    `
	searchParam := "%" + query + "%"
	rows, err := s.db.Query(searchQuery, searchParam, searchParam, searchParam)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var books []models.Book
	for rows.Next() {
		var book models.Book
		err := rows.Scan(
			&book.ID,
			&book.Title,
			&book.Author,
			&book.ISBN,
			&book.PublishedYear,
			&book.Publisher,
			&book.Description,
			&book.Category,
			&book.Stock,
			&book.CreatedBy,
			&book.CreatedAt,
			&book.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		books = append(books, book)
	}
	return books, nil
}

func (s *BookService) Delete(id int) error {
	result, err := s.db.Exec("DELETE FROM books WHERE id = ?", id)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return models.ErrNotFound
	}
	return nil
}

func (s *BookService) GetByISBN(isbn string) (*models.Book, error) {
	book := &models.Book{}
	query := `SELECT * FROM books WHERE isbn = ?`
	err := s.db.QueryRow(query, isbn).Scan(
		&book.ID, &book.Title, &book.Author, &book.ISBN,
		&book.PublishedYear, &book.Publisher, &book.Description,
		&book.Category, &book.Stock, &book.CreatedBy,
		&book.CreatedAt, &book.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, models.ErrNotFound
	}
	return book, err
}
