package db

import (
	"booksystem/internal/models"
	"database/sql"
	"fmt"
	"time"
)

type BookService struct {
	db *sql.DB
}

func NewBookService(db *sql.DB) *BookService {
	return &BookService{db: db}
}

func (s *BookService) List() ([]models.Book, error) {
	rows, err := s.db.Query(`
        SELECT 
            id, title, author, isbn, published_year,
            publisher, description, category, stock,
            created_by, created_at, updated_at,
            image_path, content_path, language,
            rights, source_url, source_id, format,
            file_size
        FROM books 
        ORDER BY id DESC
    `)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var books []models.Book
	for rows.Next() {
		var book models.Book
		err := rows.Scan(
			&book.ID, &book.Title, &book.Author, &book.ISBN,
			&book.PublishedYear, &book.Publisher, &book.Description,
			&book.Category, &book.Stock, &book.CreatedBy,
			&book.CreatedAt, &book.UpdatedAt, &book.ImagePath,
			&book.ContentPath, &book.Language, &book.Rights,
			&book.SourceURL, &book.SourceID, &book.Format,
			&book.FileSize,
		)
		if err != nil {
			return nil, err
		}
		books = append(books, book)
	}

	return books, nil
}

func (s *BookService) Update(book *models.Book) error {
	query := `
        UPDATE books SET 
            title = ?, author = ?, isbn = ?,
            published_year = ?, publisher = ?,
            description = ?, category = ?, stock = ?,
            image_path = ?, content_path = ?,
            language = ?, rights = ?, source_url = ?,
            source_id = ?, format = ?, file_size = ?,
            updated_at = ?
        WHERE id = ?
    `

	_, err := s.db.Exec(query,
		book.Title, book.Author, book.ISBN,
		book.PublishedYear, book.Publisher,
		book.Description, book.Category, book.Stock,
		book.ImagePath, book.ContentPath,
		book.Language, book.Rights, book.SourceURL,
		book.SourceID, book.Format, book.FileSize,
		time.Now(), book.ID,
	)

	return err
}

func (s *BookService) Create(book *models.Book) error {
	query := `
        INSERT INTO books (
            title, author, isbn, published_year,
            publisher, description, category, stock,
            created_by, image_path, content_path,
            language, rights, source_url, source_id,
            format, file_size, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `

	now := time.Now()
	result, err := s.db.Exec(query,
		book.Title, book.Author, book.ISBN,
		book.PublishedYear, book.Publisher,
		book.Description, book.Category, book.Stock,
		book.CreatedBy, book.ImagePath, book.ContentPath,
		book.Language, book.Rights, book.SourceURL,
		book.SourceID, book.Format, book.FileSize,
		now, now,
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
	var book models.Book
	err := s.db.QueryRow(`
        SELECT 
            id, title, author, isbn, published_year,
            publisher, description, category, stock,
            created_by, created_at, updated_at,
            image_path, content_path, language,
            rights, source_url, source_id, format,
            file_size
        FROM books 
        WHERE id = ?
    `, id).Scan(
		&book.ID, &book.Title, &book.Author, &book.ISBN,
		&book.PublishedYear, &book.Publisher, &book.Description,
		&book.Category, &book.Stock, &book.CreatedBy,
		&book.CreatedAt, &book.UpdatedAt, &book.ImagePath,
		&book.ContentPath, &book.Language, &book.Rights,
		&book.SourceURL, &book.SourceID, &book.Format,
		&book.FileSize,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("book not found")
	}
	if err != nil {
		return nil, err
	}
	return &book, nil
}

func (s *BookService) Delete(id int) error {
	result, err := s.db.Exec("DELETE FROM books WHERE id = ?", id)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return fmt.Errorf("book not found")
	}

	return nil
}

func (s *BookService) GetByISBN(isbn string) (*models.Book, error) {
	var book models.Book
	err := s.db.QueryRow(`
        SELECT 
            id, title, author, isbn, published_year,
            publisher, description, category, stock,
            created_by, created_at, updated_at,
            image_path, content_path, language,
            rights, source_url, source_id, format,
            file_size
        FROM books 
        WHERE isbn = ?
    `, isbn).Scan(
		&book.ID, &book.Title, &book.Author, &book.ISBN,
		&book.PublishedYear, &book.Publisher, &book.Description,
		&book.Category, &book.Stock, &book.CreatedBy,
		&book.CreatedAt, &book.UpdatedAt, &book.ImagePath,
		&book.ContentPath, &book.Language, &book.Rights,
		&book.SourceURL, &book.SourceID, &book.Format,
		&book.FileSize,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("book not found")
	}
	if err != nil {
		return nil, err
	}
	return &book, nil
}

func (s *BookService) Search(query string) ([]models.Book, error) {
	rows, err := s.db.Query(`
        SELECT 
            id, title, author, isbn, published_year,
            publisher, description, category, stock,
            created_by, created_at, updated_at,
            image_path, content_path, language,
            rights, source_url, source_id, format,
            file_size
        FROM books 
        WHERE title LIKE ? OR author LIKE ? OR description LIKE ?
        ORDER BY id DESC
    `, "%"+query+"%", "%"+query+"%", "%"+query+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var books []models.Book
	for rows.Next() {
		var book models.Book
		err := rows.Scan(
			&book.ID, &book.Title, &book.Author, &book.ISBN,
			&book.PublishedYear, &book.Publisher, &book.Description,
			&book.Category, &book.Stock, &book.CreatedBy,
			&book.CreatedAt, &book.UpdatedAt, &book.ImagePath,
			&book.ContentPath, &book.Language, &book.Rights,
			&book.SourceURL, &book.SourceID, &book.Format,
			&book.FileSize,
		)
		if err != nil {
			return nil, err
		}
		books = append(books, book)
	}

	return books, nil
}

func (s *BookService) UpdateStock(id int, stock int) error {
	query := `
        UPDATE books 
        SET stock = ?, updated_at = ?
        WHERE id = ?
    `

	result, err := s.db.Exec(query, stock, time.Now(), id)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return fmt.Errorf("book not found")
	}

	return nil
}
