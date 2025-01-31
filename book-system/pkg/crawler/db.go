package crawler

import (
	"database/sql"
	"time"
)

func initDB(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS books (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL,
			author TEXT NOT NULL,
			description TEXT,
			language TEXT,
			rights TEXT,
			content_path TEXT,
			image_path TEXT,
			source_url TEXT,
			source_id TEXT,
			format TEXT,
			file_size INTEGER,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`)
	return err
}

func (c *Crawler) saveBook(book *Book) error {
	_, err := c.db.Exec(`
		INSERT INTO books (
			title, author, description, language, rights,
			content_path, image_path, source_url, source_id,
			format, file_size, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		book.Title, book.Author, book.Description, book.Language,
		book.Rights, book.ContentPath, book.ImagePath, book.SourceURL,
		book.SourceID, book.Format, book.FileSize,
		time.Now(), time.Now(),
	)
	return err
}
