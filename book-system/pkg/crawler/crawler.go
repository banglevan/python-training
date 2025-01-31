package crawler

import (
	"database/sql"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Crawler struct {
	db          *sql.DB
	storageDir  string
	client      *http.Client
	rateLimiter *time.Ticker
}

func NewCrawler(dbPath, storageDir string) (*Crawler, error) {
	// Initialize database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	// Create tables if not exist
	if err := initDB(db); err != nil {
		return nil, err
	}

	// Create storage directories
	if err := os.MkdirAll(filepath.Join(storageDir, "content"), 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(storageDir, "images"), 0755); err != nil {
		return nil, err
	}

	return &Crawler{
		db:          db,
		storageDir:  storageDir,
		client:      &http.Client{Timeout: 30 * time.Second},
		rateLimiter: time.NewTicker(1 * time.Second),
	}, nil
}

func (c *Crawler) Start(startID, count int) {
	for id := startID; id < startID+count; id++ {
		<-c.rateLimiter.C // Rate limiting

		log.Printf("Fetching book ID: %d", id)
		if err := c.fetchBook(id); err != nil {
			log.Printf("Error fetching book %d: %v", id, err)
			continue
		}
	}
}

func (c *Crawler) fetchBook(id int) error {
	book, err := c.fetchMetadata(id)
	if err != nil {
		return err
	}

	// Download content
	contentPath, err := c.downloadContent(id)
	if err != nil {
		return err
	}
	book.ContentPath = contentPath

	// Download cover image
	imagePath, err := c.downloadCover(id)
	if err != nil {
		log.Printf("Warning: Could not download cover for book %d: %v", id, err)
		// Continue without image
	}
	book.ImagePath = imagePath

	// Save to database
	return c.saveBook(book)
}
