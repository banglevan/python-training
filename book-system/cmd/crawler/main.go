package main

import (
	"booksystem/pkg/crawler"
	"flag"
	"log"
)

func main() {
	// Command line flags
	dbPath := flag.String("db", "books.db", "Path to SQLite database")
	storageDir := flag.String("storage", "storage", "Path to store book content and images")
	startID := flag.Int("start", 1000, "Starting Gutenberg book ID")
	count := flag.Int("count", 100, "Number of books to fetch")
	flag.Parse()

	// Initialize crawler
	c, err := crawler.NewCrawler(*dbPath, *storageDir)
	if err != nil {
		log.Fatal(err)
	}

	// Start crawling
	log.Printf("Starting crawler from ID %d, fetching %d books", *startID, *count)
	c.Start(*startID, *count)
}
