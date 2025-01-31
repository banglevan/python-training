package crawler

import "time"

type Book struct {
	ID          int
	Title       string
	Author      string
	Description string
	Language    string
	Rights      string
	ContentPath string
	ImagePath   string
	SourceURL   string
	SourceID    string
	Format      string
	FileSize    int64
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type GutenbergMetadata struct {
	Title    string   `xml:"ebook>title"`
	Author   string   `xml:"ebook>creator"`
	Language string   `xml:"ebook>language"`
	Rights   string   `xml:"ebook>rights"`
	Subject  []string `xml:"ebook>subject"`
}
