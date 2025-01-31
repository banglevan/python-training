package crawler

import (
	"encoding/xml"
	"fmt"
	"strconv"
)

func (c *Crawler) fetchMetadata(id int) (*Book, error) {
	url := fmt.Sprintf("https://www.gutenberg.org/cache/epub/%d/pg%d.rdf", id, id)

	resp, err := c.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to fetch metadata: status %d", resp.StatusCode)
	}

	var metadata GutenbergMetadata
	if err := xml.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return nil, err
	}

	book := &Book{
		Title:     metadata.Title,
		Author:    metadata.Author,
		Language:  metadata.Language,
		Rights:    metadata.Rights,
		SourceURL: url,
		SourceID:  strconv.Itoa(id),
		Format:    "txt",
	}

	return book, nil
}
