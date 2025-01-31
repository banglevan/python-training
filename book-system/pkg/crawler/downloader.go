package crawler

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
)

func (c *Crawler) downloadContent(id int) (string, error) {
	url := fmt.Sprintf("https://www.gutenberg.org/files/%d/%d-0.txt", id, id)

	relPath := filepath.Join("content", fmt.Sprintf("%d.txt", id))
	absPath := filepath.Join(c.storageDir, relPath)

	if err := downloadFile(url, absPath); err != nil {
		return "", err
	}

	return relPath, nil
}

func (c *Crawler) downloadCover(id int) (string, error) {
	// Try different image sources
	urls := []string{
		fmt.Sprintf("https://www.gutenberg.org/cache/epub/%d/pg%d.cover.medium.jpg", id, id),
		fmt.Sprintf("https://covers.openlibrary.org/b/id/%d-M.jpg", id),
	}

	for _, url := range urls {
		relPath := filepath.Join("images", fmt.Sprintf("%d.jpg", id))
		absPath := filepath.Join(c.storageDir, relPath)

		if err := downloadFile(url, absPath); err == nil {
			return relPath, nil
		}
	}

	return "", fmt.Errorf("no cover found")
}

func downloadFile(url, destPath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status: %s", resp.Status)
	}

	out, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}
