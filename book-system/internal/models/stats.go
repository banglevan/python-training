package models

type BookStats struct {
	TotalBooks     int     `json:"total_books"`
	AvailableBooks int     `json:"available_books"`
	TotalLoans     int     `json:"total_loans"`
	ActiveLoans    int     `json:"active_loans"`
	OverdueLoans   int     `json:"overdue_loans"`
	AverageRating  float64 `json:"average_rating"`
}

// type statsService interface {
// 	GetBookStats() (*BookStats, error)
// 	GetPopularBooks(limit int) ([]Book, error)
// 	GetActiveReaders(limit int) ([]User, error)
// 	GetOverdueStats() map[string]int
// }
