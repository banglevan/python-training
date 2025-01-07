package db

import (
	"booksystem/internal/models"
	"database/sql"
)

type StatsService struct {
	db *sql.DB
}

func NewStatsService(db *sql.DB) *StatsService {
	return &StatsService{db: db}
}

func (s *StatsService) GetBookStats() (*models.BookStats, error) {
	stats := &models.BookStats{}

	// Get total and available books
	err := s.db.QueryRow(`
        SELECT 
            COUNT(*) as total_books,
            SUM(CASE WHEN stock > 0 THEN 1 ELSE 0 END) as available_books
        FROM books
    `).Scan(&stats.TotalBooks, &stats.AvailableBooks)
	if err != nil {
		return nil, err
	}

	// Get loan statistics
	err = s.db.QueryRow(`
        SELECT 
            COUNT(*) as total_loans,
            SUM(CASE WHEN status = 'borrowed' THEN 1 ELSE 0 END) as active_loans,
            SUM(CASE WHEN status = 'borrowed' AND due_date < CURRENT_TIMESTAMP THEN 1 ELSE 0 END) as overdue_loans
        FROM loans
    `).Scan(&stats.TotalLoans, &stats.ActiveLoans, &stats.OverdueLoans)
	if err != nil {
		return nil, err
	}

	// Get average rating
	err = s.db.QueryRow(`
        SELECT COALESCE(AVG(rating), 0) FROM reviews
    `).Scan(&stats.AverageRating)
	if err != nil {
		return nil, err
	}

	return stats, nil
}

func (s *StatsService) GetPopularBooks(limit int) ([]models.Book, error) {
	query := `
        SELECT b.*, COUNT(l.id) as loan_count
        FROM books b
        LEFT JOIN loans l ON b.id = l.book_id
        GROUP BY b.id
        ORDER BY loan_count DESC
        LIMIT ?
    `
	rows, err := s.db.Query(query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var books []models.Book
	for rows.Next() {
		var book models.Book
		var loanCount int
		err := rows.Scan(
			&book.ID, &book.Title, &book.Author, &book.ISBN,
			&book.PublishedYear, &book.Publisher, &book.Description,
			&book.Category, &book.Stock, &book.CreatedBy,
			&book.CreatedAt, &book.UpdatedAt, &loanCount,
		)
		if err != nil {
			return nil, err
		}
		books = append(books, book)
	}
	return books, nil
}

func (s *StatsService) GetActiveReaders(limit int) ([]models.User, error) {
	query := `
        SELECT u.id, u.username, u.email, u.role, u.created_at,
               COUNT(l.id) as loan_count
        FROM users u
        LEFT JOIN loans l ON u.id = l.user_id
        GROUP BY u.id
        ORDER BY loan_count DESC
        LIMIT ?
    `
	rows, err := s.db.Query(query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []models.User
	for rows.Next() {
		var user models.User
		var loanCount int
		err := rows.Scan(
			&user.ID, &user.Username, &user.Email,
			&user.Role, &user.CreatedAt, &loanCount,
		)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, nil
}
