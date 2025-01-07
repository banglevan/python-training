package db

import (
	"booksystem/internal/models"
	"database/sql"
	"time"
)

type LoanService struct {
	db *sql.DB
}

func NewLoanService(db *sql.DB) *LoanService {
	return &LoanService{db: db}
}

func (s *LoanService) Create(loan *models.Loan) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Check book stock
	var stock int
	err = tx.QueryRow("SELECT stock FROM books WHERE id = ?", loan.BookID).Scan(&stock)
	if err != nil {
		return err
	}
	if stock <= 0 {
		return models.ErrOutOfStock
	}

	// Create loan
	query := `
        INSERT INTO loans (book_id, user_id, loan_date, due_date, status)
        VALUES (?, ?, ?, ?, ?)
    `
	result, err := tx.Exec(query,
		loan.BookID,
		loan.UserID,
		loan.LoanDate,
		loan.DueDate,
		loan.Status,
	)
	if err != nil {
		return err
	}

	// Update book stock
	_, err = tx.Exec("UPDATE books SET stock = stock - 1 WHERE id = ?", loan.BookID)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	loan.ID = int(id)
	return nil
}

func (s *LoanService) Return(id int) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Get loan details
	var bookID int
	var status models.LoanStatus
	err = tx.QueryRow("SELECT book_id, status FROM loans WHERE id = ?", id).Scan(&bookID, &status)
	if err == sql.ErrNoRows {
		return models.ErrNotFound
	}
	if err != nil {
		return err
	}

	if status == models.LoanStatusReturned {
		return models.ErrInvalidInput
	}

	// Update loan
	now := time.Now()
	_, err = tx.Exec(`
        UPDATE loans 
        SET status = ?, return_date = ?
        WHERE id = ?
    `, models.LoanStatusReturned, now, id)
	if err != nil {
		return err
	}

	// Update book stock
	_, err = tx.Exec("UPDATE books SET stock = stock + 1 WHERE id = ?", bookID)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *LoanService) GetOverdue() ([]models.Loan, error) {
	query := `
        SELECT l.*, b.title, b.author, u.username
        FROM loans l
        JOIN books b ON l.book_id = b.id
        JOIN users u ON l.user_id = u.id
        WHERE l.status = ? AND l.due_date < ?
        ORDER BY l.due_date
    `
	rows, err := s.db.Query(query, models.LoanStatusBorrowed, time.Now())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var loans []models.Loan
	for rows.Next() {
		var loan models.Loan
		loan.Book = &models.Book{}
		loan.User = &models.User{}

		err := rows.Scan(
			&loan.ID,
			&loan.BookID,
			&loan.UserID,
			&loan.LoanDate,
			&loan.DueDate,
			&loan.ReturnDate,
			&loan.Status,
			&loan.Book.Title,
			&loan.Book.Author,
			&loan.User.Username,
		)
		if err != nil {
			return nil, err
		}
		loans = append(loans, loan)
	}
	return loans, nil
}

func (s *LoanService) GetByID(id int) (*models.Loan, error) {
	loan := &models.Loan{}
	query := `SELECT id, book_id, user_id, loan_date, due_date, return_date, status 
			 FROM loans WHERE id = ?`
	err := s.db.QueryRow(query, id).Scan(
		&loan.ID,
		&loan.BookID,
		&loan.UserID,
		&loan.LoanDate,
		&loan.DueDate,
		&loan.ReturnDate,
		&loan.Status,
	)
	if err == sql.ErrNoRows {
		return nil, models.ErrNotFound
	}
	return loan, err
}

func (s *LoanService) List() ([]models.Loan, error) {
	query := `SELECT * FROM loans ORDER BY loan_date DESC`
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var loans []models.Loan
	for rows.Next() {
		var loan models.Loan
		err := rows.Scan(
			&loan.ID, &loan.BookID, &loan.UserID,
			&loan.LoanDate, &loan.DueDate, &loan.ReturnDate,
			&loan.Status,
		)
		if err != nil {
			return nil, err
		}
		loans = append(loans, loan)
	}
	return loans, nil
}

func (s *LoanService) ListByUser(userID int) ([]models.Loan, error) {
	query := `
        SELECT l.id, l.book_id, l.user_id, l.loan_date, l.due_date, l.return_date, l.status,
               b.title, b.author,
               u.username
        FROM loans l
        JOIN books b ON l.book_id = b.id
        JOIN users u ON l.user_id = u.id
        WHERE l.user_id = ?
        ORDER BY l.loan_date DESC
    `
	rows, err := s.db.Query(query, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var loans []models.Loan
	for rows.Next() {
		var l models.Loan
		l.Book = &models.Book{}
		l.User = &models.User{}
		err := rows.Scan(
			&l.ID, &l.BookID, &l.UserID, &l.LoanDate, &l.DueDate, &l.ReturnDate, &l.Status,
			&l.Book.Title, &l.Book.Author,
			&l.User.Username,
		)
		if err != nil {
			return nil, err
		}
		loans = append(loans, l)
	}
	return loans, nil
}
