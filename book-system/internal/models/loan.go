package models

import "time"

type LoanStatus string

const (
	LoanStatusBorrowed LoanStatus = "borrowed"
	LoanStatusReturned LoanStatus = "returned"
	LoanStatusOverdue  LoanStatus = "overdue"
)

type Loan struct {
	ID         int        `json:"id"`
	BookID     int        `json:"book_id"`
	UserID     int        `json:"user_id"`
	LoanDate   time.Time  `json:"loan_date"`
	DueDate    time.Time  `json:"due_date"`
	ReturnDate *time.Time `json:"return_date,omitempty"`
	Status     string     `json:"status"`
	Book       *Book      `json:"book,omitempty"`
	User       *User      `json:"user,omitempty"`
}

// type loanService interface {
// 	Create(loan *Loan) error
// 	GetByID(id int) (*Loan, error)
// 	Update(loan *Loan) error
// 	List() ([]Loan, error)
// 	ListByUser(userID int) ([]Loan, error)
// 	ListByBook(bookID int) ([]Loan, error)
// 	Return(id int) error
// 	GetOverdue() ([]Loan, error)
// }
