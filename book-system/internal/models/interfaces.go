package models

// Service interfaces
type (
	UserService interface {
		Create(user *User) error
		GetByID(id int) (*User, error)
		GetByUsername(username string) (*User, error)
		List() ([]User, error)
		Delete(id int) error
		Authenticate(username, password string) (*User, error)
	}

	BookService interface {
		Create(book *Book) error
		GetByID(id int) (*Book, error)
		GetByISBN(isbn string) (*Book, error)
		Search(query string) ([]Book, error)
		UpdateStock(id int, quantity int) error
		Delete(id int) error
		List() ([]Book, error)
		Update(book *Book) error
	}

	LoanService interface {
		Create(loan *Loan) error
		GetByID(id int) (*Loan, error)
		List() ([]Loan, error)
		ListByUser(userID int) ([]Loan, error)
		Return(id int) error
		GetOverdue() ([]Loan, error)
	}

	CategoryService interface {
		Create(category *Category) error
		GetByID(id int) (*Category, error)
		List() ([]Category, error)
		Update(category *Category) error
		Delete(id int) error
	}

	ReviewService interface {
		Create(review *Review) error
		ListByBook(bookID int) ([]Review, error)
		ListByUser(userID int) ([]Review, error)
	}

	StatsService interface {
		GetBookStats() (*BookStats, error)
		GetPopularBooks(limit int) ([]Book, error)
		GetActiveReaders(limit int) ([]User, error)
	}
)
