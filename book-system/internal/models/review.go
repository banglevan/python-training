package models

import "time"

type Review struct {
	ID        int       `json:"id"`
	BookID    int       `json:"book_id"`
	UserID    int       `json:"user_id"`
	Rating    int       `json:"rating"` // 1-5 stars
	Comment   string    `json:"comment"`
	CreatedAt time.Time `json:"created_at"`
	Book      *Book     `json:"book,omitempty"`
	User      *User     `json:"user,omitempty"`
}

// type reviewService interface {
// 	Create(review *Review) error
// 	GetByID(id int) (*Review, error)
// 	ListByBook(bookID int) ([]Review, error)
// 	ListByUser(userID int) ([]Review, error)
// 	Update(review *Review) error
// 	Delete(id int) error
// }
