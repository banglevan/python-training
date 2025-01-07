package db

import (
	"booksystem/internal/models"
	"database/sql"
	"time"
)

type ReviewService struct {
	db *sql.DB
}

func NewReviewService(db *sql.DB) *ReviewService {
	return &ReviewService{db: db}
}

func (s *ReviewService) Create(review *models.Review) error {
	query := `
        INSERT INTO reviews (book_id, user_id, rating, comment, created_at)
        VALUES (?, ?, ?, ?, ?)
    `
	now := time.Now()
	result, err := s.db.Exec(query,
		review.BookID,
		review.UserID,
		review.Rating,
		review.Comment,
		now,
	)
	if err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	review.ID = int(id)
	review.CreatedAt = now
	return nil
}

func (s *ReviewService) ListByBook(bookID int) ([]models.Review, error) {
	query := `
        SELECT r.id, r.rating, r.comment, r.created_at,
               u.id, u.username
        FROM reviews r
        JOIN users u ON r.user_id = u.id
        WHERE r.book_id = ?
        ORDER BY r.created_at DESC
    `
	rows, err := s.db.Query(query, bookID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reviews []models.Review
	for rows.Next() {
		var r models.Review
		r.User = &models.User{}
		err := rows.Scan(
			&r.ID, &r.Rating, &r.Comment, &r.CreatedAt,
			&r.User.ID, &r.User.Username,
		)
		if err != nil {
			return nil, err
		}
		r.BookID = bookID
		reviews = append(reviews, r)
	}
	return reviews, nil
}

func (s *ReviewService) ListByUser(userID int) ([]models.Review, error) {
	query := `
        SELECT r.id, r.book_id, r.rating, r.comment, r.created_at,
               b.title, b.author
        FROM reviews r
        JOIN books b ON r.book_id = b.id
        WHERE r.user_id = ?
        ORDER BY r.created_at DESC
    `
	rows, err := s.db.Query(query, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reviews []models.Review
	for rows.Next() {
		r := models.Review{}
		r.Book = &models.Book{}
		err := rows.Scan(
			&r.ID, &r.BookID, &r.Rating, &r.Comment, &r.CreatedAt,
			&r.Book.Title, &r.Book.Author,
		)
		if err != nil {
			return nil, err
		}
		r.UserID = userID
		reviews = append(reviews, r)
	}
	return reviews, nil
}
