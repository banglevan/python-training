package db

import (
	"booksystem/internal/models"
	"database/sql"
	"time"

	"golang.org/x/crypto/bcrypt"
)

type UserService struct {
	db *sql.DB
}

func NewUserService(db *sql.DB) *UserService {
	return &UserService{db: db}
}

func (s *UserService) Create(user *models.User) error {
	// Hash password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(user.Password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	query := `
        INSERT INTO users (username, password, email, role, created_at)
        VALUES (?, ?, ?, ?, ?)
    `
	result, err := s.db.Exec(query,
		user.Username,
		string(hashedPassword),
		user.Email,
		user.Role,
		time.Now(),
	)
	if err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	user.ID = int(id)
	return nil
}

func (s *UserService) GetByID(id int) (*models.User, error) {
	user := &models.User{}
	query := `SELECT id, username, email, role, created_at FROM users WHERE id = ?`
	err := s.db.QueryRow(query, id).Scan(
		&user.ID,
		&user.Username,
		&user.Email,
		&user.Role,
		&user.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, models.ErrNotFound
	}
	return user, err
}

func (s *UserService) Authenticate(username, password string) (*models.User, error) {
	user := &models.User{}
	query := `SELECT id, username, password, email, role, created_at FROM users WHERE username = ?`
	err := s.db.QueryRow(query, username).Scan(
		&user.ID,
		&user.Username,
		&user.Password,
		&user.Email,
		&user.Role,
		&user.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, models.ErrUnauthorized
	}
	if err != nil {
		return nil, err
	}

	err = bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password))
	if err != nil {
		return nil, models.ErrUnauthorized
	}

	user.Password = ""
	return user, nil
}

func (s *UserService) Delete(id int) error {
	result, err := s.db.Exec("DELETE FROM users WHERE id = ?", id)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return models.ErrNotFound
	}
	return nil
}

func (s *UserService) GetByUsername(username string) (*models.User, error) {
	user := &models.User{}
	query := `SELECT id, username, email, role, created_at FROM users WHERE username = ?`
	err := s.db.QueryRow(query, username).Scan(
		&user.ID,
		&user.Username,
		&user.Email,
		&user.Role,
		&user.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, models.ErrNotFound
	}
	return user, err
}

func (s *UserService) List() ([]models.User, error) {
	query := `SELECT id, username, email, role, created_at FROM users ORDER BY username`
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []models.User
	for rows.Next() {
		var user models.User
		err := rows.Scan(&user.ID, &user.Username, &user.Email, &user.Role, &user.CreatedAt)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, nil
}
