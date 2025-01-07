package db

import (
	"booksystem/internal/models"
	"database/sql"
)

type CategoryService struct {
	db *sql.DB
}

func NewCategoryService(db *sql.DB) *CategoryService {
	return &CategoryService{db: db}
}

func (s *CategoryService) Create(category *models.Category) error {
	query := `
        INSERT INTO categories (name, description)
        VALUES (?, ?)
    `
	result, err := s.db.Exec(query, category.Name, category.Description)
	if err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	category.ID = int(id)
	return nil
}

func (s *CategoryService) GetByID(id int) (*models.Category, error) {
	category := &models.Category{}
	query := `SELECT id, name, description FROM categories WHERE id = ?`
	err := s.db.QueryRow(query, id).Scan(&category.ID, &category.Name, &category.Description)
	if err == sql.ErrNoRows {
		return nil, models.ErrNotFound
	}
	return category, err
}

func (s *CategoryService) List() ([]models.Category, error) {
	query := `SELECT id, name, description FROM categories ORDER BY name`
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var categories []models.Category
	for rows.Next() {
		var cat models.Category
		if err := rows.Scan(&cat.ID, &cat.Name, &cat.Description); err != nil {
			return nil, err
		}
		categories = append(categories, cat)
	}
	return categories, nil
}

func (s *CategoryService) Delete(id int) error {
	result, err := s.db.Exec("DELETE FROM categories WHERE id = ?", id)
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

func (s *CategoryService) Update(category *models.Category) error {
	query := `UPDATE categories SET name = ?, description = ? WHERE id = ?`
	result, err := s.db.Exec(query, category.Name, category.Description, category.ID)
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
