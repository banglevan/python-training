package models

type Category struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// type categoryService interface {
// 	Create(category *Category) error
// 	GetByID(id int) (*Category, error)
// 	List() ([]Category, error)
// 	Update(category *Category) error
// 	Delete(id int) error
// }
