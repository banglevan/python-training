package models

import "time"

type User struct {
	ID        int       `json:"id"`
	Username  string    `json:"username"`
	Password  string    `json:"-"` // "-" means this field won't be included in JSON
	Email     string    `json:"email"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
}

// type userService interface {
// 	Create(user *User) error
// 	GetByID(id int) (*User, error)
// 	GetByUsername(username string) (*User, error)
// 	Update(user *User) error
// 	Delete(id int) error
// 	List() ([]User, error)
// 	Authenticate(username, password string) (*User, error)
// }
