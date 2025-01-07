package models

import "errors"

var (
	ErrNotFound      = errors.New("record not found")
	ErrInvalidInput  = errors.New("invalid input")
	ErrUnauthorized  = errors.New("unauthorized")
	ErrAlreadyExists = errors.New("record already exists")
	ErrOutOfStock    = errors.New("book out of stock")
)

type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return e.Field + ": " + e.Message
}
