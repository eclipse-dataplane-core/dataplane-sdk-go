package dsdk

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrValidation Sentinel error to indicate a structural validation error, e.g. a missing property on a JSON object
	ErrValidation = errors.New("validation error")
	// ErrConflict indicates an object conflict, e.g. when creating an object that already exists
	ErrConflict = errors.New("conflict")
	// ErrNotFound indicates that a certain object does not exist
	ErrNotFound = errors.New("not found")
	// ErrInvalidInput Sentinel error to indicate a wrong input, e.g. a string when a number was expected, or an empty string
	ErrInvalidInput = errors.New("invalid input")
	// ErrInvalidTransition Sentinel error to indicate an invalid state transition, e.g. of a data flow
	ErrInvalidTransition = errors.New("invalid transition")
	// ErrUnauthorized indicates a request whose caller could not be authenticated (HTTP 401)
	ErrUnauthorized = errors.New("unauthorized")
	// ErrForbidden indicates an authenticated caller that may not access the resource (HTTP 403)
	ErrForbidden = errors.New("forbidden")
	// ErrAuthorizationNotSupported indicates an authorization type without a registered Authorization (HTTP 400)
	ErrAuthorizationNotSupported = errors.New("authorization not supported")
)

// NewValidationError Helper to create new ValidationError
func NewValidationError(messages ...string) error {
	return fmt.Errorf("%w: %s", ErrValidation, strings.Join(messages, "; "))
}

func WrapValidationError(err error) error {
	return fmt.Errorf("%w: %w", ErrValidation, err)
}
