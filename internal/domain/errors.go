package domain

import (
	"errors"
	"fmt"
)

// ErrValidation is the sentinel every ValidationError wraps. Callers
// (e.g. retry.go, PETPG-226) can use errors.Is(err, domain.ErrValidation)
// to classify a decode/validation failure as permanent - retrying a
// malformed payload can never succeed.
var ErrValidation = errors.New("domain: validation failed")

// ValidationError names exactly which field failed and why, without
// leaking the entire raw payload into logs.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation: field %q: %s", e.Field, e.Reason)
}

func (e *ValidationError) Unwrap() error {
	return ErrValidation
}

// NewValidationError is a small constructor to keep call sites short.
func NewValidationError(field, reason string) error {
	return &ValidationError{Field: field, Reason: reason}
}

// ErrIntegrityConflict marks a record whose identity already exists in
// durable history with different values. Retrying can never fix it and
// history is never overwritten, so PETPG-226 should treat it as permanent.
var ErrIntegrityConflict = errors.New("domain: conflicting duplicate telemetry")
