package domain

import (
	"errors"
	"fmt"
)

// ErrValidation is the sentinel every ValidationError wraps. Callers
// can use errors.Is(err, domain.ErrValidation) to classify a
// decode/validation failure as permanent - retrying a malformed payload
// can never succeed.
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

// ErrNotFound marks a lookup of a house or reading that does not exist, or
// that the caller is not allowed to see.
var ErrNotFound = errors.New("domain: not found")

// ErrIntegrityConflict marks a record whose identity already exists in
// durable history with different values. Retrying can never fix it and
// history is never overwritten, so it is classified as permanent.
var ErrIntegrityConflict = errors.New("domain: conflicting duplicate telemetry")

// ErrGridNotProvisioned marks a record for a grid that the in-memory registry
// accepted but the database does not have, for example during a registry
// refresh window. Retrying cannot fix it, so it is classified as permanent.
var ErrGridNotProvisioned = errors.New("domain: grid is not provisioned")

// ErrHouseIdentity marks a heartbeat whose house is unknown, or whose grid or
// meter does not match the registered house. Houses come from a separate
// registration flow and a heartbeat never creates or moves one, so retrying
// cannot fix it: it is a permanent identity/provisioning failure.
var ErrHouseIdentity = errors.New("domain: house identity does not match registration")
