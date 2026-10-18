package registration

import (
	"context"
	"fmt"
)

// ApprovalPending is the initial status while waiting for DEWA approval.
const ApprovalPending = "DEWA_APPROVAL_PENDING"

// InitiateClaimRequest contains the information needed to register a meter.
type InitiateClaimRequest struct {
	RegistrationID string
	MeterID        string
	UserReference  string
}

// InitiateClaimResult contains the result returned after initiating a claim.
type InitiateClaimResult struct {
	RegistrationID string
	Status         string
}

// Service coordinates smart meter registration.
type Service struct{}

// InitiateMeterClaim will coordinate registration initiation.
// The implementation will be added in a later commit.
func (service *Service) InitiateMeterClaim(
	ctx context.Context,
	request InitiateClaimRequest,
) (*InitiateClaimResult, error) {
	_ = ctx
	_ = request

	return nil, fmt.Errorf("meter registration service is not implemented")
}