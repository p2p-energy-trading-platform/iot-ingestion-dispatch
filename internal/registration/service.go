
package registration

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
)

// ApprovalPending is the initial status while waiting for DEWA approval.
const ApprovalPending = "DEWA_APPROVAL_PENDING"

// RegistrationRepository persists meter registration records.
type RegistrationRepository interface {
	SaveMeterRegistration(
		ctx context.Context,
		registration postgres.MeterRegistration,
	) error
}

// DEWAClientInterface sends registration requests to DEWA.
type DEWAClientInterface interface {
	CreateLinkRequest(
		ctx context.Context,
		payload ClaimPayload,
	) (*ClaimResponse, error)
}

// InitiateClaimRequest contains the details required to register a meter.
type InitiateClaimRequest struct {
	RegistrationID string
	MeterID        string
	UserReference  string
}

// InitiateClaimResult contains the result of initiating a registration.
type InitiateClaimResult struct {
	RegistrationID string
	Status         string
}

// Service coordinates persistence and DEWA registration requests.
type Service struct {
	repository RegistrationRepository
	dewaClient DEWAClientInterface
}

// NewService creates a registration service.
func NewService(
	repository RegistrationRepository,
	dewaClient DEWAClientInterface,
) *Service {
	return &Service{
		repository: repository,
		dewaClient: dewaClient,
	}
}

// InitiateMeterClaim saves a pending registration and dispatches it to DEWA.
func (service *Service) InitiateMeterClaim(
	ctx context.Context,
	request InitiateClaimRequest,
) (*InitiateClaimResult, error) {
	if service.repository == nil {
		return nil, fmt.Errorf("registration repository is required")
	}
	if service.dewaClient == nil {
		return nil, fmt.Errorf("DEWA client is required")
	}

	if request.MeterID == "" {
		return nil, fmt.Errorf("meter ID is required")
	}
	if request.UserReference == "" {
		return nil, fmt.Errorf("user reference is required")
	}

	registrationID := request.RegistrationID
	if registrationID == "" {
		registrationID = uuid.NewString()
	}

	registration := postgres.MeterRegistration{
		RegistrationID: registrationID,
		MeterID:        request.MeterID,
		UserReference:  request.UserReference,
		ApprovalStatus: ApprovalPending,
	}

	if err := service.repository.SaveMeterRegistration(ctx, registration); err != nil {
		return nil, fmt.Errorf("persist meter registration: %w", err)
	}

	payload := ClaimPayload{
		RegistrationID: registrationID,
		MeterID:        request.MeterID,
		UserReference:  request.UserReference,
	}

	response, err := service.dewaClient.CreateLinkRequest(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("dispatch registration to DEWA: %w", err)
	}

	if response == nil {
		return nil, fmt.Errorf("DEWA returned an empty response")
	}

	return &InitiateClaimResult{
		RegistrationID: registrationID,
		Status:         response.Status,
	}, nil
}
