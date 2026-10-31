package registration

import (
	"context"
	"fmt"
	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
	"strings"
)

// Service coordinates house registration with the DEWA API.
type Service struct {
	client *Client
	store  *postgres.Store
}

// NewService creates a registration service.
func NewService(client *Client, store *postgres.Store) *Service {

	return &Service{

		client: client,
		store:  store,
	}

}

// RegisterHouse submits a link request to DEWA.
func (s *Service) RegisterHouse(

	ctx context.Context,
	request LinkRequest,

) (*LinkResponse, error) {

	if s.client == nil {
		return nil, fmt.Errorf("registration client is not configured")
	}

	if strings.TrimSpace(request.RegistrationID) == "" {
		return nil, fmt.Errorf("registration ID is required")
	}

	if strings.TrimSpace(request.MeterID) == "" {
		return nil, fmt.Errorf("meter ID is required")
	}

	if strings.TrimSpace(request.UserReference) == "" {
		return nil, fmt.Errorf("user reference is required")
	}

	response, err := s.client.CreateLinkRequest(ctx, request)

	if err != nil {
		return nil, fmt.Errorf("register house with DEWA: %w", err)
	}

	if s.store == nil {
		return nil, fmt.Errorf("PostgreSQL store is not configured")
	}

	registration := postgres.MeterRegistration{

		UserID:         request.UserReference,
		MeterID:        request.MeterID,
		RegistrationID: request.RegistrationID,
		Role:           "PRIMARY_OWNER",
		ApprovalStatus: "DEWA_APPROVAL_PENDING",
	}

	if err := s.store.SaveMeterRegistration(ctx, registration); err != nil {
		return nil, fmt.Errorf("save meter registration: %w", err)
	}

	return response, nil

}
