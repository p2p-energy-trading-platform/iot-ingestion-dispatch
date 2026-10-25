package registration

import (
	"context"
	"fmt"
	"strings"
)

type RegistrationRecord struct {
	ID             string
	UserID         string
	MeterID        string
	RegistrationID string
	Role           string
	ApprovalStatus string
}

type RegistrationStore interface {
	SaveMeterRegistration(ctx context.Context, reg RegistrationRecord) error
	GetByRegistrationID(ctx context.Context, registrationID string) (*RegistrationRecord, error)
	UpdateMeterRegistrationStatus(ctx context.Context, registrationID string, status string) error
}

// Service coordinates house registration with the DEWA API.
type Service struct {
	client *Client
	poller *Poller
	store  RegistrationStore
}

// NewService creates a registration service.
func NewService(client *Client, poller *Poller, store RegistrationStore) *Service {
	return &Service{
		client: client,
		poller: poller,
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

	record := RegistrationRecord{
		UserID:         request.UserReference,
		MeterID:        request.MeterID,
		RegistrationID: request.RegistrationID,
		Role:           "PRIMARY_OWNER",
		ApprovalStatus: "DEWA_APPROVAL_PENDING",
	}
	if err := s.store.SaveMeterRegistration(ctx, record); err != nil {
		return nil, fmt.Errorf("failed to save initial registration state: %w", err)
	}

	response, err := s.client.CreateLinkRequest(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("register house with DEWA: %w", err)
	}

	if response.RequestID != "" && s.poller != nil {
		go s.runPollLoop(context.Background(), response.RequestID, request.RegistrationID)
	}

	return response, nil
}

func (s *Service) runPollLoop(ctx context.Context, serverRequestID string, registrationID string) {
	if s.poller == nil || s.store == nil {
		return
	}

	finalResp, err := s.poller.PollLinkRequest(ctx, serverRequestID)
	newStatus := "REJECTED"

	if err != nil {
		if strings.Contains(err.Error(), "timed out") {
			newStatus = "EXPIRED"
		} else {
			newStatus = "REJECTED"
		}
	} else if finalResp != nil {
		newStatus = strings.ToUpper(strings.TrimSpace(finalResp.Status))
	}

	_ = s.store.UpdateMeterRegistrationStatus(ctx, registrationID, newStatus)
}

func (s *Service) ReverifyRegistration(
	ctx context.Context,
	registrationID string,
	userReference string,
	meterID string,
) (*LinkResponse, error) {
	if s.client == nil || s.store == nil {
		return nil, fmt.Errorf("client or store not configured")
	}

	_, err := s.store.GetByRegistrationID(ctx, registrationID)
	if err != nil {
		// Retrigger process if request does not exist
		payload := LinkRequest{
			RegistrationID: registrationID,
			MeterID:        meterID,
			UserReference:  userReference,
		}
		resp, createErr := s.client.CreateLinkRequest(ctx, payload)
		if createErr != nil {
			return nil, fmt.Errorf("reverification creation failed: %w", createErr)
		}

		_ = s.store.SaveMeterRegistration(ctx, RegistrationRecord{
			UserID:         userReference,
			MeterID:        meterID,
			RegistrationID: registrationID,
			Role:           "PRIMARY_OWNER",
			ApprovalStatus: "DEWA_APPROVAL_PENDING",
		})

		if resp.RequestID != "" && s.poller != nil {
			go s.runPollLoop(context.Background(), resp.RequestID, registrationID)
		}

		return resp, nil
	}

	// If the request already exists
	payload := LinkRequest{
		RegistrationID: registrationID,
		MeterID:        meterID,
		UserReference:  userReference,
	}
	resp, err := s.client.CreateLinkRequest(ctx, payload)
	if err != nil {
		return nil, fmt.Errorf("reverification request failed: %w", err)
	}

	_ = s.store.UpdateMeterRegistrationStatus(ctx, registrationID, "DEWA_APPROVAL_PENDING")
	if resp.RequestID != "" && s.poller != nil {
		go s.runPollLoop(context.Background(), resp.RequestID, registrationID)
	}

	return resp, nil
}
