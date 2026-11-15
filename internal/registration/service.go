package registration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
)

type RegistrationRecord struct {
	ID             string
	UserID         string
	MeterID        string
	RegistrationID string
	RequestID      string
	Role           string
	ApprovalStatus string
}

type RegistrationStore interface {
	SaveMeterRegistration(ctx context.Context, reg RegistrationRecord) error
	GetByRegistrationID(ctx context.Context, registrationID string) (*RegistrationRecord, error)
	UpdateMeterRegistrationStatus(ctx context.Context, registrationID string, status string) error
	UpdateMeterRegistrationRequestID(ctx context.Context, registrationID string, requestID string) error
	GetRegistrationByMeterID(ctx context.Context, meterID string) (*RegistrationRecord, error)
	GetRegistrationByUserID(ctx context.Context, userReference string) (*RegistrationRecord, error)
}

type ActuationDispatcher interface {
	SendActivateMeter(ctx context.Context, meterID string) error
}

// Service coordinates house registration with the DEWA API.
type Service struct {
	client     *Client
	poller     *Poller
	store      RegistrationStore
	dispatcher ActuationDispatcher
	logger     *slog.Logger
}

// NewService creates a registration service.
func NewService(client *Client, poller *Poller, store RegistrationStore, dispatcher ActuationDispatcher, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}

	return &Service{
		client:     client,
		poller:     poller,
		store:      store,
		dispatcher: dispatcher,
		logger:     logger,
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
	if s.store == nil {
		return nil, fmt.Errorf("registration store is not configured")
	}

	if strings.TrimSpace(request.RegistrationID) == "" {
		return nil, domain.NewValidationError("registration_id", "is required")
	}

	if strings.TrimSpace(request.MeterID) == "" {
		return nil, domain.NewValidationError("meter_id", "is required")
	}

	if strings.TrimSpace(request.UserReference) == "" {
		return nil, domain.NewValidationError("user_reference", "is required")
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

	return s.createLinkRequest(ctx, request)
}

func (s *Service) createLinkRequest(ctx context.Context, request LinkRequest) (*LinkResponse, error) {
	response, err := s.client.CreateLinkRequest(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("register house with DEWA: %w", err)
	}
	if response == nil || strings.TrimSpace(response.RequestID) == "" {
		return nil, fmt.Errorf("DEWA link request response does not contain a request ID")
	}
	if err := s.store.UpdateMeterRegistrationRequestID(ctx, request.RegistrationID, response.RequestID); err != nil {
		return nil, fmt.Errorf("save DEWA request ID: %w", err)
	}

	s.startPolling(response.RequestID, request.RegistrationID)

	return response, nil
}

func (s *Service) startPolling(requestID, registrationID string) {
	if s.poller == nil {
		return
	}

	go s.runPollLoop(context.Background(), requestID, registrationID, s.dispatcher)
}

func (s *Service) runPollLoop(ctx context.Context, serverRequestID string, registrationID string, dispatcher ActuationDispatcher) {
	if s.poller == nil || s.store == nil {
		return
	}

	finalResp, err := s.poller.PollLinkRequest(ctx, serverRequestID)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			if updateErr := s.store.UpdateMeterRegistrationStatus(ctx, registrationID, "EXPIRED"); updateErr != nil {
				s.logger.Error("Failed to expire smart meter registration", "err", updateErr)
			}
			return
		}

		s.logger.Error("Failed to poll DEWA registration status", "err", err, "registration_id", registrationID)
		return
	}
	if finalResp == nil {
		s.logger.Error("DEWA polling completed without a status", "registration_id", registrationID)
		return
	}

	newStatus := strings.ToUpper(strings.TrimSpace(finalResp.Status))
	if newStatus != "APPROVED" && newStatus != "REJECTED" {
		s.logger.Error("DEWA polling returned a non-terminal status", "status", newStatus, "registration_id", registrationID)
		return
	}

	record, err := s.store.GetByRegistrationID(ctx, registrationID)
	if err != nil || record == nil {
		s.logger.Error("Failed to load registration after DEWA polling", "err", err, "registration_id", registrationID)
		return
	}

	if updateErr := s.store.UpdateMeterRegistrationStatus(ctx, registrationID, newStatus); updateErr != nil {
		s.logger.Error("Failed to update smart meter registration status", "err", updateErr)
		return
	}

	if newStatus != "APPROVED" || record.ApprovalStatus == "APPROVED" || dispatcher == nil {
		return
	}

	if pubErr := dispatcher.SendActivateMeter(ctx, record.MeterID); pubErr != nil {
		s.logger.Error("Failed to publish smart meter registration claim with iot device", "err", pubErr)
		return
	}
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
	if strings.TrimSpace(registrationID) == "" {
		return nil, domain.NewValidationError("registration_id", "is required")
	}

	record, err := s.store.GetByRegistrationID(ctx, registrationID)
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return nil, fmt.Errorf("get registration for reverification: %w", err)
	}
	if record == nil {
		if strings.TrimSpace(meterID) == "" {
			return nil, domain.NewValidationError("meter_id", "is required for a new registration")
		}
		if strings.TrimSpace(userReference) == "" {
			return nil, domain.NewValidationError("user_reference", "is required for a new registration")
		}

		if err := s.store.SaveMeterRegistration(ctx, RegistrationRecord{
			UserID:         userReference,
			MeterID:        meterID,
			RegistrationID: registrationID,
			Role:           "PRIMARY_OWNER",
			ApprovalStatus: "DEWA_APPROVAL_PENDING",
		}); err != nil {
			return nil, fmt.Errorf("save registration for reverification: %w", err)
		}

		return s.createLinkRequest(ctx, LinkRequest{
			RegistrationID: registrationID,
			MeterID:        meterID,
			UserReference:  userReference,
		})
	}

	if strings.TrimSpace(meterID) != "" && meterID != record.MeterID {
		return nil, domain.NewValidationError("meter_id", "does not match the registration")
	}
	if strings.TrimSpace(userReference) != "" && userReference != record.UserID {
		return nil, domain.NewValidationError("user_reference", "does not match the registration")
	}

	if strings.TrimSpace(record.RequestID) != "" {
		status := strings.ToUpper(strings.TrimSpace(record.ApprovalStatus))
		if status != "APPROVED" {
			if err := s.store.UpdateMeterRegistrationStatus(ctx, registrationID, "DEWA_APPROVAL_PENDING"); err != nil {
				return nil, fmt.Errorf("reset registration status for reverification: %w", err)
			}
			status = "DEWA_APPROVAL_PENDING"
		}

		s.startPolling(record.RequestID, registrationID)
		return &LinkResponse{
			RequestID: record.RequestID,
			Status:    status,
			Message:   "Registration status reverification started",
		}, nil
	}

	if strings.TrimSpace(meterID) == "" {
		meterID = record.MeterID
	}
	if strings.TrimSpace(userReference) == "" {
		userReference = record.UserID
	}
	if err := s.store.UpdateMeterRegistrationStatus(ctx, registrationID, "DEWA_APPROVAL_PENDING"); err != nil {
		return nil, fmt.Errorf("reset registration status for reverification: %w", err)
	}

	return s.createLinkRequest(ctx, LinkRequest{
		RegistrationID: registrationID,
		MeterID:        meterID,
		UserReference:  userReference,
	})
}

// GetRegistration retrieves a registration record by its ID.
func (s *Service) GetRegistration(ctx context.Context, registrationID string) (*RegistrationRecord, error) {
	if s.store == nil {
		return nil, fmt.Errorf("store not configured")
	}

	return s.store.GetByRegistrationID(ctx, registrationID)
}

// GetByMeterID retrieves a registration record by its meter ID.
func (s *Service) GetByMeterID(ctx context.Context, meterID string) (*RegistrationRecord, error) {
	if s.store == nil {
		return nil, fmt.Errorf("store not configured")
	}

	return s.store.GetRegistrationByMeterID(ctx, meterID)
}

func (s *Service) GetRegistrationByUser(ctx context.Context, userReference string) (*RegistrationRecord, error) {
	if s.store == nil {
		return nil, fmt.Errorf("store not configured")
	}

	dbReg, err := s.store.GetRegistrationByUserID(ctx, userReference)
	if err != nil {
		return nil, err
	}
	if dbReg == nil {
		return nil, fmt.Errorf("registration for user %q: %w", userReference, domain.ErrNotFound)
	}

	return &RegistrationRecord{
		ID:             dbReg.ID,
		UserID:         dbReg.UserID,
		MeterID:        dbReg.MeterID,
		RegistrationID: dbReg.RegistrationID,
		RequestID:      dbReg.RequestID,
		Role:           dbReg.Role,
		ApprovalStatus: dbReg.ApprovalStatus,
	}, nil
}
