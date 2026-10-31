package services

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/google/uuid"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/domain"
	registration "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/registration"
	grpctransport "github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/transport/grpc"

	iotv1 "github.com/p2p-energy-trading-platform/go-sdk/gen/gridx/iot/v1"
)

type RegistrationHandler struct {
	iotv1.UnimplementedDeviceRegistrationServiceServer
	registrationService *registration.Service
	logger              *slog.Logger
}

func NewRegistrationHandler(
	registrationService *registration.Service,
	logger *slog.Logger,
) *RegistrationHandler {
	return &RegistrationHandler{
		registrationService: registrationService,
		logger:              logger,
	}
}

func (h *RegistrationHandler) fail(ctx context.Context, method string, err error) error {
	if !errors.Is(err, domain.ErrValidation) && !errors.Is(err, domain.ErrNotFound) &&
		!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		h.logger.ErrorContext(ctx, "registration operation failed", "method", method, "error", err)
	}
	return grpctransport.ToGRPCError(err)
}

func (h *RegistrationHandler) InitiateMeterClaim(
	ctx context.Context,
	req *iotv1.InitiateMeterClaimRequest,
) (*iotv1.InitiateMeterClaimResponse, error) {
	registrationID := uuid.New().String()

	linkReq := registration.LinkRequest{
		RegistrationID: registrationID,
		MeterID:        req.GetMeterId(),
		UserReference:  req.GetUserReference(),
	}

	resp, err := h.registrationService.RegisterHouse(ctx, linkReq)
	if err != nil {
		return nil, h.fail(ctx, "InitiateMeterClaim", err)
	}

	return &iotv1.InitiateMeterClaimResponse{
		RegistrationId: registrationID,
		Status:         resp.Status,
		Message:        resp.Message,
	}, nil
}

func (h *RegistrationHandler) GetRegistrationStatus(
	ctx context.Context,
	req *iotv1.GetRegistrationStatusRequest,
) (*iotv1.GetRegistrationStatusResponse, error) {
	registrationID := req.GetRegistrationId()
	if registrationID == "" {
		return nil, h.fail(ctx, "GetRegistrationStatus", domain.NewValidationError("registration_id", "is required"))
	}

	record, err := h.registrationService.GetRegistration(ctx, registrationID)
	if err != nil {
		return nil, h.fail(ctx, "GetRegistrationStatus", err)
	}

	return &iotv1.GetRegistrationStatusResponse{
		RegistrationId: record.RegistrationID,
		MeterId:        record.MeterID,
		ApprovalStatus: record.ApprovalStatus,
	}, nil
}

func (h *RegistrationHandler) ValidateDeviceAssociation(
	ctx context.Context,
	req *iotv1.ValidateDeviceAssociationRequest,
) (*iotv1.ValidateDeviceAssociationResponse, error) {
	meterID := req.GetMeterId()
	if meterID == "" {
		return nil, h.fail(ctx, "ValidateDeviceAssociation", domain.NewValidationError("meter_id", "is required"))
	}

	record, err := h.registrationService.GetByMeterID(ctx, meterID)

	if err != nil {
		// If not found, it means the device is not registered yet
		return &iotv1.ValidateDeviceAssociationResponse{
			IsRegistered:  false,
			PrimaryUserId: "",
		}, nil
	}

	if strings.ToUpper(strings.TrimSpace(record.ApprovalStatus)) != "APPROVED" {
		return &iotv1.ValidateDeviceAssociationResponse{
			IsRegistered:  false,
			PrimaryUserId: "",
		}, nil
	}

	return &iotv1.ValidateDeviceAssociationResponse{
		IsRegistered:  true,
		PrimaryUserId: record.UserID,
	}, nil
}

func (h *RegistrationHandler) ReverifyRegistration(
	ctx context.Context,
	req *iotv1.ReverifyRegistrationRequest,
) (*iotv1.ReverifyRegistrationResponse, error) {
	registrationID := req.GetRegistrationId()
	userRef := req.GetUserReference()
	meterID := req.GetMeterId()

	if meterID == "" {
		record, err := h.registrationService.GetRegistration(ctx, registrationID)
		if err != nil {
			return nil, h.fail(ctx, "ReverifyRegistration", domain.NewValidationError("meter_id", "is required for reverification"))
		}
		meterID = record.MeterID
	}

	resp, err := h.registrationService.ReverifyRegistration(
		ctx,
		registrationID,
		userRef,
		meterID,
	)

	if err != nil {
		return nil, h.fail(ctx, "ReverifyRegistration", err)
	}

	return &iotv1.ReverifyRegistrationResponse{
		RegistrationId: registrationID,
		MeterId:        meterID,
		Status:         resp.Status,
		Message:        resp.Message,
	}, nil
}

func (h *RegistrationHandler) GetRegistrationByUser(
	ctx context.Context,
	req *iotv1.GetRegistrationByUserRequest,
) (*iotv1.GetRegistrationByUserResponse, error) {
	userRef := strings.TrimSpace(req.GetUserReference())
	if userRef == "" {
		return nil, h.fail(ctx, "GetRegistrationByUser", domain.NewValidationError("user_reference", "is required"))
	}

	record, err := h.registrationService.GetRegistrationByUser(ctx, userRef)
	if err != nil {
		return &iotv1.GetRegistrationByUserResponse{
			HasMeter:       false,
			RegistrationId: "",
			MeterId:        "",
			ApprovalStatus: "",
		}, nil
	}

	return &iotv1.GetRegistrationByUserResponse{
		HasMeter:       true,
		RegistrationId: record.RegistrationID,
		MeterId:        record.MeterID,
		ApprovalStatus: record.ApprovalStatus,
	}, nil
}
