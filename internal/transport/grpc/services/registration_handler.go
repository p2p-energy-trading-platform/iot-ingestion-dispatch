package services

import (
	"context"
	"errors"
	"strings"

	iotv1 "github.com/p2p-energy-trading-platform/go-sdk/gen/gridx/iot/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)


type RegistrationService interface {
	InitiateMeterClaim(
		ctx context.Context,
		userReference string,
		meterID string,
	) (*ClaimResult, error)

	GetRegistrationStatus(
		ctx context.Context,
		registrationID string,
	) (*RegistrationStatus, error)

	ValidateDeviceAssociation(
		ctx context.Context,
		meterID string,
	) (*DeviceAssociation, error)

	ReverifyRegistration(
		ctx context.Context,
		registrationID string,
		userReference string,
	) (*ReverificationResult, error)
}


type ClaimResult struct {
	RegistrationID string
	Status          string
}

type RegistrationStatus struct {
	RegistrationID string
	MeterID        string
	ApprovalStatus string
}

type DeviceAssociation struct {
	IsRegistered  bool
	PrimaryUserID string
}

type ReverificationResult struct {
	RegistrationID string
	MeterID        string
	PreviousStatus string
	CurrentStatus  string
	Success        bool
	Message        string
}

type RegistrationHandler struct {
	iotv1.UnimplementedDeviceRegistrationServiceServer

	registrationService RegistrationService
}

func NewRegistrationHandler(
	registrationService RegistrationService,
) *RegistrationHandler {
	return &RegistrationHandler{
		registrationService: registrationService,
	}
}


func (h *RegistrationHandler) InitiateMeterClaim(
	ctx context.Context,
	req *iotv1.ClaimMeterRequest,
) (*iotv1.ClaimMeterResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	userReference := strings.TrimSpace(req.GetUserReference())
	meterID := strings.TrimSpace(req.GetMeterId())

	if userReference == "" {
		return nil, status.Error(codes.InvalidArgument, "user reference is required")
	}

	if meterID == "" {
		return nil, status.Error(codes.InvalidArgument, "meter ID is required")
	}

	result, err := h.registrationService.InitiateMeterClaim(
		ctx,
		userReference,
		meterID,
	)
	if err != nil {
		return nil, mapRegistrationError(err)
	}

	return &iotv1.ClaimMeterResponse{
		RegistrationId: result.RegistrationID,
		Status:         result.Status,
	}, nil
}


func (h *RegistrationHandler) GetRegistrationStatus(
	ctx context.Context,
	req *iotv1.GetStatusRequest,
) (*iotv1.GetStatusResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	registrationID := strings.TrimSpace(req.GetRegistrationId())
	if registrationID == "" {
		return nil, status.Error(codes.InvalidArgument, "registration ID is required")
	}

	result, err := h.registrationService.GetRegistrationStatus(
		ctx,
		registrationID,
	)
	if err != nil {
		return nil, mapRegistrationError(err)
	}

	return &iotv1.GetStatusResponse{
		RegistrationId: result.RegistrationID,
		MeterId:        result.MeterID,
		ApprovalStatus: result.ApprovalStatus,
	}, nil
}

// ValidateDeviceAssociation checks whether a meter has an active,
// registered association.
func (h *RegistrationHandler) ValidateDeviceAssociation(
	ctx context.Context,
	req *iotv1.ValidateDeviceRequest,
) (*iotv1.ValidateDeviceResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	meterID := strings.TrimSpace(req.GetMeterId())
	if meterID == "" {
		return nil, status.Error(codes.InvalidArgument, "meter ID is required")
	}

	result, err := h.registrationService.ValidateDeviceAssociation(
		ctx,
		meterID,
	)
	if err != nil {
		return nil, mapRegistrationError(err)
	}

	return &iotv1.ValidateDeviceResponse{
		IsRegistered:  result.IsRegistered,
		PrimaryUserId: result.PrimaryUserID,
	}, nil
}


func (h *RegistrationHandler) ReverifyRegistration(
	ctx context.Context,
	req *iotv1.ReverifyRequest,
) (*iotv1.ReverifyResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	registrationID := strings.TrimSpace(req.GetRegistrationId())
	userReference := strings.TrimSpace(req.GetUserReference())

	if registrationID == "" {
		return nil, status.Error(codes.InvalidArgument, "registration ID is required")
	}

	if userReference == "" {
		return nil, status.Error(codes.InvalidArgument, "user reference is required")
	}

	result, err := h.registrationService.ReverifyRegistration(
		ctx,
		registrationID,
		userReference,
	)
	if err != nil {
		return nil, mapRegistrationError(err)
	}

	return &iotv1.ReverifyResponse{
		RegistrationId: result.RegistrationID,
		MeterId:        result.MeterID,
		PreviousStatus: result.PreviousStatus,
		CurrentStatus:  result.CurrentStatus,
		Success:        result.Success,
		Message:        result.Message,
	}, nil
}


func mapRegistrationError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "request canceled")
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "registration request timed out")
	}

	return status.Error(codes.Internal, "registration operation failed")
}