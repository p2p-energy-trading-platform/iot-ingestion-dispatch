
package registration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/p2p-energy-trading-platform/iot-ingestion-dispatch/internal/store/postgres"
)

type testRegistrationRepository struct{}

func (r *testRegistrationRepository) SaveMeterRegistration(
	ctx context.Context,
	registration postgres.MeterRegistration,
) error {
	return nil
}

type capturingRegistrationRepository struct {
	savedRegistration postgres.MeterRegistration
	called            bool
}

func (r *capturingRegistrationRepository) SaveMeterRegistration(
	ctx context.Context,
	registration postgres.MeterRegistration,
) error {
	r.savedRegistration = registration
	r.called = true
	return nil
}

type testDEWAClient struct{}

func (c *testDEWAClient) CreateLinkRequest(
	ctx context.Context,
	payload ClaimPayload,
) (*ClaimResponse, error) {
	return &ClaimResponse{
		Status:  "PENDING",
		Message: "Link request created",
	}, nil
}

type failingDEWAClient struct{}

func (c *failingDEWAClient) CreateLinkRequest(
	ctx context.Context,
	payload ClaimPayload,
) (*ClaimResponse, error) {
	return nil, fmt.Errorf("DEWA service unavailable")
}

func TestInitiateMeterClaimMissingMeterID(t *testing.T) {
	service := NewService(
		&testRegistrationRepository{},
		&testDEWAClient{},
	)

	_, err := service.InitiateMeterClaim(
		context.Background(),
		InitiateClaimRequest{
			UserReference: "USER-123",
		},
	)

	if err == nil {
		t.Fatal("expected error when meter ID is missing")
	}

	if !strings.Contains(err.Error(), "meter ID is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInitiateMeterClaimMissingUserReference(t *testing.T) {
	service := NewService(
		&testRegistrationRepository{},
		&testDEWAClient{},
	)

	_, err := service.InitiateMeterClaim(
		context.Background(),
		InitiateClaimRequest{
			MeterID: "METER-001",
		},
	)

	if err == nil {
		t.Fatal("expected error when user reference is missing")
	}

	if !strings.Contains(err.Error(), "user reference is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInitiateMeterClaimGeneratesRegistrationID(t *testing.T) {
	service := NewService(
		&testRegistrationRepository{},
		&testDEWAClient{},
	)

	result, err := service.InitiateMeterClaim(
		context.Background(),
		InitiateClaimRequest{
			MeterID:       "METER-001",
			UserReference: "USER-123",
		},
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if result.RegistrationID == "" {
		t.Fatal("expected a generated registration ID")
	}

	if result.Status != "PENDING" {
		t.Errorf("expected status PENDING, got %q", result.Status)
	}
}

func TestInitiateMeterClaimUsesProvidedRegistrationID(t *testing.T) {
	service := NewService(
		&testRegistrationRepository{},
		&testDEWAClient{},
	)

	const expectedID = "REG-123"

	result, err := service.InitiateMeterClaim(
		context.Background(),
		InitiateClaimRequest{
			RegistrationID: expectedID,
			MeterID:        "METER-001",
			UserReference:  "USER-123",
		},
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if result.RegistrationID != expectedID {
		t.Errorf(
			"expected registration ID %q, got %q",
			expectedID,
			result.RegistrationID,
		)
	}
}

func TestInitiateMeterClaimPersistsPendingStatus(t *testing.T) {
	repository := &capturingRegistrationRepository{}
	service := NewService(repository, &testDEWAClient{})

	_, err := service.InitiateMeterClaim(
		context.Background(),
		InitiateClaimRequest{
			RegistrationID: "REG-123",
			MeterID:        "METER-001",
			UserReference:  "USER-123",
		},
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !repository.called {
		t.Fatal("expected registration to be persisted")
	}

	if repository.savedRegistration.ApprovalStatus != ApprovalPending {
		t.Errorf(
			"expected approval status %q, got %q",
			ApprovalPending,
			repository.savedRegistration.ApprovalStatus,
		)
	}

	if repository.savedRegistration.RegistrationID != "REG-123" {
		t.Errorf(
			"expected registration ID REG-123, got %q",
			repository.savedRegistration.RegistrationID,
		)
	}

	if repository.savedRegistration.MeterID != "METER-001" {
		t.Errorf(
			"expected meter ID METER-001, got %q",
			repository.savedRegistration.MeterID,
		)
	}

	if repository.savedRegistration.UserReference != "USER-123" {
		t.Errorf(
			"expected user reference USER-123, got %q",
			repository.savedRegistration.UserReference,
		)
	}
}

func TestInitiateMeterClaimDEWAFailureAfterPersistence(t *testing.T) {
	repository := &capturingRegistrationRepository{}

	service := NewService(
		repository,
		&failingDEWAClient{},
	)

	_, err := service.InitiateMeterClaim(
		context.Background(),
		InitiateClaimRequest{
			RegistrationID: "REG-123",
			MeterID:        "METER-001",
			UserReference:  "USER-123",
		},
	)

	if err == nil {
		t.Fatal("expected error when DEWA dispatch fails")
	}

	if !strings.Contains(err.Error(), "dispatch registration to DEWA") {
		t.Errorf("expected DEWA dispatch error, got: %v", err)
	}

	if !repository.called {
		t.Fatal("expected registration to be saved before DEWA dispatch")
	}

	if repository.savedRegistration.ApprovalStatus != ApprovalPending {
		t.Errorf(
			"expected saved status %q, got %q",
			ApprovalPending,
			repository.savedRegistration.ApprovalStatus,
		)
	}
}
