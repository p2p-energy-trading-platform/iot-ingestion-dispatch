
func TestInitiateMeterClaimGeneratesRegistrationID(t *testing.T) {
	repository := &testRegistrationRepository{}
	client := &testDEWAClient{}
	service := NewService(repository, client)

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
