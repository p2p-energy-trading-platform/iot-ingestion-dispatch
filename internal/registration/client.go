package registration

// ClaimPayload represents a smart meter registration request
// sent to the DEWA Mock API.
type ClaimPayload struct {
	RegistrationID string `json:"registrationId"`
	MeterID        string `json:"meterId"`
	UserReference  string `json:"userReference"`
}

// ClaimResponse represents the response received from DEWA.
type ClaimResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}