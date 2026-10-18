
package registration

// ClaimPayload represents the request sent to the DEWA Mock API.
type ClaimPayload struct {
	RegistrationID string `json:"registrationId"`
	MeterID        string `json:"meterId"`
	UserReference  string `json:"userReference"`
}

// ClaimResponse represents the response received from the DEWA Mock API.
type ClaimResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}
