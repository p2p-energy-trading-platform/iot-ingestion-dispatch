
package registration

import(
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type LinkRequest struct {
	RegistrationID string `json:"registrationId"`
	MeterID        string `json:"meterId"`
	UserReference  string `json:"userReference"`
}

type LinkResponse struct {
	Status string `json:"status"`
	Message string `json:"message"`
}

type Client struct {
	baseURL string
	httpClient *http.Client
}


func NewClient(baseURL string) *Client {

	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
	
}


func(c *Client) CreateLinkRequest (
	ctx context.Context,
	request LinkRequest,
)(*LinkResponse, error){

	payload, err := json.Marshal(request)
	
	if err != nil {
		return nil, fmt.Errorf("marshal DEWA link request: %w", err)
	}

}

