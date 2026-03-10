package gosalesforce3

import "fmt"

// SalesforceError represents an error response from the Salesforce API.
type SalesforceError struct {
	StatusCode int
	Code       string
	Message    string
	Type       string
}

func (e *SalesforceError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("salesforce %d %s: %s", e.StatusCode, e.Code, e.Message)
	}
	return fmt.Sprintf("salesforce %d: %s", e.StatusCode, e.Message)
}

func (e *SalesforceError) IsNotFound() bool    { return e.StatusCode == 404 }
func (e *SalesforceError) IsRateLimited() bool { return e.StatusCode == 429 }
