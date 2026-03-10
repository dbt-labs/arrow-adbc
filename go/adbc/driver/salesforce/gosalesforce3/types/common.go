package types

// DataCloudActionResponse is the standard response for action endpoints
type DataCloudActionResponse struct {
	Success bool     `json:"success"`
	Errors  []string `json:"errors,omitempty"`
}
