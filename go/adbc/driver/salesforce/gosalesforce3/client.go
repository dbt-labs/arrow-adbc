package gosalesforce3

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
	"golang.org/x/oauth2"
	"resty.dev/v3"
)

const defaultAPIVersion = "v64.0"

type Client struct {
	config      *types.AuthConfig
	http        *resty.Client
	logger      *slog.Logger
	instanceURL string
	tokenSource oauth2.TokenSource
}

type Option func(*Client)

func WithLogger(l *slog.Logger) Option {
	return func(c *Client) { c.logger = l }
}

func WithHTTPClient(r *resty.Client) Option {
	return func(c *Client) { c.http = r }
}

func NewClient(cfg *types.AuthConfig, opts ...Option) (*Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("auth config is required")
	}
	if cfg.APIVersion == "" {
		cfg.APIVersion = defaultAPIVersion
	}

	c := &Client{
		config: cfg,
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(c)
	}

	if c.http == nil {
		c.http = resty.New()
		c.http.SetHeader("Content-Type", "application/json")
		c.http.SetRetryCount(3)
		c.http.AddRetryConditions(func(resp *resty.Response, err error) bool {
			if err != nil {
				return true
			}
			return resp.StatusCode() == 429 || resp.StatusCode() >= 500
		})
	}

	return c, nil
}

func (c *Client) Close() {
	if c.http != nil {
		c.http.Close()
	}
}

func (c *Client) ssotBaseURL() string {
	return fmt.Sprintf("%s/services/data/%s/ssot", c.instanceURL, c.config.APIVersion)
}

func (c *Client) checkError(resp *resty.Response) error {
	if resp.IsSuccess() {
		return nil
	}

	sfErr := &SalesforceError{
		StatusCode: resp.StatusCode(),
		Message:    resp.Status(),
	}

	body := resp.Bytes()
	if len(body) == 0 {
		return sfErr
	}

	// Try as single object
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err == nil {
		if msg, ok := obj["message"].(string); ok {
			sfErr.Message = msg
		}
		if code, ok := obj["errorCode"].(string); ok {
			sfErr.Code = code
		}
		if errType, ok := obj["type"].(string); ok {
			sfErr.Type = errType
		}
		return sfErr
	}

	// Try as array
	var arr []map[string]any
	if err := json.Unmarshal(body, &arr); err == nil && len(arr) > 0 {
		if msg, ok := arr[0]["message"].(string); ok {
			sfErr.Message = msg
		}
		if code, ok := arr[0]["errorCode"].(string); ok {
			sfErr.Code = code
		}
	}

	return sfErr
}
