package gosalesforce3

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
	"golang.org/x/oauth2"
	"resty.dev/v3"
)

const defaultAPIVersion = "v64.0"

// normalizeAPIVersion accepts versions in the form "XX.Y", "vXX.Y", "vXX", or "XX"
// and normalizes to "vXX.Y" (appending ".0" if no minor version is present).
func normalizeAPIVersion(v string) string {
	v = strings.TrimPrefix(v, "v")
	if !strings.Contains(v, ".") {
		v += ".0"
	}
	return "v" + v
}

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
	} else {
		cfg.APIVersion = normalizeAPIVersion(cfg.APIVersion)
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
			// TODO: not sure what all the possible status codes are across these endpoints.
			// We can start with this, but should try to identify other conditions that would warrent a retry.
			//
			// The following docs may be helpful for this: https://developer.salesforce.com/docs/atlas.en-us.chatterapi.meta/chatterapi/connect_error_responses.htm
			//
			// There is also a special `Sforce-Limit-Info` header that indicates the current API usage/limit.
			// See: https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/headers_api_usage.htm
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

// ssotRequest returns a new resty request with the context pre-set.
// Use with relative paths: c.ssotRequest(ctx).Get(c.ssotURL("/metadata"))
func (c *Client) ssotRequest(ctx context.Context) *resty.Request {
	return c.http.R().SetContext(ctx)
}

// ssotURL builds a full SSOT endpoint URL from a relative path.
func (c *Client) ssotURL(path string) string {
	return c.ssotBaseURL() + path
}

