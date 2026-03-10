package gosalesforce3

import (
	"context"
	"fmt"
	"net/url"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
)

func (c *Client) GetMetadata(ctx context.Context, req *types.MetadataRequest) (*types.MetadataResponse, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}

	params := url.Values{}
	if req.Dataspace != "" {
		params.Set("dataspace", req.Dataspace)
	}
	if req.EntityCategory != "" {
		params.Set("entityCategory", req.EntityCategory)
	}
	if req.EntityName != "" {
		params.Set("entityName", req.EntityName)
	}
	if req.EntityType != "" {
		params.Set("entityType", req.EntityType)
	}

	endpoint := c.ssotBaseURL() + "/metadata"
	if encoded := params.Encode(); encoded != "" {
		endpoint += "?" + encoded
	}

	var result types.MetadataResponse

	resp, err := c.http.R().
		SetContext(ctx).
		SetResult(&result).
		Get(endpoint)
	if err != nil {
		return nil, fmt.Errorf("metadata request failed: %w", err)
	}
	if resp.IsError() {
		return nil, c.checkError(resp)
	}

	return &result, nil
}
