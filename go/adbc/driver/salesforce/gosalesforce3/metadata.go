package gosalesforce3

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
)

func (c *Client) GetMetadata(ctx context.Context, req *types.MetadataRequest) (*types.MetadataResponse, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}

	r := c.ssotRequest(ctx)
	if req.Dataspace != "" {
		r.SetQueryParam("dataspace", req.Dataspace)
	}
	if req.EntityCategory != "" {
		r.SetQueryParam("entityCategory", req.EntityCategory)
	}
	if req.EntityName != "" {
		r.SetQueryParam("entityName", req.EntityName)
	}
	if req.EntityType != "" {
		r.SetQueryParam("entityType", req.EntityType)
	}

	var result types.MetadataResponse
	resp, err := r.SetResult(&result).Get(c.ssotURL("/metadata"))
	if err != nil {
		return nil, fmt.Errorf("metadata request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}

	return &result, nil
}
