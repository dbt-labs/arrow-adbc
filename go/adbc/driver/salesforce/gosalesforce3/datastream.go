package gosalesforce3

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
)

// TODO: We don't seem to be using DataStreams at all. Probably can consider as dead code and remove it.

// GetDataStream retrieves a Data Stream by name or ID.
func (c *Client) GetDataStream(ctx context.Context, nameOrID string) (*types.DataStream, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}

	var result types.DataStream
	resp, err := c.ssotRequest(ctx).SetResult(&result).Get(c.ssotURL("/data-streams/" + nameOrID))
	if err != nil {
		return nil, fmt.Errorf("get data stream request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}
	return &result, nil
}
