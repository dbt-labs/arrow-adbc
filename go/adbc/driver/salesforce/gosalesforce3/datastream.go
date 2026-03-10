package gosalesforce3

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
)

// GetDataStream retrieves a Data Stream by name or ID.
func (c *Client) GetDataStream(ctx context.Context, nameOrID string) (*types.DataStream, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}

	var result types.DataStream
	resp, err := c.http.R().
		SetContext(ctx).
		SetResult(&result).
		Get(fmt.Sprintf("%s/data-streams/%s", c.ssotBaseURL(), nameOrID))
	if err != nil {
		return nil, fmt.Errorf("get data stream request failed: %w", err)
	}
	if resp.IsError() {
		return nil, c.checkError(resp)
	}
	return &result, nil
}
