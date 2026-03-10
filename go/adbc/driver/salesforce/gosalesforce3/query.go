package gosalesforce3

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
)

func (c *Client) ExecuteSqlQuery(ctx context.Context, req *types.SqlQueryRequest) (*types.SqlQueryResponse, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}

	var result types.SqlQueryResponse

	resp, err := c.http.R().
		SetContext(ctx).
		SetBody(req).
		SetResult(&result).
		Post(c.ssotBaseURL() + "/query-sql")
	if err != nil {
		return nil, fmt.Errorf("sql query request failed: %w", err)
	}
	if resp.IsError() {
		return nil, c.checkError(resp)
	}

	return &result, nil
}
