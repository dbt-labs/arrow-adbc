package gosalesforce3

import (
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
)

// ExecuteSqlQuery creates and executes a SQL query, returning results inline.
// This is the "Create SQL Query" endpoint in the docs. For large result sets,
// use GetQuery to poll status and GetQueryRows to fetch paginated results.
func (c *Client) ExecuteSqlQuery(ctx context.Context, req *types.SqlQueryRequest) (*types.SqlQueryResponse, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}

	r := c.ssotRequest(ctx).SetBody(req)
	if req.Dataspace != "" {
		r.SetQueryParam("dataspace", req.Dataspace)
	}
	if req.WorkloadName != "" {
		r.SetQueryParam("workloadName", req.WorkloadName)
	}

	var result types.SqlQueryResponse
	resp, err := r.SetResult(&result).Post(c.ssotURL("/query-sql"))
	if err != nil {
		return nil, fmt.Errorf("sql query request failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}

	return &result, nil
}

// GetQuery retrieves the status of a previously created SQL query.
// Use waitTimeMs to long-poll (max 10000ms); 0 returns immediately.
func (c *Client) GetQuery(ctx context.Context, queryID string, waitTimeMs int) (*types.SqlQueryStatus, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}

	r := c.ssotRequest(ctx)
	if waitTimeMs > 0 {
		r.SetQueryParam("waitTimeMs", strconv.Itoa(waitTimeMs))
	}

	var result types.SqlQueryStatus
	resp, err := r.SetResult(&result).Get(c.ssotURL("/query-sql/" + queryID))
	if err != nil {
		return nil, fmt.Errorf("get query status failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}

	return &result, nil
}

// GetQueryRows retrieves rows from a completed SQL query.
// Use offset and rowLimit to paginate through large result sets.
func (c *Client) GetQueryRows(ctx context.Context, queryID string, offset int64, rowLimit int64) (*types.SqlQueryRowsResponse, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}

	r := c.ssotRequest(ctx).
		SetQueryParam("offset", strconv.FormatInt(offset, 10))
	if rowLimit > 0 {
		r.SetQueryParam("rowLimit", strconv.FormatInt(rowLimit, 10))
	}

	var result types.SqlQueryRowsResponse
	resp, err := r.SetResult(&result).Get(c.ssotURL("/query-sql/" + queryID + "/rows"))
	if err != nil {
		return nil, fmt.Errorf("get query rows failed: %w", err)
	}
	if resp.IsError() {
		return nil, checkError(resp)
	}

	return &result, nil
}

// CancelQuery cancels a running SQL query.
func (c *Client) CancelQuery(ctx context.Context, queryID string) error {
	if err := c.ensureAuth(); err != nil {
		return err
	}

	resp, err := c.ssotRequest(ctx).Delete(c.ssotURL("/query-sql/" + queryID))
	if err != nil {
		return fmt.Errorf("cancel query failed: %w", err)
	}
	if resp.IsError() {
		return checkError(resp)
	}

	return nil
}
