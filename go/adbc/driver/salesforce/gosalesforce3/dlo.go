package gosalesforce3

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
)

// CreateDataLakeObject creates a new Data Lake Object.
func (c *Client) CreateDataLakeObject(ctx context.Context, req *types.CreateDataLakeObjectRequest) (*types.DataLakeObject, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}

	var result types.DataLakeObject
	resp, err := c.http.R().
		SetContext(ctx).
		SetBody(req).
		SetResult(&result).
		Post(c.ssotBaseURL() + "/data-lake-objects")
	if err != nil {
		return nil, fmt.Errorf("create DLO request failed: %w", err)
	}
	if resp.IsError() {
		return nil, c.checkError(resp)
	}
	return &result, nil
}

// GetDataLakeObject retrieves a Data Lake Object by name or ID.
// The API returns a wrapper with a list; this returns the first match.
func (c *Client) GetDataLakeObject(ctx context.Context, nameOrID string) (*types.DataLakeObject, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}

	var result types.DataLakeObjects
	resp, err := c.http.R().
		SetContext(ctx).
		SetResult(&result).
		Get(fmt.Sprintf("%s/data-lake-objects/%s", c.ssotBaseURL(), nameOrID))
	if err != nil {
		return nil, fmt.Errorf("get DLO request failed: %w", err)
	}
	if resp.IsError() {
		return nil, c.checkError(resp)
	}
	if len(result.DataLakeObjects) == 0 {
		return nil, &SalesforceError{
			StatusCode: 404,
			Code:       "NOT_FOUND",
			Message:    fmt.Sprintf("no DLO found with name or ID %s", nameOrID),
		}
	}
	return &result.DataLakeObjects[0], nil
}

// DeleteDataLakeObject deletes a Data Lake Object by name or ID.
func (c *Client) DeleteDataLakeObject(ctx context.Context, nameOrID string) error {
	if err := c.ensureAuth(); err != nil {
		return err
	}

	resp, err := c.http.R().
		SetContext(ctx).
		Delete(fmt.Sprintf("%s/data-lake-objects/%s", c.ssotBaseURL(), nameOrID))
	if err != nil {
		return fmt.Errorf("delete DLO request failed: %w", err)
	}
	if resp.IsError() {
		return c.checkError(resp)
	}
	return nil
}
