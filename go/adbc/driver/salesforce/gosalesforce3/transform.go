package gosalesforce3

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
)

func (c *Client) transformURL(nameOrID ...string) string {
	base := c.ssotBaseURL() + "/data-transforms"
	if len(nameOrID) > 0 && nameOrID[0] != "" {
		return base + "/" + nameOrID[0]
	}
	return base
}

func (c *Client) CreateDataTransform(ctx context.Context, req *types.CreateDataTransformRequest) (*types.DataTransform, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}
	var result types.DataTransform
	resp, err := c.http.R().SetContext(ctx).SetBody(req).SetResult(&result).Post(c.transformURL())
	if err != nil {
		return nil, fmt.Errorf("create data transform request failed: %w", err)
	}
	if resp.IsError() {
		return nil, c.checkError(resp)
	}
	return &result, nil
}

func (c *Client) GetDataTransform(ctx context.Context, nameOrID string) (*types.DataTransform, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}
	var result types.DataTransform
	resp, err := c.http.R().SetContext(ctx).SetResult(&result).Get(c.transformURL(nameOrID))
	if err != nil {
		return nil, fmt.Errorf("get data transform request failed: %w", err)
	}
	if resp.IsError() {
		return nil, c.checkError(resp)
	}
	return &result, nil
}

func (c *Client) UpdateDataTransform(ctx context.Context, req *types.CreateDataTransformRequest) (*types.DataTransform, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}
	var result types.DataTransform
	resp, err := c.http.R().SetContext(ctx).SetBody(req).SetResult(&result).Put(c.transformURL(req.Name))
	if err != nil {
		return nil, fmt.Errorf("update data transform request failed: %w", err)
	}
	if resp.IsError() {
		return nil, c.checkError(resp)
	}
	return &result, nil
}

func (c *Client) DeleteDataTransform(ctx context.Context, nameOrID string) error {
	if err := c.ensureAuth(); err != nil {
		return err
	}
	resp, err := c.http.R().SetContext(ctx).Delete(c.transformURL(nameOrID))
	if err != nil {
		return fmt.Errorf("delete data transform request failed: %w", err)
	}
	if resp.IsError() {
		return c.checkError(resp)
	}
	return nil
}

func (c *Client) ValidateDataTransform(ctx context.Context, req *types.CreateDataTransformRequest) (*types.DataTransformValidation, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}
	var result types.DataTransformValidation
	resp, err := c.http.R().SetContext(ctx).SetBody(req).SetResult(&result).Post(c.ssotBaseURL() + "/data-transforms-validation")
	if err != nil {
		return nil, fmt.Errorf("validate data transform request failed: %w", err)
	}
	if resp.IsError() {
		return nil, c.checkError(resp)
	}
	return &result, nil
}

func (c *Client) RunDataTransform(ctx context.Context, nameOrID string) (*types.DataCloudActionResponse, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}
	var result types.DataCloudActionResponse
	resp, err := c.http.R().SetContext(ctx).SetResult(&result).Post(c.transformURL(nameOrID) + "/actions/run")
	if err != nil {
		return nil, fmt.Errorf("run data transform request failed: %w", err)
	}
	if resp.IsError() {
		return nil, c.checkError(resp)
	}
	return &result, nil
}

func (c *Client) CancelDataTransform(ctx context.Context, nameOrID string) (*types.DataCloudActionResponse, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}
	var result types.DataCloudActionResponse
	resp, err := c.http.R().SetContext(ctx).SetResult(&result).Post(c.transformURL(nameOrID) + "/actions/cancel")
	if err != nil {
		return nil, fmt.Errorf("cancel data transform request failed: %w", err)
	}
	if resp.IsError() {
		return nil, c.checkError(resp)
	}
	return &result, nil
}

func (c *Client) RefreshDataTransformStatus(ctx context.Context, nameOrID string) (*types.DataCloudActionResponse, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}
	var result types.DataCloudActionResponse
	resp, err := c.http.R().SetContext(ctx).SetResult(&result).Post(c.transformURL(nameOrID) + "/actions/refresh-status")
	if err != nil {
		return nil, fmt.Errorf("refresh data transform status request failed: %w", err)
	}
	if resp.IsError() {
		return nil, c.checkError(resp)
	}
	return &result, nil
}
