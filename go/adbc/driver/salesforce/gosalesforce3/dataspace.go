package gosalesforce3

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
)

// UpsertDataSpaceMembers adds or updates members in a data space.
func (c *Client) UpsertDataSpaceMembers(ctx context.Context, dataSpace string, members []types.DataSpaceMember) (*types.DataCloudActionResponse, error) {
	if err := c.ensureAuth(); err != nil {
		return nil, err
	}

	type requestBody struct {
		Members struct {
			Members []types.DataSpaceMember `json:"members"`
		} `json:"members"`
	}

	type responseBody struct {
		*types.DataCloudActionResponse
		Members struct {
			Members []types.DataSpaceMember `json:"members"`
		} `json:"dataSpaceMembers"`
	}

	var reqBody requestBody
	reqBody.Members.Members = members

	var result responseBody
	resp, err := c.http.R().
		SetContext(ctx).
		SetBody(&reqBody).
		SetResult(&result).
		Put(fmt.Sprintf("%s/data-spaces/%s/members", c.ssotBaseURL(), dataSpace))
	if err != nil {
		return nil, fmt.Errorf("upsert data space members request failed: %w", err)
	}
	if resp.IsError() {
		return nil, c.checkError(resp)
	}

	return result.DataCloudActionResponse, nil
}
