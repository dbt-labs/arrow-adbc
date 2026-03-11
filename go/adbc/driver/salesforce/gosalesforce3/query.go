package gosalesforce3

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce3/types"
)

// TODO: this is technically called "Create SQL Query" in the docs.
// Actually executing it to completion is a potentially multi-call process.
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

// TODO: We're missing the other endpoints:
// - Get Query: `GET /ssot/query-sql/{queryId}`
// - Get Query Rows: `GET /ssot/query-sql/{queryId}/rows`
// - Cancel Query: `DELETE /ssot/query-sql/{queryId}` (supposedly returns 200 with no body on success, but we should verify this with testing)
//
// This API is a bit weird since `Create SQL Query` returns a JSON response that is a combination of the `Get Query` and `Get Query Rows` responses, see below.
//
// type SqlQueryParams struct {
//   // used as URL query params for all query endpoints.
//   dataspace string // optional, uses "default" if not provided
//   workloadName string // optional, used to enrich log files for debugging
//
//   // this is only used for `Get Query` (maybe `Create Query` too, but it's not documented if it is)
//   waitTimeMs int // optional, max 10000 (10s), if not provided, returns immediately
//
//   // these are only used for `Get Query Rows`
//   rowLimit int // optional, must be greater than 0. Fewer rows may be returned, but will not exceed this limit
//   offset int // optional, row number to start when fetching next chunk. Must be less than total available rows.
// }
//
//
// type GetQueryResp struct {
//   completionStatus string // enum("Finished" - execution complete, results available in-mem and persisted | "ResultsProduced" - execution complete, results available in-mem (not yet persisted?) | "Running" - query executing | "Unspecified")
//   expirationTime string // appears to always be in the format "seconds: <SEC>\n" where the value is the unix epoch time at which the query results will expire (no idea what happens when it expires though, but we can assume it won't be available anymore)
//   progress float // between 0.0 and 1.0 (inclusive). 0 means not started, 1 means complete and ready to retrieve
//   queryId string // uid for the query, used to call other endpoints after Create (doubtful that this will change after creation, but the docs don't specify if it's immutable or not)
//   rowCount int // total number of available rows for extraction (theoretically, this could increase as progress is made, but unclear. I guess we could fetch results before the query is finished, as long as we keep the offset below the most recent rowCount)
// }
//
// type GetQueryRowsResp struct {
//   returnedRows int // number of rows in this response
//   metadata []struct{
//     type string // enum("ArrayOfX"|"BigInt"|"Bool"|"Char"|"Date"|"Double"|"Float"|"Integer"|"Numeric"|"Oid"|"SmallInt"|"Time"|"Timestamp"|"TimestampTZ"|"Varchar"|"Unspecified")
//     innerElement string? // used when `type` is "ArrayOfX", will be one of the other types (except "ArrayOfX"; nested arrays not allows) to specify the type of the array elements
//     name string // column name
//     nullable bool
//     precision int? // only for numeric types
//     scale int? // only for numeric types
//   }
//   data [][]any // Array of rows, where each row is an array of column values in the same order as `metadata`
//   }
// }
//
// type CreateSqlQueryResp struct {
//   GetQueryRowResp // embedded
//   Status GetQueryResp `json:"status"` // nested under the status key
// }
