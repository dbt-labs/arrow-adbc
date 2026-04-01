// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package athena

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	athenaSDK "github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

type statementImpl struct {
	conn  *connectionImpl
	query string
}

func (s *statementImpl) Close() error {
	if s.conn == nil {
		return adbc.Error{
			Msg:  "statement already closed",
			Code: adbc.StatusInvalidState,
		}
	}
	s.conn = nil
	return nil
}

func (s *statementImpl) SetOption(key, val string) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unsupported statement option: %s", key),
	}
}

func (s *statementImpl) SetSqlQuery(query string) error {
	s.query = query
	return nil
}

func (s *statementImpl) Prepare(_ context.Context) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Athena does not support prepared statements",
	}
}

// ExecuteQuery runs the SQL query on Athena, waits for completion, and returns
// an array.RecordReader over the results.
func (s *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.query == "" {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "no query set",
		}
	}

	execID, err := s.startQuery(ctx)
	if err != nil {
		return nil, -1, err
	}

	if err := s.waitForQuery(ctx, execID); err != nil {
		return nil, -1, err
	}

	rows, colInfo, err := s.fetchResults(ctx, execID)
	if err != nil {
		return nil, -1, err
	}

	rdr, err := newRecordReader(s.conn.Alloc, colInfo, rows)
	if err != nil {
		return nil, -1, err
	}

	return rdr, -1, nil
}

// ExecuteUpdate runs a DML statement on Athena and returns -1 (rows affected unknown).
func (s *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	if s.query == "" {
		return -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "no query set",
		}
	}

	execID, err := s.startQuery(ctx)
	if err != nil {
		return -1, err
	}

	if err := s.waitForQuery(ctx, execID); err != nil {
		return -1, err
	}

	return -1, nil
}

func (s *statementImpl) startQuery(ctx context.Context) (*string, error) {
	input := &athenaSDK.StartQueryExecutionInput{
		QueryString: &s.query,
		QueryExecutionContext: &types.QueryExecutionContext{
			Catalog:  nilIfEmpty(s.conn.db.catalog),
			Database: nilIfEmpty(s.conn.db.schema),
		},
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: &s.conn.db.s3StagingDir,
		},
	}
	if s.conn.db.workGroup != "" {
		input.WorkGroup = &s.conn.db.workGroup
	}

	out, err := s.conn.athenaClient.StartQueryExecution(ctx, input)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("StartQueryExecution failed: %v", err),
		}
	}
	return out.QueryExecutionId, nil
}

func (s *statementImpl) waitForQuery(ctx context.Context, execID *string) error {
	for {
		out, err := s.conn.athenaClient.GetQueryExecution(ctx, &athenaSDK.GetQueryExecutionInput{
			QueryExecutionId: execID,
		})
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("GetQueryExecution failed: %v", err),
			}
		}

		state := out.QueryExecution.Status.State
		switch state {
		case types.QueryExecutionStateSucceeded:
			return nil
		case types.QueryExecutionStateFailed:
			reason := ""
			if out.QueryExecution.Status.StateChangeReason != nil {
				reason = *out.QueryExecution.Status.StateChangeReason
			}
			return adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("query failed: %s", reason),
			}
		case types.QueryExecutionStateCancelled:
			return adbc.Error{
				Code: adbc.StatusCancelled,
				Msg:  "query was cancelled",
			}
		default:
			// QUEUED or RUNNING — poll again
			select {
			case <-ctx.Done():
				return adbc.Error{
					Code: adbc.StatusCancelled,
					Msg:  ctx.Err().Error(),
				}
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
}

func (s *statementImpl) fetchResults(ctx context.Context, execID *string) ([]types.Row, []types.ColumnInfo, error) {
	input := &athenaSDK.GetQueryResultsInput{
		QueryExecutionId: execID,
	}

	var rows []types.Row
	var colInfo []types.ColumnInfo
	firstPage := true

	paginator := athenaSDK.NewGetQueryResultsPaginator(s.conn.athenaClient, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, nil, adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("GetQueryResults failed: %v", err),
			}
		}

		if firstPage {
			if page.ResultSet != nil && page.ResultSet.ResultSetMetadata != nil {
				colInfo = page.ResultSet.ResultSetMetadata.ColumnInfo
			}
			// The first row of the first page is a header row — skip it.
			if page.ResultSet != nil && len(page.ResultSet.Rows) > 0 {
				rows = append(rows, page.ResultSet.Rows[1:]...)
			}
			firstPage = false
		} else {
			if page.ResultSet != nil {
				rows = append(rows, page.ResultSet.Rows...)
			}
		}
	}

	return rows, colInfo, nil
}

func (s *statementImpl) Bind(_ context.Context, _ arrow.RecordBatch) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Bind not implemented for Athena driver",
	}
}

func (s *statementImpl) BindStream(_ context.Context, _ array.RecordReader) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "BindStream not implemented for Athena driver",
	}
}

func (s *statementImpl) GetParameterSchema() (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "parameter schema detection not implemented",
	}
}

func (s *statementImpl) SetSubstraitPlan(_ []byte) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Substrait plans not supported",
	}
}

func (s *statementImpl) ExecutePartitions(_ context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "partitioned result sets not supported",
	}
}

// nilIfEmpty returns nil if s is empty, otherwise a pointer to s.
func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
