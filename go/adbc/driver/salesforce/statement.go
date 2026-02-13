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

package salesforce

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/api"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type statement struct {
	alloc memory.Allocator
	cnxn  *connectionImpl

	query string

	// Parameter binding
	paramBinding  *arrow.Record
	streamBinding array.RecordReader

	// Create DLO options
	dloCategory   string
	dloPrimaryKey string

	// Data Transform options
	targetDLO            string
	dataTransformTimeout time.Duration
}

// Close cleans up the statement
func (s *statement) Close() error {
	s.paramBinding = nil
	if s.streamBinding != nil {
		s.streamBinding.Release()
		s.streamBinding = nil
	}
	return nil
}

// SetSqlQuery sets the SQL query to be executed
func (s *statement) SetSqlQuery(query string) error {
	s.query = query
	return nil
}

// ExecuteQuery executes the current query and returns results
func (s *statement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.query == "" {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "no query set",
		}
	}
	return s.executeSQLQuery(ctx)
}

// executeSQLQuery executes a SQL query using the Salesforce Data Cloud APIs
func (s *statement) executeSQLQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.cnxn.client == nil || s.cnxn.client.GetDataCloudToken() == nil {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "connection not properly initialized",
		}
	}

	logger := s.cnxn.Logger.With("operation", "executeSQLQuery")

	// This is supposed to be equivalent to `CREATE OR REPLACE TABLE`
	if s.dloCategory != "" && s.dloPrimaryKey != "" && s.targetDLO != "" {
		logger := logger.With(slog.Group("opt", "dloCategory", s.dloCategory, "dloPrimaryKey", s.dloPrimaryKey, "targetDLO", s.targetDLO, "dataSpace", s.cnxn.dataSpace))

		if s.cnxn.dataSpace == "" {
			return nil, 0, adbc.Error{
				Code: adbc.StatusInvalidState,
				Msg:  "data space must be set for the DLO to be created",
			}
		}

		logger.InfoContext(ctx, "Writing sql to DT...")
		// Creates a data transform
		req := api.NewBatchDataTransformRequest(
			s.targetDLO,
			s.targetDLO,
			map[string]api.DbtDataTransformNode{
				"node": api.NewDbtDataTransformNode(
					"node",
					s.targetDLO,
					s.query,
					"TABLE",
					"OVERWRITE", // s.writeMode
					nil,
				),
			},
		)

		logger.InfoContext(ctx, "Validating batch DT", "req", req)

		valid, err := s.cnxn.client.ValidateDataTransform(ctx, req)
		if err != nil {
			return nil, 0, s.cnxn.ErrorHelper.Errorf(adbc.StatusInternal, "failed to validate the data transform for create/update: %v", err)
		}
		logger.DebugContext(ctx, "Validated", "issues", valid.Issues, "odo", valid.OutputDataObjects)

		odo := slices.Clone(valid.OutputDataObjects[req.Name])

		for i := range odo[0].Fields {
			f := &odo[0].Fields[i]
			f.IsPrimaryKey = f.Name == s.dloPrimaryKey
			f.Label = cmp.Or(f.Label, f.Name) // default label
		}

		odo[0].Category = "Profile"                      // TODO
		odo[0].Label = cmp.Or(odo[0].Label, odo[0].Name) // default label
		req.Definition.OutputDataObjects = odo

		logger.InfoContext(ctx, "Creating batch DT", "req", req)
		dt, err := s.cnxn.client.CreateOrUpdateDataTransform(ctx, req)
		if err != nil {
			return nil, 0, s.cnxn.ErrorHelper.Errorf(adbc.StatusInternal, "failed to create/update the data transform: %v", err)
		}

		logger.InfoContext(ctx, "Created batch DT. Waiting...", "dt", dt)

		dt, err = s.cnxn.client.WaitForDataTransform(ctx, dt)
		if err != nil {
			return nil, 0, s.cnxn.ErrorHelper.Errorf(adbc.StatusInternal, "failed while wating for data transform: %v", err)
		}
		if !dt.Status.IsActive() {
			return nil, 0, s.cnxn.ErrorHelper.Errorf(adbc.StatusInternal, "data transform is not active, current status: %v", dt.Status)
		}

		logger.InfoContext(ctx, "Running batch DT.", "dt", dt)

		err = s.cnxn.client.MustRunDataTransform(ctx, dt.Name)
		if err != nil {
			return nil, 0, s.cnxn.ErrorHelper.Errorf(adbc.StatusInternal, "failed to run data transform: %v", err)
		}

		logger.InfoContext(ctx, "Run started. Waiting...", "dt", dt)

		dt, err = s.cnxn.client.WaitForDataTransformRun(ctx, dt, s.dataTransformTimeout)
		if err != nil {
			return nil, 0, s.cnxn.ErrorHelper.Errorf(adbc.StatusInternal, "failed while wating for data transform run: %v", err)
		}
		if !dt.LastRunStatus.IsSuccess() {
			return nil, 0, s.cnxn.ErrorHelper.Errorf(adbc.StatusInternal, "data transform run was unsuccessful: last run status: %v", dt.LastRunStatus)
		}

		logger.InfoContext(ctx, "Run complete.")

		// Returns empty
		emptySchema := arrow.NewSchema([]arrow.Field{}, nil) // TODO
		reader, err := array.NewRecordReader(emptySchema, []arrow.RecordBatch{})
		if err != nil {
			err = fmt.Errorf("failed to create empty record reader: %w", err)
			return nil, 0, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  err.Error(),
			}
		}
		return reader, 0, nil
	}

	rowLimit := s.cnxn.getQueryRowLimit()

	queryRequest := &api.SqlQueryRequest{
		SQL:      s.query,
		RowLimit: rowLimit,
	}

	response, err := api.ExecuteSqlQuery(ctx, s.cnxn.client, queryRequest)
	if err != nil {
		err = fmt.Errorf("SQL query execution failed: %w", err)
		return nil, 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  err.Error(),
		}
	}

	// Convert the response to Arrow format
	reader, rowCount, err := s.convertSqlQueryResponseToArrow(response)
	if err != nil {
		err = fmt.Errorf("failed to convert query response to Arrow: %w", err)
		return nil, 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  err.Error(),
		}
	}

	return reader, rowCount, nil
}

// convertSqlQueryResponseToArrow converts SQL Query API response to Arrow format
func (s *statement) convertSqlQueryResponseToArrow(response *api.SqlQueryResponse) (array.RecordReader, int64, error) {
	if len(response.Data) == 0 {
		// Return empty reader with schema if available
		schema := s.buildArrowSchema(response.Metadata)
		reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{})
		return reader, 0, err
	}

	schema := s.buildArrowSchema(response.Metadata)
	records, err := s.buildArrowRecords(schema, response.Data)
	if err != nil {
		return nil, 0, err
	}

	reader, err := array.NewRecordReader(schema, records)
	if err != nil {
		return nil, 0, err
	}

	return reader, int64(response.ReturnedRows), nil
}

// Bind operations
func (s *statement) Bind(ctx context.Context, values arrow.Record) error {
	s.paramBinding = &values
	return nil
}

func (s *statement) BindStream(ctx context.Context, stream array.RecordReader) error {
	s.streamBinding = stream
	return nil
}

// ExecuteUpdate executes a statement that doesn't return results (INSERT, UPDATE, DELETE)
func (s *statement) ExecuteUpdate(ctx context.Context) (int64, error) {
	return 0, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecuteUpdate not yet implemented for Salesforce",
	}
}

// Prepare is typically used for prepared statements
func (s *statement) Prepare(ctx context.Context) error {
	// Salesforce Data Cloud doesn't support traditional prepared statements
	// We can validate the query syntax here if needed
	return nil
}

// Additional required interface methods
func (s *statement) GetOption(key string) (string, error) {
	switch key {
	case OptionStringDLOCategory:
		return s.dloCategory, nil
	case OptionStringDLOPrimaryKey:
		return s.dloPrimaryKey, nil
	case OptionsStringTargetDLO:
		return s.targetDLO, nil
	}
	return "", adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

func (s *statement) GetOptionBytes(key string) ([]byte, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

func (s *statement) GetOptionDouble(key string) (float64, error) {
	return 0, adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

func (s *statement) GetOptionInt(key string) (int64, error) {
	switch key {
	case OptionIntDataTransformRunTimeout:
		return s.dataTransformTimeout.Milliseconds(), nil
	}
	return 0, adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  fmt.Sprintf("unknown int type statement option: %s", key),
	}
}

func (s *statement) SetOption(key, value string) error {
	switch key {
	case OptionStringDLOCategory:
		s.dloCategory = value
	case OptionStringDLOPrimaryKey:
		s.dloPrimaryKey = value
	case OptionsStringTargetDLO:
		s.targetDLO = value
	default:
		return adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("unknown statement string type option: %s", key),
		}
	}
	return nil
}

func (s *statement) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

func (s *statement) SetOptionDouble(key string, value float64) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

func (s *statement) SetOptionInt(key string, value int64) error {
	switch key {
	case OptionIntDataTransformRunTimeout:
		s.dataTransformTimeout = time.Duration(value) * time.Millisecond
	default:
		return adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("unknown int type statement option: %s", key),
		}
	}
	return nil
}

func (s *statement) SetSubstraitPlan(plan []byte) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Substrait plans not supported for Salesforce",
	}
}

func (s *statement) GetParameterSchema() (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "parameter schema not yet implemented",
	}
}

func (s *statement) Cancel(ctx context.Context) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "query cancellation not yet implemented for Salesforce",
	}
}

func (s *statement) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, 0, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "partitioned execution not supported for Salesforce",
	}
}
