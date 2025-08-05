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
	"context"
	"fmt"
	"strconv"

	"github.com/apache/arrow-adbc/go/adbc"
	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/pkg"
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
}

// NewStatement creates a new statement implementation
func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	stmt := &statement{
		alloc: c.Alloc,
		cnxn:  c,
	}

	return stmt, nil
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
	if s.cnxn.client == nil || s.cnxn.token == nil {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "connection not properly initialized",
		}
	}

	// Try Query V2 API first (for Data Cloud)
	response, err := api.ExecuteQueryV2WithToken(ctx, s.cnxn.client, s.cnxn.token, s.query, false)
	if err != nil {
		// Fall back to original SQL Query API
		return s.executeFallbackSQLQuery(ctx)
	}

	// Convert the response to Arrow format
	reader, rowCount, err := s.convertQueryV2ResponseToArrow(response)
	if err != nil {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to convert query response to Arrow: %v", err),
		}
	}

	return reader, rowCount, nil
}

// executeFallbackSQLQuery uses the original SQL Query API as fallback
func (s *statement) executeFallbackSQLQuery(ctx context.Context) (array.RecordReader, int64, error) {
	rowLimit := s.cnxn.getQueryRowLimit()

	queryRequest := &api.SqlQueryRequest{
		SQL:      s.query,
		RowLimit: rowLimit,
	}

	response, err := api.ExecuteSqlQueryWithToken(ctx, s.cnxn.client, s.cnxn.token, queryRequest)
	if err != nil {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("SQL query execution failed: %v", err),
		}
	}

	// Convert the response to Arrow format
	reader, rowCount, err := s.convertSqlQueryResponseToArrow(response)
	if err != nil {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to convert query response to Arrow: %v", err),
		}
	}

	return reader, rowCount, nil
}

// convertQueryV2ResponseToArrow converts Query V2 API response to Arrow format
func (s *statement) convertQueryV2ResponseToArrow(response *api.QueryV2Response) (array.RecordReader, int64, error) {
	if len(response.Data) == 0 {
		// Return empty reader with schema if available
		schema := s.buildSchemaFromV2Metadata(response.Metadata)
		reader, err := array.NewRecordReader(schema, []arrow.Record{})
		return reader, 0, err
	}

	// Build Arrow schema from metadata
	schema := s.buildSchemaFromV2Metadata(response.Metadata)

	// Convert data to Arrow records
	records, err := s.convertDataToArrowRecords(schema, response.Data)
	if err != nil {
		return nil, 0, err
	}

	reader, err := array.NewRecordReader(schema, records)
	if err != nil {
		return nil, 0, err
	}

	return reader, int64(response.RowCount), nil
}

// convertSqlQueryResponseToArrow converts SQL Query API response to Arrow format
func (s *statement) convertSqlQueryResponseToArrow(response *api.SqlQueryResponse) (array.RecordReader, int64, error) {
	if len(response.Data) == 0 {
		// Return empty reader with schema if available
		schema := s.buildSchemaFromSqlMetadata(response.Metadata)
		reader, err := array.NewRecordReader(schema, []arrow.Record{})
		return reader, 0, err
	}

	// Build Arrow schema from metadata
	schema := s.buildSchemaFromSqlMetadata(response.Metadata)

	// Convert data to Arrow records
	records, err := s.convertDataToArrowRecords(schema, response.Data)
	if err != nil {
		return nil, 0, err
	}

	reader, err := array.NewRecordReader(schema, records)
	if err != nil {
		return nil, 0, err
	}

	return reader, int64(response.ReturnedRows), nil
}

// buildSchemaFromV2Metadata builds Arrow schema from Query V2 metadata
func (s *statement) buildSchemaFromV2Metadata(metadata map[string]api.QueryV2Metadata) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(metadata))

	// Create a slice to maintain order by PlaceInOrder
	type orderedColumn struct {
		name string
		meta api.QueryV2Metadata
	}
	ordered := make([]orderedColumn, 0, len(metadata))

	for name, meta := range metadata {
		ordered = append(ordered, orderedColumn{name: name, meta: meta})
	}

	// Sort by PlaceInOrder
	for i := 0; i < len(ordered); i++ {
		for j := i + 1; j < len(ordered); j++ {
			if ordered[i].meta.PlaceInOrder > ordered[j].meta.PlaceInOrder {
				ordered[i], ordered[j] = ordered[j], ordered[i]
			}
		}
	}

	for _, col := range ordered {
		arrowType := s.salesforceTypeToArrow(col.meta.Type)
		field := arrow.Field{
			Name:     col.name,
			Type:     arrowType,
			Nullable: true, // Salesforce fields are generally nullable
		}
		fields = append(fields, field)
	}

	return arrow.NewSchema(fields, nil)
}

// buildSchemaFromSqlMetadata builds Arrow schema from SQL Query API metadata
func (s *statement) buildSchemaFromSqlMetadata(metadata []api.SqlQueryMetadata) *arrow.Schema {
	fields := make([]arrow.Field, len(metadata))

	for i, col := range metadata {
		arrowType := s.salesforceTypeToArrow(col.Type)
		field := arrow.Field{
			Name:     col.Name,
			Type:     arrowType,
			Nullable: col.Nullable,
		}
		fields[i] = field
	}

	return arrow.NewSchema(fields, nil)
}

// salesforceTypeToArrow maps Salesforce data types to Arrow types
func (s *statement) salesforceTypeToArrow(sfType string) arrow.DataType {
	switch sfType {
	case "STRING", "TEXT", "VARCHAR":
		return arrow.BinaryTypes.String
	case "INTEGER", "INT":
		return arrow.PrimitiveTypes.Int64
	case "DECIMAL", "NUMERIC":
		return arrow.PrimitiveTypes.Float64
	case "BOOLEAN", "BOOL":
		return arrow.FixedWidthTypes.Boolean
	case "DATE":
		return arrow.FixedWidthTypes.Date32
	case "DATETIME", "TIMESTAMP":
		return arrow.FixedWidthTypes.Timestamp_us
	default:
		// Default to string for unknown types
		return arrow.BinaryTypes.String
	}
}

// convertDataToArrowRecords converts the raw data to Arrow records
func (s *statement) convertDataToArrowRecords(schema *arrow.Schema, data [][]interface{}) ([]arrow.Record, error) {
	if len(data) == 0 {
		return []arrow.Record{}, nil
	}

	// For now, create a simple single record
	// In a full implementation, you might want to batch this
	builders := make([]array.Builder, len(schema.Fields()))
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(s.alloc, field.Type)
	}
	defer func() {
		for _, builder := range builders {
			builder.Release()
		}
	}()

	// Add data to builders
	for _, row := range data {
		for i, value := range row {
			if i >= len(builders) {
				break // Skip extra columns
			}

			if value == nil {
				builders[i].AppendNull()
			} else {
				s.appendValueToBuilder(builders[i], value, schema.Field(i).Type)
			}
		}
	}

	// Build arrays
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
	}

	// Create record
	record := array.NewRecord(schema, arrays, int64(len(data)))

	// Release arrays
	for _, arr := range arrays {
		arr.Release()
	}

	return []arrow.Record{record}, nil
}

// appendValueToBuilder appends a value to the appropriate builder type
func (s *statement) appendValueToBuilder(builder array.Builder, value interface{}, dataType arrow.DataType) {
	switch b := builder.(type) {
	case *array.StringBuilder:
		if str, ok := value.(string); ok {
			b.Append(str)
		} else {
			b.Append(fmt.Sprintf("%v", value))
		}
	case *array.Int64Builder:
		if i, ok := value.(int64); ok {
			b.Append(i)
		} else if i, ok := value.(int); ok {
			b.Append(int64(i))
		} else if str, ok := value.(string); ok {
			if i, err := strconv.ParseInt(str, 10, 64); err == nil {
				b.Append(i)
			} else {
				b.AppendNull()
			}
		} else {
			b.AppendNull()
		}
	case *array.Float64Builder:
		if f, ok := value.(float64); ok {
			b.Append(f)
		} else if f, ok := value.(float32); ok {
			b.Append(float64(f))
		} else if str, ok := value.(string); ok {
			if f, err := strconv.ParseFloat(str, 64); err == nil {
				b.Append(f)
			} else {
				b.AppendNull()
			}
		} else {
			b.AppendNull()
		}
	case *array.BooleanBuilder:
		if b_val, ok := value.(bool); ok {
			b.Append(b_val)
		} else if str, ok := value.(string); ok {
			if b_val, err := strconv.ParseBool(str); err == nil {
				b.Append(b_val)
			} else {
				b.AppendNull()
			}
		} else {
			b.AppendNull()
		}
	default:
		// Fallback - treat as null for unsupported types
		builder.AppendNull()
	}
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
	return 0, adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

func (s *statement) SetOption(key, value string) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
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
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
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
