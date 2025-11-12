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

package sparklivy

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// statementImpl is the internal statement implementation
type statementImpl struct {
	cnxn       *connectionImpl
	livyClient *LivyClient
	sessionID  int
	alloc      memory.Allocator

	// Query state
	query      string
	bound      arrow.Record
	bindStream array.RecordReader
}

// Close closes the statement
func (s *statementImpl) Close() error {
	if s.bound != nil {
		s.bound.Release()
		s.bound = nil
	}
	if s.bindStream != nil {
		s.bindStream.Release()
		s.bindStream = nil
	}
	return nil
}

// GetOption retrieves a statement option
func (s *statementImpl) GetOption(key string) (string, error) {
	return "", adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

// GetOptionInt retrieves an integer statement option
func (s *statementImpl) GetOptionInt(key string) (int64, error) {
	return 0, adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

// GetOptionDouble retrieves a double statement option
func (s *statementImpl) GetOptionDouble(key string) (float64, error) {
	return 0, adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

// GetOptionBytes retrieves a bytes statement option
func (s *statementImpl) GetOptionBytes(key string) ([]byte, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

// SetOption sets a statement option
func (s *statementImpl) SetOption(key, value string) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

// SetOptionInt sets an integer statement option
func (s *statementImpl) SetOptionInt(key string, value int64) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

// SetOptionDouble sets a double statement option
func (s *statementImpl) SetOptionDouble(key string, value float64) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

// SetOptionBytes sets a bytes statement option
func (s *statementImpl) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

// SetSqlQuery sets the SQL query to execute
func (s *statementImpl) SetSqlQuery(query string) error {
	s.query = query
	return nil
}

// SetSubstraitPlan sets a Substrait plan (not supported)
func (s *statementImpl) SetSubstraitPlan(plan []byte) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Substrait plans not supported",
	}
}

// Bind binds parameters (not yet implemented)
func (s *statementImpl) Bind(ctx context.Context, values arrow.Record) error {
	s.bound = values
	values.Retain()
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "parameter binding not yet supported",
	}
}

// BindStream binds a stream of parameters (not yet implemented)
func (s *statementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	s.bindStream = stream
	stream.Retain()
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "parameter binding not yet supported",
	}
}

// GetParameterSchema gets the schema of parameters (not supported)
func (s *statementImpl) GetParameterSchema() (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "parameter schema not supported",
	}
}

// Prepare prepares the statement (no-op for now)
func (s *statementImpl) Prepare(ctx context.Context) error {
	// Livy doesn't have a separate prepare step
	// We could validate the SQL here, but for now just return success
	return nil
}

// ExecuteQuery executes a query and returns results
func (s *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.query == "" {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "no query set",
		}
	}

	// Check if we're using SQL session kind
	isSQL := s.cnxn.db.sessionKind == SessionKindSQL
	if !isSQL {
		return nil, -1, adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  "schema retrieval not supported for Spark/PySpark sessions",
		}
	}

	stmt, err := s.livyClient.CreateStatement(ctx, s.sessionID, CreateStatementRequest{Code: s.query})
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("failed to execute query: %v", err),
		}
	}

	// Wait for data statement to complete
	stmt, err = s.livyClient.WaitForStatementComplete(ctx, s.sessionID, stmt.ID, 10*time.Minute)
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("query execution failed: %v", err),
		}
	}

	// Check for errors
	if stmt.Output.Status == "error" {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidData,
			Msg:  fmt.Sprintf("query error: %s: %s", stmt.Output.Ename, stmt.Output.Evalue),
		}
	}

	// Step 2: Get schema
	var schema *arrow.Schema
	schema, err = s.parseSchemaFromSQLResult(stmt)
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to parse schema: %v", err),
		}
	}

	// Parse data
	var jsonRows []string

	// Debug: print all available output formats
	fmt.Printf("DEBUG: Available output formats: %v\n", mapKeys(stmt.Output.Data))

	// For SQL sessions, parse the table output
	jsonRows, err = s.parseDataFromSQLResult(stmt, schema)
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to parse SQL result data: %v", err),
		}
	}

	// Create a record reader from the JSON rows
	reader, err := newJSONRecordReader(s.alloc, schema, jsonRows)
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to create reader: %v", err),
		}
	}

	return reader, int64(len(jsonRows)), nil
}

// ExecuteUpdate executes a query that doesn't return results
func (s *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	if s.query == "" {
		return -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "no query set",
		}
	}

	// Check if we're using SQL session kind
	var code string
	if s.cnxn.db.sessionKind == SessionKindSQL {
		// For SQL sessions, send the query directly
		code = s.query
	} else {
		// For Spark/PySpark sessions, wrap in spark.sql()
		code = fmt.Sprintf("spark.sql(\"%s\")", escapeSQLForScala(s.query))
	}

	stmt, err := s.livyClient.CreateStatement(ctx, s.sessionID, CreateStatementRequest{Code: code})
	if err != nil {
		return -1, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("failed to execute update: %v", err),
		}
	}

	// Wait for completion
	stmt, err = s.livyClient.WaitForStatementComplete(ctx, s.sessionID, stmt.ID, 10*time.Minute)
	if err != nil {
		return -1, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("update execution failed: %v", err),
		}
	}

	// Check for errors
	if stmt.Output.Status == "error" {
		return -1, adbc.Error{
			Code: adbc.StatusInvalidData,
			Msg:  fmt.Sprintf("update error: %s: %s", stmt.Output.Ename, stmt.Output.Evalue),
		}
	}

	// For updates, we can't easily get affected row count from Livy
	// Return -1 to indicate unknown
	return -1, nil
}

// ExecutePartitions executes a query returning partitions (not supported)
func (s *statementImpl) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "partitioned execution not supported",
	}
}

// ExecuteSchema gets the schema of the result set without executing the query
func (s *statementImpl) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecuteSchema not yet implemented for Spark Livy driver",
	}
}

// mapKeys returns the keys of a map as a slice
func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// parseSchemaFromSQLResult extracts schema from SQL session result
func (s *statementImpl) parseSchemaFromSQLResult(stmt *Statement) (*arrow.Schema, error) {
	// SQL session results come in application/json with schema metadata
	if jsonData, ok := stmt.Output.Data["application/json"]; ok {
		dataMap, ok := jsonData.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("unexpected SQL result format")
		}

		// Check if schema is embedded in the response
		if schemaData, ok := dataMap["schema"]; ok {
			schemaBytes, err := json.Marshal(schemaData)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal schema: %w", err)
			}
			return parseSparkSchemaJSON(string(schemaBytes))
		}
	}
	return nil, fmt.Errorf("unable to extract schema from SQL result")
}

// parseDataFromSQLResult extracts data rows from SQL session result
func (s *statementImpl) parseDataFromSQLResult(stmt *Statement, schema *arrow.Schema) ([]string, error) {
	var jsonRows []string

	// SQL session results come in application/json format
	if jsonData, ok := stmt.Output.Data["application/json"]; ok {
		dataMap, ok := jsonData.(map[string]any)
		if ok {
			// If data is in structured format with "data" field
			if dataArray, ok := dataMap["data"].([]any); ok {
				for _, row := range dataArray {
					// Convert row to JSON object based on schema
					rowJSON, err := convertRowToJSON(row, schema)
					if err == nil {
						jsonRows = append(jsonRows, rowJSON)
					}
				}
				return jsonRows, nil
			}
		}
	}

	// Fallback: parse text/plain table output
	if textData, ok := stmt.Output.Data["text/plain"].(string); ok {
		return parseTableOutputToJSON(textData, schema)
	}

	return nil, fmt.Errorf("unable to extract data from SQL result")
}

// inferSchemaFromTableOutput infers Arrow schema from SQL table text output
func inferSchemaFromTableOutput(output string) (*arrow.Schema, error) {
	// Simple implementation: parse table headers
	// Table format is typically:
	// +------+------+
	// | col1 | col2 |
	// +------+------+
	// | val1 | val2 |
	// ...
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "|") && !strings.HasPrefix(line, "+") {
			// This is a data line, extract column names
			parts := strings.Split(line, "|")
			var fields []arrow.Field
			for _, part := range parts {
				colName := strings.TrimSpace(part)
				if colName != "" {
					// Default to string type for simplicity
					fields = append(fields, arrow.Field{Name: colName, Type: arrow.BinaryTypes.String})
				}
			}
			if len(fields) > 0 {
				return arrow.NewSchema(fields, nil), nil
			}
		}
	}
	return nil, fmt.Errorf("unable to infer schema from table output")
}

// parseTableOutputToJSON converts SQL table text output to JSON rows
func parseTableOutputToJSON(output string, schema *arrow.Schema) ([]string, error) {
	var jsonRows []string
	lines := strings.Split(output, "\n")

	// Skip header and separator lines
	dataStarted := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "+") {
			continue
		}

		if strings.HasPrefix(line, "|") {
			if !dataStarted {
				// Skip header row
				dataStarted = true
				continue
			}

			// Parse data row
			parts := strings.Split(line, "|")
			rowData := make(map[string]any)
			fieldIdx := 0
			for _, part := range parts {
				val := strings.TrimSpace(part)
				if val != "" && fieldIdx < schema.NumFields() {
					rowData[schema.Field(fieldIdx).Name] = val
					fieldIdx++
				}
			}

			if len(rowData) > 0 {
				jsonBytes, _ := json.Marshal(rowData)
				jsonRows = append(jsonRows, string(jsonBytes))
			}
		}
	}

	return jsonRows, nil
}

// convertRowToJSON converts a row from SQL result to JSON string
func convertRowToJSON(row any, schema *arrow.Schema) (string, error) {
	// If row is already a map, marshal it directly
	if rowMap, ok := row.(map[string]any); ok {
		jsonBytes, err := json.Marshal(rowMap)
		return string(jsonBytes), err
	}

	// If row is an array, map it to schema fields
	if rowArray, ok := row.([]any); ok {
		rowData := make(map[string]any)
		for i, val := range rowArray {
			if i < schema.NumFields() {
				rowData[schema.Field(i).Name] = val
			}
		}
		jsonBytes, err := json.Marshal(rowData)
		return string(jsonBytes), err
	}

	return "", fmt.Errorf("unsupported row format: %T", row)
}
