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
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// connectionImpl is the internal connection implementation
type connectionImpl struct {
	driverbase.ConnectionImplBase

	db         *databaseImpl
	livyClient *LivyClient
	sessionID  int
	catalog    string
	dbSchema   string
}

// connection wraps the internal implementation
type connection struct {
	driverbase.Connection
}

// openSession opens the connection by creating a Livy session
func (c *connectionImpl) openSession(ctx context.Context) error {
	// Build session configuration
	sessionConf := c.db.getSessionConfig()
	sessionOpts := c.db.getSessionOptions()

	// Create session request
	req := CreateSessionRequest{
		Kind: c.db.sessionKind,
		Conf: sessionConf,
	}

	// Add optional parameters
	if heartbeat, ok := sessionOpts["heartbeatTimeoutInSecond"].(int64); ok {
		req.HeartbeatTimeoutSec = int(heartbeat)
	}
	if ttl, ok := sessionOpts["ttl"].(string); ok {
		req.TTL = ttl
	}

	// Create session
	session, err := c.livyClient.CreateSession(ctx, req)
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("failed to create Livy session: %v", err),
		}
	}

	c.sessionID = session.ID

	// Wait for session to be ready (5 minute timeout)
	if err := c.livyClient.WaitForSessionReady(ctx, c.sessionID, 5*time.Minute); err != nil {
		// Try to clean up the session
		_ = c.livyClient.DeleteSession(ctx, c.sessionID)
		return adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("session failed to start: %v", err),
		}
	}

	return nil
}

// Close closes the connection by deleting the Livy session
func (c *connectionImpl) Close() error {
	if c.sessionID > 0 {
		ctx := context.Background()
		if err := c.livyClient.DeleteSession(ctx, c.sessionID); err != nil {
			return adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("failed to delete session: %v", err),
			}
		}
		c.sessionID = 0
	}
	return nil
}

// NewStatement creates a new statement
func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	stmt := &statementImpl{
		cnxn:       c,
		livyClient: c.livyClient,
		sessionID:  c.sessionID,
		alloc:      c.Alloc,
	}
	return stmt, nil
}

// Commit commits the current transaction (not supported)
func (c *connectionImpl) Commit(ctx context.Context) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "transactions not supported",
	}
}

// Rollback rolls back the current transaction (not supported)
func (c *connectionImpl) Rollback(ctx context.Context) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "transactions not supported",
	}
}

// GetInfo retrieves metadata about the driver/database
func (c *connectionImpl) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	// Delegate to base implementation which uses DriverInfo
	return c.ConnectionImplBase.GetInfo(ctx, infoCodes)
}

// GetObjects retrieves catalog/schema/table/column metadata
func (c *connectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	// This is a simplified implementation
	// A full implementation would execute Spark SQL commands to query the catalog

	// For now, return an error
	// TODO: Implement full metadata discovery using SHOW CATALOGS, SHOW DATABASES, SHOW TABLES, DESCRIBE TABLE

	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "GetObjects not yet implemented",
	}
}

// GetTableSchema retrieves the schema of a table
func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	// Build the fully qualified table name
	var fullTableName string
	if catalog != nil && *catalog != "" {
		fullTableName = *catalog + "."
	}
	if dbSchema != nil && *dbSchema != "" {
		fullTableName += *dbSchema + "."
	}
	fullTableName += tableName

	// Execute DESCRIBE TABLE to get schema
	code := fmt.Sprintf("spark.sql(\"DESCRIBE TABLE %s\").schema.json", fullTableName)

	stmt, err := c.livyClient.CreateStatement(ctx, c.sessionID, CreateStatementRequest{Code: code})
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("failed to execute DESCRIBE TABLE: %v", err),
		}
	}

	// Wait for statement to complete
	stmt, err = c.livyClient.WaitForStatementComplete(ctx, c.sessionID, stmt.ID, 2*time.Minute)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("failed to get table schema: %v", err),
		}
	}

	// Check for errors
	if stmt.Output.Status == "error" {
		return nil, adbc.Error{
			Code: adbc.StatusNotFound,
			Msg:  fmt.Sprintf("table not found or error: %s: %s", stmt.Output.Ename, stmt.Output.Evalue),
		}
	}

	// Parse the schema JSON from output
	// The output should be in stmt.Output.Data["text/plain"] or similar
	schemaJSON, ok := stmt.Output.Data["text/plain"].(string)
	if !ok {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  "unexpected schema output format",
		}
	}

	// Parse Spark schema JSON and convert to Arrow schema
	schema, err := parseSparkSchemaJSON(schemaJSON)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to parse schema: %v", err),
		}
	}

	return schema, nil
}

// GetTableTypes retrieves the list of table types
func (c *connectionImpl) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	// Common Spark table types
	// This would ideally be queried from the catalog
	tableTypes := []string{"TABLE", "VIEW", "EXTERNAL_TABLE", "MANAGED_TABLE", "TEMPORARY_VIEW"}

	// Build the result as an Arrow array
	bldr := array.NewRecordBuilder(c.Alloc, arrow.NewSchema(
		[]arrow.Field{
			{Name: "table_type", Type: arrow.BinaryTypes.String, Nullable: false},
		},
		nil,
	))
	defer bldr.Release()

	for _, tt := range tableTypes {
		bldr.Field(0).(*array.StringBuilder).Append(tt)
	}

	rec := bldr.NewRecord()
	defer rec.Release()

	// Create a reader from the single record
	reader, err := array.NewRecordReader(bldr.Schema(), []arrow.Record{rec})
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to create record reader: %v", err),
		}
	}

	return reader, nil
}

// SetAutocommit implements driverbase.AutocommitSetter
func (c *connectionImpl) SetAutocommit(enabled bool) error {
	if enabled {
		return nil
	}
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "transactions not supported",
	}
}

// GetCurrentCatalog implements driverbase.CurrentNamespacer
func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	return c.catalog, nil
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer
func (c *connectionImpl) SetCurrentCatalog(value string) error {
	c.catalog = value
	return nil
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer
func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	return c.dbSchema, nil
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer
func (c *connectionImpl) SetCurrentDbSchema(value string) error {
	c.dbSchema = value
	return nil
}

// SetOption sets a connection option
func (c *connectionImpl) SetOption(key, value string) error {
	switch key {
	case adbc.OptionKeyCurrentCatalog:
		return c.SetCurrentCatalog(value)
	case adbc.OptionKeyCurrentDbSchema:
		return c.SetCurrentDbSchema(value)
	case adbc.OptionKeyAutoCommit:
		// Transactions not supported, but we can accept this option silently
		return nil
	default:
		return c.ConnectionImplBase.SetOption(key, value)
	}
}

// GetOption retrieves a connection option
func (c *connectionImpl) GetOption(key string) (string, error) {
	switch key {
	case adbc.OptionKeyCurrentCatalog:
		return c.GetCurrentCatalog()
	case adbc.OptionKeyCurrentDbSchema:
		return c.GetCurrentDbSchema()
	case adbc.OptionKeyAutoCommit:
		return adbc.OptionValueEnabled, nil
	default:
		return c.ConnectionImplBase.GetOption(key)
	}
}

// ReadPartition reads a partition (not supported)
func (c *connectionImpl) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "partitioned reading not supported",
	}
}

// executeSQL is a helper to execute a SQL statement and get the output
func (c *connectionImpl) executeSQL(ctx context.Context, sql string) (*Statement, error) {
	// Wrap SQL in spark.sql() call
	code := fmt.Sprintf("spark.sql(\"%s\")", escapeSQLForScala(sql))

	stmt, err := c.livyClient.CreateStatement(ctx, c.sessionID, CreateStatementRequest{Code: code})
	if err != nil {
		return nil, fmt.Errorf("failed to execute SQL: %w", err)
	}

	// Wait for completion
	stmt, err = c.livyClient.WaitForStatementComplete(ctx, c.sessionID, stmt.ID, 5*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("SQL execution failed: %w", err)
	}

	return stmt, nil
}

// escapeSQLForScala escapes SQL for embedding in Scala string
func escapeSQLForScala(sql string) string {
	// Escape backslashes and quotes
	sql = strings.ReplaceAll(sql, "\\", "\\\\")
	sql = strings.ReplaceAll(sql, "\"", "\\\"")
	return sql
}
