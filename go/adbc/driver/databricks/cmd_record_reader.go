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

package databricks

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/databricks/databricks-sdk-go/service/compute"
)

type cmdReader struct {
	refCount int64

	cmdExecution  compute.CommandExecutionInterface

	// Command Execution that this reader is associated with.
	CommandId string

	CommandResult *compute.Results

	rec arrow.Record
	err error
	schema *arrow.Schema
	rec_read bool
	cancelFn context.CancelFunc	
}

func DeriveSchema(dbx_schema []map[string]interface{}) *arrow.Schema {
	fields := make([]arrow.Field, len(dbx_schema))
	for i, col := range dbx_schema {
		var arrowType arrow.DataType
		switch col["type"] {
		case "string":
			arrowType = arrow.BinaryTypes.String
		case "int":
			arrowType = arrow.PrimitiveTypes.Int32
		case "long":
			arrowType = arrow.PrimitiveTypes.Int64
		case "float":
			arrowType = arrow.PrimitiveTypes.Float32
		case "double":
			arrowType = arrow.PrimitiveTypes.Float64
		case "boolean":
			arrowType = arrow.FixedWidthTypes.Boolean
		case "timestamp":
			arrowType = arrow.FixedWidthTypes.Timestamp_us
		case "date":
			arrowType = arrow.FixedWidthTypes.Date32
		default:
			// FIXME: expand the set of suppported DBRX types
			arrowType = arrow.BinaryTypes.String
		}
		fields[i] = arrow.Field{
			Name:     col["name"].(string),
			Type:     arrowType,
			Nullable: true,
		}
	}
	// TODO: include relevant metadata from dbrx into the Arrow schema
	return arrow.NewSchema(fields, nil)
}

func NewCommandRecordReader(
	cmdExecution compute.CommandExecutionInterface, commandId string, results *compute.Results) (*cmdReader, error) {
	// Convert the Databricks schema to an Arrow schema
	schema := DeriveSchema(result.Schema)
	r := &cmdReader{
		refCount: 1,

		cmdExecution: cmdExecution,

		CommandId: commandId,
		Results:   results,

		schema: schema,
		rec:    nil,
		err:    nil,
		rec_read: false,

		cancelFn: func() {},
	}

	// For command execution, we need to convert the result to an Arrow record
	if result.ResultType == compute.ResultTypeTable {

		switch result.Data.(type) {
		case []interface{}:
			rows := result.Data.([]interface{})
			r.rec, r.err = BuildFromRows(schema, rows)
		default:
			r.err = adbc.Error{
				Code: adbc.StatusInvalidData,
				Msg:  fmt.Sprintf("Unexpected command result type: %T", result.Data),
			}
		}

	} else if result.ResultType == compute.ResultTypeText {
		// For text result, return an empty record with a single string column
		fields := []arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true}}
		schema := arrow.NewSchema(fields, nil)
		rows := result.Data.([]interface{})
		r.rec, r.err = BuildFromRows(schema, rows)
	} else {
		r.err = adbc.Error{
			Code: adbc.StatusInvalidData,
			Msg:  fmt.Sprintf("Unexpected command result type: %s", result.ResultType),
		}
	}

	return r, nil
}

// \post: if returns true, r.Record() != nil && r.err == nil
// \post: if returns false, r.Record() == nil and r.err *MUST* be checked
func (r *cmdReader) Next() bool {
	if r.rec_read {
		return false
	}
	r.rec_read = true
	return true
}
func (r *cmdReader) Record() arrow.Record {
	return r.rec
}

func (r *cmdReader) Err() error {
	return r.err
}

func (r *cmdReader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *cmdReader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.rec != nil {
			r.rec.Release()
		}
		// TODO: cancel HTTP connection
		// TODO: close channel
		r.cancelFn()
	}
}

func (r *cmdReader) TotalRowCount() int64 {
	return r.rec.NumRows()
}

func (r *cmdReader) Schema() *arrow.Schema {
	return r.schema
}