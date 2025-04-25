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
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sdk-go/service/compute"
	"github.com/databricks/databricks-sdk-go/service/sql"
)

type chunkResponse struct {
	chunkIndex int
	inner      *http.Response
	err        error

	reader array.RecordReader
}

func (c *chunkResponse) Retain() {
	if c.reader != nil {
		c.reader.Retain()
	}
}

func (c *chunkResponse) Release() {
	if c.reader != nil {
		c.reader.Release()
		c.reader = nil
	}
	if c.inner != nil {
		c.inner.Body.Close()
		c.inner = nil
	}
}

type reader struct {
	refCount int64

	stmtExecution sql.StatementExecutionInterface
	cmdExecution  compute.CommandExecutionInterface
	httpClient    *http.Client

	// Statement that this reader is associated with.
	StatementId string
	CommandId   string

	// Fields from the execution response manifest:

	// Array of result set chunk metadata.
	Chunks []sql.BaseChunkInfo
	// The total number of chunks that the result set has been divided into.
	TotalChunkCount int
	// The total number of rows in the result set.
	TotalRowCount int64

	// Command execution specific fields
	CommandResult *compute.Results

	// The chunk index that is currently being loaded in the background or -1.
	loadingChunkIdx int
	// The channel to receive chunk responses.
	chunkChan chan chunkResponse
	// The reader for the already loaded chunk. If nil, poll for the next chunk.
	activeChunk *chunkResponse

	schema *arrow.Schema
	rec    arrow.Record
	err    error

	cancelFn context.CancelFunc

	// Statistics

	// Reader's start time.
	startTime time.Time
	// All the bytes received from the server.
	BytesReceived int64
	// Time spent waiting for the server to respond in the foreground.
	WaitTime time.Duration
}

func NewRecordReader(
	stmtExecution sql.StatementExecutionInterface, statementId string, result *sql.ResultData, manifest *sql.ResultManifest) (*reader, error) {
	r := &reader{
		refCount: 1,

		stmtExecution: stmtExecution,
		httpClient:    http.DefaultClient,

		StatementId: statementId,

		Chunks:          manifest.Chunks,
		TotalChunkCount: manifest.TotalChunkCount,
		TotalRowCount:   manifest.TotalRowCount,

		loadingChunkIdx: 0,
		chunkChan:       make(chan chunkResponse),
		activeChunk:     nil,

		schema: nil,
		rec:    nil,
		err:    nil,

		cancelFn: func() {},

		startTime:     time.Now(),
		BytesReceived: 0,
		WaitTime:      0,
	}
	if len(manifest.Chunks) > 0 && len(result.ExternalLinks) > 0 {
		go r.startChunkDataRequest(r.loadingChunkIdx, &result.ExternalLinks[0])
	} else {
		close(r.chunkChan)
		r.loadingChunkIdx = -1
	}
	return r, nil
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
			arrowType = arrow.BinaryTypes.String
		}
		fields[i] = arrow.Field{
			Name:     col["name"].(string),
			Type:     arrowType,
			Nullable: true,
		}
	}
	return arrow.NewSchema(fields, nil)
}

func AddRowToBuilder(builder *array.RecordBuilder, row []interface{}) *array.RecordBuilder {
	for col_num, col := range row {
		switch col := col.(type) {
		case string:
			builder.Field(col_num).(*array.StringBuilder).Append(string(col))
		case int:
			builder.Field(col_num).(*array.Int32Builder).Append(int32(col))
		case int64:
			builder.Field(col_num).(*array.Int64Builder).Append(col)
		case float32:
			builder.Field(col_num).(*array.Float32Builder).Append(col)
		case float64:
			builder.Field(col_num).(*array.Float64Builder).Append(col)
		case bool:
			builder.Field(col_num).(*array.BooleanBuilder).Append(col)
		case nil:
			builder.Field(col_num).AppendNull()
		default:
			// For any unhandled types, convert to string
			builder.Field(col_num).(*array.StringBuilder).Append(fmt.Sprintf("%v", col))
		}
	}
	return builder
}

func BuildArrowRecord(schema *arrow.Schema, data []interface{}) (arrow.Record, error) {
	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	for _, row := range data {
		switch row_data := row.(type) {
		case []interface{}:
			builder = AddRowToBuilder(builder, row_data)
		}
	}
	return builder.NewRecord(), nil
}

func NewCommandRecordReader(
	cmdExecution compute.CommandExecutionInterface, commandId string, result *compute.Results) (*reader, error) {
	// Convert the Databricks schema to an Arrow schema
	schema := DeriveSchema(result.Schema)
	r := &reader{
		refCount: 1,

		cmdExecution: cmdExecution,
		httpClient:   http.DefaultClient,

		CommandId:     commandId,
		CommandResult: result,

		loadingChunkIdx: -1,
		chunkChan:       make(chan chunkResponse),
		activeChunk:     nil,

		schema: schema,
		rec:    nil,
		err:    nil,

		cancelFn: func() {},

		startTime:     time.Now(),
		BytesReceived: 0,
		WaitTime:      0,
	}

	// For command execution, we need to convert the result to an Arrow record
	if result.ResultType == compute.ResultTypeTable {

		switch result.Data.(type) {
		case []interface{}:
			rows := result.Data.([]interface{})
			for _, row := range rows {
				row_data := row.([]interface{})
				r.rec, r.err = BuildArrowRecord(schema, row_data)
			}
		default:
			r.err = adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("Unexpected command result type: %T", result.Data),
			}
		}
	} else if result.ResultType == compute.ResultTypeText {
		// For text result, return an empty record with a single string column
		fields := []arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true}}
		schema := arrow.NewSchema(fields, nil)
		cols := []arrow.Array{array.NewNull(0)}
		r.rec = array.NewRecord(schema, cols, 0)
	} else {
		r.err = adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("Unexpected command result type: %s", result.ResultType),
		}
	}

	close(r.chunkChan)
	return r, nil
}

func (r *reader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *reader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.activeChunk != nil {
			r.activeChunk.Release()
		}
		if r.rec != nil {
			r.rec.Release()
		}
		// TODO: cancel HTTP connection
		// TODO: close channel
		r.cancelFn()
	}
}

// \pre: loadingChunkIdx != -1 implies chunk is loading in the background (INVARIANT I)
// \pre: loadingChunkIdx == -1 implies chunkChan is closed (INVARIANT II)
// \post: if returns true, r.Record() != nil && r.err == nil
// \post: if returns false, r.Record() == nil and r.err *MUST* be checked
func (r *reader) Next() bool {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}
	if r.err != nil {
		return false // post-condition holds: r.rec == nil && r.err != nil
	}
	// PROPERTY I: r.rec == nil && r.err == nil

	// If we don't have an active chunk, we need to wait for the loading one,
	// parse it, and trigger a request for the next chunk to preserve
	// invariants I and II.
	if r.activeChunk == nil {
		if r.loadingChunkIdx == -1 {
			return false // post-condition holds because of PROPERTY I
		}

		chunk, err := r.consumeLoadingChunk()
		if err != nil {
			r.err = err
			close(r.chunkChan)
			return false // post-condition holds because of PROPERTY I
		}
		r.activeChunk = chunk
		r.activeChunk.Retain()

		// make sure r.schema is set when the first chunk is parsed if not yet
		if r.schema != nil {
			r.schema = r.activeChunk.reader.Schema()
		}
	}
	// PROPERTY II: r.activeChunk != nil

	if r.activeChunk.reader.Next() {
		r.rec = r.activeChunk.reader.Record()
		r.rec.Retain()
		return true // post-condition holds: r.rec != nil
	}
	// make sure the error (if it exists) is retained
	r.err = r.activeChunk.reader.Err()
	// release the fully consumed (or err'd) chunk
	r.activeChunk.Release()
	r.activeChunk = nil
	// PROPERTY III: r.activeChunk == nil

	// Recursively call Next() to start processing another chunk or stopping.
	// Iteration will either terminate (if r.err != nil) or an attempt will be made
	// to load the next chunk (because r.activeChunk == nil) guaranteeing progress.
	return r.Next()
}

func (r *reader) Schema() *arrow.Schema {
	if r.schema == nil {
		if r.activeChunk == nil {
			if r.loadingChunkIdx == -1 {
				return nil // TODO: need to derive schema from the JSON manifest :(
			}
			chunk, err := r.consumeLoadingChunk()
			if err != nil {
				r.err = err
				return nil // TODO: need to derive schema from the JSON manifest :(
			}
			r.activeChunk = chunk
			r.activeChunk.Retain()
		}
		r.schema = r.activeChunk.reader.Schema()
	}
	return r.schema
}

// \pre: r.activeChunk == nil && r.loadingChunkIdx != -1
func (r *reader) consumeLoadingChunk() (*chunkResponse, error) {
	// wait for the loading chunk
	startWait := time.Now()
	chunk := <-r.chunkChan
	r.WaitTime += time.Since(startWait)
	if chunk.err != nil {
		close(r.chunkChan)
		return nil, chunk.err
	}
	if chunk.chunkIndex != r.loadingChunkIdx {
		log.Fatalf("expected chunk %d, but receiving %d", r.loadingChunkIdx, chunk.chunkIndex)
	}
	// trigger a request for a new chunk in the background
	r.loadingChunkIdx += 1
	if r.loadingChunkIdx < len(r.Chunks) {
		// INVARIANT I and II are preserved
		go r.startChunkDataRequest(r.loadingChunkIdx, nil)
	} else {
		// INVARIANT I and II are preserved
		r.loadingChunkIdx = -1
		close(r.chunkChan)
	}
	return &chunk, nil
}

// \pre: Next() returned true
func (r *reader) Record() arrow.Record {
	return r.rec
}

func (r *reader) Err() error {
	return r.err
}

func (r *reader) Throughput() float64 {
	elapsed := time.Since(r.startTime)
	elapsedSeconds := elapsed.Seconds()
	return float64(r.BytesReceived) / elapsedSeconds
}

// Start an HTTP request for the chunk data and notify the chunkReceived channel
// when a response is received and data is available for streaming.
//
// NOTE: The caller is responsible for closing the response body in .inner.
func (r *reader) startChunkDataRequest(chunkIndex int, externalLink *sql.ExternalLink) {
	url := ""
	if externalLink != nil {
		url = externalLink.ExternalLink
	} else {
		// TODO(felipecrv): retry logic
		req := sql.GetStatementResultChunkNRequest{
			ChunkIndex:  chunkIndex,
			StatementId: r.StatementId,
		}
		res, err := r.stmtExecution.GetStatementResultChunkN(context.TODO(), req)
		if err != nil {
			r.chunkChan <- chunkResponse{
				chunkIndex: chunkIndex,
				inner:      nil,
				err:        err,
			}
			return
		} else {
			externalLink = &res.ExternalLinks[0]
			url = externalLink.ExternalLink
		}
	}
	// TODO: must send request headers as well
	// TODO: use context for cancellation
	res, err := r.httpClient.Get(url)
	chunkBodyReader, err := ipc.NewReader(res.Body)
	chunkRecordReader := array.RecordReader(chunkBodyReader)
	r.chunkChan <- chunkResponse{
		chunkIndex: chunkIndex,
		inner:      res,
		err:        err,
		reader:     chunkRecordReader,
	}
}
