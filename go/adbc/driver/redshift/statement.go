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

package redshift

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
)

type statement struct {
	alloc memory.Allocator
	cnxn  *connectionImpl
	input *redshiftdata.ExecuteStatementInput
}

func (st *statement) SetOption(key string, value string) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[Redshift] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (st *statement) GetOption(key string) (string, error) {
	return "", adbc.Error{
		Msg:  fmt.Sprintf("[Redshift] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}

func (st *statement) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[Redshift] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (st *statement) GetOptionBytes(key string) ([]byte, error) {
	return nil, adbc.Error{
		Msg:  fmt.Sprintf("[Redshift] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}

func (st *statement) SetOptionDouble(key string, value float64) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[Redshift] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (st *statement) GetOptionDouble(key string) (float64, error) {
	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[Redshift] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}

func (st *statement) SetOptionInt(key string, value int64) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[Redshift] Unknown statement option '%s'", key),
		Code: adbc.StatusInvalidArgument,
	}
}

func (st *statement) GetOptionInt(key string) (int64, error) {
	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[Redshift] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}

func (st *statement) Close() error {
	// TODO: implement cleanup
	st.cnxn = nil
	return nil
}

func (st *statement) SetSqlQuery(query string) error {
	st.input.Sql = &query
	return nil
}

const (
	// Redshift status codes
	RedshiftStatusFinished  = "FINISHED"
	RedshiftStatusFailed    = "FAILED"
	RedshiftStatusSubmitted = "SUBMITTED"
	RedshiftStatusPicked    = "PICKED"
	RedshiftStatusStarted   = "STARTED"
	RedshiftStatusAborted   = "ABORTED"
	RedshiftStatusAll       = "ALL"
)

func (st *statement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	// check if we have a query
	if st.input.Sql == nil {
		return nil, -1, adbc.Error{
			Msg:  "Cannot execute without a query!",
			Code: adbc.StatusInvalidArgument,
		}
	}
	output, err := st.cnxn.client.ExecuteStatement(ctx, st.input)
	if err != nil {
		return nil, -1, err
	}

	statementID := *output.Id
	maxWait := 60 * time.Second
	waited := 0 * time.Second
	var resultRows int64
	var status string
	for {
		descOut, err := st.cnxn.client.DescribeStatement(ctx, &redshiftdata.DescribeStatementInput{
			Id: aws.String(statementID),
		})
		if err != nil {
			panic(err)
		}
		resultRows = descOut.ResultRows
		status = string(descOut.Status)
		if status == RedshiftStatusFinished || status == RedshiftStatusFailed || status == RedshiftStatusAborted || waited > maxWait {
			break
		}
		toWait := 500 * time.Millisecond
		time.Sleep(toWait)
		waited += toWait
	}

	if status != RedshiftStatusFinished {
		panic("Query did not finish successfully. Status: " + status)
	}

	// var resultRows int64 = 0
	reader, err := NewRecordBatchReader(ctx, st.cnxn, st.alloc, statementID)
	if err != nil {
		return nil, -1, err
	}
	// Conduct first fetch of data and setting schema from query
	reader.Initialize()
	return reader, resultRows, err
}

func (st *statement) ExecuteUpdate(ctx context.Context) (int64, error) {
	return -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecuteUpdate not yet implemented for Redshift driver",
	}
}

func (st *statement) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	// Not sure how we'll support one, Schema only comes from executing a query
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecuteSchema not yet implemented for Redshift driver",
	}
}

func (st *statement) Prepare(ctx context.Context) error {
	// Redshift does not support a Prepare API
	return nil
}

func (st *statement) SetSubstraitPlan(plan []byte) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Substrait not yet implemented for Redshift driver",
	}
}

func (st *statement) Bind(ctx context.Context, record arrow.Record) error {
	// TODO: support binding record batch
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Bind not yet implemented for Redshift driver",
	}
}

func (st *statement) BindStream(ctx context.Context, stream array.RecordReader) error {
	// TODO: support binding stream
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "BindStream not yet implemented for Redshift driver",
	}
}

func (st *statement) GetParameterSchema() (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "GetParameterSchema not yet implemented for Redshift driver",
	}
}

func (st *statement) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecutePartitions not yet implemented for Redshift driver",
	}
}
