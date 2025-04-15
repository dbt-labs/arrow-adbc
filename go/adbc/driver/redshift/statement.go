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

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type statement struct {
	alloc memory.Allocator
	cnxn  *connectionImpl
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
	// TODO: store the SQL query string
	return nil
}

func (st *statement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	return nil, -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecuteQuery not yet implemented for Redshift driver",
	}
}

func (st *statement) ExecuteUpdate(ctx context.Context) (int64, error) {
	return -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecuteUpdate not yet implemented for Redshift driver",
	}
}

func (st *statement) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecuteSchema not yet implemented for Redshift driver",
	}
}

func (st *statement) Prepare(ctx context.Context) error {
	// Redshift does not support a Prepare API — this can be a no-op
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
	return nil
}

func (st *statement) BindStream(ctx context.Context, stream array.RecordReader) error {
	// TODO: support binding stream
	return nil
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
