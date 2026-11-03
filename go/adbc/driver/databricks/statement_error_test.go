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
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/stretchr/testify/require"
)

type fakeExecutionError struct {
	dbsqlerr.DBExecutionError
	sqlState string
	cause    error
}

func (e fakeExecutionError) Error() string    { return "server said: [CODE] details" }
func (e fakeExecutionError) Unwrap() error    { return e.cause }
func (e fakeExecutionError) SqlState() string { return e.sqlState }

func TestClassifyErrorSQLState(t *testing.T) {
	tests := []struct {
		name     string
		sqlState string
		expected adbc.Status
	}{
		{"table or view not found", "42P01", adbc.StatusNotFound},
		{"schema or catalog not found", "42704", adbc.StatusNotFound},
		{"table already exists", "42P07", adbc.StatusAlreadyExists},
		{"unresolved column", "42703", adbc.StatusInvalidArgument},
		{"unresolved routine", "42883", adbc.StatusNotFound},
		{"unknown SQLSTATE keeps status", "22012", adbc.StatusInvalidData},
		{"no SQLSTATE keeps status", "", adbc.StatusInvalidData},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyError("failed to execute query", fakeExecutionError{sqlState: tt.sqlState})
			require.Equal(t, tt.expected, got.Code)
			var want [5]byte
			copy(want[:], tt.sqlState)
			require.Equal(t, want, got.SqlState)
			require.Contains(t, got.Msg, "server said: [CODE] details")
		})
	}
}

func TestClassifyErrorWrappedSQLState(t *testing.T) {
	wrapped := fmt.Errorf("outer: %w", fakeExecutionError{cause: fakeExecutionError{sqlState: "42P01"}})
	got := classifyError("failed to execute query", wrapped)
	require.Equal(t, adbc.StatusNotFound, got.Code)
	require.Equal(t, "42P01", string(got.SqlState[:]))
}

func TestClassifyErrorContextKeepsStatus(t *testing.T) {
	require.Equal(t, adbc.StatusCancelled, classifyError("x", context.Canceled).Code)
}

func TestNewErrorKeepsCodeUnlessMapped(t *testing.T) {
	require.Equal(t, adbc.StatusNotFound, errorWithSQLState(adbc.StatusInternal, "m", fakeExecutionError{sqlState: "42704"}).Code)
	require.Equal(t, adbc.StatusAlreadyExists, errorWithSQLState(adbc.StatusInternal, "m", fakeExecutionError{sqlState: "42P07"}).Code)
	require.Equal(t, adbc.StatusInternal, errorWithSQLState(adbc.StatusInternal, "m", fakeExecutionError{sqlState: "22012"}).Code)
	require.Equal(t, adbc.StatusInternal, errorWithSQLState(adbc.StatusInternal, "m", fmt.Errorf("plain")).Code)
	require.Equal(t, "m: plain", errorWithSQLState(adbc.StatusInternal, "m", fmt.Errorf("plain")).Msg)
}
