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
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	dbsqlctx "github.com/databricks/databricks-sql-go/driverctx"
	"github.com/stretchr/testify/require"
)

func TestStatementLastQueryID(t *testing.T) {
	s := &statementImpl{}

	_, err := s.GetOption(OptionLastQueryID)
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusNotFound, adbcErr.Code)

	// The driver calls the callback in the context with the server's query id.
	ctx := s.withQueryIDCapture(context.Background())
	callback, ok := ctx.Value(dbsqlctx.QueryIdCallbackKey).(dbsqlctx.IdCallbackFunc)
	require.True(t, ok, "context carries the query id callback")
	callback("01f0-abcd")

	id, err := s.GetOption(OptionLastQueryID)
	require.NoError(t, err)
	require.Equal(t, "01f0-abcd", id)

	// A new execution starts without the previous id, so a failed statement can't report it.
	s.withQueryIDCapture(context.Background())
	_, err = s.GetOption(OptionLastQueryID)
	require.ErrorAs(t, err, &adbcErr)
	require.Equal(t, adbc.StatusNotFound, adbcErr.Code)
}
