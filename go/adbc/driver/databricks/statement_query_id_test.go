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
	"database/sql"
	"database/sql/driver"
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

// fakeExecDriver is a database/sql driver whose statements report a fixed number of affected
// rows and, like the real driver, hand the server query id to the callback in the context.
type fakeExecDriver struct {
	rowsAffected int64
	queryID      string
}

type fakeExecConn struct{ d *fakeExecDriver }

type fakeResult struct{ rows int64 }

func (r fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (r fakeResult) RowsAffected() (int64, error) { return r.rows, nil }

func (d *fakeExecDriver) Open(string) (driver.Conn, error) { return &fakeExecConn{d: d}, nil }

func (c *fakeExecConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *fakeExecConn) Close() error                        { return nil }
func (c *fakeExecConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }

func (c *fakeExecConn) ExecContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	if callback, ok := ctx.Value(dbsqlctx.QueryIdCallbackKey).(dbsqlctx.IdCallbackFunc); ok {
		callback(c.d.queryID)
	}
	return fakeResult{rows: c.d.rowsAffected}, nil
}

func executeUpdateWith(t *testing.T, d *fakeExecDriver) (*statementImpl, int64) {
	t.Helper()
	db := sql.OpenDB(fakeConnector{d: d})
	t.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	s := &statementImpl{conn: &connectionImpl{conn: conn}, query: "create view v as select 1"}
	rows, err := s.ExecuteUpdate(context.Background())
	require.NoError(t, err)
	return s, rows
}

type fakeConnector struct{ d *fakeExecDriver }

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) {
	return &fakeExecConn{d: c.d}, nil
}
func (c fakeConnector) Driver() driver.Driver { return c.d }

// ExecuteUpdate passes the driver's count through, including -1 for "unknown", and records the
// query id of the statement it ran.
func TestExecuteUpdateRowsAffectedAndQueryID(t *testing.T) {
	t.Run("unknown count stays -1", func(t *testing.T) {
		s, rows := executeUpdateWith(t, &fakeExecDriver{rowsAffected: -1, queryID: "q-view"})
		require.Equal(t, int64(-1), rows)
		id, err := s.GetOption(OptionLastQueryID)
		require.NoError(t, err)
		require.Equal(t, "q-view", id)
	})

	t.Run("reported count and zero are kept", func(t *testing.T) {
		_, rows := executeUpdateWith(t, &fakeExecDriver{rowsAffected: 5, queryID: "q-insert"})
		require.Equal(t, int64(5), rows)
		_, rows = executeUpdateWith(t, &fakeExecDriver{rowsAffected: 0, queryID: "q-empty"})
		require.Equal(t, int64(0), rows)
	})
}
