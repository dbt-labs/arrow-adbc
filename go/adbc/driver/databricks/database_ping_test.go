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
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slowWarehouse is a database/sql connector that stands in for a cold-starting
// warehouse: the session opens after connectDelay and "select 1" completes after
// queryDelay. Its Ping mirrors databricks-sql-go's, which runs "select 1" under a
// fixed deadline (pingCap) regardless of the caller's context.
type slowWarehouse struct {
	connectDelay time.Duration
	queryDelay   time.Duration
	pingCap      time.Duration

	pings   atomic.Int32
	queries atomic.Int32
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (w *slowWarehouse) Connect(ctx context.Context) (driver.Conn, error) {
	if err := sleepCtx(ctx, w.connectDelay); err != nil {
		return nil, err
	}
	return &slowConn{w: w}, nil
}

func (w *slowWarehouse) Driver() driver.Driver { return slowDriver{} }

type slowDriver struct{}

func (slowDriver) Open(string) (driver.Conn, error) { return nil, errors.New("unsupported") }

type slowConn struct{ w *slowWarehouse }

func (c *slowConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unsupported") }
func (c *slowConn) Close() error                        { return nil }
func (c *slowConn) Begin() (driver.Tx, error)           { return nil, errors.New("unsupported") }

func (c *slowConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	c.w.queries.Add(1)
	if err := sleepCtx(ctx, c.w.queryDelay); err != nil {
		return nil, err
	}
	return emptyRows{}, nil
}

func (c *slowConn) Ping(ctx context.Context) error {
	c.w.pings.Add(1)
	ctx, cancel := context.WithTimeout(ctx, c.w.pingCap)
	defer cancel()
	rows, err := c.QueryContext(ctx, "select 1", nil)
	if err != nil {
		return driver.ErrBadConn
	}
	return rows.Close()
}

type emptyRows struct{}

func (emptyRows) Columns() []string         { return []string{"1"} }
func (emptyRows) Close() error              { return nil }
func (emptyRows) Next([]driver.Value) error { return io.EOF }

// TestPingPoolOutlastsSDKPingCap is the cold-start case: the warehouse takes longer
// to answer than the SDK's fixed ping deadline but well within connect_timeout.
// The plain SDK-style ping fails; pingPool must succeed.
func TestPingPoolOutlastsSDKPingCap(t *testing.T) {
	w := &slowWarehouse{queryDelay: 400 * time.Millisecond, pingCap: 50 * time.Millisecond}
	db := sql.OpenDB(w)
	t.Cleanup(func() { _ = db.Close() })

	require.Error(t, db.PingContext(context.Background()),
		"control: the SDK-style ping should hit its fixed deadline")

	d := newTestDatabase(t)
	d.connectTimeout = 5 * time.Second
	require.NoError(t, d.pingPool(context.Background(), db))
}

// TestPingPoolFailsAtConnectTimeout verifies pingPool is bounded by connect_timeout
// when the warehouse never comes up.
func TestPingPoolFailsAtConnectTimeout(t *testing.T) {
	w := &slowWarehouse{queryDelay: time.Minute, pingCap: time.Minute}
	db := sql.OpenDB(w)
	t.Cleanup(func() { _ = db.Close() })

	d := newTestDatabase(t)
	d.connectTimeout = 200 * time.Millisecond

	start := time.Now()
	err := d.pingPool(context.Background(), db)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond)
	assert.Less(t, elapsed, 5*time.Second)
}

// TestPingPoolBudgetCoversSessionOpen verifies connect_timeout is one budget for
// session open plus the query, not a fresh allowance for each.
func TestPingPoolBudgetCoversSessionOpen(t *testing.T) {
	w := &slowWarehouse{
		connectDelay: 300 * time.Millisecond,
		queryDelay:   300 * time.Millisecond,
		pingCap:      time.Minute,
	}
	db := sql.OpenDB(w)
	t.Cleanup(func() { _ = db.Close() })

	d := newTestDatabase(t)
	d.connectTimeout = 500 * time.Millisecond
	err := d.pingPool(context.Background(), db)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	d.connectTimeout = 5 * time.Second
	require.NoError(t, d.pingPool(context.Background(), db))
}

// TestPingPoolWithoutConnectTimeoutUsesSDKPing verifies that with no connect
// timeout configured the original database/sql ping path is kept.
func TestPingPoolWithoutConnectTimeoutUsesSDKPing(t *testing.T) {
	w := &slowWarehouse{pingCap: time.Minute}
	db := sql.OpenDB(w)
	t.Cleanup(func() { _ = db.Close() })

	d := newTestDatabase(t)
	d.connectTimeout = 0
	require.NoError(t, d.pingPool(context.Background(), db))
	assert.EqualValues(t, 1, w.pings.Load())
}
