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
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDatabase(t *testing.T) *databaseImpl {
	t.Helper()
	info := driverbase.DefaultDriverInfo("Databricks")
	drvBase := driverbase.NewDriverImplBase(info, memory.DefaultAllocator)
	dbBase, err := driverbase.NewDatabaseImplBase(context.Background(), &drvBase)
	if err != nil {
		t.Fatal(err)
	}
	return &databaseImpl{
		DatabaseImplBase: dbBase,
		serverHostname:   "test.cloud.databricks.com",
		httpPath:         "/sql/1.0/warehouses/test",
		accessToken:      "test-token",
		authType:         OptionValueAuthTypePAT,
		port:             443,
	}
}

// TestConcurrentOpen verifies that concurrent Open() calls on the same
// databaseImpl do not race on connection pool initialization.
// This reproduces the scenario from dbt-core#13387 where multiple
// goroutines calling Open() simultaneously could both attempt to start
// the OAuth listener on the same port.
//
// Run with: go test -race -run TestConcurrentOpen
func TestConcurrentOpen(t *testing.T) {
	db := newTestDatabase(t)

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	errs := make([]error, goroutines)
	for i := range goroutines {
		go func() {
			defer wg.Done()
			_, errs[i] = db.Open(context.Background())
		}()
	}
	wg.Wait()

	for i, err := range errs {
		assert.Error(t, err, "goroutine %d should fail (no real server)", i)
	}
}

// TestSetOptionConnectTimeout verifies the connect-timeout option parses a Go
// duration, is stored on the database impl and surfaced via GetOption, and that
// an unparseable value is rejected.
func TestSetOptionConnectTimeout(t *testing.T) {
	db := newTestDatabase(t)

	require.NoError(t, db.SetOption(OptionConnectTimeout, "600s"))
	assert.Equal(t, 600*time.Second, db.connectTimeout)

	got, err := db.GetOption(OptionConnectTimeout)
	require.NoError(t, err)
	assert.Equal(t, (600 * time.Second).String(), got)

	err = db.SetOption(OptionConnectTimeout, "not-a-duration")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid connect timeout")
}

// TestResolveConnectionOptionsConnectTimeout verifies the connect-timeout option
// is added to the databricks-sql-go connector options only when set. (The internal
// config package can't be imported here, so we assert on the presence of the extra
// ConnOption rather than its applied effect.)
func TestResolveConnectionOptionsConnectTimeout(t *testing.T) {
	base := newTestDatabase(t)
	baseOpts, err := base.resolveConnectionOptions()
	require.NoError(t, err)

	withTimeout := newTestDatabase(t)
	withTimeout.connectTimeout = 600 * time.Second
	timeoutOpts, err := withTimeout.resolveConnectionOptions()
	require.NoError(t, err)

	assert.Equal(t, len(baseOpts)+1, len(timeoutOpts),
		"setting connectTimeout should add exactly one connector option")
}

// TestDefaultConnectTimeout verifies a database constructed via the driver gets
// the default connect timeout when the caller does not set OptionConnectTimeout.
func TestDefaultConnectTimeout(t *testing.T) {
	drv := NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(map[string]string{})
	require.NoError(t, err)

	gs, ok := db.(adbc.GetSetOptions)
	require.True(t, ok)
	got, err := gs.GetOption(OptionConnectTimeout)
	require.NoError(t, err)
	assert.Equal(t, DefaultConnectTimeout.String(), got)
}
