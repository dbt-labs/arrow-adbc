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

package bigquery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func TestExecuteGetDatasetLocation(t *testing.T) {
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/bigquery/v2/projects/test-project/datasets/present":
			_, _ = w.Write([]byte(`{"datasetReference":{"projectId":"test-project","datasetId":"present"},"location":"us-west1"}`))
		case "/bigquery/v2/projects/test-project/datasets/missing":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":404,"message":"Not found: Dataset test-project:missing","errors":[{"reason":"notFound"}]}}`))
		case "/bigquery/v2/projects/missing-project/datasets/any":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":404,"message":"Project missing-project is not found. Make sure it references valid GCP project that hasn't been deleted.; Project id: missing-project","errors":[{"reason":"notFound"}]}}`))
		default:
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"code":403,"message":"Access Denied","errors":[{"reason":"accessDenied"}]}}`))
		}
	}))
	defer srv.Close()

	client, err := bigquery.NewClient(context.Background(), "test-project",
		option.WithEndpoint(srv.URL+"/bigquery/v2/"),
		option.WithHTTPClient(srv.Client()),
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)
	defer client.Close()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)
	cnxn := &connectionImpl{ConnectionImplBase: driverbase.ConnectionImplBase{Alloc: alloc}, client: client}

	execute := func(st *statement) ([]string, int64, error) {
		rdr, n, err := st.ExecuteQuery(context.Background())
		if err != nil {
			return nil, n, err
		}
		defer rdr.Release()
		assert.Equal(t, "location", rdr.Schema().Field(0).Name)
		var out []string
		for rdr.Next() {
			col := rdr.RecordBatch().Column(0).(*array.String)
			for i := 0; i < col.Len(); i++ {
				out = append(out, col.Value(i))
			}
		}
		return out, n, rdr.Err()
	}
	locations := func(payload string) ([]string, int64, error) {
		st := &statement{cnxn: cnxn}
		require.NoError(t, st.SetOption(OptionJsonGetDatasetLocation, payload))
		got, err := st.GetOption(OptionJsonGetDatasetLocation)
		require.NoError(t, err)
		require.Equal(t, payload, got)
		return execute(st)
	}

	got, n, err := locations(`{"project":"test-project","dataset":"present"}`)
	require.NoError(t, err)
	assert.Equal(t, []string{"us-west1"}, got)
	assert.EqualValues(t, 1, n)

	got, n, err = locations(`{"project":"test-project","dataset":"missing"}`)
	require.NoError(t, err)
	assert.Empty(t, got)
	assert.EqualValues(t, 0, n)

	// A missing project is a configuration error, not an absent dataset.
	_, _, err = locations(`{"project":"missing-project","dataset":"any"}`)
	assert.ErrorContains(t, err, "Project missing-project is not found")

	_, _, err = locations(`{"project":"test-project","dataset":"forbidden"}`)
	assert.ErrorContains(t, err, "failed to get dataset metadata for test-project.forbidden")

	before := requests.Load()
	for _, payload := range []string{`not json`, `null`, `{}`, `{"project":"test-project"}`, `{"dataset":"present"}`} {
		_, _, err = locations(payload)
		var adbcErr adbc.Error
		require.ErrorAs(t, err, &adbcErr, payload)
		assert.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code, payload)
	}
	assert.Equal(t, before, requests.Load(), "invalid payloads must not reach the API")

	// Clearing the option makes a reused statement run its query again.
	st := &statement{cnxn: cnxn}
	require.NoError(t, st.SetOption(OptionJsonGetDatasetLocation, `{"project":"test-project","dataset":"present"}`))
	_, _, err = execute(st)
	require.NoError(t, err)
	require.NoError(t, st.SetOption(OptionJsonGetDatasetLocation, ""))
	_, _, err = st.ExecuteQuery(context.Background())
	assert.ErrorContains(t, err, "cannot execute without a query")
}
