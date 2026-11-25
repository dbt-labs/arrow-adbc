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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bqv2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func TestGetObjectsDatasetFilter(t *testing.T) {
	const datasets = "/bigquery/v2/projects/test-project/datasets"
	const dataset = datasets + "/a_b"
	const tables = dataset + "/tables"

	for _, tt := range []struct {
		name        string
		exact       bool
		depth       adbc.ObjectDepth
		catalog     *string
		schema      *string
		table       *string
		skipDetails bool
		status      int
		message     string
		wantError   bool
		wantSchemas []string
		wantTables  []string
		wantPaths   []string
	}{
		{name: "exact schema", exact: true, depth: adbc.ObjectDepthDBSchemas, schema: new("a_b"),
			wantSchemas: []string{"a_b"}, wantPaths: []string{dataset}},
		{name: "exact table with metadata", exact: true, depth: adbc.ObjectDepthTables, schema: new("a_b"), table: new("orders_v"),
			wantSchemas: []string{"a_b"}, wantTables: []string{"orders_v"}, wantPaths: []string{dataset, tables, tables + "/orders_v"}},
		{name: "exact table without metadata", exact: true, depth: adbc.ObjectDepthTables, schema: new("a_b"), table: new("orders_v"), skipDetails: true,
			wantSchemas: []string{"a_b"}, wantTables: []string{"orders_v"}, wantPaths: []string{dataset, tables}},
		{name: "literal percent table", exact: true, depth: adbc.ObjectDepthTables, schema: new("a_b"), table: new("orders%"), skipDetails: true,
			wantSchemas: []string{"a_b"}, wantPaths: []string{dataset, tables}},
		{name: "literal catalog", exact: true, depth: adbc.ObjectDepthDBSchemas, catalog: new("test%"), schema: new("a_b")},
		{name: "exact nil schema", exact: true, depth: adbc.ObjectDepthDBSchemas,
			wantSchemas: []string{"a_b", "axb", "a_b_c"}, wantPaths: []string{datasets}},
		{name: "exact empty schema", exact: true, depth: adbc.ObjectDepthDBSchemas, schema: new("")},
		{name: "exact empty schema at table depth", exact: true, depth: adbc.ObjectDepthTables, schema: new("")},
		{name: "wildcard empty schema", depth: adbc.ObjectDepthDBSchemas, schema: new("")},
		{name: "wildcard underscore", depth: adbc.ObjectDepthDBSchemas, schema: new("a_b"),
			wantSchemas: []string{"a_b", "axb"}, wantPaths: []string{datasets}},
		{name: "wildcard percent", depth: adbc.ObjectDepthDBSchemas, schema: new("a_b%"),
			wantSchemas: []string{"a_b", "axb", "a_b_c"}, wantPaths: []string{datasets}},
		{name: "wildcard nil schema", depth: adbc.ObjectDepthDBSchemas,
			wantSchemas: []string{"a_b", "axb", "a_b_c"}, wantPaths: []string{datasets}},
		{name: "missing dataset", exact: true, depth: adbc.ObjectDepthTables, schema: new("missing"),
			status: http.StatusNotFound, message: "Not found: Dataset test-project:missing", wantPaths: []string{datasets + "/missing"}},
		{name: "missing project", exact: true, depth: adbc.ObjectDepthDBSchemas, schema: new("a_b"),
			status: http.StatusNotFound, message: "Project test-project is not found", wantError: true, wantPaths: []string{dataset}},
		{name: "other not found", exact: true, depth: adbc.ObjectDepthDBSchemas, schema: new("a_b"),
			status: http.StatusNotFound, message: "Not found: something else", wantError: true, wantPaths: []string{dataset}},
		{name: "access denied", exact: true, depth: adbc.ObjectDepthDBSchemas, schema: new("a_b"),
			status: http.StatusForbidden, message: "Access Denied", wantError: true, wantPaths: []string{dataset}},
		{name: "server error", exact: true, depth: adbc.ObjectDepthDBSchemas, schema: new("a_b"),
			status: http.StatusNotImplemented, message: "backend unavailable", wantError: true, wantPaths: []string{dataset}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			var mu sync.Mutex
			var paths []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				paths = append(paths, r.URL.Path)
				mu.Unlock()
				assert.Equal(t, http.MethodGet, r.Method)
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.URL.Path == datasets:
					_, _ = w.Write([]byte(`{"datasets":[
						{"datasetReference":{"projectId":"test-project","datasetId":"a_b"}},
						{"datasetReference":{"projectId":"test-project","datasetId":"axb"}},
						{"datasetReference":{"projectId":"test-project","datasetId":"a_b_c"}}]}`))
				case r.URL.Path == tables:
					_, _ = w.Write([]byte(`{"tables":[
						{"type":"TABLE","tableReference":{"projectId":"test-project","datasetId":"a_b","tableId":"orders_v"}},
						{"type":"TABLE","tableReference":{"projectId":"test-project","datasetId":"a_b","tableId":"ordersXv"}}]}`))
				case strings.HasPrefix(r.URL.Path, tables+"/"):
					id := strings.TrimPrefix(r.URL.Path, tables+"/")
					_ = json.NewEncoder(w).Encode(bqv2.Table{Type: "TABLE",
						TableReference: &bqv2.TableReference{ProjectId: "test-project", DatasetId: "a_b", TableId: id},
					})
				case tt.status != 0:
					assert.Equal(t, "METADATA", r.URL.Query().Get("datasetView"))
					w.WriteHeader(tt.status)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": tt.status, "message": tt.message}})
				case r.URL.Path == dataset:
					assert.Equal(t, "METADATA", r.URL.Query().Get("datasetView"))
					_, _ = w.Write([]byte(`{"datasetReference":{"projectId":"test-project","datasetId":"a_b"},"location":"US"}`))
				default:
					t.Errorf("unexpected request: %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(srv.Close)
			opts := []option.ClientOption{option.WithEndpoint(srv.URL + "/bigquery/v2/"),
				option.WithHTTPClient(srv.Client()), option.WithoutAuthentication()}
			client, err := bigquery.NewClient(ctx, "test-project", opts...)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, client.Close()) })
			svc, err := bqv2.NewService(ctx, opts...)
			require.NoError(t, err)
			alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
			t.Cleanup(func() { alloc.AssertSize(t, 0) })
			impl := &connectionImpl{ConnectionImplBase: driverbase.ConnectionImplBase{Alloc: alloc}, client: client, tablesService: svc}
			conn := driverbase.NewConnectionBuilder(impl).WithDbObjectsEnumerator(impl).Connection()
			options := conn.(adbc.GetSetOptions)
			if tt.exact {
				require.NoError(t, options.SetOption(OptionGetObjectsDisableWildcards, adbc.OptionValueEnabled))
			}
			if tt.skipDetails {
				require.NoError(t, options.SetOption(OptionBoolGetObjectsSkipTableMetadata, adbc.OptionValueEnabled))
			}

			reader, err := conn.GetObjects(ctx, tt.depth, tt.catalog, tt.schema, tt.table, nil, nil)
			if reader != nil {
				defer reader.Release()
			}
			if tt.wantError {
				var apiErr *googleapi.Error
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.status, apiErr.Code)
				assert.Equal(t, tt.message, apiErr.Message)
			} else {
				require.NoError(t, err)
				require.NotNil(t, reader)
				var schemaNames, tableNames []string
				for reader.Next() {
					data, err := reader.RecordBatch().MarshalJSON()
					require.NoError(t, err)
					var catalogs []driverbase.GetObjectsInfo
					require.NoError(t, json.Unmarshal(data, &catalogs))
					for _, catalog := range catalogs {
						for _, schema := range catalog.CatalogDbSchemas {
							schemaNames = append(schemaNames, *schema.DbSchemaName)
							for _, table := range schema.DbSchemaTables {
								tableNames = append(tableNames, table.TableName)
							}
						}
					}
				}
				require.NoError(t, reader.Err())
				assert.ElementsMatch(t, tt.wantSchemas, schemaNames)
				assert.ElementsMatch(t, tt.wantTables, tableNames)
			}

			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, tt.wantPaths, paths)
		})
	}
}

func TestGetObjectsDisableWildcardsOption(t *testing.T) {
	conn := &connectionImpl{}
	getOption := func(want string) {
		t.Helper()
		got, err := conn.GetOption(OptionGetObjectsDisableWildcards)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}

	getOption(adbc.OptionValueDisabled)
	require.NoError(t, conn.SetOption(OptionGetObjectsDisableWildcards, adbc.OptionValueEnabled))
	getOption(adbc.OptionValueEnabled)
	for _, invalid := range []string{"1", "TRUE", "maybe"} {
		var err adbc.Error
		require.ErrorAs(t, conn.SetOption(OptionGetObjectsDisableWildcards, invalid), &err)
		assert.Equal(t, adbc.StatusInvalidArgument, err.Code)
		getOption(adbc.OptionValueEnabled)
	}
	require.NoError(t, conn.SetOption(OptionGetObjectsDisableWildcards, adbc.OptionValueDisabled))
	getOption(adbc.OptionValueDisabled)
}
