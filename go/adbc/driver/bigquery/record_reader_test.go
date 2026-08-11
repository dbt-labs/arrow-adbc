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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/api/option"
)

func TestRunQueryRecoversExistingJobAfterDuplicateInsert(t *testing.T) {
	const (
		projectID = "test-project"
		location  = "us-west1"
	)

	submittedJobIDs := make(map[string]struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/bigquery/v2/projects/test-project/jobs":
			var submitted struct {
				JobReference struct {
					JobID string `json:"jobId"`
				} `json:"jobReference"`
			}
			if err := json.NewDecoder(r.Body).Decode(&submitted); err != nil {
				t.Fatalf("decode submitted job: %v", err)
			}
			submittedJobID := submitted.JobReference.JobID
			if submittedJobID == "" {
				t.Fatal("expected the driver to submit a job ID")
			}
			if _, exists := submittedJobIDs[submittedJobID]; exists {
				t.Errorf("expected a distinct job ID for each execution, got %q twice", submittedJobID)
			}
			submittedJobIDs[submittedJobID] = struct{}{}

			w.WriteHeader(http.StatusConflict)
			_, _ = fmt.Fprint(w, `{"error":{"code":409,"message":"Already Exists: Job duplicate"}}`)

		case r.Method == http.MethodGet && r.URL.Path[:len("/bigquery/v2/projects/test-project/jobs/")] == "/bigquery/v2/projects/test-project/jobs/":
			submittedJobID := r.URL.Path[len("/bigquery/v2/projects/test-project/jobs/"):]
			_, _ = fmt.Fprintf(w, `{"configuration":{"query":{"query":"SELECT 1","useLegacySql":false}},"jobReference":{"projectId":%q,"location":%q,"jobId":%q},"status":{"state":"DONE"}}`, projectID, location, submittedJobID)

		case r.Method == http.MethodGet && r.URL.Path[:len("/bigquery/v2/projects/test-project/queries/")] == "/bigquery/v2/projects/test-project/queries/":
			submittedJobID := r.URL.Path[len("/bigquery/v2/projects/test-project/queries/"):]
			_, _ = fmt.Fprintf(w, `{"jobComplete":true,"jobReference":{"projectId":%q,"location":%q,"jobId":%q},"schema":{"fields":[]},"totalRows":"0"}`, projectID, location, submittedJobID)

		default:
			t.Fatalf("unexpected BigQuery request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	client, err := bigquery.NewClient(
		context.Background(),
		projectID,
		option.WithEndpoint(srv.URL+"/bigquery/v2/"),
		option.WithHTTPClient(srv.Client()),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("create BigQuery client: %v", err)
	}
	defer client.Close()

	query := client.Query("SELECT 1")
	query.Location = location

	ctx := context.WithValue(context.Background(), ContextKeyUseStorageApiDisabledClient, false)
	for range 2 {
		iterator, rows, err := runQuery(ctx, client, query, false, false, memory.DefaultAllocator)
		if err != nil {
			t.Fatalf("run query after duplicate insert: %v", err)
		}
		if iterator == nil {
			t.Fatal("expected an iterator for the recovered job")
		}
		if rows != 0 {
			t.Fatalf("expected zero rows, got %d", rows)
		}
	}
	if len(submittedJobIDs) != 2 {
		t.Fatalf("expected two distinct submitted job IDs, got %d", len(submittedJobIDs))
	}
}

func TestEmptyArrowIteratorNext(t *testing.T) {
	iter := emptyArrowIterator{}
	res, err := iter.Next()

	if res != nil {
		t.Errorf("Expected the result from Next to be nil, but got %v", res)
	}
	if err == nil {
		t.Errorf("Expected an error from Next, but got nil")
	}
}

func TestEmptyArrowIteratorSchema(t *testing.T) {
	iter := emptyArrowIterator{}
	schema := iter.Schema()

	if len(schema) > 0 {
		t.Errorf("Expected an empty schema, but got %d", len(schema))
	}
}

func TestEmptyArrowIteratorSerializedArrowSchema(t *testing.T) {
	iter := emptyArrowIterator{}
	bytes := iter.SerializedArrowSchema()

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	rdr, _ := ipcReaderFromArrowIterator(iter, alloc)
	if len(rdr.Schema().Fields()) > 0 {
		t.Errorf("Expected an empty schema, but got %d bytes", len(bytes))
	}
}

func TestNonEmptySchemaSerializes(t *testing.T) {
	schema := bigquery.Schema{
		&bigquery.FieldSchema{
			Name: "foo",
			Type: bigquery.BooleanFieldType,
		},
		&bigquery.FieldSchema{
			Name: "bar",
			Type: bigquery.IntegerFieldType,
		},
		&bigquery.FieldSchema{
			Name: "baz",
			Type: bigquery.RecordFieldType,
			Schema: bigquery.Schema{
				&bigquery.FieldSchema{
					Name: "a",
					Type: bigquery.StringFieldType,
				},
			},
		},
	}
	empty := emptyArrowIterator{}
	nonEmpty := emptyArrowIterator{schema}

	if bytes.Equal(empty.SerializedArrowSchema(), nonEmpty.SerializedArrowSchema()) {
		t.Errorf("Expected non-empty schema to serialize differently than empty schema")
	}

}
