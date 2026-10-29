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
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
)

func TestAddNestedFields(t *testing.T) {
	field := func(name string, fieldType bigquery.FieldType) *bigquery.FieldSchema {
		return &bigquery.FieldSchema{Name: name, Type: fieldType}
	}

	record := func(name string, fields ...*bigquery.FieldSchema) *bigquery.FieldSchema {
		return &bigquery.FieldSchema{Name: name, Type: bigquery.RecordFieldType, Schema: fields}
	}

	repeated := func(fieldSchema *bigquery.FieldSchema) *bigquery.FieldSchema {
		fieldSchema.Repeated = true
		return fieldSchema
	}

	id := field("id", bigquery.IntegerFieldType)
	integer := field("int", bigquery.IntegerFieldType)
	str := field("str", bigquery.StringFieldType)
	boolean := field("bool", bigquery.BooleanFieldType)

	tests := []struct {
		name     string
		source   bigquery.Schema
		target   bigquery.Schema
		expected bigquery.Schema
		changed  bool
	}{
		{
			name:     "new nested field",
			source:   bigquery.Schema{id, record("record", integer, str, boolean)},
			target:   bigquery.Schema{id, record("record", integer, str)},
			expected: bigquery.Schema{id, record("record", integer, str, boolean)},
			changed:  true,
		},
		{
			name:     "deeper nesting",
			source:   bigquery.Schema{record("outer", integer, record("inner", str, boolean), record("deeper", integer))},
			target:   bigquery.Schema{record("outer", integer, record("inner", str))},
			expected: bigquery.Schema{record("outer", integer, record("inner", str, boolean), record("deeper", integer))},
			changed:  true,
		},
		{
			name:     "repeated records",
			source:   bigquery.Schema{repeated(record("items", integer, boolean))},
			target:   bigquery.Schema{repeated(record("items", integer))},
			expected: bigquery.Schema{repeated(record("items", integer, boolean))},
			changed:  true,
		},
		{
			name:     "field names are case-insensitive",
			source:   bigquery.Schema{record("CasE", field("INT", bigquery.IntegerFieldType), boolean)},
			target:   bigquery.Schema{record("case", field("int", bigquery.IntegerFieldType))},
			expected: bigquery.Schema{record("case", field("int", bigquery.IntegerFieldType), boolean)},
			changed:  true,
		},
		{
			name:     "nothing to add",
			source:   bigquery.Schema{id, record("same", integer, str)},
			target:   bigquery.Schema{id, record("same", integer, str)},
			expected: bigquery.Schema{id, record("same", integer, str)},
			changed:  false,
		},
		{
			name:     "top-level columns are left to ALTER TABLE",
			source:   bigquery.Schema{id, record("dropped", integer)},
			target:   bigquery.Schema{id},
			expected: bigquery.Schema{id},
			changed:  false,
		},
		{
			name:     "missing and retyped fields are kept",
			source:   bigquery.Schema{record("filters", integer)},
			target:   bigquery.Schema{record("filters", integer, str)},
			expected: bigquery.Schema{record("filters", integer, str)},
			changed:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, changed := addNestedFields(test.source, test.target)
			assert.Equal(t, test.changed, changed)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestStringToTable(t *testing.T) {
	tests := []struct {
		name             string
		defaultProjectID string
		defaultDatasetID string
		input            string
		expected         *bigquery.Table
		expectError      bool
	}{
		{
			name:             "table only",
			defaultProjectID: "default-project",
			defaultDatasetID: "default-dataset",
			input:            "mytable",
			expected: &bigquery.Table{
				ProjectID: "default-project",
				DatasetID: "default-dataset",
				TableID:   "mytable",
			},
			expectError: false,
		},
		{
			name:             "dataset and table",
			defaultProjectID: "default-project",
			defaultDatasetID: "default-dataset",
			input:            "mydataset.mytable",
			expected: &bigquery.Table{
				ProjectID: "default-project",
				DatasetID: "mydataset",
				TableID:   "mytable",
			},
			expectError: false,
		},
		{
			name:             "project, dataset and table",
			defaultProjectID: "default-project",
			defaultDatasetID: "default-dataset",
			input:            "myproject.mydataset.mytable",
			expected: &bigquery.Table{
				ProjectID: "myproject",
				DatasetID: "mydataset",
				TableID:   "mytable",
			},
			expectError: false,
		},
		{
			name:             "too many parts",
			defaultProjectID: "default-project",
			defaultDatasetID: "default-dataset",
			input:            "a.b.c.d",
			expected:         nil,
			expectError:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &statement{
				cnxn: &connectionImpl{
					catalog:  tt.defaultProjectID,
					dbSchema: tt.defaultDatasetID,
				},
			}
			result, err := stringToTable(st, tt.input)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if result.ProjectID != tt.expected.ProjectID ||
				result.DatasetID != tt.expected.DatasetID ||
				result.TableID != tt.expected.TableID {
				t.Errorf("got %+v, want %+v", result, tt.expected)
			}
		})
	}
}
