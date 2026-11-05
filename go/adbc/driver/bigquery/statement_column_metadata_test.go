// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/stretchr/testify/require"
)

func nestedTestSchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType, Description: "the id"},
		{
			Name: "a",
			Type: bigquery.RecordFieldType,
			Schema: bigquery.Schema{
				{Name: "c", Type: bigquery.StringFieldType, Description: "a.c original", MaxLength: 100, Collation: "und:ci", PolicyTags: &bigquery.PolicyTagList{Names: []string{"existing"}}},
				{
					Name:     "b",
					Type:     bigquery.RecordFieldType,
					Repeated: true,
					Schema: bigquery.Schema{
						{Name: "c", Type: bigquery.StringFieldType},
					},
				},
				{Name: "amount", Type: bigquery.NumericFieldType, Precision: 10, Scale: 2},
				{Name: "period", Type: bigquery.RangeFieldType, RangeElementType: &bigquery.RangeElementType{Type: bigquery.DateFieldType}},
			},
		},
	}
}

func TestApplyColumnMetadataPreservesTopLevelAttributes(t *testing.T) {
	original := bigquery.Schema{
		{
			Name:                   "amount",
			Type:                   bigquery.NumericFieldType,
			Description:            "original description",
			Precision:              10,
			Scale:                  2,
			DefaultValueExpression: "0",
		},
	}
	schema := applyColumnMetadata(original, "", map[string]string{"amount": "updated description"}, nil)

	require.Equal(t, "updated description", schema[0].Description)
	require.Equal(t, bigquery.NumericFieldType, schema[0].Type)
	require.Equal(t, int64(10), schema[0].Precision)
	require.Equal(t, int64(2), schema[0].Scale)
	require.Equal(t, "0", schema[0].DefaultValueExpression)
	require.Equal(t, "original description", original[0].Description)
}

func TestApplyColumnMetadataNestedDescriptions(t *testing.T) {
	original := nestedTestSchema()
	schema := applyColumnMetadata(original, "", map[string]string{
		"a.b.c": "nested description",
		"a":     "record description",
	}, nil)

	require.Equal(t, "the id", schema[0].Description)
	require.Equal(t, "record description", schema[1].Description)
	require.Equal(t, original[1].Schema[0], schema[1].Schema[0])
	require.Equal(t, "nested description", schema[1].Schema[1].Schema[0].Description)

	// The input schema is not mutated.
	require.Equal(t, "", original[1].Description)
	require.Equal(t, "", original[1].Schema[1].Schema[0].Description)

	// Nested field properties are preserved.
	require.True(t, schema[1].Schema[1].Repeated)
	require.Equal(t, bigquery.RecordFieldType, schema[1].Type)
}

func TestApplyColumnMetadataDoesNotConfuseSameLeafName(t *testing.T) {
	schema := applyColumnMetadata(nestedTestSchema(), "", map[string]string{
		"a.c": "top level c",
	}, nil)

	require.Equal(t, "top level c", schema[1].Schema[0].Description)
	require.Equal(t, "", schema[1].Schema[1].Schema[0].Description)
}

func TestApplyColumnMetadataNestedPolicyTags(t *testing.T) {
	schema := applyColumnMetadata(nestedTestSchema(), "", nil, map[string][]string{
		"a.b.c": {"tag1"},
		"a":     {"ignored"},
		"id":    {"tag3"},
	})

	require.Equal(t, []string{"tag1"}, schema[1].Schema[1].Schema[0].PolicyTags.Names)
	require.Equal(t, []string{"tag3"}, schema[0].PolicyTags.Names)
	// RECORD columns cannot carry policy tags.
	require.Nil(t, schema[1].PolicyTags)
}

func TestApplyColumnMetadataUnknownPathsIgnored(t *testing.T) {
	original := nestedTestSchema()
	schema := applyColumnMetadata(original, "", map[string]string{
		"a.missing":   "no such field",
		"id.nested":   "id is not a record",
		"a.b.c.extra": "too deep",
	}, nil)

	require.Equal(t, original, schema)
}

func TestApplyColumnMetadataClearsNestedMetadata(t *testing.T) {
	original := nestedTestSchema()
	schema := applyColumnMetadata(original, "", map[string]string{"a.c": ""}, map[string][]string{"a.c": {}})

	require.Empty(t, schema[1].Schema[0].Description)
	require.NotNil(t, schema[1].Schema[0].PolicyTags)
	require.Empty(t, schema[1].Schema[0].PolicyTags.Names)
	require.Equal(t, nestedTestSchema(), original)
}
