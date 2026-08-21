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
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseQueryTagOptions(t *testing.T) {
	db := newTestDatabase(t)

	require.NoError(t, db.SetOption(OptionQueryTagPrefix+"team", "data-platform"))
	require.NoError(t, db.SetOption(OptionQueryTagPrefix+"env", "prod"))
	assert.Equal(t, map[string]string{"team": "data-platform", "env": "prod"}, db.queryTags)

	v, err := db.GetOption(OptionQueryTagPrefix + "team")
	require.NoError(t, err)
	assert.Equal(t, "data-platform", v)

	_, err = db.GetOption(OptionQueryTagPrefix + "missing")
	var adbcErr adbc.Error
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, adbc.StatusNotFound, adbcErr.Code)

	err = db.SetOption(OptionQueryTagPrefix, "no-name")
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)

	_, err = db.resolveConnectionOptions()
	assert.NoError(t, err)
}

func TestStatementQueryTagOptions(t *testing.T) {
	stmt := &statementImpl{
		conn: &connectionImpl{queryTags: map[string]string{"team": "data-platform"}},
	}

	require.NoError(t, stmt.SetOption(OptionQueryTagPrefix+"run_id", "42"))

	v, err := stmt.GetOption(OptionQueryTagPrefix + "run_id")
	require.NoError(t, err)
	assert.Equal(t, "42", v)

	// Falls back to the connection default
	v, err = stmt.GetOption(OptionQueryTagPrefix + "team")
	require.NoError(t, err)
	assert.Equal(t, "data-platform", v)

	var adbcErr adbc.Error
	_, err = stmt.GetOption(OptionQueryTagPrefix + "missing")
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, adbc.StatusNotFound, adbcErr.Code)

	err = stmt.SetOption(OptionQueryTagPrefix, "no-name")
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, adbc.StatusInvalidArgument, adbcErr.Code)

	err = stmt.SetOption("databricks.not_a_real_option", "x")
	require.ErrorAs(t, err, &adbcErr)
	assert.Equal(t, adbc.StatusNotImplemented, adbcErr.Code)
}

func TestEffectiveQueryTags(t *testing.T) {
	tests := []struct {
		name      string
		defaults  map[string]string
		overrides map[string]string
		expected  map[string]string
	}{
		{
			name:     "no tags",
			expected: nil,
		},
		{
			name:     "defaults only",
			defaults: map[string]string{"team": "data-platform"},
			expected: map[string]string{"team": "data-platform"},
		},
		{
			name:      "overrides only",
			overrides: map[string]string{"run_id": "42"},
			expected:  map[string]string{"run_id": "42"},
		},
		{
			name:      "overrides win and empty value unsets",
			defaults:  map[string]string{"a": "1", "b": "2"},
			overrides: map[string]string{"a": "", "b": "3", "c": "4"},
			expected:  map[string]string{"b": "3", "c": "4"},
		},
		{
			name:      "unsetting every default",
			defaults:  map[string]string{"a": "1"},
			overrides: map[string]string{"a": ""},
			expected:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &statementImpl{
				conn:      &connectionImpl{queryTags: tt.defaults},
				queryTags: tt.overrides,
			}
			assert.Equal(t, tt.expected, stmt.effectiveQueryTags())
		})
	}
}

func TestEffectiveQueryTagsDoesNotMutateConnection(t *testing.T) {
	conn := &connectionImpl{queryTags: map[string]string{"team": "data-platform"}}
	stmt := &statementImpl{
		conn:      conn,
		queryTags: map[string]string{"run_id": "42"},
	}

	tags := stmt.effectiveQueryTags()
	tags["team"] = "mutated"
	delete(tags, "run_id")

	assert.Equal(t, map[string]string{"team": "data-platform"}, conn.queryTags)
	assert.Equal(t, map[string]string{"run_id": "42"}, stmt.queryTags)
}
