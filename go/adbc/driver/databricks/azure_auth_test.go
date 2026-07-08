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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSetOptionAzureClientSecret verifies the azure service-principal option keys
// are wired into SetOption and stored on the database impl.
func TestSetOptionAzureClientSecret(t *testing.T) {
	db := newTestDatabase(t)

	require.NoError(t, db.SetOption(OptionAuthType, OptionValueAuthTypeAzureClientSecret))
	require.NoError(t, db.SetOption(OptionAzureClientID, "azure-id"))
	require.NoError(t, db.SetOption(OptionAzureClientSecret, "azure-secret"))
	require.NoError(t, db.SetOption(OptionAzureTenantID, "tenant-123"))

	assert.Equal(t, OptionValueAuthTypeAzureClientSecret, db.authType)
	assert.Equal(t, "azure-id", db.azureClientID)
	assert.Equal(t, "azure-secret", db.azureClientSecret)
	assert.Equal(t, "tenant-123", db.azureTenantID)
}

// TestResolveConnectionOptionsAzureMissingFields verifies that the azure-client-secret
// auth type requires both client id and client secret, and errors clearly otherwise.
// These validations return before any network/SDK resolution.
func TestResolveConnectionOptionsAzureMissingFields(t *testing.T) {
	t.Run("missing client id", func(t *testing.T) {
		db := newTestDatabase(t)
		db.authType = OptionValueAuthTypeAzureClientSecret
		db.accessToken = ""
		db.azureClientSecret = "secret"

		_, err := db.resolveConnectionOptions()
		require.Error(t, err)
		assert.Contains(t, err.Error(), OptionAzureClientID)
	})

	t.Run("missing client secret", func(t *testing.T) {
		db := newTestDatabase(t)
		db.authType = OptionValueAuthTypeAzureClientSecret
		db.accessToken = ""
		db.azureClientID = "id"

		_, err := db.resolveConnectionOptions()
		require.Error(t, err)
		assert.Contains(t, err.Error(), OptionAzureClientSecret)
	})

	// Tenant id is required by the driver (no discovery here — the caller resolves it).
	t.Run("missing tenant id", func(t *testing.T) {
		db := newTestDatabase(t)
		db.authType = OptionValueAuthTypeAzureClientSecret
		db.accessToken = ""
		db.azureClientID = "id"
		db.azureClientSecret = "secret"

		_, err := db.resolveConnectionOptions()
		require.Error(t, err)
		assert.Contains(t, err.Error(), OptionAzureTenantID)
	})
}
