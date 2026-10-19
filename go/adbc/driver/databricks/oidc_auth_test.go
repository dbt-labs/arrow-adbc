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

// TestSetOptionOIDC verifies the OIDC option keys are wired into SetOption and
// stored on the database impl. The token file path is optional.
func TestSetOptionOIDC(t *testing.T) {
	t.Run("env-oidc", func(t *testing.T) {
		db := newTestDatabase(t)

		require.NoError(t, db.SetOption(OptionAuthType, OptionValueAuthTypeEnvOIDC))
		require.NoError(t, db.SetOption(OptionOIDCClientID, "oidc-client-id"))

		assert.Equal(t, OptionValueAuthTypeEnvOIDC, db.authType)
		assert.Equal(t, "oidc-client-id", db.oidcClientID)
		assert.Empty(t, db.oidcTokenFilePath)
	})

	t.Run("file-oidc with token file path", func(t *testing.T) {
		db := newTestDatabase(t)

		require.NoError(t, db.SetOption(OptionAuthType, OptionValueAuthTypeFileOIDC))
		require.NoError(t, db.SetOption(OptionOIDCClientID, "oidc-client-id"))
		require.NoError(t, db.SetOption(OptionOIDCTokenFilePath, "/var/run/secrets/token"))

		assert.Equal(t, OptionValueAuthTypeFileOIDC, db.authType)
		assert.Equal(t, "oidc-client-id", db.oidcClientID)
		assert.Equal(t, "/var/run/secrets/token", db.oidcTokenFilePath)
	})

	t.Run("file-oidc without token file path", func(t *testing.T) {
		db := newTestDatabase(t)

		require.NoError(t, db.SetOption(OptionAuthType, OptionValueAuthTypeFileOIDC))
		require.NoError(t, db.SetOption(OptionOIDCClientID, "oidc-client-id"))

		assert.Equal(t, OptionValueAuthTypeFileOIDC, db.authType)
		assert.Equal(t, "oidc-client-id", db.oidcClientID)
		assert.Empty(t, db.oidcTokenFilePath)
	})
}

// TestResolveConnectionOptionsOIDCMissingClientID verifies that the env-oidc and
// file-oidc auth types require a client id, and error clearly otherwise. The
// validation returns before any network/SDK resolution.
func TestResolveConnectionOptionsOIDCMissingClientID(t *testing.T) {
	for _, authType := range []string{OptionValueAuthTypeEnvOIDC, OptionValueAuthTypeFileOIDC} {
		t.Run(authType, func(t *testing.T) {
			db := newTestDatabase(t)
			db.authType = authType
			db.accessToken = ""

			_, err := db.resolveConnectionOptions()
			require.Error(t, err)
			assert.Contains(t, err.Error(), authType)
			assert.Contains(t, err.Error(), OptionOIDCClientID)
		})
	}
}
