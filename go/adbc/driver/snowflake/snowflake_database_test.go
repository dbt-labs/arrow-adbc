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

package snowflake

import (
	"testing"

	"github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/require"
)

func TestSetOptionInternal_NormalizesAccount(t *testing.T) {
	db := &databaseImpl{cfg: &gosnowflake.Config{}}

	err_a := db.SetOptionInternal(OptionAccount, "my_account_name", nil)
	require.NoError(t, err_a)
	require.Equal(t, "my-account-name", db.cfg.Account)

	err_b := db.SetOptionInternal(OptionAccount, "my-account_name", nil)
	require.NoError(t, err_b)
	require.Equal(t, "my-account-name", db.cfg.Account)
}

func TestSetOptionInternal_MaxRetryCount(t *testing.T) {
	db := &databaseImpl{cfg: &gosnowflake.Config{}}

	// unset: 0 means "use the gosnowflake default"
	v, err := db.GetOption(OptionMaxRetryCount)
	require.NoError(t, err)
	require.Equal(t, "0", v)

	require.NoError(t, db.SetOptionInternal(OptionMaxRetryCount, "50", nil))
	require.Equal(t, 50, db.cfg.MaxRetryCount)
	v, err = db.GetOption(OptionMaxRetryCount)
	require.NoError(t, err)
	require.Equal(t, "50", v)

	for _, bad := range []string{"-1", "abc", "1.5", ""} {
		err := db.SetOptionInternal(OptionMaxRetryCount, bad, nil)
		require.Error(t, err, "value %q", bad)
		require.Equal(t, 50, db.cfg.MaxRetryCount, "invalid value %q must not change the setting", bad)
	}
}

func TestSetOptionInternal_WorkloadIdentity(t *testing.T) {
	db := &databaseImpl{cfg: &gosnowflake.Config{}}

	err := db.SetOptionInternal(OptionAuthType, OptionValueAuthWIF, nil)
	require.NoError(t, err)
	require.Equal(t, gosnowflake.AuthTypeWorkloadIdentityFederation, db.cfg.Authenticator)

	err = db.SetOptionInternal(OptionIdentityProvider, "AZURE", nil)
	require.NoError(t, err)
	require.Equal(t, "AZURE", db.cfg.WorkloadIdentityProvider)

	err = db.SetOptionInternal(OptionIdentityProviderEntraResource, "api://1111111-2222-3333-44444-55555555", nil)
	require.NoError(t, err)
	require.Equal(t, "api://1111111-2222-3333-44444-55555555", db.cfg.WorkloadIdentityEntraResource)
}

func TestSetOptionInternal_AuthTypeOAuthAuthorizationCode(t *testing.T) {
	db := &databaseImpl{cfg: &gosnowflake.Config{}}

	require.NoError(t, db.SetOptionInternal(OptionAuthType, OptionValueAuthOAuthAuthorizationCode, nil))
	require.Equal(t, gosnowflake.AuthTypeOAuthAuthorizationCode, db.cfg.Authenticator)

	v, err := db.GetOption(OptionAuthType)
	require.NoError(t, err)
	require.Equal(t, "OAUTH_AUTHORIZATION_CODE", v)

	require.NoError(t, db.SetOptionInternal(OptionOAuthClientID, "LOCAL_APPLICATION", nil))
	require.NoError(t, db.SetOptionInternal(OptionOAuthClientSecret, "the_secret", nil))
	require.Equal(t, "LOCAL_APPLICATION", db.cfg.OauthClientID)
	require.Equal(t, "the_secret", db.cfg.OauthClientSecret)

	v, err = db.GetOption(OptionOAuthClientID)
	require.NoError(t, err)
	require.Equal(t, "LOCAL_APPLICATION", v)
	v, err = db.GetOption(OptionOAuthClientSecret)
	require.NoError(t, err)
	require.Equal(t, "the_secret", v)
}
