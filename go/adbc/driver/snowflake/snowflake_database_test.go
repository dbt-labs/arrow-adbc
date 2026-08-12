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
