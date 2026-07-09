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

package redshift

import (
	"runtime/debug"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	OptionStringAWSRegion = "adbc.redshift.aws.region"

	// Auth: https://docs.aws.amazon.com/redshift/latest/mgmt/data-api-access.html
	OptionStringAuthType              = "adbc.redshift.auth_type"
	OptionValueAuthTypeSecretsManager = "adbc.redshift.auth_type.secrets_manager"
	OptionValueAuthTypeTemporary      = "adbc.redshift.auth_type.temporary"

	// For use with temporary credentials
	OptionStringUsername = "adbc.redshift.auth_type.temporary.username"
	// For use with AWS SM credentials: https://docs.aws.amazon.com/redshift/latest/mgmt/data-api-secrets.html
	OptionStringSecretArn = "adbc.redshift.auth_type.secrets_manager.secret_arn"

	// AWS Auth: https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-gosdk.html#specifying-credentials
	OptionStringAWSAuthType                     = "adbc.redshift.aws.auth_type"
	OptionValueAWSAuthTypeDefault               = "adbc.redshift.aws.auth_type.default"
	OptionValueAWSAuthTypeSharedConfigProfile   = "adbc.redshift.aws.auth_type.shared_profile"
	OptionValueAWSAuthTypeSharedConfigFile      = "adbc.redshift.aws.auth_type.shared_config_file"
	OptionValueAWSAuthTypeSharedCredentialsFile = "adbc.redshift.aws.auth_type.shared_credentials_file"
	OptionValueAWSAuthTypeStaticCredentials     = "adbc.redshift.aws.auth_type.static_credentials"

	// Profiles + Files read depending on type
	OptionStringAWSAuthCredentials = "adbc.redshift.aws.auth_credentials"

	// Used for Static AWS Auth (Be careful using this!)
	OptionStringAWSAuthAccessKeyID     = "adbc.redshift.access_key_id"
	OptionStringAWSAuthSecretAccessKey = "adbc.redshift.secret_access_key"
	OptionStringAWSAuthSessionToken    = "adbc.redshift.session_token"

	// IAM Identity Center external OAuth (trusted identity propagation).
	//
	// With this AWS auth type the driver is given an external IdP-issued OIDC
	// JWT (e.g. from Okta/Entra, brokered by dbt), exchanges it for
	// identity-enhanced AWS credentials via IAM Identity Center, and uses those
	// credentials for the Redshift Data API. This mirrors dbt-redshift's
	// `oauth_token_identity_center` / redshift_connector `IdpTokenAuthPlugin`
	// (token_type=EXT_JWT) flow.
	OptionValueAWSAuthTypeIdentityCenterToken = "adbc.redshift.aws.auth_type.identity_center_token"

	// The external IdP-issued OIDC token (EXT_JWT) to exchange for AWS
	// Identity Center credentials.
	OptionStringIdCToken = "adbc.redshift.idc.token"
	// The type of the supplied IdC token. Only EXT_JWT is supported today,
	// matching dbt-redshift/redshift_connector.
	OptionStringIdCTokenType = "adbc.redshift.idc.token_type"
	// The IAM Identity Center application's OIDC client ID (the trusted token
	// issuer audience registered with Identity Center) used for the exchange.
	OptionStringIdCClientId = "adbc.redshift.idc.client_id"
	// The IAM role ARN to assume with the Identity Center identity context to
	// obtain identity-enhanced AWS credentials for the Redshift Data API.
	OptionStringIdCRoleArn = "adbc.redshift.idc.role_arn"
	// Optional region override for the Identity Center (ssooidc) endpoint.
	// Defaults to the driver's AWS region when unset.
	OptionStringIdCRegion = "adbc.redshift.idc.region"

	// Only EXT_JWT is currently supported for the IdC token flow.
	OptionValueIdCTokenTypeExtJWT = "EXT_JWT"

	OptionStringClusterId = "adbc.redshift.cluster_id"
	OptionStringDatabase  = "adbc.redshift.database"
)

var (
	infoVendorVersion string
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch {
			case dep.Path == "github.com/aws/aws-sdk-go-v2/service/redshiftdata":
				infoVendorVersion = dep.Version
			}
		}
	}
}

type driverImpl struct {
	driverbase.DriverImplBase
}

func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("Redshift")
	if infoVendorVersion != "" {
		if err := info.RegisterInfoCode(adbc.InfoVendorVersion, infoVendorVersion); err != nil {
			panic(err)
		}
	}
	return driverbase.NewDriver(&driverImpl{
		DriverImplBase: driverbase.NewDriverImplBase(info, alloc),
	})
}

func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	db := &databaseImpl{
		DatabaseImplBase: driverbase.NewDatabaseImplBase(&d.DriverImplBase),
	}
	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
