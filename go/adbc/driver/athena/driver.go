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

// Package athena is an ADBC Driver Implementation for AWS Athena.
package athena

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	// OptionRegion is the AWS region (e.g. "us-east-1").
	OptionRegion = "athena.region"
	// OptionCatalog is the Glue catalog / dbt "database".
	OptionCatalog = "athena.catalog"
	// OptionSchema is the default Glue database / dbt "schema".
	OptionSchema = "athena.schema"
	// OptionS3StagingDir is the S3 output location (s3://bucket/prefix/).
	OptionS3StagingDir = "athena.s3_staging_dir"
	// OptionWorkGroup is the Athena workgroup name.
	OptionWorkGroup = "athena.work_group"
	// OptionAuthType selects the AWS authentication method.
	OptionAuthType = "athena.auth_type"
	// OptionAccessKeyID is the AWS access key ID for static credentials.
	OptionAccessKeyID = "athena.aws.access_key_id"
	// OptionSecretKey is the AWS secret access key for static credentials.
	OptionSecretKey = "athena.aws.secret_access_key"
	// OptionSessionToken is the optional AWS session token.
	OptionSessionToken = "athena.aws.session_token"
	// OptionProfileName is the named AWS profile to use.
	OptionProfileName = "athena.aws.profile"

	// AuthTypeDefault uses the default AWS credential chain (env vars, instance profile, etc.).
	AuthTypeDefault = "iam"
	// AuthTypeAccessKey uses static key/secret credentials.
	AuthTypeAccessKey = "access_key"
	// AuthTypeProfile uses a named AWS shared-config profile.
	AuthTypeProfile = "profile"
)

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver creates a new Athena ADBC driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("Athena")
	return driverbase.NewDriver(&driverImpl{
		DriverImplBase: driverbase.NewDriverImplBase(info, alloc),
	})
}

func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

func (d *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	dbBase, err := driverbase.NewDatabaseImplBase(ctx, &d.DriverImplBase)
	if err != nil {
		return nil, err
	}

	db := &databaseImpl{
		DatabaseImplBase: dbBase,
		authType:         AuthTypeDefault,
	}

	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
