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

package salesforce

import (
	"runtime/debug"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	// Authentication options
	OptionStringAuthType       = "adbc.salesforce.sql.auth_type"
	OptionValueAuthTypeDefault = "adbc.salesforce.sql.auth_type.default"
	OptionValueAuthTypeJWT     = "adbc.salesforce.sql.auth_type.jwt"

	// JWT Authentication options
	OptionStringJWTClientID   = "adbc.salesforce.sql.jwt.client_id"
	OptionStringJWTUsername   = "adbc.salesforce.sql.jwt.username"
	OptionStringJWTPrivateKey = "adbc.salesforce.sql.jwt.private_key"
	OptionStringJWTLoginURL   = "adbc.salesforce.sql.jwt.login_url"

	// Username/Password Authentication options
	OptionStringUsername     = "adbc.salesforce.sql.username"
	OptionStringPassword     = "adbc.salesforce.sql.password"
	OptionStringClientID     = "adbc.salesforce.sql.client_id"
	OptionStringClientSecret = "adbc.salesforce.sql.client_secret"

	// Connection options
	OptionStringInstanceURL = "adbc.salesforce.sql.instance_url"
	OptionStringVersion     = "adbc.salesforce.sql.version"

	// Query options
	OptionStringQueryRowLimit = "adbc.salesforce.sql.query.row_limit"
	OptionStringQueryTimeout  = "adbc.salesforce.sql.query.timeout"

	// Default values
	DefaultLoginURL = "https://login.salesforce.com"
	DefaultVersion  = "v59.0"
)

// Driver implements the ADBC Driver interface for Salesforce Data Cloud
type Driver struct {
	allocator memory.Allocator
}

var (
	infoVendorVersion string
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch {
			case dep.Path == "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce":
				infoVendorVersion = dep.Version
			}
		}
	}
}

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver creates a new Salesforce driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("Salesforce")
	if infoVendorVersion != "" {
		if err := info.RegisterInfoCode(adbc.InfoVendorVersion, infoVendorVersion); err != nil {
			panic(err)
		}
	}
	return driverbase.NewDriver(&driverImpl{
		DriverImplBase: driverbase.NewDriverImplBase(info, alloc),
	})
}

// NewDatabase creates a new database connection for Salesforce Data Cloud
func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	db := &databaseImpl{
		DatabaseImplBase: driverbase.NewDatabaseImplBase(&d.DriverImplBase),
		authType:         OptionValueAuthTypeDefault,
		loginURL:         DefaultLoginURL,
		version:          DefaultVersion,
	}
	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
