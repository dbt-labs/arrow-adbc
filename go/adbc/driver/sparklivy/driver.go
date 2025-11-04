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

package sparklivy

import (
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	// OptionURI is the Livy endpoint URL (required)
	OptionURI = "adbc.sparklivy.uri"

	// OptionAuthType specifies the authentication method
	// Valid values: aws_sigv4, basic, none
	OptionAuthType = "adbc.sparklivy.auth_type"

	// OptionSessionKind specifies the Livy session type
	// Valid values: spark, pyspark
	// Default: spark
	OptionSessionKind = "adbc.sparklivy.session_kind"

	// OptionTimeout specifies the HTTP request timeout in seconds
	OptionTimeout = "adbc.sparklivy.timeout"

	// AWS Authentication Options (when auth_type=aws_sigv4)

	// OptionAWSRegion specifies the AWS region (required for aws_sigv4)
	OptionAWSRegion = "adbc.sparklivy.aws_region"

	// OptionAWSProfile specifies the AWS profile name
	OptionAWSProfile = "adbc.sparklivy.aws_profile"

	// OptionAWSAccessKeyID specifies explicit AWS access key
	OptionAWSAccessKeyID = "adbc.sparklivy.aws_access_key_id"

	// OptionAWSSecretAccessKey specifies explicit AWS secret key
	OptionAWSSecretAccessKey = "adbc.sparklivy.aws_secret_access_key"

	// OptionAWSSessionToken specifies AWS session token for temporary credentials
	OptionAWSSessionToken = "adbc.sparklivy.aws_session_token"

	// Basic Authentication Options (when auth_type=basic)

	// OptionUsername specifies the username for basic auth
	OptionUsername = "adbc.sparklivy.username"

	// OptionPassword specifies the password for basic auth
	OptionPassword = "adbc.sparklivy.password"

	// EMR Serverless Options

	// OptionExecutionRoleArn specifies the EMR Serverless execution role ARN
	// This is required when connecting to EMR Serverless
	OptionExecutionRoleArn = "adbc.sparklivy.emr-serverless.session.executionRoleArn"

	// OptionHeartbeatTimeout specifies the session heartbeat timeout in seconds
	OptionHeartbeatTimeout = "adbc.sparklivy.heartbeatTimeoutInSecond"

	// OptionSessionTTL specifies the session time-to-live (e.g., "2h", "30m")
	// Available in EMR 7.8.0+
	OptionSessionTTL = "adbc.sparklivy.ttl"

	// Spark Configuration Prefix
	// Options starting with this prefix are passed to the Livy session as Spark configuration
	// Example: adbc.sparklivy.spark.executor.memory=4g -> spark.executor.memory=4g
	OptionSparkPrefix = "adbc.sparklivy.spark."
)

// Authentication types
const (
	AuthTypeNone     = "none"
	AuthTypeBasic    = "basic"
	AuthTypeAWSSigV4 = "aws_sigv4"
)

// Session kinds
const (
	SessionKindSpark   = "spark"
	SessionKindPySpark = "pyspark"
	SessionKindSQL     = "sql"
)

// driverImpl is the internal driver implementation
type driverImpl struct {
	driverbase.DriverImplBase
}

// driver wraps the internal implementation
type driver struct {
	driverbase.Driver
}

// NewDriver creates a new Spark Livy ADBC driver
func NewDriver(alloc memory.Allocator) adbc.Driver {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}

	info := driverbase.DefaultDriverInfo("Spark Livy")
	info.RegisterInfoCode(adbc.InfoDriverName, "ADBC Spark Livy Driver")
	info.RegisterInfoCode(adbc.InfoDriverArrowVersion, "18.0.0")
	info.RegisterInfoCode(adbc.InfoVendorName, "Apache Spark via Apache Livy")

	return driverbase.NewDriver(&driverImpl{
		DriverImplBase: driverbase.NewDriverImplBase(info, alloc),
	})
}

// NewDatabase creates a new database instance
func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	db := &databaseImpl{
		DatabaseImplBase: driverbase.NewDatabaseImplBase(d.Base()),
		options:          make(map[string]string),
	}

	// Copy options
	for k, v := range opts {
		db.options[k] = v
	}

	return driverbase.NewDatabase(db), nil
}
