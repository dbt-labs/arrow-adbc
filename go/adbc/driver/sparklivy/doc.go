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

// Package sparklivy implements an ADBC driver for Apache Spark via Apache Livy REST API.
//
// This driver enables connectivity to Spark clusters through Livy, with specific support
// for AWS EMR Serverless. It supports AWS SigV4 authentication for EMR Serverless and
// basic authentication for other Livy deployments.
//
// # Connection Options
//
// The following options are supported for the driver:
//
//   - adbc.sparklivy.uri: Livy endpoint URL (required)
//   - adbc.sparklivy.auth_type: Authentication type (aws_sigv4, basic, none)
//   - adbc.sparklivy.session_kind: Session type (spark, pyspark)
//
// AWS Authentication Options (when auth_type=aws_sigv4):
//
//   - adbc.sparklivy.aws_region: AWS region (required)
//   - adbc.sparklivy.aws_profile: AWS profile name
//   - adbc.sparklivy.aws_access_key_id: Explicit AWS access key
//   - adbc.sparklivy.aws_secret_access_key: Explicit AWS secret key
//   - adbc.sparklivy.aws_session_token: AWS session token
//
// Basic Authentication Options (when auth_type=basic):
//
//   - adbc.sparklivy.username: Username
//   - adbc.sparklivy.password: Password
//
// EMR Serverless Options:
//
//   - adbc.sparklivy.emr-serverless.session.executionRoleArn: Execution role ARN (required for EMR Serverless)
//   - adbc.sparklivy.heartbeatTimeoutInSecond: Session heartbeat timeout
//   - adbc.sparklivy.ttl: Session time-to-live (EMR 7.8.0+)
//
// Spark Configuration (passed to Livy session):
//
//   - adbc.sparklivy.spark.*: Any Spark configuration option
//
// Example:
//
//	import (
//	    "context"
//	    "github.com/apache/arrow-adbc/go/adbc"
//	    "github.com/apache/arrow-adbc/go/adbc/driver/sparklivy"
//	)
//
//	func main() {
//	    opts := map[string]string{
//	        "adbc.sparklivy.uri":         "https://app-id.livy.emr-serverless-services.us-east-1.amazonaws.com",
//	        "adbc.sparklivy.auth_type":   "aws_sigv4",
//	        "adbc.sparklivy.aws_region":  "us-east-1",
//	        "adbc.sparklivy.emr-serverless.session.executionRoleArn": "arn:aws:iam::123456789012:role/EMRServerlessRole",
//	        "adbc.sparklivy.spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
//	    }
//
//	    drv := sparklivy.NewDriver()
//	    db, err := drv.NewDatabase(opts)
//	    if err != nil {
//	        panic(err)
//	    }
//	    defer db.Close()
//
//	    cnxn, err := db.Open(context.Background())
//	    if err != nil {
//	        panic(err)
//	    }
//	    defer cnxn.Close()
//
//	    stmt, err := cnxn.NewStatement()
//	    if err != nil {
//	        panic(err)
//	    }
//	    defer stmt.Close()
//
//	    stmt.SetSqlQuery("SELECT * FROM my_table")
//	    reader, _, err := stmt.ExecuteQuery(context.Background())
//	    if err != nil {
//	        panic(err)
//	    }
//	    defer reader.Release()
//
//	    for reader.Next() {
//	        // Process records
//	    }
//	}
package sparklivy
