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
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// databaseImpl is the internal database implementation
type databaseImpl struct {
	driverbase.DatabaseImplBase

	options map[string]string

	// Configuration
	uri         string
	authType    string
	sessionKind string
	timeout     time.Duration

	// AWS configuration (for SigV4 auth)
	awsConfig aws.Config

	// Basic auth credentials
	username string
	password string

	// HTTP client (shared across connections)
	httpClient *http.Client

	// Livy client
	livyClient *LivyClient
}

// database wraps the internal implementation
type database struct {
	driverbase.Database
}

// GetOption retrieves a database option value
func (db *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionURI:
		return db.uri, nil
	case OptionAuthType:
		return db.authType, nil
	case OptionSessionKind:
		return db.sessionKind, nil
	case OptionTimeout:
		return fmt.Sprintf("%d", int(db.timeout.Seconds())), nil
	case OptionAWSRegion:
		return db.awsConfig.Region, nil
	case OptionAWSProfile, OptionAWSAccessKeyID, OptionAWSSecretAccessKey,
		OptionAWSSessionToken, OptionUsername, OptionPassword,
		OptionExecutionRoleArn, OptionHeartbeatTimeout, OptionSessionTTL:
		// Return from options map for sensitive/optional values
		if val, ok := db.options[key]; ok {
			return val, nil
		}
		return "", nil
	default:
		// Check if it's a spark config option
		if strings.HasPrefix(key, OptionSparkPrefix) {
			if val, ok := db.options[key]; ok {
				return val, nil
			}
			return "", nil
		}
		return db.DatabaseImplBase.GetOption(key)
	}
}

// SetOption sets a database option value
func (db *databaseImpl) SetOption(key, value string) error {
	switch key {
	case OptionURI:
		db.uri = value
		db.options[key] = value
	case OptionAuthType:
		switch value {
		case AuthTypeNone, AuthTypeBasic, AuthTypeAWSSigV4:
			db.authType = value
			db.options[key] = value
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("invalid auth_type: %s (valid values: none, basic, aws_sigv4)", value),
			}
		}
	case OptionSessionKind:
		switch value {
		case SessionKindSpark, SessionKindPySpark, SessionKindSQL:
			db.sessionKind = value
			db.options[key] = value
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("invalid session_kind: %s (valid values: spark, pyspark, sql)", value),
			}
		}
	case OptionTimeout:
		timeoutSec, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("invalid timeout value: %s", value),
			}
		}
		db.timeout = time.Duration(timeoutSec) * time.Second
		db.options[key] = value
	case OptionAWSRegion, OptionAWSProfile, OptionAWSAccessKeyID,
		OptionAWSSecretAccessKey, OptionAWSSessionToken,
		OptionUsername, OptionPassword, OptionExecutionRoleArn,
		OptionHeartbeatTimeout, OptionSessionTTL:
		db.options[key] = value
	default:
		// Allow spark config options
		if strings.HasPrefix(key, OptionSparkPrefix) {
			db.options[key] = value
			return nil
		}
		return db.DatabaseImplBase.SetOption(key, value)
	}
	return nil
}

// SetOptions sets multiple options on the database before initialization
func (db *databaseImpl) SetOptions(opts map[string]string) error {
	for k, v := range opts {
		if err := db.SetOption(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Open creates a new connection
func (db *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	// Initialize database on first Open if not already initialized
	if db.livyClient == nil {
		if err := db.initialize(ctx); err != nil {
			return nil, err
		}
	}

	conn := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&db.DatabaseImplBase),
		db:                 db,
		livyClient:         db.livyClient,
	}

	// Open the connection (create Livy session)
	if err := conn.openSession(ctx); err != nil {
		return nil, err
	}

	// Build connection with proper features
	return driverbase.NewConnectionBuilder(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		Connection(), nil
}

// Close closes the database
func (db *databaseImpl) Close() error {
	// Cleanup if needed
	return nil
}

// initialize sets up the database configuration
func (db *databaseImpl) initialize(ctx context.Context) error {
	// Get required URI
	db.uri = db.options[OptionURI]
	if db.uri == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("missing required option: %s", OptionURI),
		}
	}

	// Get auth type (default: none)
	db.authType = db.options[OptionAuthType]
	if db.authType == "" {
		db.authType = AuthTypeNone
	}

	// Validate auth type
	switch db.authType {
	case AuthTypeNone, AuthTypeBasic, AuthTypeAWSSigV4:
		// Valid
	default:
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("invalid auth_type: %s (valid values: none, basic, aws_sigv4)", db.authType),
		}
	}

	// Get session kind (default: spark)
	db.sessionKind = db.options[OptionSessionKind]
	if db.sessionKind == "" {
		db.sessionKind = SessionKindSpark
	}

	// Validate session kind
	switch db.sessionKind {
	case SessionKindSpark, SessionKindPySpark, SessionKindSQL:
		// Valid
	default:
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("invalid session_kind: %s (valid values: spark, pyspark, sql)", db.sessionKind),
		}
	}

	// Get timeout (default: 120 seconds)
	db.timeout = 120 * time.Second
	if timeoutStr := db.options[OptionTimeout]; timeoutStr != "" {
		timeoutSec, err := strconv.ParseInt(timeoutStr, 10, 64)
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("invalid timeout value: %s", timeoutStr),
			}
		}
		db.timeout = time.Duration(timeoutSec) * time.Second
	}

	// Initialize HTTP client based on auth type
	switch db.authType {
	case AuthTypeAWSSigV4:
		if err := db.initializeAWS(ctx); err != nil {
			return err
		}
	case AuthTypeBasic:
		db.username = db.options[OptionUsername]
		db.password = db.options[OptionPassword]
		db.httpClient = &http.Client{Timeout: db.timeout}
	case AuthTypeNone:
		db.httpClient = &http.Client{Timeout: db.timeout}
	}

	// Create Livy client
	db.livyClient = NewLivyClient(db.uri, db.httpClient, db.authType, db.awsConfig, db.username, db.password)

	return nil
}

// initializeAWS sets up AWS configuration for SigV4 authentication
func (db *databaseImpl) initializeAWS(ctx context.Context) error {
	region := db.options[OptionAWSRegion]
	if region == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "aws_region required for aws_sigv4 auth",
		}
	}

	// Check if explicit credentials are provided
	accessKey := db.options[OptionAWSAccessKeyID]
	secretKey := db.options[OptionAWSSecretAccessKey]
	sessionToken := db.options[OptionAWSSessionToken]

	var opts []func(*config.LoadOptions) error

	// Set region
	opts = append(opts, config.WithRegion(region))

	// Set profile if specified
	if profile := db.options[OptionAWSProfile]; profile != "" {
		opts = append(opts, config.WithSharedConfigProfile(profile))
	}

	// Use explicit credentials if provided
	if accessKey != "" && secretKey != "" {
		credProvider := credentials.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken)
		opts = append(opts, config.WithCredentialsProvider(credProvider))
	}

	// Load AWS config
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  fmt.Sprintf("failed to load AWS config: %v", err),
		}
	}

	db.awsConfig = cfg

	// Create HTTP client with timeout
	db.httpClient = &http.Client{Timeout: db.timeout}

	return nil
}

// getSessionConfig builds the session configuration from options
func (db *databaseImpl) getSessionConfig() map[string]string {
	conf := make(map[string]string)

	// Extract Spark configuration options
	for k, v := range db.options {
		if strings.HasPrefix(k, OptionSparkPrefix) {
			// Strip the prefix and add to conf
			sparkKey := strings.TrimPrefix(k, OptionSparkPrefix)
			if strings.HasPrefix(sparkKey, "spark.") {
				conf[sparkKey] = v
			} else {
				conf["spark."+sparkKey] = v
			}
		}
	}

	// Add EMR Serverless execution role if specified
	if executionRole := db.options[OptionExecutionRoleArn]; executionRole != "" {
		conf["emr-serverless.session.executionRoleArn"] = executionRole
	}

	return conf
}

// getSessionOptions builds additional session options
func (db *databaseImpl) getSessionOptions() map[string]any {
	opts := make(map[string]any)

	// Add heartbeat timeout if specified
	if heartbeat := db.options[OptionHeartbeatTimeout]; heartbeat != "" {
		if val, err := strconv.ParseInt(heartbeat, 10, 32); err == nil {
			opts["heartbeatTimeoutInSecond"] = int(val)
		}
	}

	// Add TTL if specified (EMR 7.8.0+)
	if ttl := db.options[OptionSessionTTL]; ttl != "" {
		opts["ttl"] = ttl
	}

	return opts
}

// String returns a string representation of the database
func (db *databaseImpl) String() string {
	return fmt.Sprintf("SparkLivy Database [uri=%s, auth=%s, kind=%s]",
		db.uri, db.authType, db.sessionKind)
}
