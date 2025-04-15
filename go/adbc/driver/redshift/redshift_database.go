// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package redshift

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
)

type StaticAuth struct {
	access_key_id     string
	secret_access_key string
	session_token     string
}

type databaseImpl struct {
	driverbase.DatabaseImplBase

	awsRegion string

	// Dictates how we will auth to Redshift
	authType string

	secretArn string
	username  string // used for Temporary credentials

	// Dictates what type of auth to use to connect to AWS
	awsAuthType string

	// Parsed based on aws auth type
	awsCredentials string

	// For static auth
	awsStaticCredentials *StaticAuth

	clusterId string
	database  string
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	conn := &connectionImpl{
		ConnectionImplBase:   driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		authType:             d.authType,
		awsAuthType:          d.awsAuthType,
		awsCredentials:       d.awsCredentials,
		awsRegion:            d.awsRegion,
		awsStaticCredentials: d.awsStaticCredentials,
		secretArn:            d.secretArn,
		username:             d.username,
		clusterId:            d.clusterId,
		database:             d.database,
	}

	err := conn.newClient(ctx)
	if err != nil {
		return nil, err
	}

	return driverbase.NewConnectionBuilder(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		WithTableTypeLister(conn).
		WithDbObjectsEnumerator(conn).
		Connection(), nil
}

func (d *databaseImpl) SetOption(key string, value string) error {
	if d.awsStaticCredentials == nil {
		d.awsStaticCredentials = &StaticAuth{}
	}
	switch key {
	case OptionStringAWSAuthType:
		switch value {
		case OptionValueAWSAuthTypeDefault:
			d.awsAuthType = value
		case OptionValueAWSAuthTypeSharedConfigProfile:
			d.awsAuthType = value
		case OptionValueAWSAuthTypeSharedConfigFile:
			d.awsAuthType = value
		case OptionValueAWSAuthTypeSharedCredentialsFile:
			d.awsAuthType = value
		case OptionValueAWSAuthTypeStaticCredentials:
			d.awsAuthType = value
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("unknown aws auth type value `%s`", value),
			}
		}
	case OptionStringAuthType:
		switch value {
		case OptionValueAuthTypeSecretsManager:
			d.authType = value
		case OptionValueAuthTypeTemporary:
			d.authType = value
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("unknown database auth type value `%s`", value),
			}
		}
	case OptionStringAWSAuthCredentials:
		d.awsCredentials = value
	case OptionStringAWSAuthAccessKeyID:
		d.awsStaticCredentials.access_key_id = value
	case OptionStringAWSAuthSecretAccessKey:
		d.awsStaticCredentials.secret_access_key = value
	case OptionStringAWSAuthSessionToken:
		d.awsStaticCredentials.session_token = value
	case OptionStringAWSRegion:
		d.awsRegion = value
	case OptionStringUsername:
		d.username = value
	case OptionStringSecretArn:
		d.secretArn = value
	case OptionStringClusterId:
		d.clusterId = value
	case OptionStringDatabase:
		d.database = value
	default:
		return d.DatabaseImplBase.SetOption(key, value)
	}
	return nil
}

func (d *databaseImpl) SetOptions(options map[string]string) error {
	for k, v := range options {
		err := d.SetOption(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}
