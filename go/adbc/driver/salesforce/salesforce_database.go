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
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	// Authentication settings
	authType string
	loginURL string
	version  string

	// JWT Authentication
	jwtClientID   string
	jwtUsername   string
	jwtPrivateKey string

	// Username/Password Authentication
	username     string
	password     string
	clientID     string
	clientSecret string

	// Connection settings
	instanceURL string

	// Query settings
	queryRowLimit string
	queryTimeout  string
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	conn := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),

		// Copy authentication settings
		authType:      d.authType,
		loginURL:      d.loginURL,
		version:       d.version,
		jwtClientID:   d.jwtClientID,
		jwtUsername:   d.jwtUsername,
		jwtPrivateKey: d.jwtPrivateKey,
		username:      d.username,
		password:      d.password,
		clientID:      d.clientID,
		clientSecret:  d.clientSecret,
		instanceURL:   d.instanceURL,
		queryRowLimit: d.queryRowLimit,
		queryTimeout:  d.queryTimeout,
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

func (d *databaseImpl) Close() error { return nil }

func (d *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionStringAuthType:
		return d.authType, nil
	case OptionStringJWTClientID:
		return d.jwtClientID, nil
	case OptionStringJWTUsername:
		return d.jwtUsername, nil
	case OptionStringJWTLoginURL:
		return d.loginURL, nil
	case OptionStringUsername:
		return d.username, nil
	case OptionStringClientID:
		return d.clientID, nil
	case OptionStringInstanceURL:
		return d.instanceURL, nil
	case OptionStringVersion:
		return d.version, nil
	case OptionStringQueryRowLimit:
		return d.queryRowLimit, nil
	case OptionStringQueryTimeout:
		return d.queryTimeout, nil
	default:
		return d.DatabaseImplBase.GetOption(key)
	}
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

func (d *databaseImpl) SetOption(key string, value string) error {
	switch key {
	case OptionStringAuthType:
		switch value {
		case OptionValueAuthTypeDefault, OptionValueAuthTypeJWT:
			d.authType = value
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("unknown auth type value `%s`", value),
			}
		}
	case OptionStringJWTClientID:
		d.jwtClientID = value
	case OptionStringJWTUsername:
		d.jwtUsername = value
	case OptionStringJWTPrivateKey:
		d.jwtPrivateKey = value
	case OptionStringJWTLoginURL:
		d.loginURL = value
	case OptionStringUsername:
		d.username = value
	case OptionStringPassword:
		d.password = value
	case OptionStringClientID:
		d.clientID = value
	case OptionStringClientSecret:
		d.clientSecret = value
	case OptionStringInstanceURL:
		d.instanceURL = value
	case OptionStringVersion:
		d.version = value
	case OptionStringQueryRowLimit:
		d.queryRowLimit = value
	case OptionStringQueryTimeout:
		d.queryTimeout = value
	default:
		return d.DatabaseImplBase.SetOption(key, value)
	}
	return nil
}
