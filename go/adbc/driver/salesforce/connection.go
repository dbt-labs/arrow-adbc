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
	"strconv"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/pkg"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	// Authentication settings
	authType      string
	loginURL      string
	version       string
	jwtClientID   string
	jwtUsername   string
	jwtPrivateKey string
	username      string
	password      string
	clientID      string
	clientSecret  string
	instanceURL   string
	queryRowLimit string
	queryTimeout  string

	// Salesforce client
	client *api.Client
	token  *api.Token
}

func (c *connectionImpl) newClient(ctx context.Context) error {
	switch c.authType {
	case OptionValueAuthTypeJWT:
		return c.setupJWTAuth(ctx)
	case OptionValueAuthTypeDefault:
		return c.setupUsernamePasswordAuth(ctx)
	default:
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unsupported auth type: %s", c.authType),
		}
	}
}

func (c *connectionImpl) setupJWTAuth(ctx context.Context) error {
	if c.jwtClientID == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "JWT client ID is required for JWT authentication",
		}
	}
	if c.jwtUsername == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "JWT username is required for JWT authentication",
		}
	}
	if c.jwtPrivateKey == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "JWT private key is required for JWT authentication",
		}
	}

	loginURL := c.loginURL
	if loginURL == "" {
		loginURL = DefaultLoginURL
	}

	config, err := api.NewJWTConfig(loginURL, c.jwtClientID, c.jwtUsername, c.jwtPrivateKey)
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  fmt.Sprintf("failed to create JWT config: %v", err),
		}
	}

	c.client = api.NewClient(config)

	// Authenticate and get token
	token, err := c.client.Authenticate(ctx)
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  fmt.Sprintf("JWT authentication failed: %v", err),
		}
	}

	c.token = token

	// Try to get CDP token for Data Cloud access
	cdpToken, err := c.client.GetDataCloudToken(ctx, token.InstanceURL, token.AccessToken)
	if err != nil {
		// CDP token is optional - use the regular token if CDP is not available
		c.token = token
	} else {
		c.token = cdpToken
	}

	return nil
}

func (c *connectionImpl) setupUsernamePasswordAuth(ctx context.Context) error {
	if c.username == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "Username is required for username/password authentication",
		}
	}
	if c.password == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "Password is required for username/password authentication",
		}
	}
	if c.clientID == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "Client ID is required for username/password authentication",
		}
	}
	if c.clientSecret == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "Client secret is required for username/password authentication",
		}
	}

	loginURL := c.loginURL
	if loginURL == "" {
		loginURL = DefaultLoginURL
	}

	config := api.NewUsernamePasswordConfig(loginURL, c.clientID, c.clientSecret, c.username, c.password)

	c.client = api.NewClient(config)

	// Authenticate and get token
	token, err := c.client.Authenticate(ctx)
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  fmt.Sprintf("username/password authentication failed: %v", err),
		}
	}

	c.token = token

	// Try to get CDP token for Data Cloud access
	cdpToken, err := c.client.GetDataCloudToken(ctx, token.InstanceURL, token.AccessToken)
	if err != nil {
		// CDP token is optional - use the regular token if CDP is not available
		c.token = token
	} else {
		c.token = cdpToken
	}

	return nil
}

// Autocommit support
func (c *connectionImpl) GetAutocommit() bool {
	// Salesforce Data Cloud doesn't have traditional transactions
	return true
}

func (c *connectionImpl) SetAutocommit(enabled bool) error {
	if !enabled {
		return adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  "Salesforce Data Cloud does not support manual transaction management",
		}
	}
	return nil
}

// Current namespace support (for catalog/schema)
func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	// Salesforce doesn't have a traditional catalog concept
	return "", nil
}

func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	// Salesforce doesn't have a traditional schema concept
	return "", nil
}

func (c *connectionImpl) SetCurrentCatalog(catalog string) error {
	// Salesforce doesn't support setting catalogs
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Salesforce does not support catalog operations",
	}
}

func (c *connectionImpl) SetCurrentDbSchema(schema string) error {
	// Salesforce doesn't support setting schemas
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Salesforce does not support schema operations",
	}
}

// Table type listing
func (c *connectionImpl) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	// Salesforce has tables, views, etc. - implement basic types
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "GetTableTypes not yet implemented for Salesforce",
	}
}

func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	// Salesforce Data Cloud table types
	return []string{"TABLE", "VIEW"}, nil
}

// Database objects enumeration
func (c *connectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (array.RecordReader, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "GetObjects not yet implemented for Salesforce",
	}
}

func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	// Salesforce doesn't have catalogs in the traditional sense
	return []string{}, nil
}

func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	// Salesforce doesn't have schemas in the traditional sense
	return []string{}, nil
}

func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	// For full implementation, would query Salesforce metadata API
	return []driverbase.TableInfo{}, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "GetTablesForDBSchema not yet implemented for Salesforce",
	}
}

// Helper function to parse query timeout
func (c *connectionImpl) getQueryTimeout() time.Duration {
	if c.queryTimeout == "" {
		return 30 * time.Second // default timeout
	}

	if timeout, err := strconv.Atoi(c.queryTimeout); err == nil {
		return time.Duration(timeout) * time.Second
	}

	return 30 * time.Second // fallback to default
}

// Helper function to parse row limit
func (c *connectionImpl) getQueryRowLimit() *int64 {
	if c.queryRowLimit == "" {
		return nil // no limit
	}

	if limit, err := strconv.ParseInt(c.queryRowLimit, 10, 64); err == nil {
		return &limit
	}

	return nil // fallback to no limit
}

// Base returns the underlying ConnectionImplBase
func (c *connectionImpl) Base() *driverbase.ConnectionImplBase {
	return &c.ConnectionImplBase
}
