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
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

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

	client *redshiftdata.Client
}

func (c *connectionImpl) newClient(ctx context.Context) error {
	var authOption config.LoadOptionsFunc = nil
	switch c.awsAuthType {
	case OptionValueAWSAuthTypeSharedConfigProfile:
		authOption = config.WithSharedConfigProfile(c.awsCredentials)
	case OptionValueAWSAuthTypeSharedConfigFile:
		authOption = config.WithSharedConfigFiles([]string{c.awsCredentials})
	case OptionValueAWSAuthTypeSharedCredentialsFile:
		authOption = config.WithSharedCredentialsFiles([]string{c.awsCredentials})
	case OptionValueAWSAuthTypeStaticCredentials:
		authOption = config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				c.awsStaticCredentials.access_key_id,
				c.awsStaticCredentials.secret_access_key,
				*aws.String(c.awsStaticCredentials.session_token),
			))
	}
	cfg, err := config.LoadDefaultConfig(
		ctx,
		authOption,
		config.WithRegion(c.awsRegion),
	)
	if err != nil {
		return err
	}
	c.client = redshiftdata.NewFromConfig(cfg)
	return nil
}

func (c *connectionImpl) Close() error {
	c.client = nil
	c.Closed = true
	return nil
}

func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	return &statement{
		alloc: c.Alloc,
		cnxn:  c,
		input: &redshiftdata.ExecuteStatementInput{
			ClusterIdentifier: &c.clusterId,
			Database:          &c.database,
			DbUser:            &c.username,
		},
	}, nil
}

func (c *connectionImpl) Commit(ctx context.Context) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Commit not yet implemented for Redshift",
	}
}

func (c *connectionImpl) Rollback(ctx context.Context) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Rollback not yet implemented for Redshift",
	}
}

func (c *connectionImpl) SetAutocommit(enabled bool) error {
	if !enabled {
		return adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  "Disabling autocommit is not supported in Redshift driver",
		}
	}
	return nil
}

func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	return c.database, nil
}

func (c *connectionImpl) SetCurrentCatalog(value string) error {
	c.database = value
	return nil
}

func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	// Redshift defaults to 'public' if not specified
	return "public", nil
}

func (c *connectionImpl) SetCurrentDbSchema(value string) error {
	return nil
}

func (c *connectionImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionStringAWSRegion:
		return c.awsRegion, nil
	case OptionStringDatabase:
		return c.database, nil
	case OptionStringClusterId:
		return c.clusterId, nil
	default:
		return "", adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("Option '%s' not supported", key),
		}
	}
}

func (c *connectionImpl) SetOption(key, value string) error {
	switch key {
	case OptionStringAWSRegion:
		c.awsRegion = value
	case OptionStringDatabase:
		c.database = value
	case OptionStringClusterId:
		c.clusterId = value
	default:
		return adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("Option '%s' not supported", key),
		}
	}
	return nil
}

func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	return []string{c.database}, nil
}

func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	return []string{"public"}, nil
}

func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "GetTablesForDBSchema not yet implemented for Redshift",
	}
}

func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{
		"TABLE",
		"VIEW",
	}, nil
}

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "GetTableSchema not yet implemented for Redshift",
	}
}
