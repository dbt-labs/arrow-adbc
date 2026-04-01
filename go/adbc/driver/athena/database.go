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

package athena

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	athenaSDK "github.com/aws/aws-sdk-go-v2/service/athena"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

	region       string
	catalog      string
	schema       string
	s3StagingDir string
	workGroup    string

	authType     string
	accessKeyID  string
	secretKey    string
	sessionToken string
	profileName  string
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	cfg, err := d.buildAWSConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := athenaSDK.NewFromConfig(cfg)

	conn := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		athenaClient:       client,
		db:                 d,
	}

	return driverbase.NewConnectionBuilder(conn).
		WithCurrentNamespacer(conn).
		WithTableTypeLister(conn).
		Connection(), nil
}

func (d *databaseImpl) buildAWSConfig(ctx context.Context) (aws.Config, error) {
	var opts []func(*awsconfig.LoadOptions) error

	if d.region != "" {
		opts = append(opts, awsconfig.WithRegion(d.region))
	}

	switch d.authType {
	case AuthTypeAccessKey:
		if d.accessKeyID == "" {
			return aws.Config{}, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("access key ID is required when using auth type '%s'. Set this via '%s'.", AuthTypeAccessKey, OptionAccessKeyID),
			}
		}
		if d.secretKey == "" {
			return aws.Config{}, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("secret key is required when using auth type '%s'. Set this via '%s'.", AuthTypeAccessKey, OptionSecretKey),
			}
		}
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(d.accessKeyID, d.secretKey, d.sessionToken),
		))
	case AuthTypeProfile:
		if d.profileName == "" {
			return aws.Config{}, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("profile name is required when using auth type '%s'. Set this via '%s'.", AuthTypeProfile, OptionProfileName),
			}
		}
		opts = append(opts, awsconfig.WithSharedConfigProfile(d.profileName))
	case AuthTypeDefault, "":
		// use default credential chain — no extra opts needed
	default:
		return aws.Config{}, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown auth type '%s'", d.authType),
		}
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  fmt.Sprintf("failed to build AWS config: %v", err),
		}
	}
	return cfg, nil
}

func (d *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionRegion:
		return d.region, nil
	case OptionCatalog:
		return d.catalog, nil
	case OptionSchema:
		return d.schema, nil
	case OptionS3StagingDir:
		return d.s3StagingDir, nil
	case OptionWorkGroup:
		return d.workGroup, nil
	case OptionAuthType:
		return d.authType, nil
	case OptionAccessKeyID:
		return d.accessKeyID, nil
	case OptionSecretKey:
		return d.secretKey, nil
	case OptionSessionToken:
		return d.sessionToken, nil
	case OptionProfileName:
		return d.profileName, nil
	default:
		return d.DatabaseImplBase.GetOption(key)
	}
}

func (d *databaseImpl) SetOptions(options map[string]string) error {
	for k, v := range options {
		if err := d.SetOption(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (d *databaseImpl) SetOption(key, value string) error {
	switch key {
	case OptionRegion:
		d.region = value
	case OptionCatalog:
		d.catalog = value
	case OptionSchema:
		d.schema = value
	case OptionS3StagingDir:
		d.s3StagingDir = value
	case OptionWorkGroup:
		d.workGroup = value
	case OptionAuthType:
		d.authType = value
	case OptionAccessKeyID:
		d.accessKeyID = value
	case OptionSecretKey:
		d.secretKey = value
	case OptionSessionToken:
		d.sessionToken = value
	case OptionProfileName:
		d.profileName = value
	default:
		return d.DatabaseImplBase.SetOption(key, value)
	}
	return nil
}
