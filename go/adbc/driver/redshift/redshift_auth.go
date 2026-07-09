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
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssooidc"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	ststypes "github.com/aws/aws-sdk-go-v2/service/sts/types"
)

const (
	// RFC 8693 OAuth 2.0 Token Exchange grant type, used to exchange the
	// external IdP JWT for an IAM Identity Center identity context.
	idcTokenExchangeGrantType = "urn:ietf:params:oauth:grant-type:token-exchange"
	// Subject token type for an OIDC JWT (RFC 8693).
	idcSubjectTokenTypeJWT = "urn:ietf:params:oauth:token-type:jwt"
	// The AWS-managed context provider ARN for IAM Identity Center trusted
	// identity propagation, supplied to sts:AssumeRole via ProvidedContexts.
	idcContextProviderArn = "arn:aws:iam::aws:contextProvider/IdentityCenter"
	// Session name used for the identity-enhanced assumed-role session.
	idcRoleSessionName = "adbc-redshift-identity-center"
)

// validate ensures the required Identity Center parameters are present.
func (a *IdentityCenterAuth) validate() error {
	missing := func(field string) error {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("IAM Identity Center auth requires `%s` to be set", field),
		}
	}
	if a == nil || a.token == "" {
		return missing(OptionStringIdCToken)
	}
	// token_type defaults to EXT_JWT; if set it is validated at option time too.
	if a.tokenType != "" && a.tokenType != OptionValueIdCTokenTypeExtJWT {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg: fmt.Sprintf(
				"unsupported IdC token type `%s`; only `%s` is supported",
				a.tokenType, OptionValueIdCTokenTypeExtJWT),
		}
	}
	if a.clientID == "" {
		return missing(OptionStringIdCClientId)
	}
	if a.roleArn == "" {
		return missing(OptionStringIdCRoleArn)
	}
	return nil
}

// resolveRegion returns the region for the Identity Center endpoints, falling
// back to the driver's AWS region.
func (a *IdentityCenterAuth) resolveRegion(fallback string) string {
	if a.region != "" {
		return a.region
	}
	return fallback
}

// idcAssumeRoleProvider is an aws.CredentialsProvider that produces
// identity-enhanced credentials by assuming a role with an IAM Identity Center
// identity context. Wrapping it in aws.NewCredentialsCache lets long-lived
// connections transparently refresh the assumed-role session on expiry.
type idcAssumeRoleProvider struct {
	stsClient       *sts.Client
	roleArn         string
	identityContext string
}

func (p *idcAssumeRoleProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	out, err := p.stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(p.roleArn),
		RoleSessionName: aws.String(idcRoleSessionName),
		ProvidedContexts: []ststypes.ProvidedContext{{
			ProviderArn:      aws.String(idcContextProviderArn),
			ContextAssertion: aws.String(p.identityContext),
		}},
	})
	if err != nil {
		return aws.Credentials{}, adbc.Error{
			Code: adbc.StatusUnauthenticated,
			Msg:  fmt.Sprintf("IAM Identity Center AssumeRole failed: %s", err.Error()),
		}
	}
	c := out.Credentials
	if c == nil {
		return aws.Credentials{}, adbc.Error{
			Code: adbc.StatusUnauthenticated,
			Msg:  "IAM Identity Center AssumeRole returned no credentials",
		}
	}
	return aws.Credentials{
		AccessKeyID:     aws.ToString(c.AccessKeyId),
		SecretAccessKey: aws.ToString(c.SecretAccessKey),
		SessionToken:    aws.ToString(c.SessionToken),
		Source:          "RedshiftIdentityCenter",
		CanExpire:       c.Expiration != nil,
		Expires:         aws.ToTime(c.Expiration),
	}, nil
}

// identityCenterCredentialsProvider implements the IAM Identity Center trusted
// identity propagation flow for programmatic access:
//
//  1. Exchange the external IdP JWT (EXT_JWT) for an Identity Center identity
//     context via ssooidc:CreateTokenWithIAM (RFC 8693 token exchange).
//  2. Assume the configured IAM role, passing the identity context through
//     sts:AssumeRole ProvidedContexts, to obtain identity-enhanced AWS
//     credentials.
//
// The resulting credentials carry the user's Identity Center identity and can
// be used with the Redshift Data API when the cluster is configured for IAM
// Identity Center. This mirrors dbt-redshift's `oauth_token_identity_center`
// (redshift_connector `IdpTokenAuthPlugin`, token_type=EXT_JWT) flow.
func (c *connectionImpl) identityCenterCredentialsProvider(ctx context.Context) (aws.CredentialsProvider, error) {
	if err := c.idcAuth.validate(); err != nil {
		return nil, err
	}
	region := c.idcAuth.resolveRegion(c.awsRegion)

	// Base config only reaches the public ssooidc/sts endpoints; the token
	// exchange itself is authenticated by the external JWT, not AWS creds.
	baseCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, err
	}

	oidcClient := ssooidc.NewFromConfig(baseCfg)
	tokenOut, err := oidcClient.CreateTokenWithIAM(ctx, &ssooidc.CreateTokenWithIAMInput{
		ClientId:         aws.String(c.idcAuth.clientID),
		GrantType:        aws.String(idcTokenExchangeGrantType),
		SubjectToken:     aws.String(c.idcAuth.token),
		SubjectTokenType: aws.String(idcSubjectTokenTypeJWT),
	})
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusUnauthenticated,
			Msg:  fmt.Sprintf("IAM Identity Center token exchange failed: %s", err.Error()),
		}
	}
	if tokenOut.AwsAdditionalDetails == nil || aws.ToString(tokenOut.AwsAdditionalDetails.IdentityContext) == "" {
		return nil, adbc.Error{
			Code: adbc.StatusUnauthenticated,
			Msg:  "IAM Identity Center token exchange returned no identity context",
		}
	}

	provider := &idcAssumeRoleProvider{
		stsClient:       sts.NewFromConfig(baseCfg),
		roleArn:         c.idcAuth.roleArn,
		identityContext: aws.ToString(tokenOut.AwsAdditionalDetails.IdentityContext),
	}
	return aws.NewCredentialsCache(provider), nil
}
