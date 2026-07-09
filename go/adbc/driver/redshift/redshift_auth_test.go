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
	"testing"
)

func newTestDatabase() *databaseImpl {
	return &databaseImpl{}
}

func TestSetOptionIdentityCenterAuthType(t *testing.T) {
	d := newTestDatabase()
	if err := d.SetOption(OptionStringAWSAuthType, OptionValueAWSAuthTypeIdentityCenterToken); err != nil {
		t.Fatalf("expected identity_center_token aws auth type to be accepted, got: %v", err)
	}
	if d.awsAuthType != OptionValueAWSAuthTypeIdentityCenterToken {
		t.Fatalf("awsAuthType = %q, want %q", d.awsAuthType, OptionValueAWSAuthTypeIdentityCenterToken)
	}
}

func TestSetOptionIdCParametersParsed(t *testing.T) {
	d := newTestDatabase()
	opts := map[string]string{
		OptionStringIdCToken:     "ext-jwt-value",
		OptionStringIdCTokenType: OptionValueIdCTokenTypeExtJWT,
		OptionStringIdCClientId:  "client-123",
		OptionStringIdCRoleArn:   "arn:aws:iam::123456789012:role/redshift-idc",
		OptionStringIdCRegion:    "us-east-2",
	}
	if err := d.SetOptions(opts); err != nil {
		t.Fatalf("SetOptions returned error: %v", err)
	}
	if d.idcAuth == nil {
		t.Fatal("expected idcAuth to be initialized")
	}
	if d.idcAuth.token != "ext-jwt-value" {
		t.Errorf("token = %q, want %q", d.idcAuth.token, "ext-jwt-value")
	}
	if d.idcAuth.tokenType != OptionValueIdCTokenTypeExtJWT {
		t.Errorf("tokenType = %q, want %q", d.idcAuth.tokenType, OptionValueIdCTokenTypeExtJWT)
	}
	if d.idcAuth.clientID != "client-123" {
		t.Errorf("clientID = %q, want %q", d.idcAuth.clientID, "client-123")
	}
	if d.idcAuth.roleArn != "arn:aws:iam::123456789012:role/redshift-idc" {
		t.Errorf("roleArn = %q", d.idcAuth.roleArn)
	}
	if d.idcAuth.region != "us-east-2" {
		t.Errorf("region = %q, want %q", d.idcAuth.region, "us-east-2")
	}
}

func TestSetOptionIdCTokenTypeRejectsUnsupported(t *testing.T) {
	d := newTestDatabase()
	err := d.SetOption(OptionStringIdCTokenType, "ACCESS_TOKEN")
	if err == nil {
		t.Fatal("expected error for unsupported token type ACCESS_TOKEN, got nil")
	}
}

func TestIdentityCenterAuthValidate(t *testing.T) {
	full := func() *IdentityCenterAuth {
		return &IdentityCenterAuth{
			token:     "jwt",
			tokenType: OptionValueIdCTokenTypeExtJWT,
			clientID:  "client",
			roleArn:   "arn:aws:iam::123456789012:role/r",
		}
	}

	if err := full().validate(); err != nil {
		t.Fatalf("expected full config to validate, got: %v", err)
	}

	cases := map[string]func(*IdentityCenterAuth){
		"missing token":     func(a *IdentityCenterAuth) { a.token = "" },
		"missing client id": func(a *IdentityCenterAuth) { a.clientID = "" },
		"missing role arn":  func(a *IdentityCenterAuth) { a.roleArn = "" },
		"bad token type":    func(a *IdentityCenterAuth) { a.tokenType = "ACCESS_TOKEN" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			a := full()
			mutate(a)
			if err := a.validate(); err == nil {
				t.Errorf("expected validate() to fail for %q, got nil", name)
			}
		})
	}

	var nilAuth *IdentityCenterAuth
	if err := nilAuth.validate(); err == nil {
		t.Error("expected nil IdentityCenterAuth to fail validation")
	}
}

func TestResolveRegionFallback(t *testing.T) {
	a := &IdentityCenterAuth{}
	if got := a.resolveRegion("us-west-2"); got != "us-west-2" {
		t.Errorf("resolveRegion fallback = %q, want us-west-2", got)
	}
	a.region = "eu-west-1"
	if got := a.resolveRegion("us-west-2"); got != "eu-west-1" {
		t.Errorf("resolveRegion = %q, want eu-west-1", got)
	}
}
