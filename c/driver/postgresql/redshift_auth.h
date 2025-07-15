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

#pragma once
#include <memory>
#include <optional>
#include <string>

#include <arrow-adbc/adbc.h>
#include <aws/core/Aws.h>
#include "driver/framework/status.h"

namespace adbcpq {
using adbc::driver::Status;

class AwsClientSingleton {
public:
    static AwsClientSingleton& Instance() {
        static AwsClientSingleton S;
        return S;
    }

private:
    Aws::SDKOptions options_;
    AwsClientSingleton();
    ~AwsClientSingleton();
};

// Redshift cluster credentials returned from GetClusterCredentials API
struct RedshiftCredentials {
  std::string db_user;
  std::string db_password;
  std::optional<std::string> expiration;

  RedshiftCredentials() = default;
};

// AWS authentication settings for Redshift
struct AwsAuthSettings {
  std::string host;
  std::string port;
  std::string database;
  std::optional<std::string> profile;  // IAM profile name
  std::optional<std::string> cluster_id;
  std::optional<std::string> region;
  std::optional<std::string> user;  // UID

  // Explicit IAM
  std::optional<std::string> access_key_id;
  std::optional<std::string> secret_access_key;

  AwsAuthSettings() = default;
};

class AwsAuthClient {
 public:
  AwsAuthClient();
  ~AwsAuthClient();

  // Get Redshift cluster credentials using IAM authentication
  // This calls the Redshift GetClusterCredentials API
  Status GetRedshiftCredentials(const AwsAuthSettings& settings,
                                RedshiftCredentials& credentials,
                                struct AdbcError* error) const;

 private:
  class Impl;
  std::unique_ptr<Impl> pimpl_;
};
}  // namespace adbcpq