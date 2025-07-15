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

#include "redshift_auth.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/redshift/RedshiftClient.h>
#include <aws/redshift/model/GetClusterCredentialsRequest.h>

#include <memory>
#include <string>

#include "driver/common/utils.h"

namespace adbcpq {

AwsClientSingleton::AwsClientSingleton() {
    static bool initialized = false;
    if (!initialized) {
        Aws::InitAPI(options_);
        initialized = true;
    }
}

AwsClientSingleton::~AwsClientSingleton() {
  Aws::ShutdownAPI(options_);
}

class AwsAuthClient::Impl {
 public:
  Impl() {
    // Meyer's Singleton
    AwsClientSingleton::Instance();
  }

  ~Impl() {
    // no-op
  }

  Status GetRedshiftCredentials(const AwsAuthSettings& settings,
                                RedshiftCredentials& credentials,
                                struct AdbcError* error) const {
    try {
      Aws::Auth::AWSCredentials aws_credentials;
      if (settings.profile.has_value()) {
        auto status =
            GetCredentialsFromProfile(settings.profile.value(), aws_credentials, error);
        if (!status.ok()) return status;
      } else {
        if (!settings.access_key_id.has_value() ||
            !settings.secret_access_key.has_value()) {
          SetError(error, "%s",
                   "[aws] Both access_key_id and secret_access_key must be provided when "
                   "not using AWS profile");
          return Status::InvalidArgument(
              "Both access_key_id and secret_access_key must be provided when not using "
              "AWS profile");
        }
        aws_credentials = Aws::Auth::AWSCredentials(settings.access_key_id.value(),
                                                    settings.secret_access_key.value());
      }

      return GetClusterCredentials(settings, aws_credentials, credentials, error);

    } catch (const std::exception& e) {
      SetError(error, "%s%s",
               "[aws] Exception during Redshift IAM authentication: ", e.what());
      return Status::IO("Exception during Redshift IAM authentication: ", e.what());
    }
  }

 private:
  Status GetCredentialsFromProfile(const std::string& profile_name,
                                   Aws::Auth::AWSCredentials& credentials,
                                   struct AdbcError* error) const {
    // Create credentials provider for the specified profile
    auto credentials_provider =
        Aws::MakeShared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(
            "RedshiftIAMAuth", profile_name.c_str());

    // Get credentials from the provider
    credentials = credentials_provider->GetAWSCredentials();
    if (credentials.GetAWSAccessKeyId().empty() ||
        credentials.GetAWSSecretKey().empty()) {
      SetError(error, "%s%s", "[aws] Failed to get AWS credentials from profile: ",
               profile_name.c_str());
      return Status::IO("Failed to get AWS credentials from profile");
    }

    return Status::Ok();
  }

  Status GetClusterCredentials(const AwsAuthSettings& settings,
                               const Aws::Auth::AWSCredentials& aws_credentials,
                               RedshiftCredentials& credentials,
                               struct AdbcError* error) const {
    Aws::Client::ClientConfiguration config;
    if (settings.region.has_value()) {
      config.region = settings.region.value();
    }

    Aws::Redshift::RedshiftClient redshift_client(aws_credentials, config);
    Aws::Redshift::Model::GetClusterCredentialsRequest request;

    // Strip 'IAM:' prefix if provided
    std::string db_user = settings.user.value_or("awsuser");
    if (db_user.size() >= 4 && db_user.substr(0, 4) == "IAM:") {
      db_user = db_user.substr(4);
    }

    request.SetClusterIdentifier(settings.cluster_id.value_or(""));
    request.SetDbUser(db_user);
    request.SetDbName(settings.database);

    auto outcome = redshift_client.GetClusterCredentials(request);
    if (!outcome.IsSuccess()) {
      SetError(error, "%s%s", "[libpq] Failed to get cluster credentials: ",
               outcome.GetError().GetMessage().c_str());
      return Status::IO("Failed to get cluster credentials: %s",
                        outcome.GetError().GetMessage().c_str());
    }

    // Extract the credentials
    const auto& result = outcome.GetResult();
    credentials.db_user = result.GetDbUser();
    credentials.db_password = result.GetDbPassword();
    if (result.GetExpiration().WasParseSuccessful()) {
      credentials.expiration =
          result.GetExpiration().ToGmtString(Aws::Utils::DateFormat::ISO_8601);
    }

    return Status::Ok();
  }
};

AwsAuthClient::AwsAuthClient() : pimpl_(std::make_unique<Impl>()) {}
AwsAuthClient::~AwsAuthClient() = default;

Status AwsAuthClient::GetRedshiftCredentials(const AwsAuthSettings& settings,
                                             RedshiftCredentials& credentials,
                                             struct AdbcError* error) const {
  return pimpl_->GetRedshiftCredentials(settings, credentials, error);
}
}  // namespace adbcpq