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

#include <gtest/gtest.h>

#include "driver_policy.h"

namespace adbcpq {

TEST(DriverPolicyTest, FlavorDecisions) {
  EXPECT_EQ(kPostgreSQLDriverPolicy.flavor, DriverFlavor::kPostgreSQL);
  EXPECT_EQ(kPostgreSQLDriverPolicy.vendor_name, "PostgreSQL");
  EXPECT_TRUE(kPostgreSQLDriverPolicy.default_use_copy);
  EXPECT_TRUE(kPostgreSQLDriverPolicy.UseCopyByDefault("PostgreSQL"));
  EXPECT_FALSE(kPostgreSQLDriverPolicy.UseCopyByDefault("Redshift"));

  EXPECT_EQ(kRedshiftDriverPolicy.flavor, DriverFlavor::kRedshift);
  EXPECT_EQ(kRedshiftDriverPolicy.vendor_name, "Redshift");
  EXPECT_FALSE(kRedshiftDriverPolicy.default_use_copy);
  EXPECT_FALSE(kRedshiftDriverPolicy.UseCopyByDefault("Redshift"));

  EXPECT_EQ(kMotherDuckDriverPolicy.flavor, DriverFlavor::kMotherDuck);
  EXPECT_EQ(kMotherDuckDriverPolicy.vendor_name, "MotherDuck");
  EXPECT_FALSE(kMotherDuckDriverPolicy.default_use_copy);
  EXPECT_FALSE(kMotherDuckDriverPolicy.UseCopyByDefault("MotherDuck"));
}

}  // namespace adbcpq
