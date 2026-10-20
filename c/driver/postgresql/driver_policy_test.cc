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

TEST(DriverPolicyTest, FlavorDefaults) {
  EXPECT_EQ(kPostgreSQLDriverPolicy.flavor, DriverFlavor::kPostgreSQL);
  EXPECT_EQ(kPostgreSQLDriverPolicy.default_vendor_name, "PostgreSQL");
  EXPECT_TRUE(kPostgreSQLDriverPolicy.default_use_copy);
  EXPECT_TRUE(kPostgreSQLDriverPolicy.UseCopyByDefault("PostgreSQL"));
  EXPECT_FALSE(kPostgreSQLDriverPolicy.UseCopyByDefault("Redshift"));
  EXPECT_FALSE(kPostgreSQLDriverPolicy.normalize_nullable_type_oids);
  EXPECT_FALSE(kPostgreSQLDriverPolicy.fallback_typreceive_from_typname);
  EXPECT_FALSE(kPostgreSQLDriverPolicy.restart_transaction_separately);
  EXPECT_STREQ(kPostgreSQLDriverPolicy.CommitQuery(), "COMMIT; BEGIN TRANSACTION");
  EXPECT_STREQ(kPostgreSQLDriverPolicy.RollbackQuery(), "ROLLBACK AND CHAIN");
  EXPECT_EQ(kPostgreSQLDriverPolicy.TransactionRestartQuery(), nullptr);

  EXPECT_EQ(kRedshiftDriverPolicy.flavor, DriverFlavor::kRedshift);
  EXPECT_EQ(kRedshiftDriverPolicy.default_vendor_name, "PostgreSQL");
  EXPECT_TRUE(kRedshiftDriverPolicy.default_use_copy);
  EXPECT_TRUE(kRedshiftDriverPolicy.UseCopyByDefault("PostgreSQL"));
  EXPECT_FALSE(kRedshiftDriverPolicy.UseCopyByDefault("Redshift"));
  EXPECT_FALSE(kRedshiftDriverPolicy.normalize_nullable_type_oids);
  EXPECT_FALSE(kRedshiftDriverPolicy.fallback_typreceive_from_typname);
  EXPECT_FALSE(kRedshiftDriverPolicy.restart_transaction_separately);
  EXPECT_STREQ(kRedshiftDriverPolicy.CommitQuery(), "COMMIT; BEGIN TRANSACTION");
  EXPECT_STREQ(kRedshiftDriverPolicy.RollbackQuery(), "ROLLBACK AND CHAIN");
  EXPECT_EQ(kRedshiftDriverPolicy.TransactionRestartQuery(), nullptr);

  EXPECT_EQ(kMotherDuckDriverPolicy.flavor, DriverFlavor::kMotherDuck);
  EXPECT_EQ(kMotherDuckDriverPolicy.default_vendor_name, "MotherDuck");
  EXPECT_FALSE(kMotherDuckDriverPolicy.default_use_copy);
  EXPECT_FALSE(kMotherDuckDriverPolicy.UseCopyByDefault("MotherDuck"));
  EXPECT_TRUE(kMotherDuckDriverPolicy.normalize_nullable_type_oids);
  EXPECT_TRUE(kMotherDuckDriverPolicy.fallback_typreceive_from_typname);
  EXPECT_TRUE(kMotherDuckDriverPolicy.restart_transaction_separately);
  EXPECT_STREQ(kMotherDuckDriverPolicy.CommitQuery(), "COMMIT");
  EXPECT_STREQ(kMotherDuckDriverPolicy.RollbackQuery(), "ROLLBACK");
  EXPECT_STREQ(kMotherDuckDriverPolicy.TransactionRestartQuery(), "BEGIN TRANSACTION");
}

TEST(DriverPolicyTest, SelectsCompileTimeFlavor) {
#if defined(ADBC_REDSHIFT_FLAVOR)
  EXPECT_EQ(kDriverPolicy.flavor, DriverFlavor::kRedshift);
#elif defined(ADBC_MOTHERDUCK_FLAVOR)
  EXPECT_EQ(kDriverPolicy.flavor, DriverFlavor::kMotherDuck);
#else
  EXPECT_EQ(kDriverPolicy.flavor, DriverFlavor::kPostgreSQL);
#endif
}

TEST(DriverPolicyTest, AppliesRuntimeServerDetection) {
  EXPECT_EQ(kDriverPolicy.VendorName(true), "Redshift");
  EXPECT_FALSE(kDriverPolicy.UseCopyByDefault(kDriverPolicy.VendorName(true)));

#if defined(ADBC_MOTHERDUCK_FLAVOR)
  EXPECT_EQ(kDriverPolicy.VendorName(false), "MotherDuck");
  EXPECT_FALSE(kDriverPolicy.UseCopyByDefault(kDriverPolicy.VendorName(false)));
#else
  EXPECT_EQ(kDriverPolicy.VendorName(false), "PostgreSQL");
  EXPECT_TRUE(kDriverPolicy.UseCopyByDefault(kDriverPolicy.VendorName(false)));
#endif
}

}  // namespace adbcpq
