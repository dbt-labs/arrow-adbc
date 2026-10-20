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

#include <string_view>

namespace adbcpq {

enum class DriverFlavor {
  kPostgreSQL,
  kRedshift,
  kMotherDuck,
};

struct DriverPolicy {
  DriverFlavor flavor;
  std::string_view default_vendor_name;
  bool default_use_copy;
  bool normalize_nullable_type_oids;
  bool fallback_typreceive_from_typname;
  bool restart_transaction_separately;

  std::string_view VendorName(bool redshift_server_detected) const {
    return redshift_server_detected ? "Redshift" : default_vendor_name;
  }

  bool UseCopyByDefault(std::string_view detected_vendor_name) const {
    return default_use_copy && detected_vendor_name != "Redshift";
  }

  const char* CommitQuery() const {
    return restart_transaction_separately ? "COMMIT" : "COMMIT; BEGIN TRANSACTION";
  }

  const char* RollbackQuery() const {
    return restart_transaction_separately ? "ROLLBACK" : "ROLLBACK AND CHAIN";
  }

  const char* TransactionRestartQuery() const {
    return restart_transaction_separately ? "BEGIN TRANSACTION" : nullptr;
  }
};

inline constexpr DriverPolicy kPostgreSQLDriverPolicy = {
    DriverFlavor::kPostgreSQL, "PostgreSQL", true, false, false, false};
inline constexpr DriverPolicy kRedshiftDriverPolicy = {
    DriverFlavor::kRedshift, "PostgreSQL", true, false, false, false};
inline constexpr DriverPolicy kMotherDuckDriverPolicy = {
    DriverFlavor::kMotherDuck, "MotherDuck", false, true, true, true};

#if defined(ADBC_REDSHIFT_FLAVOR) && defined(ADBC_MOTHERDUCK_FLAVOR)
#error "Only one PostgreSQL driver flavor may be selected"
#elif defined(ADBC_REDSHIFT_FLAVOR)
inline constexpr DriverPolicy kDriverPolicy = kRedshiftDriverPolicy;
#elif defined(ADBC_MOTHERDUCK_FLAVOR)
inline constexpr DriverPolicy kDriverPolicy = kMotherDuckDriverPolicy;
#else
inline constexpr DriverPolicy kDriverPolicy = kPostgreSQLDriverPolicy;
#endif

}  // namespace adbcpq
