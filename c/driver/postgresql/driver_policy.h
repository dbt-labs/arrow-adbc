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
  std::string_view vendor_name;
  bool default_use_copy;

  bool UseCopyByDefault(std::string_view detected_vendor_name) const {
    return default_use_copy && detected_vendor_name != "Redshift";
  }
};

inline constexpr DriverPolicy kPostgreSQLDriverPolicy = {DriverFlavor::kPostgreSQL,
                                                         "PostgreSQL", true};
inline constexpr DriverPolicy kRedshiftDriverPolicy = {DriverFlavor::kRedshift,
                                                       "Redshift", false};
inline constexpr DriverPolicy kMotherDuckDriverPolicy = {DriverFlavor::kMotherDuck,
                                                         "MotherDuck", false};

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
