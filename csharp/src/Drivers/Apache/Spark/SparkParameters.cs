/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Collections.Generic;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    /// <summary>
    /// Parameters used for connecting to Spark data sources.
    /// </summary>
    public class SparkParameters
    {
        // NOTE: dbt-oss's Rust core (crates/dbt-adbc/src/spark.rs in dbt-labs/dbt) sends
        // bare "spark.*" option keys over the ADBC C API, not the original upstream
        // "adbc.spark.*" convention — HostName/Port/AuthType are the new, preferred keys
        // that match its contract. The original "adbc.spark.*" keys are kept below as
        // *Legacy fallbacks (see MergeLegacyAliases) so any other caller relying on the
        // upstream convention still works.
        public const string HostName = "spark.host";
        public const string HostNameLegacy = "adbc.spark.host";
        public const string Port = "spark.port";
        public const string PortLegacy = "adbc.spark.port";
        public const string Path = "adbc.spark.path";
        public const string Token = "adbc.spark.token";

        // access_token is required when authType is oauth
        public const string AccessToken = "adbc.spark.access_token";
        public const string AuthType = "spark.auth_type";
        public const string AuthTypeLegacy = "adbc.spark.auth_type";
        public const string Type = "adbc.spark.type";
        public const string DataTypeConv = "adbc.spark.data_type_conv";
        public const string ConnectTimeoutMilliseconds = "adbc.spark.connect_timeout_ms";
        public const string UserAgentEntry = "adbc.spark.user_agent_entry";

        /// <summary>
        /// Transport selector sent by dbt-oss as `spark.api` (thrift+binary / thrift+http /
        /// livy / connect). Used by <see cref="SparkConnectionFactory"/> in place of the
        /// upstream <see cref="Type"/>/<see cref="SparkServerTypeConstants"/> selector, which
        /// dbt-oss's Rust core never sends. No legacy equivalent exists for this one — it's
        /// new, not a rename.
        /// </summary>
        public const string TransportApi = "spark.api";

        /// <summary>Kerberos service name, e.g. "kyuubi" (dbt profiles.yml's
        /// `kerberos_service_name`), sent by dbt-oss as `spark.kerberos.service_name`. New,
        /// not a rename — no legacy equivalent exists.</summary>
        public const string KerberosServiceName = "spark.kerberos.service_name";

        /// <summary>
        /// TLS enable/disable, matching dbt profiles.yml's `use_ssl` field and following
        /// this driver's own bare "spark.*" naming convention. NOT forwarded by dbt-oss's
        /// Rust core today (confirmed: `use_ssl` is parsed into its config schema but never
        /// passed through to any ADBC backend as of this writing) -- reading this key is
        /// forward-looking, for whenever that gap is closed on the Rust side. Until then,
        /// <see cref="Apache.Arrow.Adbc.Drivers.Apache.Hive2.HiveServer2TlsImpl.GetStandardTlsOptions"/>
        /// defaults TLS to disabled (matching dbt-oss's own schema default of `use_ssl:
        /// false`) whenever NEITHER this key nor the legacy
        /// <c>adbc.standard_options.tls.enabled</c> key is set. Set this explicitly (or its
        /// legacy equivalent) to enable TLS for beta/prod once the Rust side forwards it.
        /// </summary>
        public const string UseSsl = "spark.use_ssl";

        /// <summary>
        /// Returns <paramref name="properties"/> as-is, unless one of the renamed keys
        /// (<see cref="HostName"/>/<see cref="Port"/>/<see cref="AuthType"/>) is missing but
        /// its original upstream <c>adbc.spark.*</c> equivalent is present — in that case
        /// returns a copy with the new key populated from the legacy one, so every
        /// downstream read of e.g. <see cref="HostName"/> transparently also honors
        /// <see cref="HostNameLegacy"/>. The new key always wins when both are set.
        /// Centralizing this here (called once, in <see cref="SparkConnectionFactory"/>)
        /// avoids needing a fallback check at every individual read site.
        /// </summary>
        internal static IReadOnlyDictionary<string, string> MergeLegacyAliases(IReadOnlyDictionary<string, string> properties)
        {
            (string NewKey, string LegacyKey)[] aliases =
            {
                (HostName, HostNameLegacy),
                (Port, PortLegacy),
                (AuthType, AuthTypeLegacy),
                // Not a rename like the three above -- reuses the same "populate the key
                // HiveServer2TlsImpl.GetStandardTlsOptions actually reads, from whatever
                // key we found a value under" mechanism so `spark.use_ssl` (this driver's
                // own naming convention, and the key dbt-oss would send if its Rust core
                // is ever extended to forward `use_ssl`) takes effect without touching the
                // shared Hive2/Impala TLS-parsing code at all.
                (global::Apache.Arrow.Adbc.Drivers.Apache.Hive2.StandardTlsOptions.IsTlsEnabled, UseSsl),
            };

            Dictionary<string, string>? merged = null;
            foreach ((string newKey, string legacyKey) in aliases)
            {
                bool hasNew = properties.TryGetValue(newKey, out string? newValue) && !string.IsNullOrEmpty(newValue);
                if (!hasNew && properties.TryGetValue(legacyKey, out string? legacyValue) && !string.IsNullOrEmpty(legacyValue))
                {
                    merged ??= new Dictionary<string, string>(properties);
                    merged[newKey] = legacyValue;
                }
            }
            return merged ?? properties;
        }
    }

    public static class SparkTransportApiConstants
    {
        public const string ThriftBinary = "thrift+binary";
        public const string ThriftHttp = "thrift+http";
        public const string Livy = "livy";
        public const string Connect = "connect";
    }

    public static class SparkAuthTypeConstants
    {
        public const string None = "none";
        public const string UsernameOnly = "username_only";
        public const string Basic = "basic";
        public const string Token = "token";
        public const string OAuth = "oauth";

        // NOTE: these match the lowercase values of dbt-adbc's
        // `spark::auth_type` Rust constants (crates/dbt-adbc/src/spark.rs) exactly —
        // that's the literal string dbt-oss sends as `spark.auth_type`.
        public const string Plain = "plain";
        public const string NoSasl = "nosasl";
        public const string Ldap = "ldap";
        public const string Kerberos = "kerberos";
    }

    public static class SparkServerTypeConstants
    {
        public const string Http = "http";
        public const string Standard = "standard";
    }
}
