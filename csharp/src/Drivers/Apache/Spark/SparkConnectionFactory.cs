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

using System;
using System.Collections.Generic;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal class SparkConnectionFactory
    {
        public static SparkConnection NewConnection(IReadOnlyDictionary<string, string> properties)
        {
            // Promote any legacy "adbc.spark.*" values (Host/Port/AuthType) into the new
            // bare "spark.*" keys where the new key wasn't already set — done once, here,
            // so every downstream read (ValidateAuthentication/CreateTransport/etc., in
            // whichever connection class gets constructed below) transparently honors both
            // conventions without needing a fallback check at each individual call site.
            properties = SparkParameters.MergeLegacyAliases(properties);

            // NOTE: dbt-oss's Rust core sends the transport selector as
            // `spark.api` (thrift+binary / thrift+http / livy / connect) — see
            // crates/dbt-adbc/src/spark.rs in dbt-labs/dbt — never the upstream
            // `adbc.spark.type` (standard/http) property below. Prefer it when present;
            // fall back to `Type` for any other caller of this driver.
            if (properties.TryGetValue(SparkParameters.TransportApi, out string? transportApi) && !string.IsNullOrEmpty(transportApi))
            {
                return transportApi switch
                {
                    SparkTransportApiConstants.ThriftBinary => new SparkStandardConnection(properties),
                    SparkTransportApiConstants.ThriftHttp => new SparkHttpConnection(properties),
                    _ => throw new ArgumentOutOfRangeException(
                        nameof(properties),
                        $"Unsupported or unknown value '{transportApi}' given for property '{SparkParameters.TransportApi}'. " +
                        $"Supported values: {SparkTransportApiConstants.ThriftBinary}, {SparkTransportApiConstants.ThriftHttp}."),
                };
            }

            if (!properties.TryGetValue(SparkParameters.Type, out string? type) && string.IsNullOrEmpty(type))
            {
                throw new ArgumentException($"Required property '{SparkParameters.Type}' is missing. Supported types: {ServerTypeParser.SupportedList}", nameof(properties));
            }
            if (!ServerTypeParser.TryParse(type, out SparkServerType serverTypeValue))
            {
                throw new ArgumentOutOfRangeException(nameof(properties), $"Unsupported or unknown value '{type}' given for property '{SparkParameters.Type}'. Supported types: {ServerTypeParser.SupportedList}");
            }

            return serverTypeValue switch
            {
                SparkServerType.Http => new SparkHttpConnection(properties),
                SparkServerType.Standard => new SparkStandardConnection(properties),
                _ => throw new ArgumentOutOfRangeException(nameof(properties), $"Unsupported or unknown value '{type}' given for property '{SparkParameters.Type}'. Supported types: {ServerTypeParser.SupportedList}"),
            };
        }

    }
}
