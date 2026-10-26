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
*
* ---
* Real SASL/GSSAPI (RFC 4752) Kerberos mechanism for the
* Apache Spark/Hive2 Thrift driver. dbt-oss's own Spark Thrift adapter (as of
* dbt-oss 2.0.4 / Fusion) ships this auth type as an explicit "not
* implemented" stub against its CDN-distributed adbc_driver_spark; this
* class, plus GssApiNativeMethods.cs, fills that gap for our self-hosted
* Kyuubi (kyuubi.authentication KERBEROS,CUSTOM) deployment by driving the
* system MIT krb5 GSS-API library directly.
*/

using System;
using System.Security.Authentication;

namespace Apache.Arrow.Adbc.Drivers.Apache
{
    /// <summary>
    /// Implements the SASL GSSAPI mechanism (RFC 4752) for Kerberos authentication
    /// over a Thrift transport. Two phases, matching the RFC:
    ///  1. Establish the GSS security context (one or more gss_init_sec_context legs).
    ///  2. Negotiate the SASL security layer (§3.1) — we always select "no security
    ///     layer" (QOP byte 0x01), since only the initial handshake matters here; no
    ///     further messages are encrypted/wrapped by this mechanism.
    /// Relies entirely on the ambient Kerberos credential (ticket cache populated by
    /// `kinit`, or a configured keytab via standard krb5 environment variables) —
    /// this class never touches credentials itself.
    /// </summary>
    internal class GssapiSaslMechanism : ISaslMechanism, IDisposable
    {
        private enum NegotiationState
        {
            EstablishingContext,
            NegotiatingSecurityLayer,
            Done,
        }

        private readonly Gss.SafeGssName _targetName;
        private IntPtr _context = IntPtr.Zero;
        private NegotiationState _state = NegotiationState.EstablishingContext;
        private bool _isNegotiationCompleted;
        private bool _disposed;

        /// <param name="serviceName">Kerberos service name of the target, e.g. "kyuubi"
        /// (dbt/Kyuubi's `kerberos_service_name`).</param>
        /// <param name="host">Hostname of the target Thrift server, used together with
        /// <paramref name="serviceName"/> to build the GSS_C_NT_HOSTBASED_SERVICE target
        /// name "service@host" (e.g. "kyuubi@kyuubi.local"), matching the
        /// service/host@REALM principal Kyuubi authenticates as.</param>
        public GssapiSaslMechanism(string serviceName, string host)
        {
            if (string.IsNullOrWhiteSpace(serviceName))
                throw new ArgumentException("Kerberos service name must not be empty.", nameof(serviceName));
            if (string.IsNullOrWhiteSpace(host))
                throw new ArgumentException("Host must not be empty.", nameof(host));

            _targetName = Gss.ImportHostBasedServiceName($"{serviceName}@{host}");
        }

        public string Name => "GSSAPI";

        public bool IsNegotiationCompleted
        {
            get => _isNegotiationCompleted;
            set => _isNegotiationCompleted = value;
        }

        public byte[] EvaluateChallenge(byte[]? challenge)
        {
            switch (_state)
            {
                case NegotiationState.EstablishingContext:
                {
                    bool complete = Gss.InitSecContext(_targetName, challenge, ref _context, out byte[] outputToken);
                    if (complete)
                    {
                        _state = NegotiationState.NegotiatingSecurityLayer;
                    }
                    return outputToken;
                }

                case NegotiationState.NegotiatingSecurityLayer:
                {
                    if (challenge == null || challenge.Length == 0)
                    {
                        throw new AuthenticationException(
                            "GSSAPI security layer negotiation failed: server sent an empty message " +
                            "after context establishment (expected a gss_wrap'd QOP proposal).");
                    }

                    // Server sends [1 byte: supported QOP bitmask][3 bytes: max buffer size],
                    // wrapped under the now-established context (RFC 4752 §3.1).
                    byte[] unwrapped = Gss.Unwrap(_context, challenge);
                    if (unwrapped.Length < 4)
                    {
                        throw new AuthenticationException(
                            $"GSSAPI security layer negotiation failed: expected a 4-byte QOP/buffer-size " +
                            $"message, got {unwrapped.Length} bytes.");
                    }

                    // We only ever select "no security layer" (QOP bit 0x01) — this driver
                    // does not need per-message confidentiality/integrity after the initial
                    // Kerberos handshake, only real authentication. Echo QOP=1 with a zero
                    // max buffer size and an empty authorization id, gss_wrap'd per spec.
                    byte[] response = { 0x01, 0x00, 0x00, 0x00 };
                    byte[] wrapped = Gss.Wrap(_context, response);

                    _state = NegotiationState.Done;
                    _isNegotiationCompleted = true;
                    return wrapped;
                }

                default:
                    // NOTE: "Array" unqualified resolves to Apache.Arrow.Array here (this
                    // namespace nests under the Arrow library's own "Apache.Arrow" namespace,
                    // which shadows System.Array) — must fully qualify.
                    return System.Array.Empty<byte>();
            }
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;
            Gss.DeleteSecContext(ref _context);
            _targetName.Dispose();
        }
    }
}
