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
* Thin P/Invoke wrapper around the system MIT krb5
* GSS-API implementation (libgssapi_krb5), used to implement real SASL
* GSSAPI (Kerberos) authentication for the Spark/Hive2 Thrift driver.
* Only the small subset of the GSS-API C bindings needed for a SASL
* client-side "init_sec_context" + RFC 4752 security-layer negotiation
* is declared here. Credentials are never handled directly by this code:
* gss_init_sec_context is always called with GSS_C_NO_CREDENTIAL, so the
* ambient credential (Kerberos ticket cache populated by `kinit`, or
* whatever KRB5CCNAME/KRB5_CLIENT_KTNAME point to in the environment) is
* used, matching how the Java/Python Thrift SASL GSSAPI clients behave.
*/

using System;
using System.Runtime.InteropServices;
using System.Security.Authentication;
using System.Text;

namespace Apache.Arrow.Adbc.Drivers.Apache
{
    /// <summary>
    /// Minimal P/Invoke bindings to the system MIT krb5 GSS-API shared library,
    /// scoped to exactly what a SASL/GSSAPI (RFC 4752) client-side mechanism needs.
    /// </summary>
    internal static class Gss
    {
        // Versioned .so name: guaranteed present wherever the krb5 GSS-API runtime
        // package is installed, regardless of whether the unversioned dev symlink
        // (libgssapi_krb5.so) also exists.
        private const string GssLib = "libgssapi_krb5.so.2";

        // GSS-API major-status bit layout (RFC 2744 §3.9.1 / gssapi.h):
        // bits 31-24 = calling error, bits 23-16 = routine error, bits 15-0 = supplementary info.
        private const uint CallingOrRoutineErrorMask = 0xFFFF0000;
        private const uint ContinueNeededBit = 0x1;

        private const uint GSS_C_MUTUAL_FLAG = 2;
        private const uint GSS_C_SEQUENCE_FLAG = 8;

        private const int GSS_C_GSS_CODE = 1; // status_type for gss_display_status: major status
        private const int GSS_C_MECH_CODE = 2; // status_type for gss_display_status: mechanism-specific minor status

        [StructLayout(LayoutKind.Sequential)]
        private struct GssBufferDesc
        {
            public UIntPtr length;
            public IntPtr value;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct GssOidDesc
        {
            public uint length;
            public IntPtr elements;
        }

        [DllImport(GssLib, EntryPoint = "gss_import_name")]
        private static extern uint gss_import_name(
            out uint minorStatus,
            ref GssBufferDesc inputNameBuffer,
            ref GssOidDesc inputNameType,
            out IntPtr outputName);

        [DllImport(GssLib, EntryPoint = "gss_release_name")]
        private static extern uint gss_release_name(out uint minorStatus, ref IntPtr name);

        [DllImport(GssLib, EntryPoint = "gss_init_sec_context")]
        private static extern uint gss_init_sec_context(
            out uint minorStatus,
            IntPtr claimantCredHandle,
            ref IntPtr contextHandle,
            IntPtr targetName,
            ref GssOidDesc mechType,
            uint reqFlags,
            uint timeReq,
            IntPtr inputChanBindings,
            ref GssBufferDesc inputToken,
            IntPtr actualMechType,
            ref GssBufferDesc outputToken,
            IntPtr retFlags,
            IntPtr timeRec);

        [DllImport(GssLib, EntryPoint = "gss_wrap")]
        private static extern uint gss_wrap(
            out uint minorStatus,
            IntPtr contextHandle,
            int confReqFlag,
            uint qopReq,
            ref GssBufferDesc inputMessageBuffer,
            out int confState,
            ref GssBufferDesc outputMessageBuffer);

        [DllImport(GssLib, EntryPoint = "gss_unwrap")]
        private static extern uint gss_unwrap(
            out uint minorStatus,
            IntPtr contextHandle,
            ref GssBufferDesc inputMessageBuffer,
            ref GssBufferDesc outputMessageBuffer,
            out int confState,
            out uint qopState);

        [DllImport(GssLib, EntryPoint = "gss_release_buffer")]
        private static extern uint gss_release_buffer(out uint minorStatus, ref GssBufferDesc buffer);

        [DllImport(GssLib, EntryPoint = "gss_delete_sec_context")]
        private static extern uint gss_delete_sec_context(out uint minorStatus, ref IntPtr contextHandle, IntPtr outputToken);

        [DllImport(GssLib, EntryPoint = "gss_display_status")]
        private static extern uint gss_display_status(
            out uint minorStatus,
            uint statusValue,
            int statusType,
            ref GssOidDesc mechType,
            ref uint messageContext,
            ref GssBufferDesc statusString);

        // Kerberos V5 mechanism OID 1.2.840.113554.1.2.2 (DER content octets).
        private static readonly byte[] Krb5MechOidBytes =
            { 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02 };

        // GSS_C_NT_HOSTBASED_SERVICE OID 1.2.840.113554.1.2.1.4 (DER content octets).
        private static readonly byte[] HostBasedServiceOidBytes =
            { 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x01, 0x04 };

        internal sealed class SafeGssName : IDisposable
        {
            internal IntPtr Handle;
            internal SafeGssName(IntPtr handle) => Handle = handle;

            public void Dispose()
            {
                if (Handle != IntPtr.Zero)
                {
                    gss_release_name(out _, ref Handle);
                    Handle = IntPtr.Zero;
                }
            }
        }

        /// <summary>
        /// Imports "service@host" as a GSS_C_NT_HOSTBASED_SERVICE target name, e.g.
        /// "kyuubi@kyuubi.local" for a Kyuubi Thrift server whose service principal
        /// is kyuubi/kyuubi.local@REALM.
        /// </summary>
        internal static SafeGssName ImportHostBasedServiceName(string servicePrincipal)
        {
            byte[] nameBytes = Encoding.UTF8.GetBytes(servicePrincipal);
            IntPtr namePtr = Marshal.AllocHGlobal(Math.Max(nameBytes.Length, 1));
            IntPtr oidPtr = Marshal.AllocHGlobal(HostBasedServiceOidBytes.Length);
            try
            {
                Marshal.Copy(nameBytes, 0, namePtr, nameBytes.Length);
                Marshal.Copy(HostBasedServiceOidBytes, 0, oidPtr, HostBasedServiceOidBytes.Length);

                var nameBuf = new GssBufferDesc { length = (UIntPtr)nameBytes.Length, value = namePtr };
                var oid = new GssOidDesc { length = (uint)HostBasedServiceOidBytes.Length, elements = oidPtr };

                uint major = gss_import_name(out uint minor, ref nameBuf, ref oid, out IntPtr outputName);
                ThrowIfError(major, minor, "gss_import_name");
                return new SafeGssName(outputName);
            }
            finally
            {
                Marshal.FreeHGlobal(namePtr);
                Marshal.FreeHGlobal(oidPtr);
            }
        }

        /// <summary>
        /// Drives one leg of gss_init_sec_context. Always uses GSS_C_NO_CREDENTIAL
        /// (the ambient credential from the process's Kerberos ticket cache).
        /// Returns true once the security context is fully established (no more
        /// legs required) — the caller must still send back whatever non-empty
        /// output token this call produced, even on the completing call.
        /// </summary>
        internal static bool InitSecContext(SafeGssName targetName, byte[]? inputToken, ref IntPtr context, out byte[] outputToken)
        {
            IntPtr mechOidPtr = Marshal.AllocHGlobal(Krb5MechOidBytes.Length);
            IntPtr inputPtr = IntPtr.Zero;
            IntPtr outputTokenValuePtr = IntPtr.Zero;
            try
            {
                Marshal.Copy(Krb5MechOidBytes, 0, mechOidPtr, Krb5MechOidBytes.Length);
                var mechOid = new GssOidDesc { length = (uint)Krb5MechOidBytes.Length, elements = mechOidPtr };

                var inputBuf = new GssBufferDesc { length = UIntPtr.Zero, value = IntPtr.Zero };
                if (inputToken != null && inputToken.Length > 0)
                {
                    inputPtr = Marshal.AllocHGlobal(inputToken.Length);
                    Marshal.Copy(inputToken, 0, inputPtr, inputToken.Length);
                    inputBuf.length = (UIntPtr)inputToken.Length;
                    inputBuf.value = inputPtr;
                }

                var outputBuf = new GssBufferDesc { length = UIntPtr.Zero, value = IntPtr.Zero };
                uint reqFlags = GSS_C_MUTUAL_FLAG | GSS_C_SEQUENCE_FLAG;

                uint major = gss_init_sec_context(
                    out uint minor,
                    IntPtr.Zero, // GSS_C_NO_CREDENTIAL
                    ref context,
                    targetName.Handle,
                    ref mechOid,
                    reqFlags,
                    timeReq: 0, // default lifetime
                    inputChanBindings: IntPtr.Zero, // GSS_C_NO_CHANNEL_BINDINGS
                    ref inputBuf,
                    actualMechType: IntPtr.Zero,
                    ref outputBuf,
                    retFlags: IntPtr.Zero,
                    timeRec: IntPtr.Zero);

                outputTokenValuePtr = outputBuf.value;

                bool isError = (major & CallingOrRoutineErrorMask) != 0;
                bool continueNeeded = (major & ContinueNeededBit) != 0;

                if (isError)
                {
                    // "Array" unqualified resolves to Apache.Arrow.Array in this namespace.
                    outputToken = System.Array.Empty<byte>();
                    ThrowIfError(major, minor, "gss_init_sec_context");
                }

                outputToken = CopyAndReleaseBuffer(ref outputBuf);
                outputTokenValuePtr = IntPtr.Zero; // ownership transferred to / released by CopyAndReleaseBuffer

                return !continueNeeded;
            }
            finally
            {
                Marshal.FreeHGlobal(mechOidPtr);
                if (inputPtr != IntPtr.Zero)
                {
                    Marshal.FreeHGlobal(inputPtr);
                }
                // If we threw before reaching CopyAndReleaseBuffer, best-effort release here too.
                if (outputTokenValuePtr != IntPtr.Zero)
                {
                    var leftover = new GssBufferDesc { length = UIntPtr.Zero, value = outputTokenValuePtr };
                    gss_release_buffer(out _, ref leftover);
                }
            }
        }

        internal static byte[] Wrap(IntPtr context, byte[] message)
        {
            IntPtr inputPtr = Marshal.AllocHGlobal(Math.Max(message.Length, 1));
            try
            {
                Marshal.Copy(message, 0, inputPtr, message.Length);
                var inputBuf = new GssBufferDesc { length = (UIntPtr)message.Length, value = inputPtr };
                var outputBuf = new GssBufferDesc { length = UIntPtr.Zero, value = IntPtr.Zero };

                // No confidentiality/integrity requested: we only negotiate "no security
                // layer" (RFC 4752 §3.1), so conf_req_flag = false and qop_req = default (0).
                uint major = gss_wrap(out uint minor, context, confReqFlag: 0, qopReq: 0, ref inputBuf, out _, ref outputBuf);
                ThrowIfError(major, minor, "gss_wrap");
                return CopyAndReleaseBuffer(ref outputBuf);
            }
            finally
            {
                Marshal.FreeHGlobal(inputPtr);
            }
        }

        internal static byte[] Unwrap(IntPtr context, byte[] message)
        {
            IntPtr inputPtr = Marshal.AllocHGlobal(Math.Max(message.Length, 1));
            try
            {
                Marshal.Copy(message, 0, inputPtr, message.Length);
                var inputBuf = new GssBufferDesc { length = (UIntPtr)message.Length, value = inputPtr };
                var outputBuf = new GssBufferDesc { length = UIntPtr.Zero, value = IntPtr.Zero };

                uint major = gss_unwrap(out uint minor, context, ref inputBuf, ref outputBuf, out _, out _);
                ThrowIfError(major, minor, "gss_unwrap");
                return CopyAndReleaseBuffer(ref outputBuf);
            }
            finally
            {
                Marshal.FreeHGlobal(inputPtr);
            }
        }

        internal static void DeleteSecContext(ref IntPtr context)
        {
            if (context == IntPtr.Zero)
            {
                return;
            }
            gss_delete_sec_context(out _, ref context, IntPtr.Zero);
            context = IntPtr.Zero;
        }

        private static byte[] CopyAndReleaseBuffer(ref GssBufferDesc buffer)
        {
            try
            {
                int len = (int)buffer.length;
                if (len == 0 || buffer.value == IntPtr.Zero)
                {
                    return System.Array.Empty<byte>();
                }
                byte[] result = new byte[len];
                Marshal.Copy(buffer.value, result, 0, len);
                return result;
            }
            finally
            {
                // gss_release_buffer is a no-op-safe way to free memory the GSS library
                // allocated for us; must not be freed with Marshal.FreeHGlobal.
                gss_release_buffer(out _, ref buffer);
            }
        }

        private static void ThrowIfError(uint major, uint minor, string operation)
        {
            if ((major & CallingOrRoutineErrorMask) == 0)
            {
                return;
            }

            string detail = TryDisplayStatus(major, GSS_C_GSS_CODE, mechOid: null) ?? $"major=0x{major:X8}";
            // The minor status is mechanism-specific (here, always Kerberos V5 -- this
            // driver only ever uses the Krb5MechOidBytes mechanism), so decoding it
            // requires passing that mechanism's OID, unlike the major status above.
            // Without this, failures only ever surfaced as an opaque hex code (e.g.
            // "minor status 0x96C73A07") with no indication of the actual underlying
            // Kerberos problem (expired/missing ticket, wrong service principal, clock
            // skew, etc).
            string minorDetail = minor != 0
                ? $", minor status: {TryDisplayStatus(minor, GSS_C_MECH_CODE, Krb5MechOidBytes) ?? $"0x{minor:X8}"}"
                : string.Empty;
            throw new AuthenticationException($"GSSAPI {operation} failed: {detail}{minorDetail}.");
        }

        private static string? TryDisplayStatus(uint statusValue, int statusType, byte[]? mechOid)
        {
            IntPtr mechOidPtr = IntPtr.Zero;
            try
            {
                GssOidDesc mech;
                if (mechOid != null)
                {
                    mechOidPtr = Marshal.AllocHGlobal(mechOid.Length);
                    Marshal.Copy(mechOid, 0, mechOidPtr, mechOid.Length);
                    mech = new GssOidDesc { length = (uint)mechOid.Length, elements = mechOidPtr };
                }
                else
                {
                    mech = new GssOidDesc { length = 0, elements = IntPtr.Zero };
                }

                uint messageContext = 0;
                var statusBuf = new GssBufferDesc { length = UIntPtr.Zero, value = IntPtr.Zero };
                uint major = gss_display_status(out _, statusValue, statusType, ref mech, ref messageContext, ref statusBuf);
                if ((major & CallingOrRoutineErrorMask) != 0)
                {
                    return null;
                }
                byte[] bytes = CopyAndReleaseBuffer(ref statusBuf);
                return bytes.Length > 0 ? Encoding.UTF8.GetString(bytes) : null;
            }
            catch
            {
                return null;
            }
            finally
            {
                if (mechOidPtr != IntPtr.Zero)
                {
                    Marshal.FreeHGlobal(mechOidPtr);
                }
            }
        }
    }
}
