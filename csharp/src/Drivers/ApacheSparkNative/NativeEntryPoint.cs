/*
 * NativeAOT entrypoint for the Spark/Hive2 Thrift driver.
 * Exposes the standard ADBC C API symbol ("AdbcDriverInit", see
 * https://arrow.apache.org/adbc/current/format/specification.html) so this
 * assembly, once published via `dotnet publish -r linux-x64 -p:PublishAot=true`,
 * can be dlopen'd directly by dbt-oss's Rust core (crates/dbt-adbc, via the
 * `libloading` crate) exactly like any other native ADBC driver.
 */

using System.Runtime.InteropServices;
using Apache.Arrow.Adbc.C;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark.Native
{
    public static class NativeEntryPoint
    {
        /// <summary>
        /// Standard ADBC driver entrypoint. Signature must match the C ABI exactly:
        /// AdbcStatusCode AdbcDriverInit(int version, void* raw_driver, struct AdbcError* error).
        /// </summary>
        [UnmanagedCallersOnly(EntryPoint = "AdbcDriverInit")]
        public static unsafe byte AdbcDriverInit(int version, void* rawDriver, void* error)
        {
            try
            {
                return (byte)CAdbcDriverExporter.AdbcDriverInit(
                    version,
                    (CAdbcDriver*)rawDriver,
                    (CAdbcError*)error,
                    new SparkDriver());
            }
            catch
            {
                // An exception must never cross the native boundary — return
                // AdbcStatusCode.InternalError instead of letting NativeAOT abort the process.
                return (byte)AdbcStatusCode.InternalError;
            }
        }
    }
}
