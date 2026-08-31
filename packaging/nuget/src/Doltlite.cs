using System;
using System.Runtime.InteropServices;
using SQLitePCL;

namespace DoltHub.Doltlite
{
    /// <summary>
    /// Plugs the bundled libdoltlite engine into SQLitePCLRaw. Call
    /// <see cref="Init"/> once at startup, before the first connection;
    /// everything layered on SQLitePCLRaw (Microsoft.Data.Sqlite.Core,
    /// EF Core's Sqlite provider, Dapper) then runs on DoltLite unchanged.
    /// DoltLite's version control is plain SQL on the connection, e.g.
    /// <c>SELECT dolt_commit('-A', '-m', 'message')</c>.
    /// </summary>
    public static class Doltlite
    {
        private static readonly object Gate = new object();
        private static bool _initialized;

        /// <summary>
        /// Proves the loaded library is doltlite with a working export table.
        /// Deliberately not a sqlite3_* name: a stock SQLite satisfies those,
        /// so pointing DOLTLITE_NET_LIB at one would pass here and then fail
        /// far away on the first dolt_ function. This symbol is doltlite's, is
        /// defined whether or not the build has remote support, and is covered
        /// by the export filter on every platform.
        /// </summary>
        private const string Marker = "doltliteServerPort";

        public static void Init()
        {
            lock (Gate)
            {
                if (_initialized)
                {
                    return;
                }
                string source;
                IntPtr lib = LoadNativeLibrary(out source);
                // Without this, a library that loads but cannot back the API
                // fails as a null dereference inside the provider, naming
                // neither the library nor what was missing.
                if (!NativeLibrary.TryGetExport(lib, Marker, out IntPtr _))
                {
                    throw new EntryPointNotFoundException(
                        $"The native library loaded from {source} does not " +
                        $"export {Marker}, so it is not a doltlite build or " +
                        "was built without an export table.");
                }
                SQLite3Provider_dynamic_cdecl.Setup("doltlite", new Exports(lib));
                raw.SetProvider(new SQLite3Provider_dynamic_cdecl());
                _initialized = true;
            }
        }

        private static IntPtr LoadNativeLibrary(out string source)
        {
            string? overridePath =
                Environment.GetEnvironmentVariable("DOLTLITE_NET_LIB");
            if (!string.IsNullOrEmpty(overridePath))
            {
                source = $"DOLTLITE_NET_LIB ({overridePath})";
                return NativeLibrary.Load(overridePath);
            }
            // Probes the platform name variants (libdoltlite.so/.dylib,
            // doltlite.dll) through the assembly's package context, which
            // includes this package's runtimes/<rid>/native payload.
            source = "this package's bundled runtimes";
            return NativeLibrary.Load(
                "doltlite", typeof(Doltlite).Assembly, null);
        }

        private sealed class Exports : IGetFunctionPointer
        {
            private readonly IntPtr _lib;

            internal Exports(IntPtr lib)
            {
                _lib = lib;
            }

            public IntPtr GetFunctionPointer(string name)
            {
                return NativeLibrary.TryGetExport(_lib, name, out IntPtr p)
                    ? p
                    : IntPtr.Zero;
            }
        }
    }
}
