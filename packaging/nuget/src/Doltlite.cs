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

        public static void Init()
        {
            lock (Gate)
            {
                if (_initialized)
                {
                    return;
                }
                IntPtr lib = LoadNativeLibrary();
                SQLite3Provider_dynamic_cdecl.Setup("doltlite", new Exports(lib));
                raw.SetProvider(new SQLite3Provider_dynamic_cdecl());
                _initialized = true;
            }
        }

        private static IntPtr LoadNativeLibrary()
        {
            string? overridePath =
                Environment.GetEnvironmentVariable("DOLTLITE_NET_LIB");
            if (!string.IsNullOrEmpty(overridePath))
            {
                return NativeLibrary.Load(overridePath);
            }
            // Probes the platform name variants (libdoltlite.so/.dylib,
            // doltlite.dll) through the assembly's package context, which
            // includes this package's runtimes/<rid>/native payload.
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
