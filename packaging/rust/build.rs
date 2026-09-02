fn main() {
    println!("cargo:rerun-if-changed=vendor/doltlite.c");
    println!("cargo:rerun-if-changed=vendor/doltlite.h");

    // assemble.sh stamps the crate version from the release tag, so it is also
    // the engine version; unset, the amalgamation falls back to a placeholder.
    let version = format!(
        "\"v{}\"",
        std::env::var("CARGO_PKG_VERSION").expect("cargo sets CARGO_PKG_VERSION")
    );

    let mut build = cc::Build::new();
    build
        .file("vendor/doltlite.c")
        .flag_if_supported("-w")
        .define("DOLTLITE_VERSION", version.as_str())
        .define("DOLTLITE_PROLLY", "1")
        .define("SQLITE_THREADSAFE", "1")
        .define("SQLITE_ENABLE_MATH_FUNCTIONS", None)
        .define("SQLITE_ENABLE_FTS5", None)
        .define("SQLITE_ENABLE_RTREE", None)
        .define("SQLITE_ENABLE_DBSTAT_VTAB", None)
        .define("SQLITE_ENABLE_COLUMN_METADATA", None);

    // The engine compresses chunks with zlib and needs the platform's
    // threading primitives; everything else it carries itself.
    build.compile("doltlite");
    println!("cargo:rustc-link-lib=z");
    if cfg!(unix) {
        println!("cargo:rustc-link-lib=pthread");
    }
}
