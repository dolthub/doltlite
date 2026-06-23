// Entry point for @dolthub/doltlite-wasm.
//
// Mirrors @sqlite.org/sqlite-wasm: the default export is the async module
// initializer. Await it to get the sqlite3 namespace, which has the dolt_*
// version-control functions already registered (the wasm build wires
// doltliteInstallAutoExt() into sqlite3_wasm_extra_init).
import sqlite3InitModule from './sqlite3-bundler-friendly.mjs';

export default sqlite3InitModule;
export { sqlite3InitModule };
