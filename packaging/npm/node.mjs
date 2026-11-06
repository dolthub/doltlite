// Node entry for the package root.
//
// sqlite3-node.mjs reads sqlite3.wasm from disk. The bundler build only
// fetches it, and that fetch fails in Node before any SQL runs.
import sqlite3InitModule from './sqlite3-node.mjs';

export default sqlite3InitModule;
export { sqlite3InitModule };
