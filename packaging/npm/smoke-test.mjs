// Smoke test for the installed @dolthub/doltlite-wasm package.
//
// Run from a directory where the package has been `npm install`ed. Resolves the
// Node entry and the wasm binary through the package's exports map (so a broken
// "exports" subpath or a file missing from "files" fails here), instantiates
// the module, and exercises a real dolt_commit -> dolt_log round-trip to prove
// the dolt_* functions are registered in the published artifact.
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import fs from 'node:fs';

// Resolve from the consumer's cwd (where node_modules lives), not this script's
// location, so it works when invoked by absolute path from a temp consumer dir.
const require = createRequire(pathToFileURL(process.cwd() + '/'));

const nodeEntry = require.resolve('@dolthub/doltlite-wasm/sqlite3-node.mjs');
const wasmPath = require.resolve('@dolthub/doltlite-wasm/sqlite3.wasm');

const { default: sqlite3InitModule } = await import(pathToFileURL(nodeEntry).href);

const sqlite3 = await sqlite3InitModule({
  locateFile: (name) => (name === 'sqlite3.wasm' ? wasmPath : name),
  wasmBinary: fs.readFileSync(wasmPath),
});

function fail(msg) {
  throw new Error(msg);
}

const db = new sqlite3.oo1.DB(':memory:', 'c');
try {
  const version = db.selectValue('select dolt_version()');
  const engine = db.selectValue('select doltlite_engine()');
  if (!version || typeof version !== 'string') fail('dolt_version() returned no version');
  if (engine !== 'prolly') fail(`doltlite_engine() returned ${engine}`);

  db.exec('create table t(id integer primary key, v text)');
  db.exec("insert into t values(1, 'a')");
  db.exec("select dolt_commit('-A', '-m', 'c1')");

  // Initial commit plus c1 => 2 rows in dolt_log, matching the native smoke test.
  const commits = db.selectValue('select count(*) from dolt_log');
  if (commits !== 2) fail(`expected 2 commits in dolt_log, got ${commits}`);

  console.log(`@dolthub/doltlite-wasm package smoke OK: version=${version}, engine=${engine}, commits=${commits}`);
} finally {
  db.close();
}
