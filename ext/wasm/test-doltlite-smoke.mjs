import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const nodeEntry = path.resolve('ext/wasm/jswasm/sqlite3-node.mjs');
const wasmPath = path.resolve('ext/wasm/jswasm/sqlite3.wasm');
let nodeEntryText = fs.readFileSync(nodeEntry, 'utf8');
if(nodeEntryText.includes("__dirname + '/'")){
  nodeEntryText = nodeEntryText.replaceAll(
    "__dirname + '/'",
    "new URL('.', import.meta.url).href"
  );
  fs.writeFileSync(nodeEntry, nodeEntryText);
}
const { default: sqlite3InitModule } = await import(pathToFileURL(nodeEntry).href);

const sqlite3 = await sqlite3InitModule({
  locateFile: (name)=> name === 'sqlite3.wasm' ? wasmPath : name,
  wasmBinary: fs.readFileSync(wasmPath)
});
const dbFile = path.join(
  fs.mkdtempSync(path.join(os.tmpdir(), 'doltlite-wasm-')),
  'smoke.db'
);
const db = new sqlite3.oo1.DB(dbFile, 'ct');

function fail(msg){
  throw new Error(msg);
}

try {
  const version = db.selectValue("select dolt_version()");
  const engine = db.selectValue("select doltlite_engine()");
  if(!version || typeof version !== 'string') fail('dolt_version() returned no version');
  if(engine !== 'prolly') fail(`doltlite_engine() returned ${engine}`);

  db.exec("select dolt_config('user.name','Wasm Test')");
  db.exec("select dolt_config('user.email','wasm@example.com')");
  db.exec("create table t(id integer primary key, v text)");
  db.exec("insert into t values(1,'a')");
  db.exec("select dolt_add('-A')");

  const commit = db.selectValue("select dolt_commit('-m','wasm smoke')");
  const logCount = db.selectValue("select count(*) from dolt_log");
  const activeBranch = db.selectValue("select name from active_branch");

  if(!commit || typeof commit !== 'string') fail('dolt_commit() returned no hash');
  if(logCount !== 1) fail(`dolt_log count was ${logCount}, expected 1`);
  if(activeBranch !== 'main') fail(`active_branch was ${activeBranch}, expected main`);

  console.log(`dolt_version=${version}`);
  console.log(`doltlite_engine=${engine}`);
  console.log(`dolt_commit=${commit}`);
  console.log(`dolt_log_count=${logCount}`);
  console.log(`active_branch=${activeBranch}`);
} finally {
  db.close();
}
