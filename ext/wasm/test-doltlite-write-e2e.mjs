import fs from 'node:fs';
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

function fail(msg){
  throw new Error(msg);
}

function checkDb(zName){
  const db = new sqlite3.oo1.DB(zName, 'c');
  try {
    const version = db.selectValue("select dolt_version()");
    const engine = db.selectValue("select doltlite_engine()");
    if(!version || typeof version !== 'string') fail(`${zName}: dolt_version() returned no version`);
    if(engine !== 'prolly') fail(`${zName}: doltlite_engine() returned ${engine}`);

    db.exec("create table if not exists t(id integer primary key, v text)");
    db.exec("delete from t");
    db.exec({
      sql: "insert into t values(?, ?), (?, ?)",
      bind: [1, 'alpha', 2, 'beta']
    });
    const count = db.selectValue("select count(*) from t");
    const value = db.selectValue("select v from t where id = 2");
    if(count !== 2) fail(`${zName}: expected 2 rows, got ${count}`);
    if(value !== 'beta') fail(`${zName}: expected beta, got ${value}`);

    console.log(`${zName}: version=${version}`);
    console.log(`${zName}: engine=${engine}`);
    console.log(`${zName}: count=${count}`);
  } finally {
    db.close();
  }
}

checkDb(':memory:');
checkDb('/doltlite-write-e2e.db');
