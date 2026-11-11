import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

const nodeEntry = path.resolve('ext/wasm/jswasm/sqlite3-node.mjs');
const wasmPath = path.resolve('ext/wasm/jswasm/sqlite3.wasm');
const nativeDoltlite = path.resolve('build/doltlite');

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

if(!fs.existsSync(nativeDoltlite)){
  fail(`native doltlite binary not found at ${nativeDoltlite}`);
}

const db = new sqlite3.oo1.DB('/roundtrip.db', 'c');
try {
  db.exec("create table t(id integer primary key, v text)");
  db.exec({
    sql: "insert into t values(?, ?), (?, ?)",
    bind: [1, 'alpha', 2, 'beta']
  });
  const bytes = sqlite3.capi.sqlite3_js_db_export(db.pointer);
  const outDir = fs.mkdtempSync(path.join(os.tmpdir(), 'doltlite-wasm-export-'));
  const outFile = path.join(outDir, 'roundtrip.sqlite3');
  fs.writeFileSync(outFile, Buffer.from(bytes));

  const query = "select group_concat(id || ':' || v, ',') from t order by id";
  const result = spawnSync(nativeDoltlite, [outFile, query], {
    encoding: 'utf8'
  });
  if(result.status !== 0){
    fail(`native doltlite failed with code ${result.status}: ${result.stderr || result.stdout}`);
  }
  const output = (result.stdout || '').trim();
  if(output !== '1:alpha,2:beta'){
    fail(`unexpected native output: ${JSON.stringify(output)}`);
  }
  console.log(`roundtrip_file=${outFile}`);
  console.log(`roundtrip_rows=${output}`);
} finally {
  db.close();
}

function loadImage(bytes, readOnly=false){
  const loaded = new sqlite3.oo1.DB(':memory:');
  const p = sqlite3.wasm.allocFromTypedArray(bytes);
  const flags = sqlite3.capi.SQLITE_DESERIALIZE_FREEONCLOSE |
    (readOnly ? sqlite3.capi.SQLITE_DESERIALIZE_READONLY
              : sqlite3.capi.SQLITE_DESERIALIZE_RESIZEABLE);
  const rc = sqlite3.capi.sqlite3_deserialize(
    loaded.pointer, 'main', p, bytes.length, bytes.length, flags
  );
  if(rc){
    loaded.close();
    fail(`deserialize returned ${rc}`);
  }
  return loaded;
}

function checkSnapshot(bytes, rows, commits){
  const copy = loadImage(bytes, true);
  try {
    if(copy.selectValue('SELECT count(*) FROM t') !== rows ||
       copy.selectValue('SELECT group_concat(v) FROM (SELECT v FROM t ORDER BY id)') !==
         (rows===1 ? 'first' : 'first,second'+',generation'.repeat(rows-2)) ||
       copy.selectValue('SELECT count(*) FROM dolt_log') !== commits ||
       copy.selectValue("SELECT count(*) FROM dolt_branches WHERE name='feature'") !== 1 ||
       copy.selectValue("SELECT count(*) FROM sqlite_schema WHERE name='t_v'") !== 1 ||
       copy.selectValue('PRAGMA integrity_check') !== 'ok'){
      fail('re-exported image lost rows, history, refs, or schema');
    }
    const again = sqlite3.capi.sqlite3_js_db_export(copy.pointer);
    if(!again.length) fail('read-only image re-export is empty');
  } finally {
    copy.close();
  }
}

for(const source of [':memory:', '/reserialize.db']){
  const original = new sqlite3.oo1.DB(source, 'c');
  let bytes;
  try {
    original.exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);" +
                  "CREATE INDEX t_v ON t(v); INSERT INTO t(v) VALUES('first');" +
                  "SELECT dolt_commit('-Am','first'); SELECT dolt_branch('feature');");
    bytes = sqlite3.capi.sqlite3_js_db_export(original.pointer);
  } finally {
    original.close();
  }
  const loaded = loadImage(bytes);
  try {
    for(let phase=0; phase<3; phase++){
      if(phase===1) loaded.exec("INSERT INTO t(v) VALUES('second')");
      if(phase===2) loaded.exec("SELECT dolt_commit('-Am','second')");
      bytes = sqlite3.capi.sqlite3_js_db_export(loaded.pointer);
      checkSnapshot(bytes, phase ? 2 : 1, phase===2 ? 3 : 2);
    }
  } finally {
    loaded.close();
  }
  for(let generation=0; generation<3; generation++){
    const copy = loadImage(bytes);
    try {
      copy.exec("INSERT INTO t(v) VALUES('generation')");
      bytes = sqlite3.capi.sqlite3_js_db_export(copy.pointer);
      checkSnapshot(bytes, generation+3, 3);
    } finally {
      copy.close();
    }
  }
  console.log(`reserialize=${source}: unchanged, edited, committed, read-only and repeated generations pass`);
}
