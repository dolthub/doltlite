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
