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
const db = new sqlite3.oo1.DB(dbFile, 'c');

function fail(msg){
  throw new Error(msg);
}

try {
  const version = db.selectValue("select dolt_version()");
  const engine = db.selectValue("select doltlite_engine()");
  if(!version || typeof version !== 'string') fail('dolt_version() returned no version');
  if(engine !== 'prolly') fail(`doltlite_engine() returned ${engine}`);

  console.log(`dolt_version=${version}`);
  console.log(`doltlite_engine=${engine}`);
} finally {
  db.close();
}
