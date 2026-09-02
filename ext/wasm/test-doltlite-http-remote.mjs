import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

const nodeEntry = path.resolve('ext/wasm/jswasm/sqlite3-node.mjs');
const wasmPath = path.resolve('ext/wasm/jswasm/sqlite3.wasm');
const nativeDoltlite = path.resolve('build/doltlite');
const nativeRelay = path.resolve('build/doltlite-remotesrv');

function fail(msg){
  throw new Error(msg);
}

for(const bin of [nativeDoltlite, nativeRelay]){
  if(!fs.existsSync(bin)) fail(`native binary not found at ${bin}`);
}

let nodeEntryText = fs.readFileSync(nodeEntry, 'utf8');
if(nodeEntryText.includes("__dirname + '/'")){
  nodeEntryText = nodeEntryText.replaceAll(
    "__dirname + '/'",
    "new URL('.', import.meta.url).href"
  );
  fs.writeFileSync(nodeEntry, nodeEntryText);
}

const serveDir = fs.mkdtempSync(path.join(os.tmpdir(), 'doltlite-wasm-remote-'));
const sourceDb = path.join(serveDir, 'source.db');
const port = 8000 + (process.pid % 2000);
const url = `http://127.0.0.1:${port}/room.db`;

function native(db, sql){
  const r = spawnSync(nativeDoltlite, [db, sql], { encoding: 'utf8' });
  if(r.status !== 0){
    fail(`native doltlite failed: ${r.stderr || r.stdout}`);
  }
  return (r.stdout || '').trim();
}

const relay = spawn(nativeRelay, ['-p', String(port), serveDir], {
  stdio: ['ignore', 'pipe', 'pipe']
});
let relayOut = '';
relay.stdout.on('data', (d)=> relayOut += d);
relay.stderr.on('data', (d)=> relayOut += d);

async function waitForRelay(){
  for(let i = 0; i < 100; i++){
    try {
      await fetch(url);
      return;
    } catch(e) {
      await new Promise((r)=> setTimeout(r, 100));
    }
  }
  fail(`relay never came up on ${port}: ${relayOut}`);
}

try {
  await waitForRelay();

  /* Seed the relay from the native side so the wasm build is reading a store
   * it did not write. */
  native(sourceDb,
    "select dolt_config('user.name','native');" +
    "select dolt_config('user.email','native@example.com');" +
    "create table t(id integer primary key, v text);" +
    "insert into t values(1,'alpha'),(2,'beta');" +
    "select dolt_commit('-Am','seed');" +
    `select dolt_remote('add','origin','${url}');` +
    "select dolt_push('origin','main');");

  const { default: sqlite3InitModule } = await import(pathToFileURL(nodeEntry).href);
  const sqlite3 = await sqlite3InitModule({
    locateFile: (name)=> name === 'sqlite3.wasm' ? wasmPath : name,
    wasmBinary: fs.readFileSync(wasmPath)
  });

  /* Clone: the request leaves wasm through the host's HTTP stack. */
  const db = new sqlite3.oo1.DB('/http-remote-clone.db', 'c');
  try {
    const rc = db.selectValue("select dolt_clone(?)", [url]);
    if(rc !== 0) fail(`dolt_clone returned ${rc}`);
    const count = db.selectValue("select count(*) from t");
    if(count !== 2) fail(`cloned ${count} rows, expected 2`);
    console.log(`clone: rows=${count}`);

    /* Push back, so the write direction is covered too. */
    db.exec("select dolt_config('user.name','wasm')");
    db.exec("select dolt_config('user.email','wasm@example.com')");
    db.exec("insert into t values(3,'from-wasm')");
    db.selectValue("select dolt_commit('-Am','written in wasm')");
    const pushed = db.selectValue("select dolt_push('origin','main')");
    if(pushed !== 0) fail(`dolt_push returned ${pushed}`);
    console.log('push: ok');
  } finally {
    db.close();
  }

  /* Fetch onto a second wasm handle. */
  const peer = new sqlite3.oo1.DB('/http-remote-fetch.db', 'c');
  try {
    peer.exec("select dolt_config('user.name','wasm')");
    peer.exec("select dolt_config('user.email','wasm@example.com')");
    const added = peer.selectValue("select dolt_remote('add','origin',?)", [url]);
    if(added !== 0) fail(`dolt_remote add returned ${added}`);
    const fetched = peer.selectValue("select dolt_fetch('origin','main')");
    if(fetched !== 0) fail(`dolt_fetch returned ${fetched}`);
    const messages = peer.selectObjects("select message from dolt_log('origin/main')")
          .map((r)=> r.message);
    if(!messages.includes('written in wasm')){
      fail(`fetched log missing the wasm commit: ${messages.join(',')}`);
    }
    console.log(`fetch: log=${messages.length} entries`);
  } finally {
    peer.close();
  }

  /* The native side must see what wasm pushed. */
  const verifyDb = path.join(serveDir, 'verify.db');
  const rows = native(verifyDb,
    `select dolt_clone('${url}');` +
    "select group_concat(v, ',') from (select v from t order by id);");
  if(!rows.includes('from-wasm')){
    fail(`native clone does not show the wasm row: ${JSON.stringify(rows)}`);
  }
  console.log(`native readback: ${rows.split('\n').pop()}`);
} finally {
  relay.kill('SIGKILL');
  fs.rmSync(serveDir, { recursive: true, force: true });
}
