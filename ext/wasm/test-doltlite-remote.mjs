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

/* Remote ops nest a second chunk store open under an already-deep VDBE
 * stack. That is the deepest path the engine runs in a browser, and the
 * one that overflowed the 64K wasm stack while the release build had no
 * check to report it. */
function seedSource(){
  const db = new sqlite3.oo1.DB('/remote-source.db', 'c');
  try {
    db.exec("select dolt_config('user.name','wasm')");
    db.exec("select dolt_config('user.email','wasm@example.com')");
    db.exec("create table t(id integer primary key, v text)");
    db.exec("insert into t values(1,'alpha')");
    db.selectValue("select dolt_commit('-Am','c1')");
    db.exec("insert into t values(2,'beta')");
    db.selectValue("select dolt_commit('-Am','c2')");
  } finally {
    db.close();
  }
}

function checkFetch(){
  const db = new sqlite3.oo1.DB('/remote-fetch.db', 'c');
  try {
    db.exec("select dolt_config('user.name','wasm')");
    db.exec("select dolt_config('user.email','wasm@example.com')");
    const added = db.selectValue(
      "select dolt_remote('add','peer','file:///remote-source.db')");
    if(added !== 0) fail(`dolt_remote add returned ${added}`);

    const fetched = db.selectValue("select dolt_fetch('peer')");
    if(fetched !== 0) fail(`dolt_fetch returned ${fetched}`);

    const messages = db.selectObjects("select message from dolt_log('peer/main')")
          .map((r)=>r.message);
    if(!messages.includes('c2')) fail(`fetched log missing c2: ${messages.join(',')}`);

    console.log(`fetch: refs=peer/main log=${messages.length} entries`);
  } finally {
    db.close();
  }
}

function checkClone(){
  const db = new sqlite3.oo1.DB('/remote-clone.db', 'c');
  try {
    const cloned = db.selectValue("select dolt_clone('file:///remote-source.db')");
    if(cloned !== 0) fail(`dolt_clone returned ${cloned}`);

    const count = db.selectValue("select count(*) from t");
    const value = db.selectValue("select v from t where id = 2");
    if(count !== 2) fail(`cloned db has ${count} rows, expected 2`);
    if(value !== 'beta') fail(`cloned db has ${value}, expected beta`);

    console.log(`clone: rows=${count}`);
  } finally {
    db.close();
  }
}

function checkPush(){
  const db = new sqlite3.oo1.DB('/remote-push.db', 'c');
  try {
    db.exec("select dolt_config('user.name','wasm')");
    db.exec("select dolt_config('user.email','wasm@example.com')");
    db.exec("create table u(id integer primary key)");
    db.exec("insert into u values(1)");
    db.selectValue("select dolt_commit('-Am','push me')");
    const added = db.selectValue(
      "select dolt_remote('add','out','file:///remote-pushed.db')");
    if(added !== 0) fail(`dolt_remote add returned ${added}`);
    const pushed = db.selectValue("select dolt_push('out','main')");
    if(pushed !== 0) fail(`dolt_push returned ${pushed}`);
    console.log('push: ok');
  } finally {
    db.close();
  }
}

function checkPushedContent(){
  const db = new sqlite3.oo1.DB('/remote-pushed-clone.db', 'c');
  try {
    const cloned = db.selectValue("select dolt_clone('file:///remote-pushed.db')");
    if(cloned !== 0) fail(`dolt_clone of pushed store returned ${cloned}`);
    const count = db.selectValue("select count(*) from u");
    if(count !== 1) fail(`pushed store has ${count} rows, expected 1`);
    console.log(`pushed content: rows=${count}`);
  } finally {
    db.close();
  }
}

seedSource();
checkFetch();
checkClone();
checkPush();
checkPushedContent();
