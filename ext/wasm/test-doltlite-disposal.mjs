import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const entry = path.resolve('ext/wasm/jswasm/sqlite3-node.mjs');
const wasmPath = path.resolve('ext/wasm/jswasm/sqlite3.wasm');
const { default: sqlite3InitModule } = await import(pathToFileURL(entry).href);
const sqlite3 = await sqlite3InitModule({
  wasmBinary: fs.readFileSync(wasmPath)
});
const { capi, oo1 } = sqlite3;

for(const filename of [':memory:', '/disposal.db']){
  const db = new oo1.DB(filename, 'c');
  const stmt = db.prepare('SELECT 1');
  assert.equal(stmt.finalize(), capi.SQLITE_OK);
  assert.equal(stmt.pointer, undefined, 'finalize must invalidate the statement');
  assert.equal(stmt.finalize(), undefined, 'finalize must be idempotent');
  assert.equal(db.openStatementCount(), 0);

  const rawStmt = db.prepare('SELECT 2');
  assert.equal(capi.sqlite3_finalize(rawStmt.pointer), capi.SQLITE_OK);
  assert.equal(rawStmt.pointer, undefined, 'C finalize must invalidate the wrapper');
  assert.equal(db.openStatementCount(), 0);

  db.exec('CREATE TABLE t(a)');
  for(let i = 0; i < 8; ++i){
    db.exec(`INSERT INTO t VALUES(${i})`);
    assert.equal(typeof db.selectValue(`SELECT dolt_commit('-Am', 'commit ${i}')`), 'string');
    assert.equal(db.selectValue('SELECT count(*) FROM t'), i + 1);
    assert.equal(db.openStatementCount(), 0, 'queries must release their statements');
  }
  const pending = db.prepare('SELECT * FROM t');
  db.close();
  assert.equal(pending.pointer, undefined, 'close must finalize pending statements');
  assert.equal(db.pointer, undefined);
  db.close();

  const rawDb = new oo1.DB(':memory:');
  const rawPending = rawDb.prepare('SELECT 3');
  assert.equal(capi.sqlite3_close_v2(rawDb.pointer), capi.SQLITE_OK);
  assert.equal(rawDb.pointer, undefined, 'C close must invalidate the wrapper');
  assert.equal(rawPending.pointer, undefined, 'C close must finalize wrapped statements');
  rawDb.close();
  console.log(`${filename}: WASM disposal passed`);
}
