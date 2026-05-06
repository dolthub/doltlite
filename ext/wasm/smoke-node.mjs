import sqlite3InitModule from './jswasm/sqlite3-node.mjs';
const sqlite3 = await sqlite3InitModule();
const db = new sqlite3.oo1.DB('/tmp/doltlite-wasm-smoke.db','ct');
try {
  const ver = db.selectValue("select doltlite_version()");
  const eng = db.selectValue("select doltlite_engine()");
  console.log('version=', ver);
  console.log('engine=', eng);
  db.exec("select dolt_config('user.name','Wasm Test');");
  db.exec("select dolt_config('user.email','wasm@example.com');");
  db.exec("create table t(id integer primary key, v text);");
  db.exec("insert into t values(1,'a');");
  db.exec("select dolt_add('-A');");
  const h = db.selectValue("select dolt_commit('-m','wasm smoke')");
  const c = db.selectValue("select count(*) from dolt_log");
  console.log('commit=', h);
  console.log('log_count=', c);
} finally {
  db.close();
}
