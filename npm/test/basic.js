'use strict';

const Database = require('../lib');
const fs = require('fs');
const os = require('os');
const path = require('path');

let pass = 0, fail = 0;
function assert(cond, msg) {
  if (cond) { pass++; }
  else { fail++; console.error('  FAIL:', msg); }
}

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'doltlite-test-'));
const dbpath = path.join(tmp, 'test.db');

console.log('=== doltlite npm binding smoke test ===');
console.log('SQLite version:', Database.version());
console.log('DB path:', dbpath);
console.log();

// 1. Open and basic SQL.
const db = new Database(dbpath);
assert(db.open, 'db.open is true after construction');
db.exec(`CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, age INT)`);
const insert = db.prepare(`INSERT INTO users(name, age) VALUES (?, ?)`);
const r1 = insert.run('alice', 30);
assert(r1.changes === 1, 'INSERT alice changes=1');
assert(r1.lastInsertRowid === 1, 'INSERT alice rowid=1');
const r2 = insert.run('bob', 25);
assert(r2.lastInsertRowid === 2, 'INSERT bob rowid=2');

// 2. Select via prepare + all/get.
const all = db.prepare(`SELECT * FROM users ORDER BY id`).all();
assert(all.length === 2, 'all() returns 2 rows');
assert(all[0].name === 'alice' && all[0].age === 30, 'row 0 correct');
assert(all[1].name === 'bob' && all[1].age === 25, 'row 1 correct');

const one = db.prepare(`SELECT name FROM users WHERE id = ?`).get(2);
assert(one && one.name === 'bob', 'get(2).name === bob');

const none = db.prepare(`SELECT name FROM users WHERE id = ?`).get(999);
assert(none === undefined, 'get(non-existent) === undefined');

// 3. dolt_commit / dolt_log — the whole point of doltlite.
db.exec(`SELECT dolt_commit('-A', '-m', 'initial users')`);
const log = db.prepare(`SELECT message FROM dolt_log ORDER BY date DESC`).all();
assert(log.length >= 2, 'dolt_log has at least seed + first commit');
assert(log[0].message === 'initial users',
       `latest commit message = "${log[0].message}"`);

// 4. Branch.
db.exec(`SELECT dolt_branch('feat')`);
db.exec(`SELECT dolt_checkout('feat')`);
db.prepare(`INSERT INTO users(name, age) VALUES (?, ?)`).run('carol', 40);
db.exec(`SELECT dolt_commit('-A', '-m', 'add carol')`);
const branchAll = db.prepare(`SELECT count(*) AS n FROM users`).get();
assert(branchAll.n === 3, 'feat has 3 users');

db.exec(`SELECT dolt_checkout('main')`);
const mainAll = db.prepare(`SELECT count(*) AS n FROM users`).get();
assert(mainAll.n === 2, 'main still has 2 users');

// 5. Merge feat back into main.
db.exec(`SELECT dolt_merge('feat')`);
const merged = db.prepare(`SELECT count(*) AS n FROM users`).get();
assert(merged.n === 3, 'after merge main has 3 users');

// 6. close() cleanup.
db.close();
assert(!db.open, 'db.open is false after close()');

console.log();
console.log(`=== Results: ${pass} passed, ${fail} failed ===`);
fs.rmSync(tmp, { recursive: true, force: true });
process.exit(fail > 0 ? 1 : 0);
