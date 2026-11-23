import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import {createRequire} from 'node:module';
import {spawnSync} from 'node:child_process';

const require = createRequire(path.resolve(
  process.env.DOLTLITE_PLAYWRIGHT_PREFIX || 'build/wasm-browser', 'package.json'
));
const {chromium} = require('playwright');
const assets = path.resolve(process.env.DOLTLITE_WASM_ASSETS || 'ext/wasm/jswasm');
const native = path.resolve(process.env.DOLTLITE || 'build/doltlite');
const fixture = `CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, payload BLOB);
  WITH RECURSIVE r(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM r WHERE i<640)
  INSERT INTO t SELECT i,i,randomblob(700) FROM r;
  SELECT dolt_commit('-Am','base');
  SELECT dolt_branch('saved');
  UPDATE t SET payload=randomblob(700), n=n+1;
  UPDATE t SET payload=randomblob(700), n=n+1;`;

async function workerTest({backend, action, sql, failAt=Infinity}) {
  const {default:init} = await import('./assets/sqlite3.mjs');
  const {capi, oo1, wasm, installOpfsSAHPoolVfs} = await init({print(){}, printErr(){}});
  const name = '/gc-recovery.db';
  const pool = backend==='pool' ? await installOpfsSAHPoolVfs({
    name:'gc-recovery-pool', initialCapacity:12
  }) : null;
  const vfsName = pool ? 'gc-recovery-pool' : 'opfs';
  const Db = pool?.OpfsSAHPoolDb || oo1.OpfsDb;
  const vfs = new capi.sqlite3_vfs(capi.sqlite3_vfs_find(vfsName));
  const files = new Map(), installed = new Set();
  const operations = [];
  let armed = false;

  const tracked = file => file===name || file===name+'-gc-tmp'
    || file===name+'-gc-replace';
  function intercept(obj, method, invoke) {
    const member = '$'+method;
    const base = wasm.functionEntry(obj[member]);
    obj[member] = wasm.installFunction(obj.memberSignature(method),
      (...args) => invoke(base, args));
  }
  function fault(file, op) {
    if (!armed || !tracked(file)) return false;
    operations.push([file.slice(name.length), op]);
    return operations.length>failAt;
  }
  intercept(vfs, 'xOpen', (base, args) => {
    const rc = base(...args);
    if (rc!==capi.SQLITE_OK || !args[1]) return rc;
    const file = wasm.cstrToJs(args[1]);
    const pointer = args[2];
    files.set(pointer, file);
    const methods = new capi.sqlite3_file(pointer).$pMethods;
    if (installed.has(methods)) return rc;
    installed.add(methods);
    const io = new capi.sqlite3_io_methods(methods);
    for (const op of ['xWrite', 'xTruncate', 'xSync']) {
      intercept(io, op, (call, argv) => {
        if (!fault(files.get(argv[0]), op)) return call(...argv);
        if (op==='xWrite' && operations.length===failAt+1 && argv[2]>1) {
          argv[2] = Math.floor(argv[2]/2);
          call(...argv);
        }
        return capi.SQLITE_IOERR;
      });
    }
    return rc;
  });
  intercept(vfs, 'xDelete', (base, args) =>
    fault(wasm.cstrToJs(args[1]), 'xDelete') ? capi.SQLITE_IOERR : base(...args));

  function reset() {
    const exists = wasm.scopedAlloc(4);
    for (const suffix of ['', '-gc-tmp', '-gc-replace']) {
      const file = wasm.scopedAllocCString(name+suffix);
      const access = wasm.functionEntry(vfs.$xAccess);
      const remove = wasm.functionEntry(vfs.$xDelete);
      const rc = access(vfs.pointer, file, capi.SQLITE_ACCESS_EXISTS, exists);
      if (rc!==capi.SQLITE_OK) throw new Error('access failed: '+rc);
      if (wasm.peek32(exists)) {
        const deleted = remove(vfs.pointer, file, 0);
        if (deleted!==capi.SQLITE_OK) throw new Error('delete failed: '+deleted);
      }
    }
  }

  let db, error, result, postRecoveryWrites = false;
  try {
    if (action==='compact') {
      const scope = wasm.scopedAllocPush();
      try { reset(); } finally { wasm.scopedAllocPop(scope); }
      const bytes = new Uint8Array(await (await fetch('/fixture.db')).arrayBuffer());
      if (pool) {
        await pool.importDb(name,bytes);
      } else {
        const root = await navigator.storage.getDirectory();
        const file = await root.getFileHandle(name.slice(1),{create:true});
        const handle = await file.createSyncAccessHandle();
        try {
          if (handle.write(bytes)!==bytes.length) throw new Error('short fixture write');
          handle.flush();
        } finally {
          handle.close();
        }
      }
      db = new Db(name,'w');
      armed = true;
      db.exec(sql);
    } else {
      armed = true;
      db = new Db(name, action==='readonly' ? 'r' : 'w');
      armed = false;
      if (action==='write') {
        db.exec('INSERT INTO t VALUES(641,641,zeroblob(700))');
        postRecoveryWrites = true;
      }
      result = [
        db.selectValue('SELECT count(*) FROM t'),
        db.selectValue('SELECT sum(n) FROM t'),
        db.selectValue('SELECT sum(length(payload)) FROM t'),
        db.selectValue('SELECT count(*) FROM dolt_log'),
        db.selectValue("SELECT count(*) FROM dolt_branches WHERE name='saved'"),
        db.selectValue('PRAGMA integrity_check')
      ];
    }
  } catch(e) {
    error = String(e);
  } finally {
    db?.close();
  }
  return {error, result, operations, postRecoveryWrites};
}

const expected = [640, 206400, 448000, 2, 1, 'ok'];
const expectedWithWrite = [641, 207041, 448700, 2, 1, 'ok'];
const temp = await fs.mkdtemp(path.join(os.tmpdir(),'doltlite-opfs-gc-'));
let server, browser;
try {
  const image = path.join(temp,'fixture.db');
  const built = spawnSync(native,[image,fixture],{encoding:'utf8'});
  assert.equal(built.status,0,built.stderr || String(built.error));
  const bytes = await fs.readFile(image);
  server = http.createServer(async(req,res) => {
    try {
      const url = new URL(req.url,'http://localhost');
      let body, type = 'text/javascript';
      if (url.pathname==='/') {
        body = '<!doctype html>';
        type = 'text/html';
      } else if (url.pathname==='/fixture.db') {
        body = bytes;
        type = 'application/octet-stream';
      } else if (url.pathname==='/worker.mjs') {
        body = `onmessage=async e=>{
          try {postMessage(await (${workerTest.toString()})(e.data))}
          catch(e){postMessage({fatal:e.stack})}
        };`;
      } else if (/^\/assets\/[\w.-]+$/.test(url.pathname)) {
        body = await fs.readFile(path.join(assets,path.basename(url.pathname)));
        if (url.pathname.endsWith('.wasm')) type = 'application/wasm';
      } else {
        throw new Error('unknown path');
      }
      res.writeHead(200,{
        'Content-Type':type,
        'Cross-Origin-Opener-Policy':'same-origin',
        'Cross-Origin-Embedder-Policy':'require-corp'
      });
      res.end(body);
    } catch(e) {
      res.writeHead(404);
      res.end(String(e));
    }
  });
  await new Promise(done => server.listen(0,'127.0.0.1',done));
  browser = await chromium.launch({executablePath:process.env.DOLTLITE_BROWSER_EXECUTABLE || undefined});
  for (const backend of ['pool','opfs']) {
    const page = await browser.newPage();
    await page.goto('http://127.0.0.1:'+server.address().port+'/');
    const ask = async data => {
      const answer = await page.evaluate(data => new Promise((done, reject) => {
        const worker = new Worker('/worker.mjs',{type:'module'});
        const timer = setTimeout(() => {
          worker.terminate();
          reject(new Error('GC worker timed out'));
        }, 15000);
        worker.onerror = e => {
          clearTimeout(timer);
          worker.terminate();
          reject(new Error(e.message));
        };
        worker.onmessage = ({data}) => {
          clearTimeout(timer);
          worker.terminate();
          setTimeout(() => done(data), 50);
        };
        worker.postMessage(data);
      }), {backend,...data});
      assert(!answer.fatal, answer.fatal);
      return answer;
    };
    for (const sql of ['SELECT dolt_gc()', 'VACUUM']) {
      const complete = await ask({action:'compact',sql});
      assert(!complete.error, backend+': '+complete.error);
      const prepared = complete.operations.findIndex(([file,op])=>file==='-gc-tmp' && op==='xSync');
      assert(prepared>=0, backend+': temp image was never synced');
      const first = complete.operations.findIndex(([file,op],i)=>i>prepared
        && file==='' && op==='xWrite');
      assert(first>=0, backend+': no database write');
      const last = complete.operations.findLastIndex(([file])=>file==='' || file==='-gc-replace');
      const before = complete.operations.findIndex(([file])=>file==='-gc-replace');
      const cases = [first+1];
      if (before>=0 && sql==='SELECT dolt_gc()') {
        cases.push(...Array.from({length:last-before+1},(_,i)=>before+i)
          .filter(i=>i!==first+1));
      }
      for (const failAt of cases) {
        const detail = backend+' '+sql+' after mutation '+failAt;
        await ask({action:'compact',sql,failAt});
        if (failAt===first+1 && before>=0) {
          const readonly = await ask({action:'readonly'});
          assert.match(readonly.error || '', /READONLY|read.?only/i, detail);
          const cutRecovery = await ask({action:'open',failAt:1});
          assert.match(cutRecovery.error || '', /IOERR|disk I\/O/i, detail);
        }
        const reopened = await ask({action:'open'});
        assert.deepEqual(reopened.result, expected, detail+' '+(reopened.error || ''));
      }
      const write = await ask({action:'write'});
      assert.equal(write.postRecoveryWrites, true, write.error);
      assert.deepEqual(write.result,expectedWithWrite);
      assert.deepEqual((await ask({action:'open'})).result,expectedWithWrite);
      console.log('PASS: '+backend+' '+sql+' '+cases.length+' interruption points');
    }
    await page.close();
  }
} finally {
  await browser?.close();
  if (server) await new Promise(done => server.close(done));
  await fs.rm(temp,{recursive:true,force:true});
}
