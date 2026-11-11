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
const native = path.resolve('build/doltlite');
const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'doltlite-opfs-import-'));

async function browserTest(){
  const {default:init} = await import('./assets/sqlite3.mjs');
  const sqlite3 = await init();
  const pool = await sqlite3.installOpfsSAHPoolVfs({
    name:'doltlite-import-test', initialCapacity:32
  });
  const failures = [], exports = [];
  let checks = 0;
  const check = (value, message)=>{if(!value) throw new Error(message); checks++;};
  const modes = ['bytes','arraybuffer','stream','split-header'];
  const filename = (source, mode)=>`/${source}-${mode}.db`;
  try {
    if(new URL(location.href).searchParams.get('phase')==='reopen'){
      for(const source of ['native','wasm']) for(const mode of modes){
        const db = new pool.OpfsSAHPoolDb(filename(source,mode));
        try {
          check(db.selectValue('SELECT count(*) FROM t')===3,'reloaded rows');
          check(db.selectValue('SELECT count(*) FROM dolt_log')===3,'reloaded commits');
          check(db.selectValue('PRAGMA integrity_check')==='ok','reloaded integrity');
        }finally{db.close();}
      }
    }else{
      const nativeBytes = new Uint8Array(await (await fetch('./native.db')).arrayBuffer());
      const original = new sqlite3.oo1.DB(':memory:');
      let wasmBytes;
      try {
        original.exec("CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT);"+
          "CREATE INDEX t_v ON t(v); INSERT INTO t VALUES(1,'one'),(2,'two');"+
          "SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feature');");
        wasmBytes = sqlite3.capi.sqlite3_js_db_export(original.pointer);
      }finally{original.close();}
      const importBytes = async(name, bytes, mode)=>{
        if(mode==='bytes') return pool.importDb(name,bytes);
        if(mode==='arraybuffer') return pool.importDb(name,bytes.buffer.slice(bytes.byteOffset,bytes.byteOffset+bytes.byteLength));
        let pos = 0, part = 0;
        return pool.importDb(name,()=>{
          if(pos===bytes.length) return undefined;
          const n = mode==='split-header' && part<3 ? ++part : 257;
          const next = bytes.slice(pos,pos+n);
          pos += next.length;
          return part%2 ? next.buffer : next;
        });
      };
      for(const [source,bytes] of [['native',nativeBytes],['wasm',wasmBytes]]){
        for(const mode of modes){
          const name = filename(source,mode);
          try {
            check(await importBytes(name,bytes,mode)===bytes.length,'import size');
            const saved = pool.exportFile(name);
            check(saved.length===bytes.length && saved.every((v,i)=>v===bytes[i]),'DoltLite bytes changed during import');
            const db = new pool.OpfsSAHPoolDb(name);
            try {
              check(db.selectValue('SELECT group_concat(v) FROM (SELECT v FROM t ORDER BY id)')==='one,two','imported values');
              check(db.selectValue("SELECT count(*) FROM sqlite_schema WHERE name='t_v'")===1,'imported index');
              check(db.selectValue("SELECT count(*) FROM dolt_branches WHERE name='feature'")===1,'imported branch');
              check(db.selectValue('SELECT count(*) FROM dolt_log')===2,'imported history');
              db.exec("INSERT INTO t VALUES(3,'three'); SELECT dolt_commit('-Am','import edit');");
              check(db.selectValue('PRAGMA integrity_check')==='ok','edited integrity');
            }finally{db.close();}
            exports.push({name,bytes:Array.from(pool.exportFile(name))});
          }catch(error){failures.push(`${source}/${mode}: ${error.message}`);}
        }
      }
      const stock = new Uint8Array(await (await fetch('./stock.db')).arrayBuffer());
      stock[18] = stock[19] = 2;
      for(const mode of modes){
        try {
          const name = filename('stock',mode);
          await importBytes(name,stock,mode);
          const saved = pool.exportFile(name);
          check(saved[18]===1 && saved[19]===1,'SQLite WAL flags were not cleared');
          check(stock[18]===2 && stock[19]===2,'input bytes changed');
          const db = new pool.OpfsSAHPoolDb(name,'r');
          try{check(db.selectValue('SELECT count(*) FROM t')===2,'SQLite rows');}
          finally{db.close();}
        }catch(error){failures.push(`stock/${mode}: ${error.message}`);}
      }
      const badInputs = [new Uint8Array(),new Uint8Array(512),new TextEncoder().encode('SQLite format 3\0'),new Uint8Array([67,84,76,68]),new Uint8Array(167)];
      badInputs[4].set([67,84,76,68]);
      for(const bytes of badInputs) for(const mode of ['bytes','split-header']){
        const name = '/invalid.db';
        let rejected = false;
        try{await importBytes(name,bytes,mode);}catch{rejected=true;}
        check(rejected,'invalid image was accepted');
        check(!pool.getFileNames().includes(name),'invalid image has a pool entry');
      }
    }
  }finally{pool.pauseVfs();}
  postMessage({ok:failures.length===0,failures,exports,checks});
}

let server, browser;
try {
  const fixture = path.join(tmp,'native.db');
  const sql = "CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); CREATE INDEX t_v ON t(v);"+
    "INSERT INTO t VALUES(1,'one'),(2,'two'); SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feature');";
  const built = spawnSync(native,[fixture,sql],{encoding:'utf8'});
  assert.equal(built.status,0,built.stderr || String(built.error));
  const stockPath = path.join(tmp,'stock.db');
  const stock = spawnSync(process.env.PYTHON || 'python3',['-c',
    "import sqlite3,sys; d=sqlite3.connect(sys.argv[1]); d.executescript(\"CREATE TABLE t(id INTEGER PRIMARY KEY,v TEXT); INSERT INTO t VALUES(1,'one'),(2,'two');\"); d.close()",stockPath],{encoding:'utf8'});
  assert.equal(stock.status,0,stock.stderr || String(stock.error));
  server = http.createServer(async(req,res)=>{
    try {
      const url = new URL(req.url,'http://localhost');
      const isolated = url.pathname.startsWith('/isolated/');
      const relative = url.pathname.replace(/^\/(isolated|plain)\//,'');
      const headers = {'Cache-Control':'no-store'};
      if(isolated) Object.assign(headers,{'Cross-Origin-Opener-Policy':'same-origin','Cross-Origin-Embedder-Policy':'require-corp'});
      let body, type='text/javascript';
      if(relative===''){
        type='text/html';
        body=`<script>window.result=null;const w=new Worker('worker.mjs'+location.search,{type:'module'});w.onmessage=e=>window.result=e.data;w.onerror=e=>window.result={ok:false,failures:[e.message]};</script>`;
      }else if(relative==='worker.mjs'){
        body=`(${browserTest.toString()})().catch(e=>postMessage({ok:false,failures:[e.stack]}));`;
      }else if(relative==='native.db' || relative==='stock.db'){
        body=await fs.readFile(relative==='native.db'?fixture:stockPath);type='application/octet-stream';
      }else if(relative.startsWith('assets/') && path.basename(relative)===relative.slice(7)){
        const file=relative.slice(7);body=await fs.readFile(path.join(assets,file));
        if(file.endsWith('.wasm'))type='application/wasm';
      }else{throw new Error('unknown path');}
      res.writeHead(200,{'Content-Type':type,...headers});res.end(body);
    }catch(error){res.writeHead(404);res.end(String(error));}
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  browser=await chromium.launch({executablePath:process.env.DOLTLITE_BROWSER_EXECUTABLE || undefined});
  for(const mode of ['isolated','plain']){
    const context=await browser.newContext();
    try {
      const page=await context.newPage();
      for(const phase of ['import','reopen']){
        await page.goto(`http://127.0.0.1:${server.address().port}/${mode}/?phase=${phase}`);
        assert.equal(await page.evaluate(()=>crossOriginIsolated),mode==='isolated');
        await page.waitForFunction(()=>window.result,null,{timeout:120000});
        const result=await page.evaluate(()=>window.result);
        assert.equal(result.ok,true,(result.failures || []).join('\n'));
        for(const exported of result.exports || []){
          const output=path.join(tmp,mode+'-'+path.basename(exported.name));
          await fs.writeFile(output,new Uint8Array(exported.bytes));
          const verified=spawnSync(native,[output,"SELECT count(*) FROM t; SELECT count(*) FROM dolt_log; SELECT count(*) FROM dolt_branches WHERE name='feature'; PRAGMA integrity_check;"],{encoding:'utf8'});
          assert.equal(verified.status,0,verified.stderr);
          assert.equal(verified.stdout.trim().replaceAll('\r',''),'3\n3\n1\nok');
        }
        console.log(`PASS: ${mode} ${phase}: ${result.checks} browser checks`);
      }
    }finally{await context.close();}
  }
}finally{
  if(browser)await browser.close();
  if(server)await new Promise(resolve=>server.close(resolve));
  await fs.rm(tmp,{recursive:true,force:true});
}
