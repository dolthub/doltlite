import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import http from 'node:http';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const consumerRequire = createRequire(path.join(process.cwd(), 'package.json'));
const playwrightRoot = process.env.DOLTLITE_PLAYWRIGHT_PREFIX
  || fileURLToPath(new URL('../../build/wasm-browser', import.meta.url));
const playwrightRequire = createRequire(path.resolve(playwrightRoot, 'package.json'));
const { chromium } = playwrightRequire('playwright');
const name = '@dolthub/doltlite-wasm';
const packageDir = path.dirname(consumerRequire.resolve(name + '/package.json'));
const browserEntry = path.relative(packageDir, consumerRequire.resolve(name));
const rowCount = 5000;

async function runWorker(init, rowCount) {
  if (!self.crossOriginIsolated) throw new Error('Worker is missing cross-origin isolation');
  const sqlite3 = await init();
  const phase = new URL(self.location.href).searchParams.get('phase');
  const commitMessage = 'browser OPFS package smoke';
  const db = new sqlite3.oo1.OpfsDb('/package-smoke.db', phase === 'write' ? 'c' : 'r');
  let result;
  try {
    if (phase === 'write') {
      db.exec('CREATE TABLE t(id INTEGER PRIMARY KEY, v INT); BEGIN');
      const statement = db.prepare('INSERT INTO t(v) VALUES(?)');
      try {
        for (let i = 0; i < rowCount; ++i) statement.bind([i]).stepReset();
      } finally {
        statement.finalize();
      }
      db.exec('COMMIT');
      db.selectValue("SELECT dolt_commit('-A', '-m', ?)", [commitMessage]);
    }
    result = {
      phase,
      engine: db.selectValue('SELECT doltlite_engine()'),
      version: db.selectValue('SELECT dolt_version()'),
      rows: db.selectValue('SELECT count(*) FROM t'),
      badRows: db.selectValue(`SELECT count(*) FROM t WHERE id NOT BETWEEN 1 AND ${rowCount} OR v IS NOT id - 1`),
      commits: db.selectValue('SELECT count(*) FROM dolt_log'),
      commit: db.selectValue('SELECT commit_hash FROM dolt_log WHERE message = ?', [commitMessage]),
      integrity: db.selectValue('PRAGMA integrity_check'),
    };
  } finally {
    db.close();
  }
  if (phase === 'write') {
    const memory = new sqlite3.oo1.DB(':memory:');
    try {
      memory.exec("CREATE TABLE t(a); SELECT dolt_commit('-Am', 'memory commit')");
    } finally {
      memory.close();
    }
  }
  self.postMessage(result);
}

const pageSource = `<!doctype html><meta charset="utf-8">
<script>
  const phase = sessionStorage.getItem('phase') || 'write';
  const worker = new Worker('/worker.mjs?phase=' + phase, { type: 'module' });
  worker.onmessage = event => window.smokeResult = event.data;
  worker.onerror = event => window.smokeResult = { error: event.message };
</script>`;
const workerSource = `
import { sqlite3InitModule } from ${JSON.stringify('/package/' + browserEntry)};
(${runWorker})(sqlite3InitModule, ${rowCount}).catch(error => self.postMessage({ error: error.stack || String(error) }));
`;
const headers = {
  'Cache-Control': 'no-store',
  'Cross-Origin-Opener-Policy': 'same-origin',
  'Cross-Origin-Embedder-Policy': 'require-corp',
};
const server = http.createServer(async (request, response) => {
  const { pathname } = new URL(request.url, 'http://localhost');
  let type, body;
  if (pathname === '/') {
    type = 'text/html; charset=utf-8';
    body = pageSource;
  } else if (pathname === '/worker.mjs') {
    type = 'text/javascript';
    body = workerSource;
  } else if (/^\/package\/[\w.-]+$/.test(pathname)) {
    const filename = path.basename(pathname);
    type = filename.endsWith('.wasm') ? 'application/wasm' : 'text/javascript';
    body = await fs.readFile(path.join(packageDir, filename)).catch(() => undefined);
  }
  if (body === undefined) {
    response.writeHead(404);
    response.end('Missing ' + pathname);
  } else {
    response.writeHead(200, { ...headers, 'Content-Type': type });
    response.end(body);
  }
});

let browser;
try {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  browser = await chromium.launch({ executablePath: process.env.DOLTLITE_BROWSER_EXECUTABLE || undefined });
  const page = await browser.newPage();
  await page.goto(`http://127.0.0.1:${server.address().port}/`);

  async function check(phase) {
    assert.equal(await page.evaluate(() => crossOriginIsolated), true);
    await page.waitForFunction(() => window.smokeResult, null, { timeout: 60000 });
    const { error, version, commit, ...stats } = await page.evaluate(() => window.smokeResult);
    assert.ok(!error, error);
    assert.deepEqual(stats, { phase, engine: 'prolly', rows: rowCount, badRows: 0, commits: 2, integrity: 'ok' });
    assert.ok(version, 'missing engine version');
    if (process.env.DOLTLITE_EXPECT_VERSION) assert.equal(version, process.env.DOLTLITE_EXPECT_VERSION);
    assert.ok(commit, 'missing Dolt commit');
    return { version, commit };
  }

  const written = await check('write');
  await page.evaluate(() => sessionStorage.setItem('phase', 'reopen'));
  await page.reload();
  const reopened = await check('reopen');
  assert.equal(reopened.commit, written.commit);
  assert.equal(reopened.version, written.version);
  console.log(`WASM npm browser smoke passed: ${reopened.version}, ${rowCount} OPFS rows and commit survived reload`);
} finally {
  if (browser) await browser.close();
  if (server.listening) await new Promise(resolve => server.close(resolve));
}
