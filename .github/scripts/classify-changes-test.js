'use strict';

const assert = require('assert');
const fs = require('fs');
const path = require('path');
const {classify, OFF_PR_WORKFLOWS, JOBS} = require('./classify-changes.js');

const workflows = path.join(__dirname, '..', 'workflows');
let checks = 0;

function expect(paths, jobs, labels) {
  const result = classify(paths, labels);
  const actual = JOBS.filter(job => result[job]);
  assert.deepStrictEqual(actual, jobs, `${paths.join(' ')}: ${JSON.stringify(result)}`);
  assert.strictEqual(result.full, jobs.length === JOBS.length, paths.join(' '));
  checks++;
  return result;
}

const all = [...JOBS];
expect(['src/btree.c'], all);
expect(['ext/fts5/fts5_main.c'], all);
expect(['tool/mksqlite3c.tcl'], all);
expect(['main.mk'], all);
expect(['VERSION'], all);
expect(['some/new/dir/file.txt'], all);
expect(['.github/workflows/pr-ci.yml'], all);
expect(['.github/workflows/test.yml'], all);
expect(['.github/actions/checked-build/action.yml'], all);
expect(['.github/scripts/classify-changes.js'], all);
expect([], all);

expect(['test/doltlite_merge.sh'], ['suites', 'platforms']);
expect(['test/regression-buckets/storage.txt', 'test/known_testfixture_divergences.txt'], ['suites', 'platforms']);
expect(['test/doltlite_vc_matrix.test'], ['suites', 'platforms']);
expect(['test/concurrent_stress_test.c'], ['suites', 'platforms', 'compile']);
expect(['test/lib/harness.h'], ['suites', 'platforms', 'compile']);

expect(['test/sysbench_compare.sh', 'test/lib/sysbench_report.py'], ['benchmarks']);
expect(['test/performance_hotspots.py', 'test/vc_perf_ceiling.sh', 'test/remote_perf.py'], ['benchmarks']);
expect(['.github/workflows/benchmark.yml'], ['benchmarks']);
expect(['test/sysbench_timer.c'], ['suites', 'platforms', 'benchmarks', 'compile']);

expect(['packaging/rust/Cargo.toml'], ['platforms', 'wasm']);
expect(['.dolt-oracle-version'], ['suites']);

let docs = expect(['doc/doltlite/merge.md', 'AGENTS.md'], []);
assert.strictEqual(docs.docs_changed, true);
assert.strictEqual(docs.light, false);
docs = expect(['README.md'], []);
assert.strictEqual(docs.light, true);
docs = expect(['README.md', 'src/btree.c'], all);
assert.strictEqual(docs.light, false);
checks++;

expect(['.github/workflows/nightly-fuzz.yml', '.github/workflows/release.yml'], ['suites']);
expect(['.github/workflows/nightly-hotspots.yml'], ['suites', 'benchmarks']);
expect(['doc/doltlite/pragmas.md'], ['suites']);
expect(['LICENSE.md'], ['platforms', 'wasm']);
expect(['test/lib/vc_perf_benchmark.sh', 'test/release_performance_summary_test.py'], ['benchmarks']);
expect(['test/build_stock_reference.sh', 'test/savepoint_rollback_perf.py'], ['suites', 'platforms', 'benchmarks']);
expect(['test/wasm_incremental_build_test.sh'], ['suites', 'platforms', 'wasm']);
expect(['test/testrunner_data.tcl'], ['suites', 'platforms', 'compile']);
expect(['doc/doltlite/merge.md'], all, ['full-ci']);
expect(['README.md', 'test/doltlite_merge.sh'], ['suites', 'platforms']);
expect(['src/btree.c', 'test/doltlite_merge.sh'], all);

function called(file, seen) {
  if (seen.has(file)) return;
  seen.add(file);
  const text = fs.readFileSync(path.join(workflows, file), 'utf8');
  for (const match of text.matchAll(/uses:\s*\.\/\.github\/workflows\/([\w.-]+\.yml)/g)) {
    called(match[1], seen);
  }
}
const reachable = new Set();
called('pr-ci.yml', reachable);
for (const file of fs.readdirSync(workflows).filter(name => name.endsWith('.yml'))) {
  if (reachable.has(file)) {
    assert(!OFF_PR_WORKFLOWS.has(file), `${file} runs in PR CI but is classified off-PR`);
  } else {
    assert(OFF_PR_WORKFLOWS.has(file), `${file} is not called by PR CI; list it in OFF_PR_WORKFLOWS`);
  }
  checks++;
}
for (const file of OFF_PR_WORKFLOWS) {
  assert(fs.existsSync(path.join(workflows, file)), `${file} no longer exists`);
  checks++;
}

const root = path.join(__dirname, '..', '..');
const read = file => fs.readFileSync(path.join(root, file), 'utf8');
const jobsOf = file => new Set(JOBS.filter(job => classify([file])[job]));
function scripts(dir) {
  const out = [];
  for (const entry of fs.readdirSync(path.join(root, dir), {withFileTypes: true})) {
    const rel = `${dir}/${entry.name}`;
    if (entry.isDirectory()) out.push(...scripts(rel));
    else if (/\.(sh|py|js|tcl)$/.test(entry.name)) out.push(rel);
  }
  return out;
}
const readers = [...scripts('test'), ...scripts('packaging')];

// A file a test reads must run at least one job that runs that test.
const docJobTests = new Set(read('.github/workflows/pr-ci.yml').match(/test\/[\w.-]+/g));
function readBy(data, reader) {
  const want = jobsOf(reader);
  assert([...jobsOf(data)].some(job => want.has(job)),
    `${data} is read by ${reader} but runs none of its jobs`);
  checks++;
}
for (const file of OFF_PR_WORKFLOWS) {
  assert(jobsOf(`.github/workflows/${file}`).has('suites'),
    `${file}: lint_orphaned_suites.sh reads every workflow`);
  for (const reader of readers) {
    if (read(reader).includes(file)) readBy(`.github/workflows/${file}`, reader);
  }
}
for (const reader of readers) {
  if (docJobTests.has(reader)) continue;
  const text = read(reader);
  for (const doc of new Set(text.match(/doc\/doltlite\/[\w.-]+\.md/g) || [])) {
    readBy(doc, reader);
  }
  if (/ROOT[}"]*\/LICENSE\.md/.test(text)) readBy('LICENSE.md', reader);
}

// Every test file a single-job workflow runs, and every test file those
// shell and Python scripts run, source or import, must classify into that
// job. Tcl is not followed: compile-checks only runs testrunner --buildonly.
const SINGLE_JOB = {'benchmark.yml': 'benchmarks', 'wasm.yml': 'wasm', 'compile-checks.yml': 'compile'};
for (const [workflow, job] of Object.entries(SINGLE_JOB)) {
  let text = read(`.github/workflows/${workflow}`);
  for (const [, action] of text.matchAll(/uses:\s*\.\/(\.github\/actions\/[\w-]+)/g)) {
    text += read(`${action}/action.yml`);
  }
  const queue = [...new Set(text.match(/test\/[\w./-]+\.\w+/g))].filter(f => fs.existsSync(path.join(root, f)));
  const seen = new Set(queue);
  while (queue.length) {
    const file = queue.shift();
    assert(jobsOf(file).has(job), `${file} runs in ${workflow} but does not classify into ${job}`);
    checks++;
    if (!/\.(sh|py)$/.test(file)) continue;
    const body = read(file);
    const names = [...body.matchAll(/([\w-]+\.(?:sh|py|tcl))\b/g)].map(m => m[1]);
    for (const [, mod] of body.matchAll(/^\s*(?:from|import)\s+(\w+)/gm)) names.push(`${mod}.py`);
    for (const name of names) {
      for (const dep of [`test/${name}`, `test/lib/${name}`]) {
        if (!seen.has(dep) && fs.existsSync(path.join(root, dep))) {
          seen.add(dep);
          queue.push(dep);
        }
      }
    }
  }
}

const {spawnSync} = require('child_process');
const JOB_RESULTS = ['BUILD_UBUNTU', 'TEST_UBUNTU', 'COMPILE_CHECKS', 'WASM', 'BUILD_MACOS',
  'TEST_MACOS', 'BUILD_WINDOWS', 'TEST_WINDOWS', 'WINDOWS_SUITES'];
function gate(paths, results) {
  const c = classify(paths);
  const env = {PATH: process.env.PATH, CLASSIFY: 'success', LIGHT: String(c.light),
    SUITES: String(c.suites), PLATFORMS: String(c.platforms), BENCHMARKS: String(c.benchmarks),
    COMPILE: String(c.compile), WASM_WANTED: String(c.wasm), DOC_LINKS: 'skipped'};
  const wanted = {BUILD_UBUNTU: !c.light, TEST_UBUNTU: c.suites || c.platforms || c.benchmarks,
    COMPILE_CHECKS: c.compile, WASM: c.wasm};
  for (const job of JOB_RESULTS) {
    env[job] = (wanted[job] ?? c.platforms) ? 'success' : 'skipped';
  }
  Object.assign(env, results);
  checks++;
  return spawnSync('bash', [path.join(__dirname, 'ci-gate.sh')], {env}).status;
}
assert.strictEqual(gate(['src/btree.c'], {}), 0);
assert.strictEqual(gate(['doc/doltlite/merge.md'], {DOC_LINKS: 'success'}), 0);
assert.strictEqual(gate(['test/doltlite_merge.sh'], {}), 0);
assert.strictEqual(gate(['README.md'], {BUILD_UBUNTU: 'skipped', DOC_LINKS: 'success'}), 0);
assert.strictEqual(gate(['src/btree.c'], {WINDOWS_SUITES: 'skipped'}), 1);
assert.strictEqual(gate(['test/doltlite_merge.sh'], {TEST_UBUNTU: 'failure'}), 1);
assert.strictEqual(gate(['doc/doltlite/merge.md'], {DOC_LINKS: 'failure'}), 1);
assert.strictEqual(gate(['doc/doltlite/merge.md'], {WASM: 'cancelled'}), 1);
assert.strictEqual(gate(['src/btree.c'], {CLASSIFY: 'failure'}), 1);

console.log(`Change classification and CI Gate: ${checks} checks passed`);
