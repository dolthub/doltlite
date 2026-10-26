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

expect(['.github/workflows/nightly-fuzz.yml', '.github/workflows/release.yml'], []);
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
