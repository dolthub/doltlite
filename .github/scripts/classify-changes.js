'use strict';

const JOBS = ['suites', 'platforms', 'benchmarks', 'compile', 'wasm'];
const ALL = new Set(JOBS);

const LIGHT = new Set(['README.md', 'performance-report.md']);

// Workflows PR CI never calls; the self-test proves none is reachable from
// pr-ci.yml, so a change to one cannot alter what a PR run checks.
const OFF_PR_WORKFLOWS = new Set([
  'cancel-conflicted-ci.yml',
  'dolt-oracle-upgrade.yml',
  'go-driver-dev.yml',
  'inventory-issue-liveness.yml',
  'nightly-fuzz.yml',
  'nightly-heartbeat.yml',
  'nightly-hotspots.yml',
  'nightly-performance.yml',
  'nightly-scale.yml',
  'nightly-stress.yml',
  'release.yml',
  'seed-ci-caches.yml',
  'sqlite-upstream-drift.yml',
]);

const BENCHMARK_HARNESS = [
  /^test\/sysbench/,
  /^test\/lib\/sysbench/,
  /^test\/benchmark/,
  /^test\/performance_/,
  /^test\/vc_perf/,
  /^test\/remote_perf/,
];

function jobsFor(path) {
  if (path === '.github/workflows/benchmark.yml') return new Set(['benchmarks']);
  if (path.startsWith('.github/workflows/')) {
    return OFF_PR_WORKFLOWS.has(path.slice('.github/workflows/'.length)) ? new Set() : ALL;
  }
  if (path.startsWith('test/')) {
    const jobs = new Set();
    const bench = BENCHMARK_HARNESS.some(pattern => pattern.test(path));
    if (bench) jobs.add('benchmarks');
    if (/\.(c|h)$/.test(path)) {
      ['suites', 'platforms', 'compile'].forEach(job => jobs.add(job));
    } else if (!bench) {
      ['suites', 'platforms'].forEach(job => jobs.add(job));
    }
    return jobs;
  }
  if (path.startsWith('packaging/')) return new Set(['platforms', 'wasm']);
  if (path === '.dolt-oracle-version') return new Set(['suites']);
  if (path.startsWith('doc/') || path.endsWith('.md')) return new Set();
  if (['.gitignore', '.clangd', 'APACHE_LICENSE'].includes(path)) return new Set();
  return ALL;
}

function classify(paths, labels = []) {
  const result = {
    docs_changed: paths.some(path => path.endsWith('.md')),
    light: paths.length > 0 && paths.every(path => LIGHT.has(path)),
  };
  const jobs = new Set();
  if (paths.length === 0 || labels.includes('full-ci')) {
    JOBS.forEach(job => jobs.add(job));
  } else {
    for (const path of paths) jobsFor(path).forEach(job => jobs.add(job));
  }
  if (result.light) jobs.clear();
  for (const job of JOBS) result[job] = jobs.has(job);
  result.full = JOBS.every(job => jobs.has(job));
  return result;
}

async function run({github, context, core}) {
  const pull = context.payload.pull_request;
  let result;
  if (!pull) {
    result = classify([]);
    result.docs_changed = true;
  } else {
    const params = {owner: context.repo.owner, repo: context.repo.repo};
    const files = await github.paginate(github.rest.pulls.listFiles,
      {...params, pull_number: pull.number, per_page: 100});
    // Read labels live: a re-run reuses the original event payload, so a
    // full-ci label added afterwards would otherwise be invisible.
    const {data} = await github.rest.pulls.get({...params, pull_number: pull.number});
    const paths = [];
    for (const file of files) {
      paths.push(file.filename);
      if (file.previous_filename) paths.push(file.previous_filename);
    }
    result = classify(paths, data.labels.map(label => label.name));
  }
  for (const [key, value] of Object.entries(result)) core.setOutput(key, String(value));
  core.summary.addRaw(`Change classification: ${JSON.stringify(result)}\n`).write();
}

module.exports = {classify, jobsFor, OFF_PR_WORKFLOWS, JOBS, run};
