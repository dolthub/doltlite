const assert = require('node:assert/strict');
const test = require('node:test');
const cancel = require('./cancel-conflicted-pr-runs.js');

const repo = {owner: 'dolthub', repo: 'doltlite'};
const statuses = ['queued', 'in_progress', 'pending', 'waiting', 'requested'];
const pull = (overrides = {}) => ({
  state: 'open', merged: false, mergeable: false,
  base: {ref: 'master', sha: 'master-tip'}, head: {sha: 'pr-tip'},
  ...overrides,
});
const run = (overrides = {}) => ({
  id: 100, event: 'pull_request', status: 'queued', head_sha: 'pr-tip',
  pull_requests: [{number: 42, base: {ref: 'master'}}],
  ...overrides,
});
const apiError = status => Object.assign(new Error(`HTTP ${status}`), {status});

function fixture({runs = [run()], pulls = [pull()], cancelError} = {}) {
  const listed = [], fetched = [], cancelled = [], sleeps = [], logs = [];
  const github = {rest: {
    actions: {
      listWorkflowRuns() {},
      async cancelWorkflowRun(args) {
        assert.deepEqual(args, {...repo, run_id: args.run_id});
        cancelled.push(args.run_id);
        if (cancelError) throw cancelError;
      },
    },
    pulls: {async get(args) {
      assert.deepEqual(args, {...repo, pull_number: args.pull_number});
      const index = fetched.length;
      fetched.push(args.pull_number);
      const value = typeof pulls === 'function' ? pulls(args.pull_number, index) :
        pulls[Math.min(index, pulls.length - 1)];
      if (value instanceof Error) throw value;
      return {data: value};
    }},
  }};
  github.paginate = async (method, args) => {
    assert.equal(method, github.rest.actions.listWorkflowRuns);
    assert.deepEqual(args, {...repo, workflow_id: 'pr-ci.yml',
      event: 'pull_request', status: args.status, per_page: 100});
    listed.push(args.status);
    return typeof runs === 'function' ? runs(args.status) :
      runs.filter(item => item.status === args.status);
  };
  return {
    listed, fetched, cancelled, sleeps, logs,
    execute: () => cancel({github, context: {repo, sha: 'master-tip'},
      core: {info: message => logs.push(message)},
      sleep: async ms => sleeps.push(ms)}),
  };
}

for (const status of statuses) {
  test(`cancels ${status} CI for a confirmed conflict`, async () => {
    const f = fixture({runs: [run({status})]});
    assert.equal(await f.execute(), 1);
    assert.deepEqual(f.cancelled, [100]);
    assert.deepEqual(f.fetched, [42, 42]);
    assert.deepEqual(f.listed, statuses);
  });
}

test('retries unknown mergeability before cancelling', async () => {
  const f = fixture({pulls: [pull({mergeable: null}), pull(), pull()]});
  assert.equal(await f.execute(), 1);
  assert.deepEqual(f.sleeps, [5000]);
  assert.deepEqual(f.cancelled, [100]);
});

test('leaves CI running when mergeability stays unknown', async () => {
  const f = fixture({pulls: [pull({mergeable: null})]});
  assert.equal(await f.execute(), 0);
  assert.equal(f.fetched.length, 6);
  assert.deepEqual(f.sleeps, Array(5).fill(5000));
  assert.deepEqual(f.cancelled, []);
  assert.ok(f.logs.some(message => message.includes('mergeability remains unknown')));
});

const changes = {
  'mergeable PR': {mergeable: true},
  'closed PR': {state: 'closed'},
  'merged PR': {merged: true},
  'different base branch': {base: {ref: 'release', sha: 'master-tip'}},
  'new master tip': {base: {ref: 'master', sha: 'new-master-tip'}},
};
for (const [name, change] of Object.entries(changes)) {
  test(`leaves ${name} alone`, async () => {
    const f = fixture({pulls: [pull(change)]});
    assert.equal(await f.execute(), 0);
    assert.deepEqual(f.cancelled, []);
  });
}

for (const [name, change] of Object.entries({...changes,
  'updated PR head': {head: {sha: 'new-pr-tip'}},
  'unknown mergeability': {mergeable: null},
})) {
  test(`rechecks ${name} immediately before cancellation`, async () => {
    const f = fixture({pulls: [pull(), pull(change)]});
    assert.equal(await f.execute(), 0);
    assert.deepEqual(f.fetched, [42, 42]);
    assert.deepEqual(f.cancelled, []);
  });
}

test('does not cancel a run for an older PR head', async () => {
  const f = fixture({runs: [run({head_sha: 'old-pr-tip'})]});
  assert.equal(await f.execute(), 0);
  assert.deepEqual(f.cancelled, []);
});

test('ignores unrelated events, completed runs, and other PR bases', async () => {
  const f = fixture({runs: () => [
    run({event: 'workflow_dispatch'}), run({status: 'completed'}),
    run({pull_requests: []}), run({pull_requests: undefined}),
    run({pull_requests: [{number: 42, base: {ref: 'release'}}]}),
  ]});
  assert.equal(await f.execute(), 0);
  assert.deepEqual(f.fetched, []);
  assert.deepEqual(f.cancelled, []);
});

test('deduplicates runs that change status during enumeration', async () => {
  const f = fixture({runs: status => [run({status})]});
  assert.equal(await f.execute(), 1);
  assert.deepEqual(f.cancelled, [100]);
});

test('cancels all matching runs across pages and leaves a healthy PR running', async () => {
  const runs = Array.from({length: 105}, (_, id) => run({id}));
  runs.push(run({id: 200, pull_requests: [{number: 43, base: {ref: 'master'}}]}));
  const f = fixture({runs, pulls: number => pull({mergeable: number === 43})});
  assert.equal(await f.execute(), 105);
  assert.deepEqual(f.cancelled, Array.from({length: 105}, (_, id) => id));
});

test('rechecks the PR between cancellations', async () => {
  const f = fixture({runs: [run(), run({id: 101})],
    pulls: [pull(), pull(), pull({mergeable: true})]});
  assert.equal(await f.execute(), 1);
  assert.deepEqual(f.cancelled, [100]);
});

test('tolerates a run finishing before cancellation', async () => {
  const f = fixture({cancelError: apiError(409)});
  assert.equal(await f.execute(), 0);
  assert.ok(f.logs.some(message => message.includes('no longer cancellable')));
});

for (const pulls of [[apiError(404)], [pull(), apiError(404)]]) {
  test('tolerates a PR disappearing during inspection', async () => {
    const f = fixture({pulls});
    assert.equal(await f.execute(), 0);
    assert.deepEqual(f.cancelled, []);
  });
}

test('surfaces cancellation API errors', async () => {
  const error = apiError(403);
  await assert.rejects(fixture({cancelError: error}).execute(), error);
});

test('surfaces pull request API errors', async () => {
  const error = apiError(500);
  await assert.rejects(fixture({pulls: [error]}).execute(), error);
});
