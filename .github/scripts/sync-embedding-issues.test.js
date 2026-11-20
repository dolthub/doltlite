'use strict';

const assert = require('node:assert/strict');
const {test} = require('node:test');
const {sync} = require('./sync-embedding-issues.js');

const target = 'dolthub/doltlite';
const since = '2026-11-19T10:00:00Z';
const earlier = '2026-11-12T12:00:00Z';
const today = '2026-11-20T08:00:00Z';
const now = Date.parse('2026-11-20T10:00:00Z');
const mainIssues = `/repos/${target}/issues`;
const outsider = {user: {type: 'User', id: 11}};
const member = {user: {type: 'User', id: 12}, author_association: 'CONTRIBUTOR'};
const owner = {user: {type: 'User', id: 13}, author_association: 'OWNER'};
const bot = {user: {type: 'Bot', id: 14}};
const members = new Set([12, 13]);

function issue(id, number, created, extra = {}) {
  return {
    id, number, title: `Issue ${id}`, created_at: created, comments: 0,
    html_url: `https://github.com/dolthub/doltlite-node/issues/${number}`,
    ...outsider, ...extra,
  };
}

function comment(id, time, extra = {}) {
  return {
    id, created_at: time, updated_at: today,
    ...outsider, ...extra,
    html_url: `https://github.com/dolthub/doltlite-node/issues/1#issuecomment-${id}`,
  };
}

test('mirrors outside contributor activity once', async () => {
  const readmeEdit = issue(505, 5, earlier);
  const pull = issue(503, 3, today, {pull_request: {}, comments: 2});
  const upstream = [
    issue(501, 1, earlier, {comments: 2}), issue(502, 2, today, {comments: 1}),
    pull, issue(504, 4, today, {...member, comments: 1}), readmeEdit,
    issue(506, 6, today, {...owner, comments: 1}), issue(507, 7, today, bot),
    issue(508, 8, earlier), issue(509, 9, earlier),
  ];
  const mirrored = {
    number: 70, state: 'closed', body: '<!-- doltlite-embedding-issue:501 -->',
  };
  const main = [mirrored];
  const threads = new Map();
  const messages = [
    comment(700, earlier), comment(701, today), comment(704, today, member),
    comment(705, today, bot),
  ];
  const writes = [];
  const labels = [];

  async function api(endpoint, {fields, all} = {}) {
    const url = new URL(endpoint, 'https://api.github.com/');
    const path = url.pathname;
    if (fields) {
      writes.push({path, fields});
      if (path === mainIssues) {
        const record = {number: 70 + main.length, body: fields.body};
        main.push(record);
        return record;
      }
      const number = Number(path.split('/').at(-2));
      if (!threads.has(number)) threads.set(number, []);
      threads.get(number).push({body: fields.body});
      return {};
    }
    assert.equal(all, true);
    if (path === mainIssues) {
      assert.equal(url.searchParams.get('state'), 'all');
      assert.equal(url.searchParams.get('labels'), 'embedding-issue');
      return main;
    }
    if (path === `/repos/${target}/labels`) return labels;
    if (path.startsWith(`${mainIssues}/`)) {
      return threads.get(Number(path.split('/').at(-2))) || [];
    }
    if (path === '/repos/dolthub/doltlite-node/issues') return upstream;
    if (path === '/repos/dolthub/doltlite-node/issues/1/comments') {
      assert.equal(url.searchParams.get('since'), '2026-11-19T10:00:00.000Z');
      return messages;
    }
    if (path === '/repos/dolthub/doltlite-node/issues/2/comments') return [comment(702, today)];
    if (path === '/repos/dolthub/doltlite-node/issues/4/comments') {
      return [comment(706, today, {author_association: 'COLLABORATOR'})];
    }
    if (path === '/repos/dolthub/doltlite-node/issues/6/comments') return [comment(707, today, member)];
    if (path === '/repos/dolthub/doltlite-node/issues/8/events') {
      return [{event: 'reopened', id: 805, created_at: today, actor: outsider.user}];
    }
    if (path === '/repos/dolthub/doltlite-node/issues/9/events') {
      return [{event: 'reopened', id: 806, created_at: today, actor: member.user}];
    }
    assert.match(path, /\/issues$|\/issues\/(1|4|5|6|7)\/events$/);
    return [];
  }

  const log = () => {};
  const options = {api, target, members, now, requestedSince: since, log};
  await sync(options);
  assert.deepEqual(writes, []);

  await sync({...options, apply: true});
  assert.deepEqual(writes.map(write => write.path), [
    `${mainIssues}/70/comments`, mainIssues, mainIssues, mainIssues,
  ]);
  assert.match(writes[0].fields.body, /issuecomment-701/);
  assert.doesNotMatch(writes[0].fields.body, /issuecomment-(700|704|705)/);
  assert.match(writes[1].fields.body, /issuecomment-702/);
  assert.match(writes[2].fields.body, /issuecomment-706/);
  assert.match(writes[3].fields.body, /Issue reopened/);
  assert.equal(mirrored.state, 'closed');

  await sync({...options, apply: true});
  assert.equal(writes.length, 4);
  messages.push(comment(703, today));
  await sync({...options, apply: true});
  assert.equal(writes.length, 5);
  assert.match(writes[4].fields.body, /issuecomment-703/);
  assert.doesNotMatch(writes[4].fields.body, /issuecomment-701/);
});

test('resumes from the last successful scheduled run with overlap', async () => {
  const windows = [];
  async function api(endpoint) {
    const url = new URL(endpoint, 'https://api.github.com/');
    if (url.pathname.includes('/actions/workflows/')) {
      assert.equal(url.searchParams.get('event'), 'schedule');
      assert.equal(url.searchParams.get('status'), 'success');
      return {workflow_runs: [{created_at: since}]};
    }
    windows.push(url.searchParams.get('since'));
    return [];
  }
  await sync({api, target, members, now, log: () => {}});
  assert.deepEqual(windows, Array(7).fill('2026-11-19T09:50:00.000Z'));
  await assert.rejects(sync({api, target, members, now, requestedSince: 'yesterday'}), /Invalid sync start/);
});

test('creates a label before the first tracker', async () => {
  const writes = [];
  async function api(endpoint, {fields} = {}) {
    const path = new URL(endpoint, 'https://api.github.com/').pathname;
    if (fields) {
      writes.push({path, fields});
      return {number: 80, body: fields.body};
    }
    if (path === '/repos/dolthub/doltlite-node/issues') return [issue(501, 1, today)];
    return [];
  }
  await sync({api, target, members, now, requestedSince: since, apply: true, log: () => {}});
  assert.deepEqual(writes.map(write => write.path), [`/repos/${target}/labels`, mainIssues]);
  assert.equal(writes[0].fields.name, 'embedding-issue');
  assert.deepEqual(writes[1].fields.labels, ['embedding-issue']);
  assert.match(writes[1].fields.body, /<!-- doltlite-embedding-issue:501 -->/);
});
