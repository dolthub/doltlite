'use strict';

const {execFile} = require('node:child_process');
const {promisify} = require('node:util');

const run = promisify(execFile);
const sources = [
  'doltlite-android',
  'doltlite-driver',
  'doltlite-node',
  'doltlite-php',
  'doltlite-python',
  'doltlite-ruby',
  'doltlite-swift',
];
const label = 'embedding-issue';
const overlap = 10 * 60 * 1000;
const firstRunWindow = 48 * 60 * 60 * 1000;
const issueMarker = id => `<!-- doltlite-embedding-issue:${id} -->`;
const activityMarker = (type, id) => `<!-- doltlite-embedding-${type}:${id} -->`;

async function gh(endpoint, {all = false, fields} = {}) {
  const args = ['api', endpoint];
  if (all) args.push('--paginate', '--slurp');
  if (fields) {
    args.push('--method', 'POST');
    for (const [key, value] of Object.entries(fields)) {
      if (Array.isArray(value)) {
        for (const entry of value) args.push('--field', `${key}[]=${entry}`);
      } else {
        args.push('--raw-field', `${key}=${value}`);
      }
    }
  }
  const {stdout} = await run('gh', args, {maxBuffer: 32 * 1024 * 1024});
  const data = JSON.parse(stdout);
  return all ? data.flat() : data;
}

function list(endpoint, filters = {}) {
  return `${endpoint}?${new URLSearchParams({per_page: 100, ...filters})}`;
}

async function findStart(api, target, requestedSince, now) {
  let start = now - firstRunWindow;
  if (requestedSince) {
    start = Date.parse(requestedSince);
  } else {
    const {workflow_runs: [last]} = await api(
      `repos/${target}/actions/workflows/nightly-embedding-issues.yml/runs?event=schedule&status=success&per_page=1`,
    );
    if (last) start = Date.parse(last.created_at) - overlap;
  }
  if (!Number.isFinite(start) || start > now) {
    throw new Error(`Invalid sync start time: ${requestedSince}`);
  }
  return new Date(start).toISOString();
}

async function collectActivity(api, since) {
  const activity = [];
  const start = Date.parse(since);
  const seen = new Set();
  for (const repo of sources) {
    const path = `repos/dolthub/${repo}/issues`;
    const issues = await api(list(path, {state: 'all', since}), {all: true});
    for (const issue of issues) {
      if (issue.pull_request || seen.has(issue.id)) continue;
      seen.add(issue.id);
      const opened = Date.parse(issue.created_at) >= start;
      const details = [];
      if (issue.comments > 0) {
        const comments = await api(list(`${path}/${issue.number}/comments`, {since}), {all: true});
        for (const comment of comments) {
          if (Date.parse(comment.created_at) < start) continue;
          details.push({
            marker: activityMarker('comment', comment.id),
            link: `[New comment](${comment.html_url})`,
            createdAt: comment.created_at,
          });
        }
      }
      if (!opened) {
        const events = await api(list(`${path}/${issue.number}/events`), {all: true});
        for (const event of events) {
          if (event.event !== 'reopened' || Date.parse(event.created_at) < start) continue;
          details.push({
            marker: activityMarker('event', event.id),
            link: `[Issue reopened](${issue.html_url}#event-${event.id})`,
            createdAt: event.created_at,
          });
        }
      }
      if (opened || details.length) {
        details.sort((a, b) => a.createdAt.localeCompare(b.createdAt));
        activity.push({repo, issue, details});
      }
    }
  }
  return activity;
}

function activityBody(details) {
  return details.map(({link, marker, createdAt}) => `- ${link} · ${createdAt} ${marker}`).join('\n');
}

async function sync({api = gh, target, requestedSince, apply = false, now = Date.now(), log = console.log}) {
  const since = await findStart(api, target, requestedSince, now);
  log(`Checking embedding issue activity since ${since}`);
  const activity = await collectActivity(api, since);
  if (!activity.length) {
    log('No embedding issue activity in this window.');
    return;
  }

  const path = `repos/${target}/issues`;
  const tracked = await api(list(path, {state: 'all', labels: label}), {all: true});
  const bySource = new Map();
  for (const issue of tracked) {
    const match = (issue.body || '').match(/<!-- doltlite-embedding-issue:(\d+) -->/);
    if (match && !issue.pull_request) bySource.set(match[1], issue);
  }
  let labelReady = tracked.length > 0;

  for (const {repo, issue, details} of activity) {
    const tracker = bySource.get(String(issue.id));
    const source = `[${repo}#${issue.number}](${issue.html_url})`;
    if (!tracker) {
      log(`${apply ? 'Creating' : 'Would create'} a tracker for ${source}`);
      if (!apply) continue;
      if (!labelReady) {
        const labels = await api(list(`repos/${target}/labels`), {all: true});
        if (!labels.some(entry => entry.name === label)) {
          await api(`repos/${target}/labels`, {fields: {
            name: label, color: '0e8a16', description: 'Issue activity from a DoltLite embedding repository',
          }});
        }
        labelReady = true;
      }
      const title = `[${repo}] ${issue.title}`.slice(0, 256);
      const body = [`Source issue ${source}`, activityBody(details), issueMarker(issue.id)].filter(Boolean).join('\n\n');
      const created = await api(path, {fields: {title, body, labels: [label]}});
      bySource.set(String(issue.id), created);
      continue;
    }

    if (!details.length) continue;
    const thread = await api(list(`${path}/${tracker.number}/comments`), {all: true});
    const recorded = [tracker.body || '', ...thread.map(comment => comment.body || '')];
    const newDetails = details.filter(item => !recorded.some(body => body.includes(item.marker)));
    if (!newDetails.length) continue;
    log(`${apply ? 'Updating' : 'Would update'} #${tracker.number} with ${newDetails.length} item(s) from ${source}`);
    if (apply) {
      const body = `Activity on ${source}\n\n${activityBody(newDetails)}`;
      await api(`${path}/${tracker.number}/comments`, {fields: {body}});
    }
  }
}

if (require.main === module) {
  sync({
    target: process.env.GITHUB_REPOSITORY || 'dolthub/doltlite',
    requestedSince: process.env.EMBEDDING_ISSUES_SINCE,
    apply: process.env.EMBEDDING_ISSUES_APPLY === 'true',
  }).catch(error => {
    console.error(error.message);
    process.exitCode = 1;
  });
}

module.exports = {sync};
