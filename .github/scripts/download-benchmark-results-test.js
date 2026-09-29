const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {execFileSync} = require('node:child_process');
const download = require('./download-benchmark-results.js');

async function main() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'benchmark-download-test-'));
  const old = {id: 11053180437, name: 'benchmark-result-textpk', created_at: '2026-09-29T18:04:11Z'};
  const fresh = {id: 11052863756, name: old.name, created_at: '2026-09-29T18:17:27Z'};
  const unchanged = {id: 11051519694, name: 'benchmark-result-int', created_at: '2026-09-29T17:54:45Z'};
  const context = {repo: {owner: 'dolthub', repo: 'doltlite'}, runId: 36603726757};
  const archives = new Map();
  const calls = [];
  const messages = [];
  try {
    for (const [artifact, result] of [[old, 'old-failure'], [fresh, 'new-pass'], [unchanged, 'unchanged']]) {
      const directory = path.join(root, String(artifact.id));
      fs.mkdirSync(directory);
      const suite = artifact.name.slice('benchmark-result-'.length);
      fs.writeFileSync(path.join(directory, `${suite}.tsv`), result);
      fs.writeFileSync(path.join(directory, `${suite}-attempts.tsv`), result);
      const zip = path.join(root, `${artifact.id}.zip`);
      execFileSync('zip', ['-q', zip, `${suite}.tsv`, `${suite}-attempts.tsv`], {cwd: directory});
      archives.set(artifact.id, fs.readFileSync(zip));
    }
    async function run(artifacts, failDownload = false) {
      calls.length = 0;
      const destination = fs.mkdtempSync(path.join(root, 'results-'));
      const listWorkflowRunArtifacts = {};
      await download({
        context,
        destination,
        core: {info: message => messages.push(message)},
        github: {
          paginate: async (method, options) => {
            assert.equal(method, listWorkflowRunArtifacts);
            assert.deepEqual(options, {...context.repo, run_id: context.runId, per_page: 100});
            return artifacts;
          },
          rest: {actions: {
            listWorkflowRunArtifacts,
            downloadArtifact: async options => {
              assert.deepEqual(options, {...context.repo, artifact_id: options.artifact_id, archive_format: 'zip'});
              calls.push(options.artifact_id);
              if (failDownload) throw new Error('download failed');
              return {data: archives.get(options.artifact_id)};
            },
          }},
        },
      });
      return destination;
    }

    for (const artifacts of [[old, fresh, unchanged], [unchanged, fresh, old]]) {
      const destination = await run([...artifacts, {name: 'unrelated-artifact'}]);
      assert.deepEqual([...calls].sort(), [fresh.id, unchanged.id].sort());
      assert.equal(fs.readFileSync(path.join(destination, 'textpk.tsv'), 'utf8'), 'new-pass');
      assert.equal(fs.readFileSync(path.join(destination, 'textpk-attempts.tsv'), 'utf8'), 'new-pass');
      assert.equal(fs.readFileSync(path.join(destination, 'int.tsv'), 'utf8'), 'unchanged');
    }
    const newerFailure = {...old, created_at: '2026-09-29T18:30:00Z'};
    await run([old, {...old, id: 1}, fresh]);
    assert.deepEqual(calls, [fresh.id]);
    const failed = await run([fresh, newerFailure]);
    assert.deepEqual(calls, [old.id]);
    assert.equal(fs.readFileSync(path.join(failed, 'textpk.tsv'), 'utf8'), 'old-failure');
    await assert.rejects(run([]), /No benchmark result artifacts/);
    await assert.rejects(run([old, {...fresh, expired: true}]), /has expired/);
    await assert.rejects(run([{...fresh, created_at: null}]), /Invalid creation time/);
    await assert.rejects(run([fresh, {...fresh, id: 1}]), /Ambiguous artifacts/);
    await assert.rejects(run([old, fresh], true), /download failed/);
    assert.deepEqual(calls, [fresh.id]);
    archives.set(fresh.id, Buffer.from('not a zip archive'));
    await assert.rejects(run([old, fresh]), /unzip/);
    assert.deepEqual(calls, [fresh.id]);
    assert(messages.some(message => message.includes(String(fresh.id)) && message.includes(fresh.created_at)));
    console.log('Benchmark artifact rerun selection, failure handling, and archive download checks passed');
  } finally {
    fs.rmSync(root, {recursive: true, force: true});
  }
}

main().catch(error => {console.error(error); process.exitCode = 1;});
