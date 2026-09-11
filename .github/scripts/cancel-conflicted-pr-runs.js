module.exports = async function cancelConflictedPrRuns({
  github, context, core,
  sleep = ms => new Promise(resolve => setTimeout(resolve, ms)),
}) {
  const repo = context.repo;
  const runsByPull = new Map();
  for (const status of ['queued', 'in_progress', 'pending', 'waiting', 'requested']) {
    const runs = await github.paginate(github.rest.actions.listWorkflowRuns, {
      ...repo, workflow_id: 'pr-ci.yml', event: 'pull_request', status, per_page: 100,
    });
    for (const run of runs) {
      if (run.event !== 'pull_request' || run.status === 'completed') continue;
      for (const pull of run.pull_requests || []) {
        if (pull.base.ref !== 'master') continue;
        if (!runsByPull.has(pull.number)) runsByPull.set(pull.number, new Map());
        runsByPull.get(pull.number).set(run.id, run);
      }
    }
  }

  async function getPull(number) {
    try {
      return (await github.rest.pulls.get({...repo, pull_number: number})).data;
    } catch (error) {
      if (error.status !== 404) throw error;
      return null;
    }
  }

  function current(pull) {
    return pull && pull.state === 'open' && !pull.merged &&
      pull.base.ref === 'master' && pull.base.sha === context.sha;
  }

  let pending = [...runsByPull.keys()];
  let cancelled = 0;
  const cancelledRuns = new Set();
  for (let attempt = 0; attempt < 6 && pending.length; attempt++) {
    const retry = [];
    for (const number of pending) {
      const pull = await getPull(number);
      if (!current(pull)) continue;
      if (pull.mergeable == null) {
        retry.push(number);
        continue;
      }
      if (pull.mergeable !== false) continue;
      for (const run of runsByPull.get(number).values()) {
        if (run.head_sha !== pull.head.sha || cancelledRuns.has(run.id)) continue;
        const confirmed = await getPull(number);
        if (!current(confirmed) || confirmed.head.sha !== pull.head.sha ||
            confirmed.mergeable !== false) break;
        try {
          await github.rest.actions.cancelWorkflowRun({...repo, run_id: run.id});
          cancelledRuns.add(run.id);
          cancelled++;
          core.info(`Cancelled CI run ${run.id}: PR #${number} conflicts with master ${context.sha}`);
        } catch (error) {
          if (error.status !== 409) throw error;
          core.info(`CI run ${run.id} is no longer cancellable`);
        }
      }
    }
    pending = retry;
    if (pending.length && attempt < 5) await sleep(5000);
  }
  for (const number of pending) {
    core.info(`PR #${number} mergeability remains unknown; leaving CI running`);
  }
  core.info(`Cancelled ${cancelled} conflicted PR CI run(s)`);
  return cancelled;
};
