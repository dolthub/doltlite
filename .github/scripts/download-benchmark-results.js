const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {execFileSync} = require('node:child_process');

module.exports = async ({github, context, core, destination = 'benchmark-results'}) => {
  const repository = context.repo;
  const artifacts = await github.paginate(github.rest.actions.listWorkflowRunArtifacts, {
    ...repository,
    run_id: context.runId,
    per_page: 100,
  });
  const latest = new Map();
  for (const artifact of artifacts) {
    if (!artifact.name.startsWith('benchmark-result-')) continue;
    const created = Date.parse(artifact.created_at);
    if (!Number.isFinite(created)) {
      throw new Error(`Invalid creation time for artifact ${artifact.id}`);
    }
    const previous = latest.get(artifact.name);
    if (!previous || created > Date.parse(previous.created_at)) {
      latest.set(artifact.name, artifact);
    }
  }
  if (!latest.size) throw new Error('No benchmark result artifacts found');

  fs.mkdirSync(destination, {recursive: true});
  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), 'benchmark-artifacts-'));
  try {
    for (const artifact of latest.values()) {
      const matches = artifacts.filter(candidate => candidate.name === artifact.name
        && Date.parse(candidate.created_at) === Date.parse(artifact.created_at));
      if (matches.length !== 1) throw new Error(`Ambiguous artifacts for ${artifact.name}`);
      if (artifact.expired) throw new Error(`Artifact ${artifact.id} has expired`);
      core.info(`Downloading ${artifact.name}: ID ${artifact.id}, created ${artifact.created_at}`);
      const archive = await github.rest.actions.downloadArtifact({
        ...repository,
        artifact_id: artifact.id,
        archive_format: 'zip',
      });
      const archivePath = path.join(temporary, `${artifact.id}.zip`);
      fs.writeFileSync(archivePath, Buffer.from(archive.data));
      execFileSync('unzip', ['-o', archivePath, '-d', destination]);
    }
  } finally {
    fs.rmSync(temporary, {recursive: true, force: true});
  }
};
