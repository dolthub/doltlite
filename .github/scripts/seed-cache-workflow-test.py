# Two invariants about workflows that nothing watches.
#
# A seeded cache is only seeded when it is cold, so a seed job that is given
# less time than the work it warms passes every run that finds a warm cache and
# times out on exactly the runs that matter. Nothing downstream fails: PR CI
# just quietly rebuilds from scratch forever. So each seed job must carry at
# least the budget CI already gives the same work.
#
# And a scheduled workflow that fails reports nothing by itself. Every
# scheduled workflow therefore has to cut an issue somehow.

import re
from pathlib import Path

repo = Path(__file__).resolve().parents[2]
workflows = repo / '.github/workflows'
checks = 0


def jobs(name):
    """Map job id -> (body, timeout-minutes or None) for one workflow."""
    text = (workflows / name).read_text()
    start = re.search(r'^jobs:[ \t]*$', text, re.M)
    assert start, f'{name}: no jobs block'
    parts = re.split(r'^  ([A-Za-z0-9_-]+):[ \t]*$', text[start.end():], flags=re.M)
    found = {}
    for job, body in zip(parts[1::2], parts[2::2]):
        limit = re.search(r'^    timeout-minutes:[ \t]*(\d+)[ \t]*$', body, re.M)
        found[job] = (body, int(limit[1]) if limit else None)
    assert found, f'{name}: parsed no jobs'
    return found


# Work a seed job warms -> the CI job that already budgets for it. A seed job
# that warms several caches serially needs the sum, counted per occurrence.
REFERENCE = {
    'uses: ./.github/actions/compatibility-cache': ('ci-build.yml', 'compat-build'),
    'uses: ./.github/actions/sqllogictest-cache': ('ci-build.yml', 'sqllogictest-build'),
    'build-optimized-benchmark.sh': ('ci-build.yml', 'benchmark-build'),
    'macos-package-tests.sh': ('platform-test.yml', 'build-and-test'),
    'uses: ./.github/actions/macos-build-cache': ('macos-build.yml', 'checked'),
}


def seed_budgets():
    global checks
    seed = jobs('seed-ci-caches.yml')
    budgeted = 0
    for job, (body, limit) in seed.items():
        needed = 0
        for marker, (workflow, reference) in REFERENCE.items():
            count = body.count(marker)
            if not count:
                continue
            budget = jobs(workflow)[reference][1]
            assert budget, f'{workflow}:{reference} has no timeout to compare against'
            needed += budget * count
        if not needed:
            continue
        assert limit, f'seed job {job} warms a cache but sets no timeout-minutes'
        assert limit >= needed, (
            f'seed job {job} has {limit} minutes for work CI budgets {needed} '
            f'minutes for; it will time out whenever the cache is actually cold')
        budgeted += 1
        checks += 1
    assert budgeted >= 2, f'expected several seed jobs to warm caches, found {budgeted}'
    # The mapping is only worth anything if every marker still appears.
    for marker, (workflow, reference) in REFERENCE.items():
        assert any(marker in body for body, _ in seed.values()), \
            f'no seed job warms {marker}; drop it from REFERENCE or restore the seeding'
        assert reference in jobs(workflow), f'{workflow} no longer defines {reference}'
        checks += 1


# Cuts issues with `gh issue create` rather than the shared action.
REPORTS_DIRECTLY = {'sqlite-upstream-drift.yml'}
# Ratchet: scheduled workflows that still report nothing when they fail. Empty
# this set, never add to it.
KNOWN_SILENT = set()


def scheduled_workflows_report():
    global checks
    scheduled = 0
    for path in sorted(workflows.glob('*.yml')):
        text = path.read_text()
        if not re.search(r'^  schedule:[ \t]*$', text, re.M):
            continue
        scheduled += 1
        if path.name in KNOWN_SILENT:
            assert 'report-failure-issue' not in text, \
                f'{path.name} reports failures now; remove it from KNOWN_SILENT'
            continue
        reporter = ('report-failure-issue' in text or
                    (path.name in REPORTS_DIRECTLY and 'gh issue create' in text))
        assert reporter, (
            f'{path.name} runs on a schedule but reports nothing when it fails')
        checks += 1
    assert scheduled >= 6, f'expected the scheduled workflows, found {scheduled}'


# A watchdog only fires while its own run survives; a whole-run cancellation
# kills it along with everything else. So a watchdog is only half the story:
# the weekly heartbeat has to audit that workflow from outside as well.
def heartbeat_covers_every_watchdog():
    global checks
    text = (workflows / 'nightly-heartbeat.yml').read_text()
    listed = re.search(r'^        for wf in ([^;]+); do', text, re.M)
    assert listed, 'nightly-heartbeat no longer lists the workflows it audits'
    audited = set(listed[1].split())
    covered = 0
    for path in sorted(workflows.glob('*.yml')):
        body = path.read_text()
        if not re.search(r'^  schedule:[ \t]*$', body, re.M):
            continue
        if not re.search(r'^  watchdog:[ \t]*$', body, re.M):
            continue
        assert path.stem in audited, (
            f'{path.name} has a watchdog but nightly-heartbeat does not audit it; '
            f'a whole-run cancellation would kill the watchdog and report nothing')
        covered += 1
        checks += 1
    assert covered >= 5, f'expected the watchdog workflows, found {covered}'


def watchdog_arms_on_every_bad_end():
    global checks
    seed = jobs('seed-ci-caches.yml')
    assert 'watchdog' in seed, 'seed-ci-caches has no watchdog job'
    body, _ = seed['watchdog']
    assert re.search(r'^    if:.*failure\(\).*cancelled\(\)', body, re.M), \
        'the seed watchdog must arm on failure() and cancelled()'
    assert 'issues: write' in body, 'the seed watchdog cannot open an issue'
    assert 'uses: ./.github/actions/report-failure-issue' in body
    needed = re.search(r'^    needs:[ \t]*\[([^\]]*)\]', body, re.M)
    assert needed, 'the seed watchdog does not declare what it watches'
    watched = {name.strip() for name in needed[1].split(',')}
    working = {job for job, (text, _) in seed.items()
               if job != 'watchdog' and any(m in text for m in REFERENCE)}
    assert working <= watched, \
        f'seed jobs not watched by the watchdog: {sorted(working - watched)}'
    checks += 1


seed_budgets()
scheduled_workflows_report()
heartbeat_covers_every_watchdog()
watchdog_arms_on_every_bad_end()
print(f'Seed cache budgets and scheduled-workflow reporting: {checks} checks passed')
