#!/usr/bin/env python3

import argparse
import json
import os
import pathlib
import shutil
import subprocess
import sys


REPORT_PATH = "performance-report.md"
LABEL = "automated-performance-report"
BRANCH_PREFIX = "automation/performance-report-"


class PublishError(RuntimeError):
    pass


def command(arguments, check=True):
    result = subprocess.run(
        arguments,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=None,
    )
    if check and result.returncode:
        raise PublishError(
            f"command failed ({result.returncode}): {' '.join(arguments)}"
        )
    return result


def gh_json(arguments):
    result = command(["gh", *arguments])
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise PublishError(f"invalid gh JSON for: {' '.join(arguments)}") from exc


def validate_previous_pr(pr, current_login):
    author = (pr.get("author") or {}).get("login")
    if author != current_login:
        raise PublishError(
            f"refusing to merge PR #{pr.get('number')}: "
            f"author is {author!r}, expected {current_login!r}"
        )
    branch = pr.get("headRefName", "")
    if not branch.startswith(BRANCH_PREFIX):
        raise PublishError(
            f"refusing to merge PR #{pr.get('number')}: "
            f"unexpected branch {branch!r}"
        )
    files = [entry.get("path") for entry in pr.get("files", [])]
    if files != [REPORT_PATH]:
        raise PublishError(
            f"refusing to merge PR #{pr.get('number')}: "
            f"expected only {REPORT_PATH}, found {files}"
        )
    if not pr.get("headRefOid"):
        raise PublishError(
            f"refusing to merge PR #{pr.get('number')}: missing head commit"
        )


def merge_previous_report(repository, current_login):
    existing = gh_json(
        [
            "pr",
            "list",
            "--repo",
            repository,
            "--state",
            "open",
            "--label",
            LABEL,
            "--json",
            "number",
        ]
    )
    if len(existing) > 1:
        numbers = ", ".join(f"#{pr['number']}" for pr in existing)
        raise PublishError(
            f"multiple open performance report PRs found: {numbers}"
        )
    if not existing:
        return None

    number = existing[0]["number"]
    pr = gh_json(
        [
            "pr",
            "view",
            str(number),
            "--repo",
            repository,
            "--json",
            "number,author,headRefName,headRefOid,files",
        ]
    )
    validate_previous_pr(pr, current_login)
    command(
        [
            "gh",
            "pr",
            "merge",
            str(number),
            "--repo",
            repository,
            "--squash",
            "--delete-branch",
            "--match-head-commit",
            pr["headRefOid"],
        ]
    )
    return number


def publish_report(args):
    report = args.report.resolve()
    if not report.is_file() or report.stat().st_size == 0:
        raise PublishError(f"report is missing or empty: {report}")
    if not os.environ.get("GH_TOKEN"):
        raise PublishError("GH_TOKEN is required")

    login = command(["gh", "api", "user", "--jq", ".login"]).stdout.strip()
    if not login:
        raise PublishError("unable to determine report bot login")
    command(["gh", "auth", "setup-git"])

    merged = merge_previous_report(args.repository, login)
    if merged is not None:
        print(f"Merged previous performance report PR #{merged}")

    command(["git", "fetch", "origin", "master"])
    branch = f"{BRANCH_PREFIX}{args.run_id}-{args.run_attempt}"
    command(["git", "switch", "-C", branch, "origin/master"])
    shutil.copyfile(report, REPORT_PATH)
    command(["git", "diff", "--check"])
    command(["git", "add", REPORT_PATH])
    if command(["git", "diff", "--cached", "--quiet"], check=False).returncode == 0:
        raise PublishError("generated performance report is unchanged")

    command(["git", "config", "user.name", "doltlite performance bot"])
    command(
        [
            "git",
            "config",
            "user.email",
            "performance-bot@users.noreply.github.com",
        ]
    )
    command(
        [
            "git",
            "commit",
            "-m",
            f"Update nightly performance report ({args.generated_date})",
        ]
    )
    command(["git", "push", "origin", f"HEAD:refs/heads/{branch}"])

    command(
        [
            "gh",
            "label",
            "create",
            LABEL,
            "--repo",
            args.repository,
            "--color",
            "1d76db",
            "--description",
            "Automated nightly performance report",
        ],
        check=False,
    )
    body = (
        f"Automated performance results from [{args.generated_date}]"
        f"({args.run_url}).\n\n"
        "This PR changes only `performance-report.md`. If it remains open, "
        "the next successful nightly run will merge it before publishing "
        "the next report."
    )
    create = [
        "gh",
        "pr",
        "create",
        "--repo",
        args.repository,
        "--base",
        "master",
        "--head",
        branch,
        "--title",
        f"Nightly performance report — {args.generated_date}",
        "--body",
        body,
        "--label",
        LABEL,
    ]
    url = command(create).stdout.strip()
    if not url:
        raise PublishError("gh pr create did not return a PR URL")
    print(f"Created {url}")
    if args.reviewer:
        review = command(
            [
                "gh",
                "pr",
                "edit",
                url,
                "--repo",
                args.repository,
                "--add-reviewer",
                args.reviewer,
            ],
            check=False,
        )
        if review.returncode:
            print(
                f"warning: unable to request review from {args.reviewer}",
                file=sys.stderr,
            )


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=pathlib.Path, required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-attempt", default="1")
    parser.add_argument("--run-url", required=True)
    parser.add_argument("--generated-date", required=True)
    parser.add_argument("--reviewer", default="")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        publish_report(args)
    except PublishError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
