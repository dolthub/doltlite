#!/usr/bin/env python3

import importlib.util
import pathlib
import unittest
from unittest import mock


MODULE_PATH = (
    pathlib.Path(__file__).parents[1]
    / "tool"
    / "publish-performance-report.py"
)
SPEC = importlib.util.spec_from_file_location(
    "publish_performance_report", MODULE_PATH
)
publisher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publisher)


def pull_request(**overrides):
    value = {
        "number": 42,
        "author": {"login": "report-bot"},
        "headRefName": "automation/performance-report-123-1",
        "headRefOid": "abcdef123456",
        "files": [{"path": "performance-report.md"}],
    }
    value.update(overrides)
    return value


class PublishPerformanceReportTest(unittest.TestCase):
    @mock.patch.object(publisher, "command")
    def test_uses_actions_bot_identity_in_github_actions(self, command):
        with mock.patch.dict(
            publisher.os.environ,
            {"GITHUB_ACTIONS": "true"},
        ):
            self.assertEqual(
                publisher.report_login(),
                "github-actions[bot]",
            )
        command.assert_not_called()

    @mock.patch.object(publisher, "command")
    def test_discovers_login_outside_github_actions(self, command):
        command.return_value.stdout = "report-bot\n"
        with mock.patch.dict(
            publisher.os.environ,
            {"GITHUB_ACTIONS": ""},
        ):
            self.assertEqual(publisher.report_login(), "report-bot")

    def test_accepts_expected_bot_report(self):
        publisher.validate_previous_pr(pull_request(), "report-bot")

    def test_rejects_unexpected_author(self):
        with self.assertRaisesRegex(publisher.PublishError, "author"):
            publisher.validate_previous_pr(pull_request(), "other-bot")

    def test_rejects_unexpected_branch(self):
        with self.assertRaisesRegex(publisher.PublishError, "branch"):
            publisher.validate_previous_pr(
                pull_request(headRefName="feature/not-a-report"),
                "report-bot",
            )

    def test_rejects_additional_files(self):
        with self.assertRaisesRegex(publisher.PublishError, "expected only"):
            publisher.validate_previous_pr(
                pull_request(
                    files=[
                        {"path": "performance-report.md"},
                        {"path": "src/doltlite.c"},
                    ]
                ),
                "report-bot",
            )

    @mock.patch.object(publisher, "command")
    @mock.patch.object(publisher, "gh_json")
    def test_merges_valid_existing_report(self, gh_json, command):
        gh_json.side_effect = [
            [
                {
                    "number": 42,
                    "headRefName": (
                        "automation/performance-report-123-1"
                    ),
                }
            ],
            pull_request(),
        ]
        merged = publisher.merge_previous_report(
            "dolthub/doltlite", "report-bot"
        )
        self.assertEqual(merged, 42)
        command.assert_called_once_with(
            [
                "gh",
                "pr",
                "merge",
                "42",
                "--repo",
                "dolthub/doltlite",
                "--squash",
                "--delete-branch",
                "--match-head-commit",
                "abcdef123456",
            ]
        )

    @mock.patch.object(publisher, "gh_json")
    def test_rejects_multiple_existing_reports(self, gh_json):
        gh_json.return_value = [
            {
                "number": 42,
                "headRefName": "automation/performance-report-123-1",
            },
            {
                "number": 43,
                "headRefName": "automation/performance-report-456-1",
            },
        ]
        with self.assertRaisesRegex(publisher.PublishError, "multiple open"):
            publisher.merge_previous_report(
                "dolthub/doltlite", "report-bot"
            )

    @mock.patch.object(publisher, "gh_json")
    def test_ignores_unrelated_open_pull_requests(self, gh_json):
        gh_json.return_value = [
            {"number": 41, "headRefName": "feature/unrelated"}
        ]
        self.assertIsNone(
            publisher.merge_previous_report(
                "dolthub/doltlite",
                "report-bot",
            )
        )


if __name__ == "__main__":
    unittest.main()
