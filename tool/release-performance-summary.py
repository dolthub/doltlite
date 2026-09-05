#!/usr/bin/env python3

import argparse
import pathlib
import sys


HEADERS = (
    "Operation",
    "SQLite median total",
    "DoltLite median total",
    "Ratio",
    "Paired-ratio noise",
    "Result",
)
SECTIONS = ("### In-memory", "### File-backed")


def cells(line):
    line = line.strip()
    if not line.startswith("|") or not line.endswith("|"):
        raise ValueError("invalid markdown table row")
    return [value.strip() for value in line[1:-1].split("|")]


def render_table(lines, section):
    try:
        section_index = lines.index(section)
    except ValueError as exc:
        raise ValueError(f"missing {section} benchmark section") from exc

    table_index = section_index + 1
    while table_index < len(lines) and not lines[table_index].strip():
        table_index += 1
    if table_index + 2 >= len(lines):
        raise ValueError(f"missing table under {section}")
    if tuple(cells(lines[table_index])) != HEADERS:
        raise ValueError(f"unexpected columns under {section}")

    output = [section, ""]
    row_index = table_index
    while row_index < len(lines) and lines[row_index].lstrip().startswith("|"):
        row = cells(lines[row_index])
        if len(row) != len(HEADERS):
            raise ValueError(f"unexpected row width under {section}")
        output.append("| " + " | ".join(row[:4]) + " |")
        row_index += 1
    if len(output) < 5:
        raise ValueError(f"missing results under {section}")
    return output


def render_summary(report):
    lines = report.splitlines()
    if "## SQL workload summary" not in lines:
        raise ValueError("missing SQL workload summary")
    output = []
    for section in SECTIONS:
        output.extend(render_table(lines, section))
        output.append("")
    return "\n".join(output)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=pathlib.Path)
    args = parser.parse_args(argv)
    try:
        report = args.report.read_text(encoding="utf-8")
        sys.stdout.write(render_summary(report))
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
