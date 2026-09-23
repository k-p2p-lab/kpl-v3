#!/usr/bin/env python3
"""Verify the inventory documented at https://github.com/k-p2p-lab/v3/wiki/V2-Analysis-Coverage.

This detects unreviewed v2 files/outputs; it does NOT prove v3 feature parity.
It uses only the Python standard library and is independent of the running app.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import sys


def main():
    project = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v2", type=Path, default=project.parent / "v2")
    args = parser.parse_args()
    manifest = json.loads((project / "scripts/data/v2-analysis-coverage.json").read_text())
    root = args.v2
    log_path = root / "kpl-parser/internal/analysis/log.go"
    metric_path = root / "kpl-parser/internal/analysis/metric.go"
    if not log_path.is_file() or not metric_path.is_file():
        parser.error("v2 sources not found; pass --v2 /path/to/v2")
    fields = set(re.findall(r'result\["([a-z_]+)"\]\s*=', log_path.read_text()))
    fields.update(re.findall(r'results\[key\]\["([a-z_]+)"\]\s*=', metric_path.read_text()))
    if '"degree_distribution-%d"' in log_path.read_text():
        fields.add("degree_distribution-*")
    recorded = {item["name"] for item in manifest["parserFields"]}
    problems = []
    if fields != recorded:
        problems.append(f"Parser fields changed: new={sorted(fields-recorded)}, removed={sorted(recorded-fields)}")
    paths = set(root.glob("kpl-parser/internal/analysis/*.go"))
    paths.update(root.glob("kpl-parser/internal/data/*.go"))
    paths.update(root.glob("kpl-viewer/**/*.py"))
    paths.update(root.glob("kpl-viewer/**/*.sh"))
    actual = {p.relative_to(root).as_posix(): p for p in paths}
    expected = {item["path"]: item for item in manifest["sources"]}
    if actual.keys() != expected.keys():
        problems.append(f"Sources changed: new={sorted(actual.keys()-expected.keys())}, removed={sorted(expected.keys()-actual.keys())}")
    for name in actual.keys() & expected.keys():
        if hashlib.sha256(actual[name].read_bytes()).hexdigest() != expected[name]["sha256"]:
            problems.append(f"Re-audit changed source: {name}")
    if problems:
        print("\n".join(problems), file=sys.stderr)
        return 1
    print(f"PASS: {len(fields)} parser fields/families and {len(actual)} source files match the audit. Source inventory verified; numerical equivalence is not asserted.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
