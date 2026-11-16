#!/usr/bin/env python3
"""CLI regression tests using temporary synthetic Run logs only."""

import csv
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


EXTRACTOR = Path(__file__).resolve().with_name("kpl_path_extract.py")
RUN_ID = "run-synthetic-path-test"
CSV_FILES = {
    "control_summary.csv",
    "duplicate_candidates.csv.gz",
    "hop_quality.csv",
    "hop_timing.csv",
    "message_summary.csv",
    "path_status_summary.csv",
    "placement_timing.csv",
    "receiver_paths.csv.gz",
    "sender_fanout.csv",
    "session_quality.csv",
}


class PathExtractorCLITests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="kpl-path-cli-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.output_number = 0

    def extract(self, source, *options):
        self.output_number += 1
        output = self.root / f"output-{self.output_number}"
        result = subprocess.run(
            [sys.executable, str(EXTRACTOR), str(source), "--out", str(output),
             "--quiet", *options],
            cwd=self.root, capture_output=True, text=True, timeout=30,
        )
        return result, output

    def assert_success(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def snapshot(self, directory):
        return {
            str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in directory.rglob("*") if path.is_file()
        }

    def csv_bytes(self, path):
        data = path.read_bytes()
        return gzip.decompress(data) if path.suffix == ".gz" else data

    def csv_rows(self, output, name):
        return list(csv.DictReader(io.StringIO(
            self.csv_bytes(output / name).decode("utf-8-sig")
        )))

    def chain_input(self):
        source = self.root / "input"
        source.mkdir()
        nodes = ("publisher", "relay", "receiver")
        groups = ("publishers", "relays", "receivers")
        sequences = dict.fromkeys(nodes, 0)
        events = []
        start = datetime(2026, 11, 16, tzinfo=timezone.utc)

        def timestamp(milliseconds):
            return (start + timedelta(milliseconds=milliseconds)).isoformat().replace("+00:00", "Z")

        def event(kind, node, milliseconds, remote="", fields=None):
            sequences[node] += 1
            record = {
                "timestamp": timestamp(milliseconds),
                "eventId": f"{node}-{sequences[node]}",
                "sessionId": f"session-{node}",
                "sequence": sequences[node],
                "runId": RUN_ID,
                "type": kind,
                "nodeId": node,
                "peerId": f"peer-{node}",
                "agentId": f"agent-{node}",
                "group": groups[nodes.index(node)],
                "topic": "topic-test",
                "messageId": "message-test",
                "remotePeerId": f"peer-{remote}" if remote else "",
                "fields": fields if fields is not None else {
                    "payloadEncoding": "raw", "payloadBytes": 128, "wireBytes": 160,
                    "pubsubMessageId": "wire-test",
                    "clockBasis": "controller-offset-v1", "clockUncertaintyMs": 0.1,
                },
            }
            events.append(record)
            return record

        for node in nodes:
            event("measurement_start", node, -1000, fields={"subscribedTopics": ["topic-test"]})
        event("publish", "publisher", 0)
        relay_delivery = event("deliver", "relay", 10, "publisher")
        receiver_delivery = event("deliver", "receiver", 20, "relay")
        receiver_delivery["fields"].pop("clockBasis")
        event("duplicate", "receiver", 25, "publisher")
        event("send_ihave", "publisher", 5, fields={
            "controlEntries": 1, "messageIdCount": 1, "messageIdsComplete": True,
        })
        for direction, node, remote, milliseconds in (
            ("send", "publisher", "relay", 1),
            ("recv", "relay", "publisher", 9),
            ("send", "relay", "receiver", 11),
            ("recv", "receiver", "relay", 19),
        ):
            event("rpc_metadata", node, milliseconds, remote, fields={
                "direction": direction,
                "rpcObservationId": f"{direction}-{node}-{remote}",
                "messages": [{"topic": "topic-test", "pubsubMessageId": "wire-test"}],
            })
        # Put the competing equal-time parent in a later JSON batch.
        for _ in range(2048):
            event("peer_note", "publisher", 30, fields={})
        event("deliver", "receiver", 20, "publisher")
        events.append(dict(relay_delivery))
        for node in nodes:
            event("measurement_stop", node, 1000, fields={"subscribedTopics": ["topic-test"]})
        (source / "events.jsonl").write_text(
            "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in events),
            encoding="utf-8",
        )
        observation = {
            "runId": RUN_ID, "at": timestamp(-100),
            "graphs": [{"protocol": "gossipsub", "nodes": nodes,
                        "groups": groups, "edges": [[0, 1], [1, 2]]}],
        }
        (source / "observations.jsonl").write_text(json.dumps(observation) + "\n", encoding="utf-8")
        (source / "experiment.json").write_text(json.dumps({
            "id": RUN_ID, "name": "Synthetic chain", "state": "completed", "dataState": "complete",
        }), encoding="utf-8")
        return source

    def test_parallel_outputs_preserve_paths_ties_and_replay_deduplication(self):
        source = self.chain_input()
        original = self.snapshot(source)
        serial, serial_out = self.extract(source, "--workers", "1")
        parallel, parallel_out = self.extract(source, "--workers", "2")
        self.assert_success(serial)
        self.assert_success(parallel)
        for output in (serial_out, parallel_out):
            self.assertEqual({p.name for p in output.iterdir() if ".csv" in p.name}, CSV_FILES)
        for name in sorted(CSV_FILES):
            with self.subTest(csv=name):
                self.assertEqual(self.csv_bytes(serial_out / name), self.csv_bytes(parallel_out / name))
        manifests = [json.loads((output / "manifest.json").read_text())
                     for output in (serial_out, parallel_out)]
        for key in ("counts", "event_type_counts", "definitions", "sources", "warnings"):
            with self.subTest(manifest=key):
                self.assertEqual(manifests[0][key], manifests[1][key])
        self.assertEqual(manifests[1]["counts"]["telemetry_replays_skipped"], 1)
        receivers = {row["node"]: row for row in self.csv_rows(parallel_out, "receiver_paths.csv.gz")}
        self.assertEqual(set(receivers), {"relay", "receiver"})
        self.assertEqual((receivers["relay"]["parent"], receivers["relay"]["hop"]), ("publisher", "1"))
        self.assertEqual((receivers["receiver"]["parent"], receivers["receiver"]["hop"]), ("relay", "2"))
        self.assertEqual(float(receivers["receiver"]["parent_delta_raw"]), 10.0)
        self.assertEqual(receivers["receiver"]["parent_clock"], "unverified-cross-agent")
        self.assertEqual(receivers["receiver"]["parent_delta"], "")
        self.assertEqual(self.snapshot(source), original)

    def test_bad_json_precedes_later_oversized_line_and_skip_remains_bounded(self):
        source = self.root / "malformed.jsonl"
        first = json.dumps({
            "runId": RUN_ID, "type": "publish", "nodeId": "publisher",
            "timestamp": "2026-11-16T00:00:00Z", "eventId": "first",
        }).encode()
        original = first + b"\n{bad-json\n" + b"x" * ((1 << 20) + 1) + b"\n"
        source.write_bytes(original)
        for skip in (False, True):
            with self.subTest(skip_bad_lines=skip):
                options = ["--workers", "2", "--max-line-mib", "1"]
                if skip:
                    options.append("--skip-bad-lines")
                result, output = self.extract(source, *options)
                self.assertEqual(result.returncode, 1, result.stderr)
                if skip:
                    self.assertIn(f"{source}:3: line exceeds 1 MiB", result.stderr)
                else:
                    self.assertIn(f"{source}:2:", result.stderr)
                    self.assertNotIn("line exceeds", result.stderr)
                self.assertFalse((output / "manifest.json").exists())
                self.assertEqual(source.read_bytes(), original)

    def test_negative_workers_fail_before_missing_input_or_output_creation(self):
        missing = self.root / "missing-input"
        result, output = self.extract(missing, "--workers", "-1")
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("--workers must be nonnegative", result.stderr)
        self.assertNotIn("Input file or directory does not exist", result.stderr)
        self.assertFalse(output.exists())
        self.assertFalse(missing.exists())
        self.assertEqual(list(self.root.iterdir()), [])

    def test_receiver_group_filter_keeps_parent_paths_across_other_groups(self):
        source = self.chain_input()
        original = self.snapshot(source)
        result, output = self.extract(source, "--workers", "2", "--group", "receivers")
        self.assert_success(result)
        rows = self.csv_rows(output, "receiver_paths.csv.gz")
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]["node"], rows[0]["grp"], rows[0]["parent"], rows[0]["hop"]),
                         ("receiver", "receivers", "relay", "2"))
        messages = self.csv_rows(output, "message_summary.csv")
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["remote_first_receipts"], "1")
        self.assertEqual(messages[0]["hop_resolved"], "1")
        self.assertEqual(float(messages[0]["mean_hop"]), 2.0)
        self.assertEqual(self.snapshot(source), original)

    def archive_input(self):
        source = self.chain_input()
        archived = self.root / RUN_ID
        source.rename(archived)
        manifest = {"version": 1, "runId": RUN_ID, "files": {}, "logs": {}}
        for name in ("events.jsonl", "observations.jsonl"):
            data = (archived / name).read_bytes()
            manifest["logs"][name] = [{"object": name, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}]
        manifest["files"]["experiment.json"] = {"object": "experiment.json"}
        return archived, manifest

    def test_explicit_archive_verification_requires_every_expected_hash(self):
        source, original_manifest = self.archive_input()
        cases = ("empty", "missing", "partial", "null", "malformed", "uppercase", "wrong")
        for kind in cases:
            with self.subTest(kind=kind):
                manifest = json.loads(json.dumps(original_manifest))
                entry = manifest["logs"]["observations.jsonl" if kind == "partial" else "events.jsonl"][0]
                if kind in ("missing", "partial"):
                    entry.pop("sha256")
                elif kind == "empty":
                    entry["sha256"] = ""
                elif kind == "null":
                    entry["sha256"] = None
                elif kind == "malformed":
                    entry["sha256"] = "not-a-sha256"
                elif kind == "uppercase":
                    entry["sha256"] = entry["sha256"].upper()
                else:
                    entry["sha256"] = "0" * 64
                (source / ".kpl-archive.json").write_text(json.dumps(manifest))
                before = self.snapshot(source)
                result, output = self.extract(source, "--verify-archive-sha256", "--list-sources")
                self.assertNotEqual(result.returncode, 0)
                self.assertRegex(result.stderr, r"SHA-256|archive object/size/hash")
                self.assertFalse(output.exists())
                self.assertEqual(self.snapshot(source), before)
                if kind in ("empty", "missing", "partial", "null", "wrong"):
                    legacy, output = self.extract(source, "--list-sources")
                    self.assert_success(legacy)
                    self.assertFalse(output.exists())

    def test_archive_verification_keeps_scientific_outputs_unchanged(self):
        source, manifest = self.archive_input()
        (source / ".kpl-archive.json").write_text(json.dumps(manifest))
        before = self.snapshot(source)
        plain, plain_out = self.extract(source, "--workers", "1")
        verified, verified_out = self.extract(source, "--workers", "1", "--verify-archive-sha256")
        self.assert_success(plain)
        self.assert_success(verified)
        for name in CSV_FILES:
            self.assertEqual(self.csv_bytes(plain_out / name), self.csv_bytes(verified_out / name), name)
        unverified = json.loads((plain_out / "manifest.json").read_text())
        confirmed = json.loads((verified_out / "manifest.json").read_text())
        for key in ("run_id", "counts", "event_type_counts", "input_experiment", "definitions"):
            self.assertEqual(unverified[key], confirmed[key], key)
        self.assertEqual(self.snapshot(source), before)


if __name__ == "__main__":
    unittest.main()
