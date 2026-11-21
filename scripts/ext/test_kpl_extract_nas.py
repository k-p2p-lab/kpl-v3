#!/usr/bin/env python3
"""Batch extraction regression tests using temporary synthetic inputs only."""

import csv
from contextlib import redirect_stdout
import gzip
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().with_name("kpl_extract_nas.py")
EXTRACTOR = SCRIPT.with_name("kpl_path_extract.py")
REQUIRED_OUTPUTS = ("hop_quality.csv", "hop_timing.csv", "message_summary.csv")

FAKE_EXTRACTOR = r'''#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path
import signal
import sys
import time

p = argparse.ArgumentParser()
p.add_argument("source", type=Path)
p.add_argument("--out", type=Path, required=True)
p.add_argument("--workers", type=int, required=True)
p.add_argument("--label", required=True)
p.add_argument("--list-sources", action="store_true")
args, extra = p.parse_known_args()
root = Path(os.environ["KPL_BATCH_TEST_CONTROL"])
rid = args.source.name
plan = json.loads((root / "plan.json").read_text()).get(rid, {})

def record(kind, **values):
    data = dict(kind=kind, run=rid, pid=os.getpid(), pgid=os.getpgrp(), **values)
    fd = os.open(root / "events.jsonl", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, (json.dumps(data) + "\n").encode())
    finally:
        os.close(fd)

def wait_for(path):
    deadline = time.monotonic() + 25
    while not path.exists():
        if time.monotonic() >= deadline:
            raise RuntimeError("Synthetic extractor gate timed out: " + str(path))
        time.sleep(0.01)

def interrupted(signum, frame):
    record("signal", signal=signum)
    (root / "signaled" / rid).touch()
    if plan.get("cleanup_gate"):
        wait_for(root / "cleanup" / rid)
    raise SystemExit(130)

signal.signal(signal.SIGINT, interrupted)
signal.signal(signal.SIGTERM, interrupted)
record("start", workers=args.workers, output=str(args.out), label=args.label,
       arguments=sys.argv[1:], extra=extra)
(root / "started" / rid).touch()
print("SOURCE " + rid, flush=True)
print("STDERR " + rid, file=sys.stderr, flush=True)
try:
    if plan.get("gate"):
        wait_for(root / "release" / rid)
    status = plan.get("exit", 0)
    if not status and not args.list_sources:
        args.out.mkdir(parents=True, exist_ok=True)
        (args.out / "manifest.json").write_text(json.dumps({
            "tool": "kpl_path_extract", "version": "1.1.0-nas",
        }))
        for name in ("hop_quality.csv", "hop_timing.csv", "message_summary.csv"):
            (args.out / name).write_text("run_id\n" + rid + "\n")
    raise SystemExit(status)
finally:
    record("finish")
    (root / "finished" / rid).touch()
'''

FAKE_SUDO = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import signal
import sys
import time

root = Path(os.environ["KPL_BATCH_TEST_CONTROL"])
args = sys.argv[1:]
fd = os.open(root / "events.jsonl", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
try:
    os.write(fd, (json.dumps({"kind": "sudo", "arguments": args, "pid": os.getpid()}) + "\n").encode())
finally:
    os.close(fd)
if args == ["-v"]:
    if (root / "sudo-gate").exists():
        def interrupted(signum, frame):
            (root / "signaled" / "sudo-auth").touch()
            raise SystemExit(130)
        signal.signal(signal.SIGINT, interrupted)
        (root / "started" / "sudo-auth").touch()
        try:
            while not (root / "release" / "sudo-auth").exists():
                time.sleep(0.01)
        finally:
            (root / "finished" / "sudo-auth").touch()
    sys.exit(0)
if len(args) >= 3 and args[:2] == ["-n", "python3"]:
    os.execv(sys.executable, [sys.executable, *args[2:]])
raise SystemExit("Unexpected synthetic sudo arguments: " + repr(args))
'''


def load_script():
    name = "kpl_batch_extractor_under_test"
    spec = importlib.util.spec_from_file_location(name, SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class ConcurrencyBudgetTests(unittest.TestCase):
    def test_default_manifest_name_matches_renamed_extractor(self):
        self.assertEqual(load_script().parser().parse_args([]).manifest.name, "kpl_extract_manifest.csv")

    def test_cpu_affinity_precedes_host_count_and_fallback_never_returns_zero(self):
        module = load_script()
        with mock.patch.object(module.os, "sched_getaffinity", return_value={2, 7}, create=True), \
                mock.patch.object(module.os, "cpu_count", return_value=99):
            self.assertEqual(module.available_cpus(), 2)
        with mock.patch.object(module.os, "sched_getaffinity", side_effect=OSError, create=True), \
                mock.patch.object(module.os, "cpu_count", return_value=None):
            self.assertEqual(module.available_cpus(), 1)
        with mock.patch.object(module.os, "sched_getaffinity", return_value=set(), create=True):
            self.assertEqual(module.available_cpus(), 1)

    def test_auto_budget_respects_selected_runs_and_explicit_overrides(self):
        module = load_script()
        examples = (
            (1, 0, 0, 50, (1, 0)),
            (2, 0, 0, 50, (2, 1)),
            (7, 0, 0, 50, (4, 1)),
            (8, 0, 0, 50, (4, 2)),
            (32, 0, 0, 50, (4, 4)),
            (32, 0, 0, 2, (2, 4)),
            (32, 0, 0, 1, (1, 0)),
            (8, 1, 0, 50, (1, 0)),
            (8, 2, 0, 50, (2, 4)),
            (2, 12, 0, 10, (10, 1)),
            (8, 0, 12, 50, (4, 12)),
            (8, 2, 12, 50, (2, 12)),
        )
        for cpus, jobs, workers, total, expected in examples:
            with self.subTest(cpus=cpus, jobs=jobs, workers=workers, selected=total), \
                    mock.patch.object(module, "available_cpus", return_value=cpus):
                self.assertEqual(module.concurrency(jobs, workers, total), expected)


class BatchExtractorCLITests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="kpl-batch-cli-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name) / "paths with spaces"
        self.root.mkdir()
        self.control = self.root / "control"
        for name in ("started", "finished", "release", "signaled", "cleanup"):
            (self.control / name).mkdir(parents=True, exist_ok=True)
        self.nas = self.root / "synthetic NAS"
        self.nas.mkdir()
        self.output = self.root / "output"
        self.work = self.root / "work"
        self.manifest = self.root / "cases.csv"
        self.fake = self.root / "fake extractor.py"
        self.fake.write_text(FAKE_EXTRACTOR, encoding="utf-8")
        self.environment = dict(os.environ, KPL_BATCH_TEST_CONTROL=str(self.control))
        self.environment["PYTHONDONTWRITEBYTECODE"] = "1"
        self.process = None
        self.parent_log = self.root / "parent.log"
        self.addCleanup(self.stop_children)

    def cases(self, count=4, plan=None, missing=()):
        self.rows = [dict(payload_bytes="128", degree="6", run_id=f"run-test-{i}", label="shared-label")
                     for i in range(1, count + 1)]
        with self.manifest.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=("payload_bytes", "degree", "run_id", "label"))
            writer.writeheader()
            writer.writerows(self.rows)
        for row in self.rows:
            if row["run_id"] not in missing:
                (self.nas / row["run_id"]).mkdir()
        (self.control / "plan.json").write_text(json.dumps(plan or {}), encoding="utf-8")
        return [row["run_id"] for row in self.rows]

    def command(self, *options, extractor=None):
        return [sys.executable, str(SCRIPT), "--manifest", str(self.manifest),
                "--extractor", str(extractor or self.fake), "--nas-root", str(self.nas),
                "--out", str(self.output), *options]

    def run_batch(self, *options, extractor=None):
        return subprocess.run(self.command(*options, extractor=extractor), cwd=self.root,
                              env=self.environment, capture_output=True, text=True, timeout=35)

    def test_exported_manifest_allows_independently_unknown_numeric_labels(self):
        self.manifest.write_text("payload_bytes,degree,seed,run_id,label\n"
                                 ",6,9007199254740993,run-one,case\n"
                                 "1000,,,run-two,case\n", encoding="utf-8")
        result = self.run_batch("--dry-run")
        self.assertEqual(result.returncode, 0, result.stderr)
        commands = [line for line in result.stdout.splitlines() if "--label" in line]
        self.assertEqual(len(commands), 2)
        self.assertIn("--degree 6", commands[0])
        self.assertNotIn("--payload-bytes", commands[0])
        self.assertIn("--payload-bytes 1000", commands[1])
        self.assertNotIn("--degree", commands[1])

    def start_batch(self, *options, extractor=None):
        stream = self.parent_log.open("w", encoding="utf-8")
        self.addCleanup(stream.close)
        self.process = subprocess.Popen(self.command(*options, extractor=extractor), cwd=self.root,
                                        env=self.environment, stdout=stream, stderr=subprocess.STDOUT,
                                        start_new_session=True)
        return self.process

    def stop_children(self):
        if self.process is not None and self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=5)
        # Each fake extractor is a new process group owned by this test.
        for event in self.events():
            if event["kind"] == "start" and event["pgid"] == event["pid"]:
                try:
                    os.killpg(event["pgid"], signal.SIGKILL)
                except ProcessLookupError:
                    pass
            elif event["kind"] == "sudo" and event["arguments"] == ["-v"]:
                if (self.control / "started" / "sudo-auth").exists() and not (self.control / "finished" / "sudo-auth").exists():
                    try:
                        os.kill(event["pid"], signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def events(self):
        path = self.control / "events.jsonl"
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]

    def parent_output(self):
        return self.parent_log.read_text(encoding="utf-8") if self.parent_log.exists() else ""

    def wait_for(self, predicate, description):
        deadline = time.monotonic() + 12
        while not predicate():
            if self.process is not None and self.process.poll() is not None:
                self.fail(f"Batch exited before {description}:\n{self.parent_output()}")
            if time.monotonic() >= deadline:
                self.fail(f"Timed out waiting for {description}:\n{self.parent_output()}\n{self.events()}")
            time.sleep(0.01)

    def wait_marker(self, kind, rid):
        self.wait_for(lambda: (self.control / kind / rid).exists(), f"{kind}: {rid}")

    def release(self, rid, kind="release"):
        (self.control / kind / rid).touch()

    def complete(self, status=0):
        self.assertEqual(self.process.wait(timeout=20), status, self.parent_output())
        return self.parent_output()

    def destination(self, rid):
        return self.output / "shared-label" / rid

    def completed_output(self, rid):
        output = self.destination(rid)
        output.mkdir(parents=True)
        (output / "manifest.json").write_text(json.dumps({
            "tool": "kpl_path_extract", "version": "1.1.0-nas",
        }), encoding="utf-8")
        for name in REQUIRED_OUTPUTS:
            (output / name).write_text("preserved\n", encoding="utf-8")
        return output

    def assert_success(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_jobs_overlap_at_the_limit_and_keep_run_outputs_and_logs_separate(self):
        runs = self.cases(plan={f"run-test-{i}": {"gate": True} for i in range(1, 5)})
        self.start_batch("--jobs", "2", "--workers", "3", "--work-dir", str(self.work), "--no-rpc")
        for rid in runs[:2]:
            self.wait_marker("started", rid)
        self.assertEqual({event["run"] for event in self.events() if event["kind"] == "start"}, set(runs[:2]))
        self.assertFalse((self.control / "finished" / runs[0]).exists())
        self.release(runs[1])
        self.wait_marker("started", runs[2])
        self.assertFalse((self.control / "finished" / runs[0]).exists())
        self.release(runs[2])
        self.wait_marker("started", runs[3])
        self.release(runs[3])
        self.release(runs[0])
        output = self.complete()
        self.assertIn("Processed=4, already_done=0, errors=0, selected=4", output)
        active = peak = 0
        for event in self.events():
            if event["kind"] == "start":
                active += 1
                peak = max(peak, active)
                self.assertLessEqual(active, 2)
                self.assertEqual(event["workers"], 3)
                self.assertEqual(event["output"], str(self.destination(event["run"])))
                self.assertIn("--no-rpc", event["arguments"])
                self.assertIn(str(self.work), event["arguments"])
                self.assertEqual(event["arguments"][event["arguments"].index("--write-lock") + 1],
                                 str(self.output / ".kpl-write.lock"))
            elif event["kind"] == "finish":
                active -= 1
        self.assertEqual((active, peak), (0, 2))
        for rid in runs:
            self.assertEqual((self.destination(rid) / "hop_quality.csv").read_text(), f"run_id\n{rid}\n")
            log = (self.output / ".logs" / f"{rid}.log").read_text(encoding="utf-8")
            self.assertIn("SOURCE " + rid, log)
            self.assertIn("STDERR " + rid, log)

    def test_jobs_one_preserves_serial_admission_and_auto_workers(self):
        runs = self.cases(2, plan={"run-test-1": {"gate": True}, "run-test-2": {"gate": True}})
        self.start_batch("--jobs", "1")
        self.wait_marker("started", runs[0])
        self.assertEqual([event["run"] for event in self.events() if event["kind"] == "start"], runs[:1])
        self.release(runs[0])
        self.wait_marker("started", runs[1])
        self.assertTrue((self.control / "finished" / runs[0]).exists())
        self.release(runs[1])
        self.complete()
        self.assertEqual([event["workers"] for event in self.events() if event["kind"] == "start"], [0, 0])

    def test_auto_jobs_respect_available_cpus_and_selection_and_divide_worker_budget(self):
        cpus = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count() or 1
        runs = self.cases(5, plan={f"run-test-{i}": {"gate": True} for i in range(1, 6)})
        jobs = min(4, cpus, len(runs))
        self.start_batch()
        for rid in runs[:jobs]:
            self.wait_marker("started", rid)
        starts = [event for event in self.events() if event["kind"] == "start"]
        self.assertEqual({event["run"] for event in starts}, set(runs[:jobs]))
        expected_workers = 0 if jobs == 1 else max(1, min(4, cpus // jobs))
        self.assertEqual({event["workers"] for event in starts}, {expected_workers})
        for rid in runs:
            self.release(rid)
        self.complete()

    def check_failure_admission(self, continue_on_error):
        runs = self.cases(plan={
            "run-test-1": {"gate": True}, "run-test-2": {"gate": True, "exit": 7},
            "run-test-3": {"gate": True}, "run-test-4": {"gate": True},
        })
        options = ["--jobs", "2", "--workers", "1"]
        if continue_on_error:
            options.append("--continue-on-error")
        self.start_batch(*options)
        for rid in runs[:2]:
            self.wait_marker("started", rid)
        self.release(runs[1])
        self.wait_for(lambda: "ERROR" in self.parent_output(), "observed extraction failure")
        self.assertFalse((self.control / "finished" / runs[0]).exists())
        if continue_on_error:
            self.wait_marker("started", runs[2])
            self.release(runs[2])
            self.wait_marker("started", runs[3])
            self.release(runs[3])
        else:
            self.assertFalse((self.control / "started" / runs[2]).exists())
        self.release(runs[0])
        output = self.complete(1)
        expected = runs if continue_on_error else runs[:2]
        self.assertEqual({event["run"] for event in self.events() if event["kind"] == "start"}, set(expected))
        self.assertTrue((self.destination(runs[0]) / "manifest.json").is_file())
        done = 3 if continue_on_error else 1
        self.assertIn(f"Processed={done}, already_done=0, errors=1, selected=4", output)

    def test_failure_stops_new_admissions_and_waits_for_already_running_child(self):
        self.check_failure_admission(False)

    def test_continue_on_error_admits_remaining_runs(self):
        self.check_failure_admission(True)

    def test_complete_outputs_are_skipped_and_partial_and_missing_inputs_are_preserved(self):
        runs = self.cases(missing=("run-test-3",))
        completed = self.completed_output(runs[0])
        original = {path.name: path.read_bytes() for path in completed.iterdir()}
        partial = self.destination(runs[1])
        partial.mkdir(parents=True)
        (partial / "keep.txt").write_text("do not overwrite", encoding="utf-8")
        result = self.run_batch("--jobs", "2", "--continue-on-error")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("Processed=1, already_done=1, errors=2, selected=4", result.stdout)
        self.assertEqual([event["run"] for event in self.events() if event["kind"] == "start"], runs[3:])
        self.assertEqual({path.name: path.read_bytes() for path in completed.iterdir()}, original)
        self.assertEqual((partial / "keep.txt").read_text(), "do not overwrite")
        self.assertFalse((self.nas / runs[2]).exists())

    @unittest.skipUnless(hasattr(os, "symlink"), "Requires symbolic links")
    def test_output_symlinks_cannot_create_nested_run_destinations(self):
        runs = self.cases(2)
        (self.output / "shared-label").mkdir(parents=True)
        target = self.output / "base"
        self.destination(runs[0]).symlink_to(target, target_is_directory=True)
        self.destination(runs[1]).symlink_to(target / "nested", target_is_directory=True)
        result = self.run_batch("--jobs", "2")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.events(), [])
        self.assertFalse(target.exists())
        self.assertFalse((self.output / ".logs").exists())

    @unittest.skipUnless(hasattr(os, "symlink"), "Requires symbolic links")
    def test_log_file_symlink_cannot_append_to_an_unrelated_file(self):
        runs = self.cases(1)
        sentinel = self.root / "preserved.txt"
        sentinel.write_text("preserve these bytes\n", encoding="utf-8")
        logs = self.output / ".logs"
        logs.mkdir(parents=True)
        (logs / f"{runs[0]}.log").symlink_to(sentinel)
        result = self.run_batch("--jobs", "1", "--list-sources")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(sentinel.read_text(encoding="utf-8"), "preserve these bytes\n")
        self.assertNotIn("preserve these bytes", result.stdout)
        self.assertEqual(self.events(), [])

    @unittest.skipUnless(hasattr(os, "symlink"), "Requires symbolic links")
    def test_log_directory_symlink_cannot_create_files_outside_output(self):
        self.cases(1)
        external = self.root / "external logs"
        external.mkdir()
        sentinel = external / "preserved.txt"
        sentinel.write_bytes(b"preserved\n")
        self.output.mkdir()
        (self.output / ".logs").symlink_to(external, target_is_directory=True)
        result = self.run_batch("--jobs", "1")
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual({path.name: path.read_bytes() for path in external.iterdir()}, {"preserved.txt": b"preserved\n"})
        self.assertEqual(self.events(), [])

    def test_list_sources_prints_each_completed_job_output_and_preserves_prior_outputs(self):
        runs = self.cases(2, plan={rid: {"gate": True} for rid in ("run-test-1", "run-test-2")})
        completed = self.completed_output(runs[0])
        original = {path.name: path.read_bytes() for path in completed.iterdir()}
        partial = self.destination(runs[1])
        partial.mkdir(parents=True)
        (partial / "keep.txt").write_text("preserved", encoding="utf-8")
        self.start_batch("--jobs", "2", "--list-sources")
        for rid in runs:
            self.wait_marker("started", rid)
        self.assertNotIn("SOURCE ", self.parent_output())
        self.release(runs[1])
        self.wait_for(lambda: "SOURCE " + runs[1] in self.parent_output(), "completed source listing")
        self.assertNotIn("SOURCE " + runs[0], self.parent_output())
        self.release(runs[0])
        output = self.complete()
        self.assertIn("SOURCE " + runs[0], output)
        self.assertIn("Processed=2, already_done=0, errors=0, selected=2", output)
        self.assertEqual({path.name: path.read_bytes() for path in completed.iterdir()}, original)
        self.assertEqual((partial / "keep.txt").read_text(), "preserved")

    def install_fake_sudo(self):
        binary = self.root / "bin"
        binary.mkdir()
        sudo = binary / "sudo"
        sudo.write_text(FAKE_SUDO, encoding="utf-8")
        sudo.chmod(0o700)
        self.environment["PATH"] = str(binary) + os.pathsep + self.environment.get("PATH", "")

    def test_sudo_validates_in_parent_and_invokes_only_noninteractive_children(self):
        runs = self.cases(3)
        self.install_fake_sudo()
        result = self.run_batch("--sudo", "--jobs", "2", "--workers", "1")
        self.assert_success(result)
        calls = [event["arguments"] for event in self.events() if event["kind"] == "sudo"]
        self.assertEqual(calls.count(["-v"]), len(runs))
        child_calls = [args for args in calls if args != ["-v"]]
        self.assertEqual(len(child_calls), len(runs))
        self.assertTrue(all(args[:2] == ["-n", "python3"] for args in child_calls))
        validated = launched = 0
        for event in self.events():
            if event["kind"] == "sudo" and event["arguments"] == ["-v"]:
                validated += 1
            elif event["kind"] == "sudo":
                launched += 1
                self.assertGreaterEqual(validated, launched)

    def test_explicit_write_lock_is_forwarded_to_every_run(self):
        self.cases(3)
        lock = self.root / "shared locks" / "disk.lock"
        result = self.run_batch("--jobs", "2", "--write-lock", str(lock))
        self.assert_success(result)
        starts = [event for event in self.events() if event["kind"] == "start"]
        self.assertEqual(len(starts), 3)
        for event in starts:
            args = event["arguments"]
            self.assertEqual(args[args.index("--write-lock") + 1], str(lock))

    def test_list_sources_does_not_forward_an_output_write_lock(self):
        self.cases(2)
        lock = self.root / "missing lock parent" / "write.lock"
        result = self.run_batch("--jobs", "2", "--list-sources", "--write-lock", str(lock))
        self.assert_success(result)
        for event in self.events():
            if event["kind"] == "start":
                self.assertNotIn("--write-lock", event["arguments"])
        self.assertFalse(lock.parent.exists())

    def test_write_lock_cannot_be_nested_inside_or_replace_a_run_output(self):
        runs = self.cases(1)
        for lock in (self.destination(runs[0]) / "write.lock", self.destination(runs[0]), self.output):
            with self.subTest(lock=lock):
                result = self.run_batch("--write-lock", str(lock))
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertEqual(self.events(), [])
                self.assertFalse(self.output.exists())

    def test_dry_run_quotes_commands_and_does_not_create_paths_or_invoke_sudo(self):
        runs = self.cases(2, missing=("run-test-1", "run-test-2"))
        self.install_fake_sudo()
        current = self.root / "missing current-run"
        result = self.run_batch("--dry-run", "--sudo", "--jobs", "2", "--workers", "3",
                                "--work-dir", str(self.work), "--local-current-root", str(current),
                                "--verify-archive-sha256", "--no-rpc", "--list-sources")
        self.assert_success(result)
        commands = [shlex.split(line.strip()) for line in result.stdout.splitlines()
                    if line.startswith("  ") and "fake extractor.py" in line]
        self.assertEqual(len(commands), len(runs), result.stdout)
        for rid, command in zip(runs, commands):
            self.assertEqual(command[:3], ["sudo", "-n", "python3"])
            self.assertIn(str(self.nas / rid), command)
            self.assertIn(str(self.destination(rid)), command)
            self.assertEqual(command[command.index("--workers") + 1], "3")
            self.assertIn(str(current), command)
            self.assertIn("--list-sources", command)
            self.assertIn("--verify-archive-sha256", command)
        self.assertFalse(self.output.exists())
        self.assertFalse(self.work.exists())
        self.assertFalse(current.exists())
        self.assertEqual(self.events(), [])

    def test_dry_run_reduces_budget_after_selection_and_never_spawns_or_makes_directories(self):
        self.cases(5)
        module = load_script()
        output = io.StringIO()
        forbidden = AssertionError("Dry run attempted a process or directory creation")
        with mock.patch.object(module, "available_cpus", return_value=8), \
                mock.patch.object(module.subprocess, "Popen", side_effect=forbidden), \
                mock.patch.object(module.subprocess, "run", side_effect=forbidden), \
                mock.patch.object(Path, "mkdir", side_effect=forbidden), redirect_stdout(output):
            status = module.main(self.command("--dry-run", "--sudo", "--limit", "2",
                                              "--work-dir", str(self.work))[2:])
        self.assertEqual(status, 0)
        self.assertIn("Concurrent Runs=2, extraction workers/Run=4", output.getvalue())
        commands = [shlex.split(line.strip()) for line in output.getvalue().splitlines()
                    if line.startswith("  ") and "fake extractor.py" in line]
        self.assertEqual(len(commands), 2)
        self.assertEqual([command[command.index("--workers") + 1] for command in commands], ["4", "4"])
        self.assertEqual([command[command.index("--write-lock") + 1] for command in commands],
                         [str(self.output / ".kpl-write.lock")] * 2)
        self.assertFalse(self.output.exists())
        self.assertFalse(self.work.exists())

    def test_invalid_numeric_options_fail_before_creating_output_or_work(self):
        self.cases(1)
        for option in ("--jobs", "--workers", "--limit"):
            with self.subTest(option=option):
                result = self.run_batch(option, "-1", "--work-dir", str(self.work))
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertIn(option, result.stderr)
                self.assertFalse(self.output.exists())
                self.assertFalse(self.work.exists())
                self.assertEqual(self.events(), [])

    @unittest.skipUnless(os.name == "posix", "Process-group cancellation requires POSIX")
    def test_parent_sigint_waits_for_child_cleanup_and_stops_new_jobs(self):
        self.check_parent_signal(signal.SIGINT)

    @unittest.skipUnless(os.name == "posix", "Process-group cancellation requires POSIX")
    def test_parent_sigterm_waits_for_child_cleanup_and_stops_new_jobs(self):
        self.check_parent_signal(signal.SIGTERM)

    @unittest.skipUnless(os.name == "posix", "Signal cancellation requires POSIX")
    def test_parent_sigterm_during_sudo_validation_reaps_auth_without_launching_extractor(self):
        self.cases(2)
        self.install_fake_sudo()
        (self.control / "sudo-gate").touch()
        self.start_batch("--sudo", "--jobs", "2")
        self.wait_marker("started", "sudo-auth")
        self.process.send_signal(signal.SIGTERM)
        self.complete(143)
        self.assertTrue((self.control / "signaled" / "sudo-auth").exists())
        self.assertTrue((self.control / "finished" / "sudo-auth").exists())
        self.assertEqual([event for event in self.events() if event["kind"] == "start"], [])
        self.assertFalse((self.output / ".logs").exists())

    def check_parent_signal(self, signum):
        runs = self.cases(3, plan={rid: {"gate": True, "cleanup_gate": True}
                                  for rid in ("run-test-1", "run-test-2")})
        self.start_batch("--jobs", "2", "--workers", "1")
        for rid in runs[:2]:
            self.wait_marker("started", rid)
        starts = [event for event in self.events() if event["kind"] == "start"]
        self.assertEqual(len({event["pgid"] for event in starts}), 2)
        self.assertTrue(all(event["pgid"] == event["pid"] for event in starts))
        self.assertTrue(all(event["pgid"] != os.getpgid(self.process.pid) for event in starts))
        self.process.send_signal(signum)
        for rid in runs[:2]:
            self.wait_marker("signaled", rid)
        self.assertIsNone(self.process.poll(), self.parent_output())
        self.assertFalse((self.control / "started" / runs[2]).exists())
        # Further terminal signals must not interrupt cooperative cleanup twice.
        self.process.send_signal(signal.SIGTERM if signum == signal.SIGINT else signal.SIGINT)
        for rid in runs[:2]:
            self.release(rid, "cleanup")
        self.assertEqual(self.process.wait(timeout=15), 128 + signum, self.parent_output())
        signals = [event for event in self.events() if event["kind"] == "signal"]
        self.assertEqual(sorted((event["run"], event["signal"]) for event in signals),
                         [(rid, signal.SIGINT) for rid in runs[:2]])
        self.assertEqual({event["run"] for event in self.events() if event["kind"] == "finish"}, set(runs[:2]))
        self.assertFalse((self.control / "started" / runs[2]).exists())
        for event in starts:
            with self.assertRaises(ProcessLookupError):
                os.kill(event["pid"], 0)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Worker lifecycle observation requires Linux /proc")
    def test_parent_sigint_reaps_real_extractor_worker_pools_and_preserves_inputs(self):
        runs = self.cases(2)
        hashes = {}
        for rid in runs:
            source = self.nas / rid
            (source / "experiment.json").write_text(json.dumps({"id": rid}), encoding="utf-8")
            with (source / "events.jsonl").open("w", encoding="utf-8") as stream:
                for sequence in range(80000):
                    stream.write(json.dumps({
                        "runId": rid, "type": "peer_note", "eventId": f"e-{sequence}",
                        "sessionId": "session", "sequence": sequence, "nodeId": "peer",
                        "timestamp": "2026-11-18T00:00:00Z", "fields": {"padding": "x" * 160},
                    }, separators=(",", ":")) + "\n")
            hashes[rid] = hashlib.sha256((source / "events.jsonl").read_bytes()).hexdigest()

        def children(pid):
            try:
                return [int(value) for value in Path(f"/proc/{pid}/task/{pid}/children").read_text().split()]
            except FileNotFoundError:
                return []

        def alive(pid):
            try:
                return Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()[0] != "Z"
            except FileNotFoundError:
                return False

        seen = set()
        extractors = set()

        def pools_started():
            current = children(self.process.pid)
            extractors.update(current)
            seen.update(current)
            descendants = {pid: children(pid) for pid in current}
            for pids in descendants.values():
                seen.update(pids)
            # Spawn starts one resource tracker plus the two requested workers.
            return len(current) == 2 and all(len(descendants[pid]) >= 3 for pid in current)

        def cleanup():
            if self.process.poll() is None:
                self.process.send_signal(signal.SIGINT)
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
            for pid in extractors:
                if alive(pid):
                    try:
                        if os.getpgid(pid) == pid:
                            os.killpg(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
            for pid in seen:
                if alive(pid):
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

        self.start_batch("--jobs", "2", "--workers", "2", "--work-dir", str(self.work), extractor=EXTRACTOR)
        self.addCleanup(cleanup)
        self.wait_for(pools_started, "both real extractor worker pools")
        self.process.send_signal(signal.SIGINT)
        self.complete(130)
        deadline = time.monotonic() + 3
        while any(alive(pid) for pid in seen) and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertFalse([pid for pid in seen if alive(pid)], self.parent_output())
        for rid in runs:
            self.assertEqual(hashlib.sha256((self.nas / rid / "events.jsonl").read_bytes()).hexdigest(), hashes[rid])
            log = (self.output / ".logs" / f"{rid}.log").read_text(encoding="utf-8")
            self.assertIn("Interrupted. Input files were not modified.", log)
            self.assertFalse((self.destination(rid) / "manifest.json").exists())

    def test_real_extractor_produces_identical_csvs_for_serial_and_parallel_runs(self):
        runs = self.cases(2)
        spec = importlib.util.spec_from_file_location("batch_test_path_fixtures", SCRIPT.with_name("test_kpl_path_extract.py"))
        fixture_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fixture_module)
        fixture = fixture_module.PathExtractorCLITests(methodName="runTest")
        fixture.root = self.root / "fixture"
        fixture.root.mkdir()
        source = fixture.chain_input()
        for rid in runs:
            for path in source.iterdir():
                (self.nas / rid / path.name).write_text(
                    path.read_text(encoding="utf-8").replace(fixture_module.RUN_ID, rid), encoding="utf-8")
        original = {str(path.relative_to(self.nas)): path.read_bytes()
                    for path in self.nas.rglob("*") if path.is_file()}
        serial = self.run_batch("--jobs", "1", "--workers", "1", extractor=EXTRACTOR)
        self.assert_success(serial)
        serial_output = self.root / "serial-output"
        self.output.rename(serial_output)
        parallel = self.run_batch("--jobs", "2", "--workers", "1", extractor=EXTRACTOR)
        self.assert_success(parallel)
        for rid in runs:
            first = serial_output / "shared-label" / rid
            second = self.destination(rid)
            csvs = {path.name for path in first.iterdir() if ".csv" in path.name}
            self.assertEqual(csvs, fixture_module.CSV_FILES)
            self.assertEqual(csvs, {path.name for path in second.iterdir() if ".csv" in path.name})
            for name in sorted(csvs):
                with self.subTest(run=rid, csv=name):
                    before = (first / name).read_bytes()
                    after = (second / name).read_bytes()
                    if name.endswith(".gz"):
                        before, after = gzip.decompress(before), gzip.decompress(after)
                    self.assertEqual(before, after)
            manifests = [json.loads((out / "manifest.json").read_text(encoding="utf-8")) for out in (first, second)]
            for key in ("counts", "event_type_counts", "definitions", "sources", "warnings"):
                with self.subTest(run=rid, manifest=key):
                    self.assertEqual(manifests[0][key], manifests[1][key])
        self.assertEqual({str(path.relative_to(self.nas)): path.read_bytes()
                          for path in self.nas.rglob("*") if path.is_file()}, original)


if __name__ == "__main__":
    unittest.main()
