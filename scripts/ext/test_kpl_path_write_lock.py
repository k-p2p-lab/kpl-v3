#!/usr/bin/env python3
"""Cross-process output-lock regressions using temporary synthetic logs only."""

import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


EXTRACTOR = Path(__file__).resolve().with_name("kpl_path_extract.py")

DRIVER = r'''#!/usr/bin/env python3
import importlib.util
import json
import os
from pathlib import Path
import sys
import time

root = Path(os.environ["KPL_LOCK_TEST_CONTROL"])
rid = os.environ["KPL_LOCK_TEST_RUN"]
plan = json.loads((root / "plan.json").read_text()).get(rid, {})
spec = importlib.util.spec_from_file_location("gated_path_extractor", sys.argv[1])
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

def record(kind, **values):
    fd = os.open(root / "events.jsonl", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, (json.dumps(dict(kind=kind, run=rid, pid=os.getpid(), **values)) + "\n").encode())
    finally:
        os.close(fd)
    (root / kind / rid).touch()

def gate(name):
    deadline = time.monotonic() + 25
    while not (root / ("release-" + name) / rid).exists():
        if time.monotonic() >= deadline:
            raise RuntimeError("Synthetic lock gate timed out: " + name)
        time.sleep(0.01)

original_ingest = module.Extractor.ingest_observations
def observed_ingest(ex, sources):
    result = original_ingest(ex, sources)
    record("ingested")
    return result
module.Extractor.ingest_observations = observed_ingest

original_analyze = module.analyze
def gated_analyze(ex, out):
    record("entered")
    try:
        if plan.get("write_gate"):
            gate("write")
        if plan.get("fail"):
            raise OSError("Synthetic file-generation failure")
        return original_analyze(ex, out)
    finally:
        record("left")
module.analyze = gated_analyze

original_init = module.Extractor.__init__
class ObservedConnection:
    def __init__(self, connection):
        self.connection = connection
        self.closed = False
    def __getattr__(self, key):
        return getattr(self.connection, key)
    def close(self):
        if not self.closed:
            record("closing")
            if plan.get("close_gate"):
                gate("close")
            self.connection.close()
            self.closed = True
            record("closed")
def observed_init(ex, *args, **kwargs):
    original_init(ex, *args, **kwargs)
    ex.db = ObservedConnection(ex.db)
module.Extractor.__init__ = observed_init

original_unlink = Path.unlink
def observed_unlink(path, *args, **kwargs):
    if path.name.startswith("kpl-path-") and path.suffix == ".sqlite3":
        record("cleaning")
        if plan.get("cleanup_gate"):
            gate("cleanup")
    return original_unlink(path, *args, **kwargs)
Path.unlink = observed_unlink

raise SystemExit(module.main(sys.argv[2:]))
'''


@unittest.skipUnless(os.name == "posix", "Output file locking requires POSIX")
class PathWriteLockTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="kpl-output-lock-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "paths with spaces"
        self.root.mkdir()
        self.control = self.root / "control"
        for kind in ("ingested", "entered", "left", "closing", "closed", "cleaning",
                     "release-write", "release-close", "release-cleanup"):
            (self.control / kind).mkdir(parents=True, exist_ok=True)
        self.plan = {}
        self.plan_path = self.control / "plan.json"
        self.plan_path.write_text("{}", encoding="utf-8")
        self.driver = self.root / "gated extractor.py"
        self.driver.write_text(DRIVER, encoding="utf-8")
        self.lock = self.root / "shared write.lock"
        self.processes = {}
        self.logs = {}
        self.sources = {}
        self.original_inputs = {}
        self.addCleanup(self.assert_inputs_unchanged)
        self.addCleanup(self.stop_processes)

        spec = importlib.util.spec_from_file_location("lock_test_path_fixtures", EXTRACTOR.with_name("test_kpl_path_extract.py"))
        self.fixture_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.fixture_module)
        fixture = self.fixture_module.PathExtractorCLITests(methodName="runTest")
        fixture.root = self.root / "fixture"
        fixture.root.mkdir()
        self.fixture_source = fixture.chain_input()

    def source(self, rid):
        if rid in self.sources:
            return self.sources[rid]
        source = self.root / "inputs" / rid
        source.mkdir(parents=True)
        for path in self.fixture_source.iterdir():
            target = source / path.name
            target.write_text(path.read_text(encoding="utf-8").replace(self.fixture_module.RUN_ID, rid), encoding="utf-8")
            self.original_inputs[target] = hashlib.sha256(target.read_bytes()).hexdigest()
        self.sources[rid] = source
        return source

    def assert_inputs_unchanged(self):
        for path, digest in self.original_inputs.items():
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), digest, str(path))

    def out(self, rid):
        return self.root / "outputs" / rid

    def arguments(self, rid, lock=True):
        args = [str(self.source(rid)), "--out", str(self.out(rid)), "--workers", "1"]
        if lock:
            args += ["--write-lock", str(self.lock)]
        return args

    def start(self, rid, plan=None):
        self.plan[rid] = plan or {}
        self.plan_path.write_text(json.dumps(self.plan), encoding="utf-8")
        log_path = self.root / f"{rid}.log"
        log = log_path.open("w", encoding="utf-8")
        environment = dict(os.environ, KPL_LOCK_TEST_CONTROL=str(self.control),
                           KPL_LOCK_TEST_RUN=rid, PYTHONDONTWRITEBYTECODE="1")
        try:
            process = subprocess.Popen([sys.executable, str(self.driver), str(EXTRACTOR), *self.arguments(rid)],
                                       cwd=self.root, env=environment, stdout=log,
                                       stderr=subprocess.STDOUT, start_new_session=True)
        finally:
            log.close()
        self.processes[rid] = process
        self.logs[rid] = log_path
        return process

    def stop_processes(self):
        for process in self.processes.values():
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
        for process in self.processes.values():
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    def log(self, rid):
        return self.logs[rid].read_text(encoding="utf-8", errors="replace")

    def wait_for(self, predicate, description, rid):
        deadline = time.monotonic() + 12
        while not predicate():
            if self.processes[rid].poll() is not None:
                self.fail(f"{rid} exited before {description}:\n{self.log(rid)}")
            if time.monotonic() >= deadline:
                self.fail(f"Timed out before {description}:\n{self.log(rid)}")
            time.sleep(0.01)

    def wait_marker(self, kind, rid):
        self.wait_for(lambda: (self.control / kind / rid).exists(), f"{kind} marker", rid)

    def wait_blocked(self, rid):
        self.wait_marker("ingested", rid)
        self.wait_for(lambda: "[write lock] waiting" in self.log(rid), "write-lock wait", rid)
        self.assertFalse((self.control / "entered" / rid).exists(), self.log(rid))

    def release(self, kind, rid):
        (self.control / ("release-" + kind) / rid).touch()

    def finish(self, rid, status=0):
        self.assertEqual(self.processes[rid].wait(timeout=15), status, self.log(rid))

    def assert_locked(self):
        import fcntl
        with self.lock.open("rb") as handle:
            with self.assertRaises(BlockingIOError):
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def assert_unlocked(self):
        import fcntl
        with self.lock.open("rb") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def events(self):
        path = self.control / "events.jsonl"
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()] if path.exists() else []

    def test_final_writers_serialize_while_other_runs_finish_ingestion(self):
        self.start("run-holder", {"write_gate": True})
        self.wait_marker("entered", "run-holder")
        self.assert_locked()
        inode = self.lock.stat().st_ino
        self.start("run-waiter", {"write_gate": True})
        self.wait_blocked("run-waiter")
        self.assertFalse((self.out("run-waiter") / "message_summary.csv").exists())
        self.release("write", "run-holder")
        self.finish("run-holder")
        self.wait_marker("entered", "run-waiter")
        self.assert_locked()
        self.release("write", "run-waiter")
        self.finish("run-waiter")
        order = [(event["kind"], event["run"]) for event in self.events() if event["kind"] in ("entered", "left")]
        self.assertEqual(order, [("entered", "run-holder"), ("left", "run-holder"),
                                 ("entered", "run-waiter"), ("left", "run-waiter")])
        self.assertEqual(self.lock.stat().st_ino, inode)
        self.assert_unlocked()

    def test_lock_covers_database_close_and_successful_temporary_file_cleanup(self):
        self.start("run-holder", {"close_gate": True, "cleanup_gate": True})
        self.wait_marker("closing", "run-holder")
        self.assert_locked()
        self.start("run-waiter", {"write_gate": True})
        self.wait_blocked("run-waiter")
        self.release("close", "run-holder")
        self.wait_marker("cleaning", "run-holder")
        self.assert_locked()
        self.assertFalse((self.control / "entered" / "run-waiter").exists())
        self.release("cleanup", "run-holder")
        self.finish("run-holder")
        self.wait_marker("entered", "run-waiter")
        self.assertEqual(list(self.out("run-holder").glob("kpl-path-*.sqlite3*")), [])
        self.release("write", "run-waiter")
        self.finish("run-waiter")

    def test_file_generation_failure_releases_lock_for_next_run(self):
        self.start("run-holder", {"write_gate": True, "fail": True})
        self.wait_marker("entered", "run-holder")
        self.start("run-waiter")
        self.wait_blocked("run-waiter")
        self.release("write", "run-holder")
        self.finish("run-holder", 1)
        self.finish("run-waiter")
        self.assertIn("Synthetic file-generation failure", self.log("run-holder"))
        self.assertTrue(list(self.out("run-holder").glob("kpl-path-*.sqlite3")))
        self.assertTrue((self.out("run-waiter") / "manifest.json").is_file())
        self.assert_unlocked()

    def test_interrupting_waiter_does_not_release_holder_lock(self):
        self.start("run-holder", {"write_gate": True})
        self.wait_marker("entered", "run-holder")
        waiter = self.start("run-cancelled")
        self.wait_blocked("run-cancelled")
        waiter.send_signal(signal.SIGINT)
        self.finish("run-cancelled", 130)
        self.assert_locked()
        self.start("run-next")
        self.wait_blocked("run-next")
        self.release("write", "run-holder")
        self.finish("run-holder")
        self.finish("run-next")
        self.assertFalse((self.out("run-cancelled") / "manifest.json").exists())
        self.assertFalse((self.control / "entered" / "run-cancelled").exists())
        self.assert_unlocked()

    def test_interrupting_holder_releases_lock_for_waiter(self):
        holder = self.start("run-holder", {"write_gate": True})
        self.wait_marker("entered", "run-holder")
        self.start("run-waiter")
        self.wait_blocked("run-waiter")
        holder.send_signal(signal.SIGINT)
        self.finish("run-holder", 130)
        self.finish("run-waiter")
        self.assert_unlocked()

    def test_abrupt_holder_exit_releases_kernel_lock_without_deleting_lock_file(self):
        holder = self.start("run-holder", {"write_gate": True})
        self.wait_marker("entered", "run-holder")
        inode = self.lock.stat().st_ino
        self.start("run-waiter")
        self.wait_blocked("run-waiter")
        holder.kill()
        self.finish("run-holder", -signal.SIGKILL)
        self.finish("run-waiter")
        self.assertEqual(self.lock.stat().st_ino, inode)
        self.assert_unlocked()

    def test_direct_cli_locked_and_unlocked_outputs_have_identical_csv_contents(self):
        rid = "run-equivalence"
        source = self.source(rid)
        outputs = []
        for number, lock in enumerate((False, True)):
            out = self.root / f"equivalence-{number}"
            args = [sys.executable, str(EXTRACTOR), str(source), "--out", str(out), "--workers", "2", "--quiet"]
            if lock:
                args += ["--write-lock", str(self.lock)]
            result = subprocess.run(args, cwd=self.root, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            outputs.append(out)
        for name in sorted(self.fixture_module.CSV_FILES):
            with self.subTest(csv=name):
                values = [(out / name).read_bytes() for out in outputs]
                if name.endswith(".gz"):
                    values = [gzip.decompress(value) for value in values]
                self.assertEqual(values[0], values[1])
        manifests = [json.loads((out / "manifest.json").read_text(encoding="utf-8")) for out in outputs]
        for key in ("counts", "event_type_counts", "definitions", "sources", "warnings"):
            self.assertEqual(manifests[0][key], manifests[1][key], key)
        self.assert_unlocked()

    def test_list_sources_never_creates_output_or_lock_paths(self):
        rid = "run-list"
        lock = self.root / "missing lock parent" / "write.lock"
        result = subprocess.run([sys.executable, str(EXTRACTOR), *self.arguments(rid, lock=False),
                                 "--write-lock", str(lock), "--list-sources"],
                                cwd=self.root, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("[EVENTS]", result.stdout)
        self.assertFalse(lock.parent.exists())
        self.assertFalse(self.out(rid).exists())

    def test_existing_read_only_lock_file_is_reused_without_modifying_contents(self):
        rid = "run-read-only-lock"
        self.lock.write_bytes(b"preserved lock file\n")
        inode = self.lock.stat().st_ino
        self.lock.chmod(0o444)
        result = subprocess.run([sys.executable, str(EXTRACTOR), *self.arguments(rid), "--quiet"],
                                cwd=self.root, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.lock.read_bytes(), b"preserved lock file\n")
        self.assertEqual(self.lock.stat().st_ino, inode)
        self.assertEqual(self.lock.stat().st_mode & 0o777, 0o444)
        self.assertNotIn("[write lock]", result.stdout + result.stderr)
        self.assert_unlocked()

    def test_fifo_lock_path_is_rejected_without_blocking(self):
        rid = "run-fifo-lock"
        os.mkfifo(self.lock)
        result = subprocess.run([sys.executable, str(EXTRACTOR), *self.arguments(rid)],
                                cwd=self.root, capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("regular file", result.stderr)
        self.assertFalse((self.out(rid) / "message_summary.csv").exists())

    def test_symbolic_lock_file_is_rejected_without_modifying_target(self):
        rid = "run-symlink"
        sentinel = self.root / "preserved.txt"
        sentinel.write_bytes(b"preserve these bytes\n")
        self.lock.symlink_to(sentinel)
        result = subprocess.run([sys.executable, str(EXTRACTOR), *self.arguments(rid)],
                                cwd=self.root, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(sentinel.read_bytes(), b"preserve these bytes\n")
        self.assertFalse((self.out(rid) / "message_summary.csv").exists())


if __name__ == "__main__":
    unittest.main()
