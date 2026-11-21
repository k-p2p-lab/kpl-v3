#!/usr/bin/env python3
"""Process K-P2PLab v3 archived Run logs directly from mounted NAS.

No Controller HTTP or raw ZIP assembly. Runs have independent outputs.
Compatible with KPL v3 .kpl-archive.json logs object/segment manifest.
"""
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import BinaryIO


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--manifest', type=Path, default=Path(__file__).with_name('kpl_extract_manifest.csv'))
    p.add_argument('--extractor', type=Path, default=Path(__file__).with_name('kpl_path_extract.py'))
    p.add_argument('--nas-root', type=Path, default=Path('/mnt/nas/kpl/runs'))
    p.add_argument('--out', type=Path, default=Path('./kpl-path-analysis-runs'))
    p.add_argument('--work-dir', type=Path, default=None, help='Optional local SSD directory for temporary SQLite')
    p.add_argument('--write-lock', type=Path, help='Shared local result-generation lock (default: OUT/.kpl-write.lock; POSIX only)')
    p.add_argument('--jobs', type=int, default=0, help='Concurrent Runs (0=auto, up to 4; 1=sequential)')
    p.add_argument('--workers', type=int, default=0, help='Extraction workers per Run (0=auto, up to 4; 1=serial)')
    p.add_argument('--local-current-root', type=Path, default=None, help='Optional Controller current-run root for not-yet-archived tails')
    p.add_argument('--python', default=sys.executable, help='Python interpreter for local extraction')
    p.add_argument('--sudo', action='store_true', help='Run extractor with sudo python3')
    p.add_argument('--limit', type=int, default=0, help='Only first N selected Runs (0=all)')
    p.add_argument('--case', default='', help='Select case substring, e.g. d6-p50kb')
    p.add_argument('--dry-run', action='store_true', help='List commands without reading logs')
    p.add_argument('--list-sources', action='store_true', help='Show archive objects selected for each Run, without extraction')
    p.add_argument('--verify-archive-sha256', action='store_true', help='Slow optional integrity scan')
    p.add_argument('--no-rpc', action='store_true', help='Skip large RPC metadata processing')
    p.add_argument('--continue-on-error', action='store_true', help='Keep starting Runs after failures; otherwise finish only already running Runs')
    return p


def read_cases(file: Path):
    with file.open(newline='', encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))
    required = {'payload_bytes', 'degree', 'run_id', 'label'}
    if not rows or not all(required <= set(r) for r in rows):
        raise ValueError('Bad manifest schema or empty CSV')
    seen = set()
    for row in rows:
        rid = row['run_id']
        if not re.fullmatch(r'(?:run-request-[0-9a-f]{64}|run-[a-zA-Z0-9-]+)', rid):
            raise ValueError(f'Unsafe run ID: {rid}')
        if rid in seen:
            raise ValueError(f'Duplicate run ID: {rid}')
        seen.add(rid)
        if row['label'] in ('.', '..') or not re.fullmatch(r'[a-zA-Z0-9_.-]+', row['label']):
            raise ValueError(f'Unsafe label: {row["label"]}')
        if any(value is None or not re.fullmatch(r'[0-9]*', value)
               for value in (row['degree'], row['payload_bytes'])):
            raise ValueError('Bad degree/payload values')
    return rows


def finished(out: Path):
    required = ('manifest.json', 'hop_quality.csv', 'hop_timing.csv', 'message_summary.csv')
    if not all((out / x).is_file() for x in required):
        return False
    try:
        info = json.loads((out / 'manifest.json').read_text(encoding='utf-8'))
        return isinstance(info, dict) and info.get('tool') == 'kpl_path_extract' and info.get('version') == '1.1.0-nas'
    except (ValueError, OSError):
        return False


def available_cpus():
    try:
        return max(1, len(os.sched_getaffinity(0)))
    except (AttributeError, OSError):
        return max(1, os.cpu_count() or 1)


def concurrency(jobs, workers, total):
    cpus = available_cpus()
    jobs = min(total, jobs or min(4, cpus))
    if not workers and jobs > 1:
        workers = max(1, min(4, cpus // jobs))
    return jobs, workers


def command(args, row, extractor, src, dst, work_root, workers):
    cmd = (['sudo', '-n', 'python3'] if args.sudo else [args.python]) + [
        str(extractor), str(src),
        '--label', row['label'], '--out', str(dst), '--workers', str(workers),
    ]
    if row['degree']:
        cmd += ['--degree', row['degree']]
    if row['payload_bytes']:
        cmd += ['--payload-bytes', row['payload_bytes']]
    if args.local_current_root:
        cmd += ['--local-current-root', str(args.local_current_root.expanduser().resolve())]
    if args.verify_archive_sha256:
        cmd += ['--verify-archive-sha256']
    if args.no_rpc:
        cmd += ['--no-rpc']
    if work_root:
        cmd += ['--work-dir', str(work_root)]
    if args.list_sources:
        cmd += ['--list-sources']
    else:
        lock = args.write_lock or args.out.expanduser().resolve() / '.kpl-write.lock'
        cmd += ['--write-lock', str(lock.expanduser().absolute())]
    return cmd


@dataclass
class ActiveRun:
    process: subprocess.Popen
    name: str
    log_path: Path
    log: BinaryIO
    offset: int


def process_group_options():
    if os.name != 'posix':
        return {}
    # Keep the controlling terminal and sudo ticket, but isolate Ctrl-C delivery.
    if sys.version_info >= (3, 11):
        return {'process_group': 0}
    return {'preexec_fn': os.setpgrp}


def stop_runs(active):
    # The extractor handles SIGINT by stopping and joining its worker pools.
    # Signal only its parent (or sudo monitor), avoiding repeated worker interrupts.
    for run in active:
        if run.process.poll() is None and os.name == 'posix':
            try:
                run.process.send_signal(signal.SIGINT)
            except ProcessLookupError:
                pass
    for run in active:
        try:
            run.process.wait()
        finally:
            run.log.close()


def authenticate_sudo(stop_requested):
    process = subprocess.Popen(['sudo', '-v'])
    try:
        while process.poll() is None:
            if stop_requested():
                return False
            time.sleep(0.05)
        return process.returncode == 0
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        process.wait()


def open_run_log(out_root, rid):
    log_root = out_root / '.logs'
    if log_root.is_symlink():
        raise ValueError(f'Execution log directory must not be a symlink: {log_root}')
    log_root.mkdir(parents=True, exist_ok=True)
    path = log_root / f'{rid}.log'
    if path.is_symlink():
        raise ValueError(f'Execution log must not be a symlink: {path}')
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, 'O_NOFOLLOW', 0)
    return path, os.fdopen(os.open(path, flags, 0o666), 'ab')


def main(argv=None):
    args = parser().parse_args(argv)
    if args.limit < 0:
        raise ValueError('--limit must be >= 0')
    if args.workers < 0:
        raise ValueError('--workers must be >= 0')
    if args.jobs < 0:
        raise ValueError('--jobs must be >= 0')
    entries = read_cases(args.manifest.expanduser().resolve())
    if args.case:
        entries = [r for r in entries if args.case in r['label']]
    if args.limit:
        entries = entries[:args.limit]
    if not entries:
        raise ValueError('No Runs selected')
    extractor = args.extractor.expanduser().resolve()
    if not extractor.is_file():
        raise ValueError(f'Extractor not found: {extractor}')
    nas_root = args.nas_root.expanduser().resolve()
    out_root = args.out.expanduser().resolve()
    work_root = args.work_dir.expanduser().resolve() if args.work_dir else None
    destinations = set()
    for row in entries:
        dst = (out_root / row['label'] / row['run_id']).resolve()
        if not dst.is_relative_to(out_root) or any(
            dst == other or dst in other.parents or other in dst.parents
            for other in destinations
        ):
            raise ValueError(f'Unsafe or overlapping output directory: {dst}')
        destinations.add(dst)
    if not args.list_sources:
        lock = (args.write_lock or out_root / '.kpl-write.lock').expanduser().resolve()
        if any(lock == dst or lock in dst.parents or dst in lock.parents for dst in destinations):
            raise ValueError(f'Write lock must be outside per-Run output directories: {lock}')
    jobs, workers = concurrency(args.jobs, args.workers, len(entries))
    print(f'Concurrent Runs={jobs}, extraction workers/Run={workers or "auto (up to 4)"}', flush=True)
    if not args.dry_run:
        out_root.mkdir(parents=True, exist_ok=True)
        if work_root:
            work_root.mkdir(parents=True, exist_ok=True)
    done = skipped = errors = 0
    total = len(entries)
    if args.dry_run:
        for i, row in enumerate(entries, 1):
            src, dst = nas_root / row['run_id'], out_root / row['label'] / row['run_id']
            print(f'[{i}/{total}] {row["label"]} {row["run_id"]}', flush=True)
            print('  ' + shlex.join(command(args, row, extractor, src, dst, work_root, workers)), flush=True)
        print(f'Processed=0, already_done=0, errors=0, selected={total}', flush=True)
        return 0

    active = []
    next_index = 0
    stopped = 0
    handlers = {}

    def request_stop(signum, _frame):
        nonlocal stopped
        if not stopped:
            stopped = signum

    def reap_completed():
        nonlocal done, errors
        for run in active[:]:
            status = run.process.poll()
            if status is None:
                continue
            run.log.close()
            active.remove(run)
            if args.list_sources:
                print(f'{run.name} selected sources:', flush=True)
                with run.log_path.open(encoding='utf-8', errors='replace') as log:
                    log.seek(run.offset)
                    shutil.copyfileobj(log, sys.stdout)
            if status:
                errors += 1
                print(f'{run.name} ERROR: exit status {status}; log: {run.log_path}', flush=True)
            else:
                done += 1
                print(f'{run.name} OK; log: {run.log_path}', flush=True)

    try:
        signals = [signal.SIGINT]
        if os.name == 'posix':
            signals += [signal.SIGTERM, signal.SIGHUP]
        for signum in signals:
            handlers[signum] = signal.signal(signum, request_stop)
        while next_index < total or active:
            reap_completed()
            if stopped:
                break
            if errors and not args.continue_on_error:
                if not active:
                    break
            elif next_index < total and len(active) < jobs:
                row = entries[next_index]
                next_index += 1
                rid, label = row['run_id'], row['label']
                src, dst = nas_root / rid, out_root / label / rid
                name = f'[{next_index}/{total}] {label} {rid}'
                if finished(dst) and not args.list_sources:
                    skipped += 1
                    print(f'{name} SKIP: extraction completed previously', flush=True)
                    continue
                try:
                    if not src.is_dir():
                        raise ValueError(f'NAS Run directory does not exist: {src}')
                    if dst.exists() and not args.list_sources and (not dst.is_dir() or any(dst.iterdir())):
                        raise ValueError(f'partial or older extraction at {dst}; rename/delete this output directory manually')
                    if args.sudo:
                        # Authenticate in the foreground; children must never prompt concurrently.
                        if not authenticate_sudo(lambda: bool(stopped)):
                            raise ValueError('sudo authentication failed')
                    reap_completed()
                    if stopped:
                        break
                    if errors and not args.continue_on_error:
                        continue
                    log_path, log = open_run_log(out_root, rid)
                    try:
                        log.write(f'\n--- {datetime.now(timezone.utc).isoformat()} {name} ---\n'.encode())
                        log.flush()
                        offset = log.tell()
                        cmd = command(args, row, extractor, src, dst, work_root, workers)
                        process = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=log,
                                                   stderr=subprocess.STDOUT, **process_group_options())
                    except BaseException:
                        log.close()
                        raise
                    active.append(ActiveRun(process, name, log_path, log, offset))
                    print(f'{name} START; log: {log_path}', flush=True)
                except (OSError, ValueError) as exc:
                    errors += 1
                    print(f'{name} ERROR: {exc}', flush=True)
                continue
            if active:
                time.sleep(0.05)
    finally:
        try:
            if active:
                print('Stopping active extractions; waiting for worker cleanup...', flush=True)
                stop_runs(active)
        finally:
            for signum, handler in handlers.items():
                signal.signal(signum, handler)
    print(f'Processed={done}, already_done={skipped}, errors={errors}, selected={total}', flush=True)
    if stopped:
        return 128 + stopped
    return 1 if errors else 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError) as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(2)
