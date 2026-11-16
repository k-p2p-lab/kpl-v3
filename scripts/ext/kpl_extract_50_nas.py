#!/usr/bin/env python3
"""Process K-P2PLab v3 archived Run logs directly from mounted NAS.

No Controller HTTP, no raw ZIP assembly, one Run processed at a time.
Compatible with KPL v3 .kpl-archive.json logs object/segment manifest.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from pathlib import Path


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--manifest', type=Path, default=Path(__file__).with_name('kpl_extract_50_manifest.csv'))
    p.add_argument('--extractor', type=Path, default=Path(__file__).with_name('kpl_path_extract.py'))
    p.add_argument('--nas-root', type=Path, default=Path('/mnt/nas/kpl/runs'))
    p.add_argument('--out', type=Path, default=Path('./kpl-path-analysis-runs'))
    p.add_argument('--work-dir', type=Path, default=None, help='Optional local SSD directory for temporary SQLite')
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
    p.add_argument('--continue-on-error', action='store_true', help='Attempt next Run even if this Run fails')
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
        if not re.fullmatch(r'[a-zA-Z0-9_.-]+', row['label']):
            raise ValueError(f'Unsafe label: {row["label"]}')
        if not row['degree'].isdigit() or not row['payload_bytes'].isdigit():
            raise ValueError('Bad degree/payload values')
    return rows


def finished(out: Path):
    required = ('manifest.json', 'hop_quality.csv', 'hop_timing.csv', 'message_summary.csv')
    if not all((out / x).is_file() for x in required):
        return False
    try:
        info = json.loads((out / 'manifest.json').read_text(encoding='utf-8'))
        return info.get('tool') == 'kpl_path_extract' and info.get('version') == '1.1.0-nas'
    except (ValueError, OSError):
        return False


def main():
    args = parser().parse_args()
    if args.limit < 0:
        raise ValueError('--limit must be >= 0')
    if args.workers < 0:
        raise ValueError('--workers must be >= 0')
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
    if not args.dry_run:
        out_root.mkdir(parents=True, exist_ok=True)
        if work_root:
            work_root.mkdir(parents=True, exist_ok=True)
    done = skipped = errors = 0
    total = len(entries)
    for i, row in enumerate(entries, 1):
        rid, label = row['run_id'], row['label']
        src = nas_root / rid
        dst = out_root / label / rid
        print(f'[{i}/{total}] {label} {rid}', flush=True)
        if not args.dry_run and finished(dst) and not args.list_sources:
            print('  SKIP: extraction completed previously', flush=True)
            skipped += 1
            continue
        cmd = (['sudo', 'python3'] if args.sudo else [args.python]) + [
            str(extractor), str(src), '--degree', row['degree'], '--payload-bytes', row['payload_bytes'],
            '--label', label, '--out', str(dst), '--workers', str(args.workers),
        ]
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
        if args.dry_run:
            import shlex
            print('  ' + shlex.join(cmd), flush=True)
            continue
        if not src.is_dir():
            print(f'  ERROR: NAS Run directory does not exist: {src}', flush=True)
            errors += 1
            if not args.continue_on_error:
                break
            continue
        if dst.is_dir() and any(dst.iterdir()) and not args.list_sources:
            print(f'  ERROR: partial or older extraction at {dst}; rename/delete this output directory manually', flush=True)
            errors += 1
            if not args.continue_on_error:
                break
            continue
        result = subprocess.run(cmd, check=False)
        if result.returncode:
            print(f'  ERROR: exit status {result.returncode}', flush=True)
            errors += 1
            if not args.continue_on_error:
                break
        else:
            print('  OK', flush=True)
            done += 1
    print(f'Processed={done}, already_done={skipped}, errors={errors}, selected={total}', flush=True)
    return 1 if errors else 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError) as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(2)
