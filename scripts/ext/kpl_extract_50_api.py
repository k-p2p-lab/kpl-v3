#!/usr/bin/env python3
"""K-P2PLab v3 Run ZIP downloader + per-Run first-reception path extractor.

A fix for NAS archive directories that contain .kpl-archive.json and segmented
logs: request the *complete single-run result* through the Controller's
supported authenticated API instead of guessing the NAS segment layout.

Python 3.10+, standard library only. Downloads one Run at a time to local disk.
"""
from __future__ import annotations

import argparse
import csv
import getpass
import http.cookiejar
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath


def cli():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--url', default=os.environ.get('KPL_URL', ''),
                   help='KPL Controller URL, e.g. http://control-node:8080 (or KPL_URL)')
    p.add_argument('--username', default=os.environ.get('KPL_USER', ''),
                   help='Dashboard login user; prompted if missing')
    p.add_argument('--cookie-jar', type=Path, default=None,
                   help='Optional preauthenticated curl/Netscape cookie jar; if unauthorized prompts login')
    p.add_argument('--manifest', type=Path,
                   default=Path(__file__).with_name('kpl_extract_50_manifest.csv'))
    p.add_argument('--extractor', type=Path,
                   default=Path('scripts/ext/kpl_path_extract.py'))
    p.add_argument('--out', type=Path, default=Path('./kpl-path-analysis-runs'))
    p.add_argument('--download-dir', type=Path, default=Path('./kpl-download-tmp'),
                   help='Local disk path with enough space for the largest Run ZIP')
    p.add_argument('--work-dir', type=Path, default=None,
                   help='Optional directory for extractor SQLite working storage')
    p.add_argument('--sudo', action='store_true', help='Run the extractor with sudo python3')
    p.add_argument('--python', default=sys.executable, help='Python used for extractor without --sudo')
    p.add_argument('--limit', type=int, default=0, help='Process first N entries (0 = all)')
    p.add_argument('--case', default='', help='Only run a case: e.g. d6-p1kb')
    p.add_argument('--dry-run', action='store_true', help='Print selections; do not log in or download')
    p.add_argument('--continue-on-error', action='store_true', help='Continue after a failed run')
    return p.parse_args()


def load_manifest(path: Path):
    if not path.is_file():
        raise RuntimeError(f'Manifest not found: {path}')
    with path.open(newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    required = {'payload_bytes', 'degree', 'seed', 'run_id', 'label'}
    if not rows:
        raise RuntimeError('Manifest has no entries')
    if any(not required.issubset(r) for r in rows):
        raise RuntimeError('Manifest missing required columns: ' + ', '.join(sorted(required)))
    seen = set()
    for r in rows:
        rid = r['run_id']
        if not re.fullmatch(r'(?:run-request-[0-9a-f]{64}|run-[A-Za-z0-9-]+)', rid):
            raise RuntimeError('Invalid run ID in manifest: ' + repr(rid))
        if rid in seen:
            raise RuntimeError('Duplicate Run ID in manifest: ' + rid)
        seen.add(rid)
        if not r['degree'].isdigit() or not r['payload_bytes'].isdigit():
            raise RuntimeError('Non-numeric degree/payload in manifest: ' + rid)
        if not re.fullmatch(r'[A-Za-z0-9._-]+', r['label']):
            raise RuntimeError('Unsafe label: ' + repr(r['label']))
    return rows


class Session:
    def __init__(self, base: str, username: str, cookie_jar: Path | None):
        self.base = base.rstrip('/')
        parsed = urllib.parse.urlsplit(self.base)
        if parsed.scheme not in {'http', 'https'} or not parsed.hostname:
            raise RuntimeError('Invalid Controller URL; use http://... or https://...')
        self.user = username
        self.password = None
        self.cookies = http.cookiejar.MozillaCookieJar()
        if cookie_jar is not None:
            self.cookies.load(str(cookie_jar.expanduser()), ignore_discard=True, ignore_expires=False)
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookies))

    def login(self):
        if not self.user:
            self.user = input('KPL username: ').strip()
        if not self.password:
            self.password = getpass.getpass('KPL password: ')
        body = json.dumps({'user': self.user, 'password': self.password}).encode('utf-8')
        req = urllib.request.Request(self.base + '/api/v1/auth/login', body,
                                     {'Content-Type': 'application/json'}, method='POST')
        try:
            with self.opener.open(req, timeout=60) as resp:
                resp.read(4096)
        except urllib.error.HTTPError as exc:
            explanation = exc.read(4096).decode('utf-8', 'replace')
            raise RuntimeError(f'KPL login failed (HTTP {exc.code}): {explanation}') from exc
        print('Authenticated to KPL Controller', flush=True)

    def get(self, path: str):
        req = urllib.request.Request(self.base + path, method='GET')
        try:
            return self.opener.open(req, timeout=120)
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                exc.close()
                self.login()
                return self.opener.open(req, timeout=120)
            explanation = exc.read(2048).decode('utf-8', 'replace')
            raise RuntimeError(f'KPL GET failed (HTTP {exc.code}): {explanation}') from exc

    def check(self):
        with self.get('/api/v1/results') as response:
            # We only need to establish authorization. Avoid caching all result metadata.
            response.read(1024)


def download_run(session: Session, rid: str, dest: Path):
    endpoint = '/api/v1/experiments/' + urllib.parse.quote(rid, safe='') + '/download'
    total = 0
    with session.get(endpoint) as response, dest.open('wb') as out:
        if response.status != 200:
            raise RuntimeError(f'{rid}: HTTP {response.status}')
        last_report = 0
        while True:
            chunk = response.read(4 * 1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
            total += len(chunk)
            if total - last_report >= 256 * 1024 * 1024:
                print(f'  downloaded {total / (1024**3):.2f} GiB', flush=True)
                last_report = total
    if total == 0:
        raise RuntimeError(f'{rid}: empty downloaded ZIP')
    try:
        with zipfile.ZipFile(dest) as zf:
            bad = zf.testzip() if total < 16 * 1024 * 1024 else None
            if bad:
                raise RuntimeError(f'ZIP CRC failure in {bad}')
            members = [name for name in zf.namelist()
                       if PurePosixPath(name).name in {'events.jsonl', 'events.jsonl.gz'}]
            if len(members) != 1:
                raise RuntimeError(f'{rid}: result ZIP has {len(members)} events.jsonl members, expected 1')
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f'{rid}: invalid downloaded ZIP') from exc
    print(f'  complete ZIP: {total / (1024**3):.2f} GiB', flush=True)


def main():
    args = cli()
    try:
        rows = load_manifest(args.manifest)
        if args.case:
            rows = [r for r in rows if args.case in r['label']]
        if args.limit:
            if args.limit < 0:
                raise RuntimeError('--limit must be nonnegative')
            rows = rows[:args.limit]
        if not rows:
            raise RuntimeError('No cases selected')
        print(f'Selected {len(rows)} Runs', flush=True)
        for i, r in enumerate(rows, 1):
            print(f"{i:2}/{len(rows)}: {r['label']}  {r['run_id']}", flush=True)
        if args.dry_run:
            return 0
        if not args.url:
            raise RuntimeError('Specify --url http://CONTROL:8080 or export KPL_URL')
        extractor = args.extractor.expanduser().resolve()
        if not extractor.is_file():
            raise RuntimeError(f'Extractor missing: {extractor}')
        args.download_dir = args.download_dir.expanduser().resolve()
        args.out = args.out.expanduser().resolve()
        args.download_dir.mkdir(parents=True, exist_ok=True)
        args.out.mkdir(parents=True, exist_ok=True)
        session = Session(args.url, args.username, args.cookie_jar)
        if args.cookie_jar is None:
            session.login()
        session.check()
        fails = 0
        finished = 0
        skipped = 0
        for i, row in enumerate(rows, 1):
            rid = row['run_id']
            out = (args.out / row['label'] / rid).resolve()
            if all((out / name).is_file() for name in ('hop_quality.csv', 'hop_timing.csv', 'message_summary.csv', 'manifest.json')):
                print(f'SKIP completed [{i}/{len(rows)}]: {out}', flush=True)
                skipped += 1
                continue
            if out.exists() and any(out.iterdir()):
                msg = f'Existing incomplete output directory: {out}; please move it aside before retrying'
                if not args.continue_on_error:
                    raise RuntimeError(msg)
                print('ERROR: ' + msg, file=sys.stderr, flush=True)
                fails += 1
                continue
            out.parent.mkdir(parents=True, exist_ok=True)
            print(f'EXTRACT [{i}/{len(rows)}] {row["label"]} {rid}', flush=True)
            try:
                with tempfile.TemporaryDirectory(prefix='kpl-single-run-', dir=args.download_dir) as td:
                    archive = Path(td) / f'{rid}.zip'
                    download_run(session, rid, archive)
                    if args.sudo:
                        cmd = ['sudo', 'python3']
                    else:
                        cmd = [args.python]
                    cmd += [str(extractor), str(archive), '--degree', row['degree'],
                            '--payload-bytes', row['payload_bytes'], '--label', row['label'],
                            '--run-id', rid, '--out', str(out)]
                    if args.work_dir is not None:
                        cmd += ['--work-dir', str(args.work_dir.expanduser().resolve())]
                    subprocess.run(cmd, check=True)
                if not all((out / name).is_file() for name in ('hop_quality.csv', 'hop_timing.csv', 'message_summary.csv', 'manifest.json')):
                    raise RuntimeError(f'Extractor finished but expected summary files are missing: {rid}')
                finished += 1
                print(f'OK: {out}', flush=True)
            except (OSError, urllib.error.URLError, subprocess.CalledProcessError,
                    RuntimeError, zipfile.BadZipFile) as exc:
                fails += 1
                print(f'ERROR for {rid}: {exc}', file=sys.stderr, flush=True)
                if not args.continue_on_error:
                    print('Stopping on first failure; previously completed outputs remain.', file=sys.stderr)
                    return 1
        print(f'Completed {finished}, already completed {skipped}, errors {fails}', flush=True)
        return 1 if fails else 0
    except (RuntimeError, OSError, urllib.error.URLError) as exc:
        print('ERROR: ' + str(exc), file=sys.stderr, flush=True)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
