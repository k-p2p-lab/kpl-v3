#!/usr/bin/env python3
"""K-P2PLab v3: disk-backed first-reception path diagnostics.

Python >=3.10, standard library only. One Run per invocation.
Inputs: a complete Run directory, a Run results ZIP, or events.jsonl[.gz].
No network access, no payload bodies, no changes to input files.
See README.md for source references and measurement limitations.
"""
from __future__ import annotations

import argparse
import calendar
import csv
import gzip
import io
import json
import math
import os
import re
import sqlite3
import sys
import tempfile
import time
import zipfile
from collections import Counter, defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterator

VERSION = "1.0.0"
CLOCK_BASIS = "controller-offset-v1"
NS_PER_MS = 1_000_000
TIMESTAMP_RE = re.compile(
    r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)$"
)
SOURCES = [
    "https://github.com/k-p2p-lab/kpl-v3/wiki/experiment_metrics",
    "https://github.com/k-p2p-lab/kpl-v3/wiki/results",
    "https://github.com/k-p2p-lab/kpl-v3/wiki/api#detailed-peer-logs",
    "https://github.com/k-p2p-lab/kpl-v3/blob/master/internal/model/model.go",
    "https://github.com/k-p2p-lab/kpl-v3/blob/master/internal/peer/publish.go",
    "https://github.com/k-p2p-lab/kpl-v3/blob/master/internal/peer/telemetry.go",
    "https://github.com/k-p2p-lab/kpl-v3/blob/master/internal/peer/tracer_metadata.go",
    "https://github.com/k-p2p-lab/kpl-v3/blob/master/internal/controller/run_metrics.go",
    "https://github.com/k-p2p-lab/kpl-v3/blob/master/internal/controller/analysis_research_messages.go",
    "https://github.com/k-p2p-lab/kpl-v3/blob/master/internal/controller/analysis_observations.go",
]


def number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        x = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return x if math.isfinite(x) else None


def nonnegative(value: Any) -> float | None:
    x = number(value)
    return x if x is not None and x >= 0 else None


def text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def timestamp_ns(value: Any) -> int | None:
    """Parse Go time.Time RFC3339Nano without float epoch/microsecond loss."""
    if not isinstance(value, str) or value.startswith("0001-"):
        return None
    match = TIMESTAMP_RE.fullmatch(value)
    if not match:
        return None
    sec, fraction, zone = match.groups()
    try:
        whole = datetime.fromisoformat(sec + ("+00:00" if zone == "Z" else zone))
        epoch_sec = calendar.timegm(whole.utctimetuple())
    except (ValueError, OverflowError):
        return None
    return epoch_sec * 1_000_000_000 + int((fraction or "").ljust(9, "0") or "0")


def iso_ns(value: int | None) -> str:
    if value is None:
        return ""
    sec, ns = divmod(int(value), 1_000_000_000)
    return datetime.fromtimestamp(sec, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + f".{ns:09d}Z"


def ratio(a: int | float, b: int | float) -> float | None:
    return a / b if b else None


@dataclass(frozen=True)
class Source:
    path: Path
    member: str | None = None

    @property
    def name(self) -> str:
        return str(self.path) + (f"!{self.member}" if self.member else "")

    @contextmanager
    def open(self):
        if self.member is not None:
            with zipfile.ZipFile(self.path) as archive:
                with archive.open(self.member) as stream:
                    if self.member.endswith(".gz"):
                        with gzip.GzipFile(fileobj=stream) as zipped:
                            yield zipped
                    else:
                        yield stream
        elif self.path.suffix == ".gz":
            with gzip.open(self.path, "rb") as stream:
                yield stream
        else:
            with self.path.open("rb", buffering=1 << 20) as stream:
                yield stream


def locate_inputs(args: argparse.Namespace) -> tuple[list[Source], list[Source], dict]:
    path = Path(args.input).expanduser().resolve()
    if not path.exists():
        raise ValueError(f"Input file or directory does not exist: {path}")
    events: list[Source] = []
    observations: list[Source] = []
    metadata: dict = {}

    def read_small(source: Source) -> dict:
        with source.open() as stream:
            body = stream.read(32 * 1024 * 1024 + 1)
        if len(body) > 32 * 1024 * 1024:
            raise ValueError(f"Metadata exceeds 32 MiB: {source.name}")
        obj = json.loads(body)
        if not isinstance(obj, dict):
            raise ValueError(f"Not a JSON object: {source.name}")
        return obj

    if path.is_dir():
        # A NAS archive directory can hold only an early segment. Do not guess
        # the private segment/manifest layout or silently concatenate duplicates.
        if (path / ".kpl-archive.json").exists():
            raise ValueError(
                "This is a NAS directory with an archive manifest. Use a complete Run results ZIP, or "
                "provide a verified complete events.jsonl and specify additional segments with --events-extra."
            )
        for stem, dest in (("events.jsonl", events), ("observations.jsonl", observations)):
            options = [path / stem, path / (stem + ".gz")]
            existing = [p for p in options if p.is_file()]
            if len(existing) > 1:
                raise ValueError(f"{stem} and its .gz version are both present. Specify one file.")
            if existing:
                dest.append(Source(existing[0]))
        if (path / "experiment.json").is_file():
            metadata = read_small(Source(path / "experiment.json"))
    elif path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
        ev_names = [n for n in names if PurePosixPath(n).name in ("events.jsonl", "events.jsonl.gz")]
        if len(ev_names) != 1:
            raise ValueError(
                f"This ZIP contains {len(ev_names)} events.jsonl[.gz] files. "
                "Use a single Run results ZIP instead of an analysis/CSV ZIP."
            )
        name = ev_names[0]
        prefix = str(PurePosixPath(name).parent)
        prefix = "" if prefix == "." else prefix + "/"
        events.append(Source(path, name))
        for stem in ("observations.jsonl", "observations.jsonl.gz"):
            if prefix + stem in names:
                observations.append(Source(path, prefix + stem))
        if len(observations) > 1:
            raise ValueError("The ZIP contains both observations.jsonl and observations.jsonl.gz.")
        if prefix + "experiment.json" in names:
            metadata = read_small(Source(path, prefix + "experiment.json"))
    else:
        events.append(Source(path))

    for value in args.events_extra:
        p = Path(value).expanduser().resolve()
        if not p.is_file():
            raise ValueError(f"Additional event file does not exist: {p}")
        events.append(Source(p))
    if args.observations is not None:
        observations = []
        for value in args.observations:
            p = Path(value).expanduser().resolve()
            if not p.is_file():
                raise ValueError(f"Observation file does not exist: {p}")
            observations.append(Source(p))
    if not events:
        raise ValueError("Could not find events.jsonl or events.jsonl.gz.")
    if len(set(s.name for s in events)) != len(events):
        raise ValueError("The same event file was specified more than once.")
    return events, observations, metadata


SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = FILE;
PRAGMA cache_size = -65536;
CREATE TABLE seen_seq(node TEXT, session TEXT, seq INTEGER,
 PRIMARY KEY(node,session,seq)) WITHOUT ROWID;
CREATE TABLE seen_id(node TEXT, eid TEXT, PRIMARY KEY(node,eid)) WITHOUT ROWID;
CREATE TABLE identities(peer TEXT, node TEXT, session TEXT, agent TEXT, grp TEXT,
 PRIMARY KEY(peer,node,session,agent,grp)) WITHOUT ROWID;
CREATE TABLE life(node TEXT, session TEXT, kind TEXT, ts INTEGER, topics TEXT);
CREATE TABLE groups_evidence(node TEXT, grp TEXT, PRIMARY KEY(node,grp)) WITHOUT ROWID;
CREATE INDEX life_session ON life(node,session);
CREATE TABLE pubs(id INTEGER PRIMARY KEY, topic TEXT, mid TEXT, node TEXT, peer TEXT,
 session TEXT, agent TEXT, ts INTEGER, clock TEXT, unc REAL, wire_id TEXT,
 encoding TEXT, payload INTEGER, wire INTEGER, window TEXT);
CREATE INDEX pub_message ON pubs(topic,mid);
CREATE TABLE deliveries(id INTEGER PRIMARY KEY, topic TEXT, mid TEXT, node TEXT,
 peer TEXT, session TEXT, agent TEXT, grp TEXT, remote TEXT, ts INTEGER,
 clock TEXT, unc REAL, latency REAL, latency_available INTEGER,
 latency_sync INTEGER, latency_unc REAL, wire_id TEXT,
 encoding TEXT, payload INTEGER, wire INTEGER);
CREATE INDEX delivery_message ON deliveries(topic,mid,node,ts,id);
CREATE TABLE dup_edges(topic TEXT, mid TEXT, node TEXT, session TEXT, peer TEXT,
 remote TEXT, tmin INTEGER, tmax INTEGER, n INTEGER,
 PRIMARY KEY(topic,mid,node,session,peer,remote)) WITHOUT ROWID;
CREATE TABLE rpc(id INTEGER PRIMARY KEY, topic TEXT, wid TEXT, direction TEXT,
 node TEXT, session TEXT, peer TEXT, remote TEXT, ts INTEGER,
 clock TEXT, unc REAL, oid TEXT);
CREATE INDEX rpc_message ON rpc(topic,wid);
CREATE TABLE snapshots(ts INTEGER PRIMARY KEY, data TEXT, conflict INTEGER DEFAULT 0);
CREATE TABLE detail(
 topic TEXT, mid TEXT, node TEXT, grp TEXT, payload INTEGER, wire INTEGER,
 encoding TEXT, parent TEXT, hop INTEGER, hop_status TEXT, identity_safe INTEGER,
 latency REAL, latency_basis TEXT, latency_raw REAL,
 parent_delta_raw REAL, parent_delta REAL, parent_clock TEXT,
 parent_delta_unc REAL, parent_delta_lower REAL, parent_delta_upper REAL,
 shortest INTEGER, snapshot_ratio REAL, snapshot_age REAL,
 path_in_snapshot INTEGER, graph_nodes INTEGER, graph_degree REAL,
 recv_to_app REAL, parent_to_send REAL, send_to_recv REAL,
 send_count INTEGER, recv_count INTEGER, rpc_status TEXT,
 receiver_ts INTEGER, parent_ts INTEGER, receipt_sessions INTEGER,
 receiver_peer TEXT, parent_peer TEXT, receiver_session TEXT, parent_session TEXT,
 node_tree_hop INTEGER, receiver_in_snapshot INTEGER, parent_edge_in_snapshot INTEGER,
 session_start_minus_publish REAL, receiver_agent TEXT, parent_agent TEXT, same_agent INTEGER
);
CREATE INDEX detail_group ON detail(topic,payload,encoding,grp,hop);
"""


class Extractor:
    def __init__(self, args: argparse.Namespace, db_path: Path):
        self.args = args
        self.db = sqlite3.connect(db_path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self.run_id = args.run_id or ""
        self.explicit_run = bool(args.run_id)
        self.counts = Counter()
        self.types = Counter()
        self.controls = defaultdict(Counter)
        self.identity_cache: set[tuple] = set()
        self.source_stats: list[dict] = []
        self.warnings: list[str] = []
        self.metadata: dict = {}
        self.publication_topics: set[str] = set()
        self.subscription_topics: set[str] = set()
        self.last_progress = time.monotonic()

    def progress(self, phase: str, force: bool = False):
        now = time.monotonic()
        if not self.args.quiet and (force or now - self.last_progress > 5):
            print(f"[{phase}] lines={self.counts['input_lines']:,} "
                  f"events={self.counts['unique_events']:,} "
                  f"deliver={self.types['deliver']:,}", file=sys.stderr, flush=True)
            self.last_progress = now

    def objects(self, sources: list[Source], kind: str) -> Iterator[dict]:
        limit = self.args.max_line_mib * 1024 * 1024
        for source in sources:
            info = {"source": source.name, "kind": kind, "lines": 0, "bytes": 0, "bad_lines": 0}
            self.source_stats.append(info)
            with source.open() as stream:
                while True:
                    line = stream.readline(limit + 1)
                    if not line:
                        break
                    info["lines"] += 1
                    info["bytes"] += len(line)
                    self.counts["input_lines"] += 1
                    if len(line) > limit:
                        raise ValueError(f"{source.name}:{info['lines']}: line exceeds {self.args.max_line_mib} MiB")
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line)
                        if not isinstance(obj, dict):
                            raise ValueError("JSON object required")
                    except (ValueError, UnicodeDecodeError) as exc:
                        if not self.args.skip_bad_lines:
                            raise ValueError(f"{source.name}:{info['lines']}: {exc}") from exc
                        info["bad_lines"] += 1
                        self.counts["bad_lines"] += 1
                        continue
                    yield obj
                    self.progress(kind)

    def accept_run(self, obj: dict) -> bool:
        run = text(obj.get("runId"))
        if not run:
            self.counts["records_missing_run_id"] += 1
            return False
        if not self.run_id:
            self.run_id = run
        if run != self.run_id:
            if self.explicit_run:
                self.counts["other_run_records_skipped"] += 1
                return False
            raise ValueError(f"Multiple Runs are mixed: {self.run_id}, {run}. Select one with --run-id.")
        return True

    def unique(self, e: dict) -> bool:
        node, session = text(e.get("nodeId")), text(e.get("sessionId"))
        seq = e.get("sequence")
        if session and isinstance(seq, int) and not isinstance(seq, bool) and 0 < seq <= 2**63 - 1:
            cur = self.db.execute("INSERT OR IGNORE INTO seen_seq VALUES(?,?,?)", (node, session, seq))
        elif text(e.get("eventId")):
            cur = self.db.execute("INSERT OR IGNORE INTO seen_id VALUES(?,?)", (node, e["eventId"]))
        else:
            self.counts["events_without_dedup_identity"] += 1
            return True
        if cur.rowcount == 0:
            self.counts["telemetry_replays_skipped"] += 1
            return False
        return True

    def ingest_events(self, sources: list[Source]):
        for e in self.objects(sources, "events"):
            if not self.accept_run(e) or not self.unique(e):
                continue
            self.counts["unique_events"] += 1
            kind = text(e.get("type"))
            self.types[kind or "<missing>"] += 1
            fields = e.get("fields") if isinstance(e.get("fields"), dict) else {}
            node, peer = text(e.get("nodeId")), text(e.get("peerId"))
            session, agent = text(e.get("sessionId")), text(e.get("agentId"))
            grp, remote = text(e.get("group")), text(e.get("remotePeerId"))
            topic, mid = text(e.get("topic")), text(e.get("messageId"))
            identity = (peer, node, session, agent, grp)
            if peer and node and identity not in self.identity_cache:
                self.db.execute("INSERT OR IGNORE INTO identities VALUES(?,?,?,?,?)", identity)
                if len(self.identity_cache) >= 100_000:
                    self.identity_cache.clear()
                self.identity_cache.add(identity)
            ts = timestamp_ns(e.get("timestamp"))
            clock = text(fields.get("clockBasis"))
            unc = nonnegative(fields.get("clockUncertaintyMs"))
            if kind == "telemetry_drop":
                self.counts["reported_telemetry_drops"] += nonnegative(fields.get("count")) or 0
            if kind.startswith(("send_", "recv_", "drop_")):
                ctl = self.controls[kind]
                ctl["rpc_records"] += 1
                ctl["entries"] += nonnegative(fields.get("controlEntries")) or 0
                ctl["id_references"] += nonnegative(fields.get("messageIdCount")) or 0
                ctl["truncated_records"] += fields.get("messageIdsComplete") is False
                ctl["omitted_ids"] += nonnegative(fields.get("omittedMessageIds")) or 0
            if ts is None:
                self.counts["events_missing_timestamp"] += 1
                if kind in ("publish", "deliver", "duplicate", "rpc_metadata"):
                    self.counts["message_records_missing_timestamp"] += 1
            elif kind in ("measurement_start", "measurement_checkpoint", "measurement_stop", "measurement_terminated"):
                topics = fields.get("subscribedTopics", [])
                if not isinstance(topics, list):
                    topics = []
                self.subscription_topics.update(t for t in topics if isinstance(t, str))
                self.db.execute("INSERT INTO life VALUES(?,?,?,?,?)", (node, session, kind, ts, json.dumps(topics)))
            elif kind in ("publish", "deliver"):
                if not mid or not node:
                    self.counts["message_records_missing_identity"] += 1
                    continue
                encoding = text(fields.get("payloadEncoding"))
                payload = nonnegative(fields.get("payloadBytes"))
                wire = nonnegative(fields.get("wireBytes"))
                wid = text(fields.get("pubsubMessageId"))
                if kind == "publish":
                    self.publication_topics.add(topic)
                    self.db.execute(
                        "INSERT INTO pubs(topic,mid,node,peer,session,agent,ts,clock,unc,wire_id,encoding,payload,wire,window) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (topic, mid, node, peer, session, agent, ts, clock, unc, wid, encoding,
                         payload, wire, text(fields.get("deliveryWindow"))))
                elif fields.get("localDelivery") is True:
                    self.counts["local_deliveries_skipped"] += 1
                else:
                    self.db.execute(
                        "INSERT INTO deliveries(topic,mid,node,peer,session,agent,grp,remote,ts,clock,unc,latency,"
                        "latency_available,latency_sync,latency_unc,wire_id,encoding,payload,wire) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (topic, mid, node, peer, session, agent, grp, remote, ts, clock, unc,
                         number(e.get("latencyMs", 0)), int(fields.get("latencyAvailable") is True),
                         int(fields.get("latencyClockSynchronized") is True),
                         nonnegative(fields.get("latencyUncertaintyMs")), wid, encoding, payload, wire))
            elif kind == "duplicate":
                if fields.get("localDelivery") is True or not mid or not node:
                    continue
                self.db.execute("""INSERT INTO dup_edges VALUES(?,?,?,?,?,?,?,?,1)
                    ON CONFLICT(topic,mid,node,session,peer,remote) DO UPDATE SET
                    tmin=min(tmin,excluded.tmin),tmax=max(tmax,excluded.tmax),n=n+1""",
                    (topic, mid, node, session, peer, remote, ts, ts))
            elif kind == "rpc_metadata":
                self.counts["rpc_metadata_truncated"] += fields.get("messageIdsComplete") is False
                self.counts["rpc_metadata_omitted_ids"] += nonnegative(fields.get("omittedMessageIds")) or 0
                if self.args.no_rpc:
                    continue
                direction = text(fields.get("direction"))
                messages = fields.get("messages", [])
                if direction not in ("send", "recv", "drop") or not isinstance(messages, list):
                    continue
                # A callback can list an ID more than once; one occurrence is
                # sufficient for correlating that callback with this message.
                listed: set[tuple[str, str]] = set()
                for m in messages:
                    if isinstance(m, dict):
                        t, w = text(m.get("topic")), text(m.get("pubsubMessageId"))
                        if w:
                            listed.add((t, w))
                self.db.executemany("INSERT INTO rpc(topic,wid,direction,node,session,peer,remote,ts,clock,unc,oid) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    [(t,w,direction,node,session,peer,remote,ts,clock,unc,text(fields.get("rpcObservationId")))
                     for t,w in listed])
            if self.counts["unique_events"] % 10000 == 0:
                self.db.commit()
        self.db.commit()
        self.progress("events complete", True)
        if not self.run_id:
            raise ValueError("No TraceEvent with a runId was found. Verify that the input is an original events.jsonl file.")
        self.db.executescript("""
        CREATE TABLE firsts AS
        SELECT * FROM (
          SELECT d.*,ROW_NUMBER() OVER (PARTITION BY topic,mid,node ORDER BY ts,id) AS rn,
                 COUNT(*) OVER (PARTITION BY topic,mid,node) AS receipt_events
          FROM deliveries d
        ) WHERE rn=1;
        CREATE UNIQUE INDEX first_message_node ON firsts(topic,mid,node);
        CREATE TABLE session_counts AS
          SELECT topic,mid,node,COUNT(DISTINCT session) AS n
          FROM deliveries GROUP BY topic,mid,node;
        CREATE UNIQUE INDEX session_counts_key ON session_counts(topic,mid,node);
        """)
        self.db.commit()

    def ingest_observations(self, sources: list[Source]):
        for obj in self.objects(sources, "observations"):
            if not self.accept_run(obj):
                continue
            ts = timestamp_ns(obj.get("at"))
            if ts is None:
                self.counts["observations_missing_timestamp"] += 1
                continue
            candidates = [g for g in obj.get("graphs", [])
                          if isinstance(g, dict) and g.get("protocol") == "gossipsub"]
            if len(candidates) != 1:
                self.counts["observations_without_one_gossipsub_graph"] += 1
                continue
            g = candidates[0]
            nodes, edges = g.get("nodes"), g.get("edges")
            if not isinstance(nodes, list) or not all(isinstance(n, str) and n for n in nodes) or len(set(nodes)) != len(nodes) or not isinstance(edges, list):
                raise ValueError("observations.jsonl: unexpected graphs[].nodes/edges schema.")
            clean: set[tuple[int, int]] = set()
            for edge in edges:
                if not isinstance(edge, list) or len(edge) != 2 or any(isinstance(i, bool) or not isinstance(i, int) for i in edge):
                    raise ValueError("observations.jsonl: each edge must be a pair of integer node indices.")
                a, b = edge
                if not (0 <= a < len(nodes) and 0 <= b < len(nodes)):
                    raise ValueError("observations.jsonl: edge indices are outside the nodes range.")
                if a != b:
                    clean.add(tuple(sorted((a,b))))
            groups = g.get("groups", [])
            if not isinstance(groups, list) or len(groups) != len(nodes):
                groups = [""] * len(nodes)
            self.db.executemany("INSERT OR IGNORE INTO groups_evidence VALUES(?,?)",
                [(node,grp) for node,grp in zip(nodes,groups) if isinstance(grp,str) and grp])
            data = json.dumps({"nodes": nodes, "edges": sorted(clean), "groups": groups}, separators=(",", ":"))
            self.db.execute("""INSERT INTO snapshots VALUES(?,?,0)
                ON CONFLICT(ts) DO UPDATE SET conflict=CASE WHEN data!=excluded.data THEN 1 ELSE conflict END""", (ts,data))
            self.counts["gossipsub_snapshots"] += 1
        self.db.commit()
        self.progress("observations complete", True)

    def graph_at(self, when: int | None, publisher: str):
        if when is None:
            return {}, {}, {"status": "no_publication"}
        row = self.db.execute("SELECT * FROM snapshots WHERE ts<=? ORDER BY ts DESC LIMIT 1", (when,)).fetchone()
        if row is None:
            return {}, {}, {"status": "no_preceding_snapshot"}
        age = (when-row["ts"])/1e9
        info = {"at": iso_ns(row["ts"]), "age_seconds": age, "status": "ok"}
        if row["conflict"]:
            info["status"] = "conflicting_snapshot"
            return {}, {}, info
        if age > self.args.max_snapshot_age:
            info["status"] = "stale_snapshot"
            return {}, {}, info
        g = json.loads(row["data"])
        adj = {node: set() for node in g["nodes"]}
        for a,b in g["edges"]:
            u,v = g["nodes"][a],g["nodes"][b]
            adj[u].add(v)
            adj[v].add(u)
        info["nodes"] = len(adj)
        info["degree"] = ratio(2*len(g["edges"]),len(adj))
        distances: dict[str,int] = {}
        if publisher in adj:
            distances[publisher] = 0
            queue = deque([publisher])
            while queue:
                u = queue.popleft()
                for v in adj[u]:
                    if v not in distances:
                        distances[v] = distances[u]+1
                        queue.append(v)
        else:
            info["status"] = "publisher_absent"
        return adj, distances, info


def clock_relation(a: dict, b: dict) -> str:
    if a.get("clock") == CLOCK_BASIS and b.get("clock") == CLOCK_BASIS:
        return "controller-offset-v1"
    if a.get("agent") and a.get("agent") == b.get("agent"):
        return "same-agent-estimate"
    return "unverified-cross-agent"


def resolve_hops(parents: dict[str,str], publisher: str, peer_issues: dict[str,bool], enabled: bool = True):
    """Iterative functional-graph traversal: no recursion-depth failures."""
    result: dict[str,tuple[int|None,str,bool]] = {}
    for start in parents:
        if start in result:
            continue
        path, position = [], {}
        cur = start
        if not enabled:
            result[start] = (None,"no_unambiguous_publication",False)
            continue
        while True:
            if cur == publisher:
                base = (0,"resolved",True)
                break
            if cur in result:
                base = result[cur]
                break
            if not cur:
                base = (None,"parent_unmapped_or_missing",False)
                break
            if cur not in parents:
                base = (None,"parent_without_delivery",False)
                break
            if cur in position:
                base = (None,"cycle",False)
                break
            position[cur] = len(path)
            path.append(cur)
            cur = parents[cur]
        hop,status,safe = base
        for n in reversed(path):
            if hop is not None:
                hop += 1
            safe = safe and not peer_issues.get(n,False)
            result[n] = (hop,status,safe if hop is not None else False)
    return result


def nearest_stats(values: list[float]) -> dict:
    values = sorted(x for x in values if math.isfinite(x))
    if not values:
        return {"n":0,"mean":None,"p50":None,"p90":None,"p95":None,"p99":None,"min":None,"max":None}
    return {"n":len(values),"mean":math.fsum(values)/len(values),
            **{f"p{q}":values[max(0,math.ceil(q/100*len(values))-1)] for q in (50,90,95,99)},
            "min":values[0],"max":values[-1]}


def csv_writer(path: Path, headers: list[str]):
    handle = gzip.open(path,"wt",encoding="utf-8",newline="") if path.suffix == ".gz" else path.open("w",encoding="utf-8-sig",newline="")
    writer = csv.DictWriter(handle,fieldnames=headers,extrasaction="ignore")
    writer.writeheader()
    return handle,writer


DETAIL_COLUMNS = [
    "topic","mid","node","grp","payload","wire","encoding","parent","hop","hop_status","identity_safe",
    "latency","latency_basis","latency_raw","parent_delta_raw","parent_delta","parent_clock",
    "parent_delta_unc","parent_delta_lower","parent_delta_upper","shortest","snapshot_ratio","snapshot_age",
    "path_in_snapshot","graph_nodes","graph_degree","recv_to_app","parent_to_send","send_to_recv",
    "send_count","recv_count","rpc_status","receiver_ts","parent_ts","receipt_sessions",
    "receiver_peer","parent_peer","receiver_session","parent_session","node_tree_hop",
    "receiver_in_snapshot","parent_edge_in_snapshot","session_start_minus_publish",
    "receiver_agent","parent_agent","same_agent"
]
MESSAGE_HEADERS = ["run_id","label","degree","topic","message_id","publisher","publish_at","publication_records",
 "payload_bytes","wire_bytes","encoding","size_source","size_conflict","remote_first_receipts","hop_resolved",
 "hop_resolved_fraction","identity_safe_hops","mean_hop","mean_hop_identity_safe","latency_n","latency_mean_ms",
 "latency_p95_ms","parent_delta_n","parent_delta_mean_ms","parent_delta_p50_ms","parent_delta_p90_ms",
 "negative_parent_deltas","unknown_cross_agent_deltas","snapshot_status","snapshot_at","snapshot_age_s",
 "snapshot_nodes","snapshot_average_degree","matched_pairs","mean_snapshot_shortest_hop",
 "mean_hop_on_matched_pairs","mean_snapshot_hop_ratio","paths_present_in_snapshot",
 "receivers_absent_in_snapshot","sessions_started_after_publication"]
FANOUT_HEADERS = ["run_id","label","degree","topic","message_id","payload_bytes","node_id","session_id","peer_id",
 "send_callback_records","distinct_remote_peers","drop_callback_records","first_send_at","last_send_at",
 "send_callback_span_ms","first_receipt_to_first_send_ms","first_receipt_to_last_send_ms"]
DUP_HEADERS = ["run_id","label","degree","topic","message_id","payload_bytes","receiver","receiver_session",
 "duplicate_source","duplicate_remote_peer","duplicate_count","first_duplicate_at","first_delivery_at",
 "duplicate_minus_app_first_ms","first_tree_hop","source_first_tree_hop_plus_one",
 "shorter_extension_candidate","source_path_contains_receiver","same_receiver_incarnation"]


def analyze(ex: Extractor, out: Path):
    db,args = ex.db,ex.args
    peer_map,peer_conflicts = {},set()
    for row in db.execute("SELECT peer,MIN(node) node,COUNT(DISTINCT node) n FROM identities GROUP BY peer"):
        if row["n"]==1:
            peer_map[row["peer"]]=row["node"]
        else:
            peer_conflicts.add(row["peer"])
    ex.counts["ambiguous_peer_mappings"] = len(peer_conflicts)
    group_map = {}
    for row in db.execute("SELECT node,MIN(grp) grp,COUNT(DISTINCT grp) n FROM "
                          "(SELECT node,grp FROM identities WHERE grp!='' UNION SELECT node,grp FROM groups_evidence) GROUP BY node"):
        group_map[row["node"]] = row["grp"] if row["n"] == 1 else ""
        if row["n"] > 1:
            ex.counts["conflicting_node_groups"] += 1
    common = {"run_id":ex.run_id,"label":args.label,"degree":args.degree}
    mf,mw=csv_writer(out/"message_summary.csv",MESSAGE_HEADERS)
    ff,fw=csv_writer(out/"sender_fanout.csv",FANOUT_HEADERS)
    df,dw=csv_writer(out/"duplicate_candidates.csv.gz",DUP_HEADERS)
    try:
        keys = db.execute("SELECT topic,mid FROM pubs UNION SELECT topic,mid FROM firsts ORDER BY topic,mid")
        for key in keys:
            topic,mid=key
            if args.topic is not None and topic!=args.topic:
                continue
            pubs = [dict(r) for r in db.execute("SELECT * FROM pubs WHERE topic=? AND mid=? ORDER BY ts,id",(topic,mid))]
            pub = pubs[0] if pubs else None
            # Different publication instances with the same ID are not one tree.
            ambiguous_pub = len({(p["node"],p["session"],p["ts"]) for p in pubs})>1
            if ambiguous_pub:
                ex.counts["ambiguous_publication_messages"] += 1
            rows=[dict(r) for r in db.execute("SELECT f.*,s.n AS session_n FROM firsts f JOIN session_counts s USING(topic,mid,node) "
                                               "WHERE f.topic=? AND f.mid=?",(topic,mid))]
            publisher=pub["node"] if pub else ""
            for r in rows:
                r["grp"] = group_map.get(r["node"],r["grp"])
            rec={r["node"]:r for r in rows if r["node"]!=publisher}
            if not pub:
                ex.counts["orphan_message_ids"] += 1
                ex.counts["orphan_receivers"] += len(rec)
            parents={n:peer_map.get(r["remote"],"") for n,r in rec.items()}
            issues={}
            for n,r in rec.items():
                p=parents[n]
                pr=pub if p==publisher and pub else rec.get(p)
                issues[n]=r["session_n"]>1 or (pr is not None and bool(pr.get("peer")) and pr["peer"]!=r["remote"])
            hops=resolve_hops(parents,publisher,issues,bool(pub and not ambiguous_pub))
            adj,dist,graph=ex.graph_at(pub["ts"] if pub else None,publisher)
            # Wire ID correlation is per topic + native pubsub ID, not application ID.
            wids={r["wire_id"] for r in rows if r["wire_id"]}
            if pub and pub["wire_id"]:
                wids.add(pub["wire_id"])
            if not wids and mid.startswith("pubsub-"):
                wids.add(mid[len("pubsub-"):])
            rpc_by=defaultdict(list)
            wire_ambiguous=len(wids)>1
            if len(wids)==1 and not args.no_rpc:
                wid=next(iter(wids))
                for r0 in db.execute("SELECT * FROM rpc WHERE topic=? AND wid=? ORDER BY ts,id",(topic,wid)):
                    r=dict(r0)
                    rpc_by[(r["node"],r["session"],r["direction"],r["remote"])].append(r)
            sizes={int(r["payload"]) for r in ([pub] if pub else [])+rows if r.get("payload") is not None}
            wires={int(r["wire"]) for r in ([pub] if pub else [])+rows if r.get("wire") is not None}
            encodings={r["encoding"] for r in ([pub] if pub else [])+rows if r.get("encoding")}
            payload=next(iter(sizes)) if len(sizes)==1 else None
            size_source="recorded" if payload is not None else "unavailable"
            if not sizes and args.payload_bytes is not None:
                payload=args.payload_bytes
                size_source="cli-label-only"
            wire=next(iter(wires)) if len(wires)==1 else None
            encoding=next(iter(encodings)) if len(encodings)==1 else ("mixed" if encodings else "")
            size_conflict=len(sizes)>1 or len(wires)>1
            local_details=[]
            for n,r in rec.items():
                if args.group is not None and r["grp"]!=args.group:
                    continue
                p=parents[n]
                parent=pub if pub and p==publisher else rec.get(p)
                hop,status,safe=hops[n]
                if r["remote"] in peer_conflicts:
                    status="ambiguous_parent_peer"
                raw_lat=(r["ts"]-pub["ts"])/NS_PER_MS if pub else None
                latency,latbasis=None,"unavailable"
                if pub and not ambiguous_pub:
                    if r["encoding"]=="envelope" and r["latency_available"] and r["latency"] is not None and r["latency"]>=0:
                        latency,latbasis=r["latency"],"recorded-envelope"
                    elif clock_relation(pub,r)!="unverified-cross-agent" and raw_lat is not None and raw_lat>=0:
                        latency,latbasis=raw_lat,"log-time:"+clock_relation(pub,r)
                delta=(r["ts"]-parent["ts"])/NS_PER_MS if parent else None
                relation=clock_relation(parent,r) if parent else "no-parent-time"
                # Keep raw negative and unsynchronized differences in receiver CSV.
                # Summary samples use only nonnegative, comparable observations.
                usable_delta=delta if delta is not None and delta>=0 and relation not in ("no-parent-time","unverified-cross-agent") and not issues[n] else None
                uncertainty=None
                if parent and relation==CLOCK_BASIS and parent.get("unc") is not None and r["unc"] is not None:
                    uncertainty=parent["unc"]+r["unc"]
                shortest=dist.get(n)
                path_present=None
                if adj and hop is not None:
                    cur=n
                    path_present=True
                    for _ in range(hop):
                        prev=parents.get(cur,"")
                        if cur not in adj or prev not in adj.get(cur,set()):
                            path_present=False
                            break
                        cur=prev
                    if cur!=publisher:
                        path_present=False
                recv_to_app,parent_to_send,send_to_recv=None,None,None
                send_count,recv_count=0,0
                rpcstatus="disabled" if args.no_rpc else ("ambiguous_wire_ids" if wire_ambiguous else "no_matching_rpc")
                if not args.no_rpc and not wire_ambiguous and r["remote"]:
                    rr=[x for x in rpc_by.get((n,r["session"],"recv",r["remote"]),[]) if x["ts"]<=r["ts"]]
                    recv_count=len(rr)
                    if len(rr)==1:
                        rx=rr[0]
                        recv_to_app=(r["ts"]-rx["ts"])/NS_PER_MS
                        rpcstatus="unique_recv_callback"
                        if parent:
                            ss=rpc_by.get((p,parent["session"],"send",r["peer"]),[])
                            send_count=len(ss)
                            if len(ss)==1:
                                sx=ss[0]
                                parent_to_send=(sx["ts"]-parent["ts"])/NS_PER_MS
                                if clock_relation(sx,rx)==CLOCK_BASIS or parent.get("agent") and parent["agent"]==r["agent"]:
                                    send_to_recv=(rx["ts"]-sx["ts"])/NS_PER_MS
                                rpcstatus="unique_endpoint_callbacks_not_global_rpc_id"
                            elif len(ss)>1:
                                rpcstatus="ambiguous_send_callbacks"
                    elif len(rr)>1:
                        rpcstatus="ambiguous_recv_callbacks"
                session_start_row=db.execute("SELECT MIN(ts) FROM life WHERE node=? AND session=? AND kind='measurement_start' AND ts<=?",
                                            (n,r["session"],r["ts"])).fetchone()
                start_ns=session_start_row[0] if session_start_row else None
                start_delta=(start_ns-pub["ts"])/NS_PER_MS if start_ns is not None and pub is not None else None
                detail=dict(zip(DETAIL_COLUMNS,[
                    topic,mid,n,r["grp"],payload,wire,encoding,p,hop,status,int(safe),
                    latency,latbasis,raw_lat,delta,usable_delta,relation,uncertainty,
                    delta-uncertainty if delta is not None and uncertainty is not None else None,
                    delta+uncertainty if delta is not None and uncertainty is not None else None,
                    shortest,hop/shortest if hop is not None and shortest is not None and shortest>0 else None,
                    graph.get("age_seconds"),int(path_present) if path_present is not None else None,
                    graph.get("nodes"),graph.get("degree"),recv_to_app,parent_to_send,send_to_recv,
                    send_count,recv_count,rpcstatus,r["ts"],parent["ts"] if parent else None,r["session_n"],
                    r["peer"],r["remote"],r["session"],parent["session"] if parent else "",hop,
                    int(n in adj) if graph["status"] in ("ok","publisher_absent") else None,
                    int(p in adj.get(n,set())) if graph["status"] in ("ok","publisher_absent") else None,start_delta,
                    r["agent"],parent["agent"] if parent else "",
                    int(r["agent"]==parent["agent"]) if parent and parent["agent"] and r["agent"] else None]))
                db.execute("INSERT INTO detail VALUES("+",".join("?" for _ in DETAIL_COLUMNS)+")",list(detail.values()))
                local_details.append(detail)
            # Local callback fanout spans: NOT actual write-completion times.
            by_sender=defaultdict(lambda:{"send":[],"drop":[]})
            for (node,session,direction,remote), rr in rpc_by.items():
                if direction in ("send","drop"):
                    by_sender[(node,session)][direction].extend(rr)
            for (node,session),kinds in by_sender.items():
                ss=sorted(kinds["send"],key=lambda r:r["ts"])
                base=pub if pub and node==publisher else rec.get(node)
                if base and base["session"]!=session:
                    base=None
                fw.writerow({**common,"topic":topic,"message_id":mid,"payload_bytes":payload,"node_id":node,
                    "session_id":session,"peer_id":ss[0]["peer"] if ss else "", "send_callback_records":len(ss),
                    "distinct_remote_peers":len({x["remote"] for x in ss}),"drop_callback_records":len(kinds["drop"]),
                    "first_send_at":iso_ns(ss[0]["ts"]) if ss else "","last_send_at":iso_ns(ss[-1]["ts"]) if ss else "",
                    "send_callback_span_ms":(ss[-1]["ts"]-ss[0]["ts"])/NS_PER_MS if ss else None,
                    "first_receipt_to_first_send_ms":(ss[0]["ts"]-base["ts"])/NS_PER_MS if ss and base else None,
                    "first_receipt_to_last_send_ms":(ss[-1]["ts"]-base["ts"])/NS_PER_MS if ss and base else None})
            for dr0 in db.execute("SELECT * FROM dup_edges WHERE topic=? AND mid=?",(topic,mid)):
                dr=dict(dr0)
                first=rec.get(dr["node"])
                if args.group is not None and (first is None or first["grp"]!=args.group):
                    continue
                source=peer_map.get(dr["remote"],"")
                sh=0 if source and source==publisher else hops.get(source,(None,"",False))[0]
                fh=hops.get(dr["node"],(None,"",False))[0]
                candidate=sh+1 if sh is not None else None
                contains=False
                cur=source
                for _ in range((sh or 0)+1):
                    if cur==dr["node"]:
                        contains=True
                        break
                    cur=parents.get(cur,"")
                    if not cur or cur==publisher:
                        break
                same=bool(first and first["session"]==dr["session"] and first["peer"]==dr["peer"])
                dw.writerow({**common,"topic":topic,"message_id":mid,"payload_bytes":payload,
                    "receiver":dr["node"],"receiver_session":dr["session"],"duplicate_source":source,
                    "duplicate_remote_peer":dr["remote"],"duplicate_count":dr["n"],"first_duplicate_at":iso_ns(dr["tmin"]),
                    "first_delivery_at":iso_ns(first["ts"]) if first else "",
                    "duplicate_minus_app_first_ms":(dr["tmin"]-first["ts"])/NS_PER_MS if same else None,
                    "first_tree_hop":fh,"source_first_tree_hop_plus_one":candidate,
                    "shorter_extension_candidate":int(candidate<fh and not contains and same) if candidate is not None and fh is not None else None,
                    "source_path_contains_receiver":int(contains),"same_receiver_incarnation":int(same)})
            hs=nearest_stats([d["hop"] for d in local_details if d["hop"] is not None])
            safehs=nearest_stats([d["hop"] for d in local_details if d["hop"] is not None and d["identity_safe"]])
            ls=nearest_stats([d["latency"] for d in local_details if d["latency"] is not None])
            ds=nearest_stats([d["parent_delta"] for d in local_details if d["parent_delta"] is not None])
            matched=[d for d in local_details if d["hop"] is not None and d["shortest"] is not None and d["shortest"]>0]
            mw.writerow({**common,"topic":topic,"message_id":mid,"publisher":publisher,"publish_at":iso_ns(pub["ts"]) if pub else "",
                "publication_records":len(pubs),"payload_bytes":payload,"wire_bytes":wire,"encoding":encoding,
                "size_source":size_source,"size_conflict":int(size_conflict),"remote_first_receipts":len(local_details),
                "hop_resolved":hs["n"],"hop_resolved_fraction":ratio(hs["n"],len(local_details)),
                "identity_safe_hops":safehs["n"],"mean_hop":hs["mean"],"mean_hop_identity_safe":safehs["mean"],
                "latency_n":ls["n"],"latency_mean_ms":ls["mean"],"latency_p95_ms":ls["p95"],
                "parent_delta_n":ds["n"],"parent_delta_mean_ms":ds["mean"],"parent_delta_p50_ms":ds["p50"],"parent_delta_p90_ms":ds["p90"],
                "negative_parent_deltas":sum(d["parent_delta_raw"] is not None and d["parent_delta_raw"]<0 for d in local_details),
                "unknown_cross_agent_deltas":sum(d["parent_clock"]=="unverified-cross-agent" for d in local_details),
                "snapshot_status":graph["status"],"snapshot_at":graph.get("at"),"snapshot_age_s":graph.get("age_seconds"),
                "snapshot_nodes":graph.get("nodes"),"snapshot_average_degree":graph.get("degree"),"matched_pairs":len(matched),
                "mean_snapshot_shortest_hop":nearest_stats([d["shortest"] for d in matched])["mean"],
                "mean_hop_on_matched_pairs":nearest_stats([d["hop"] for d in matched])["mean"],
                "mean_snapshot_hop_ratio":nearest_stats([d["snapshot_ratio"] for d in matched])["mean"],
                "paths_present_in_snapshot":sum(d["path_in_snapshot"]==1 for d in matched),
                "receivers_absent_in_snapshot":sum(d["receiver_in_snapshot"]==0 for d in local_details),
                "sessions_started_after_publication":sum(d["session_start_minus_publish"] is not None and d["session_start_minus_publish"]>0 for d in local_details)})
            ex.counts["analyzed_message_ids"] += 1
            if ex.counts["analyzed_message_ids"]%100==0:
                db.commit()
                ex.progress("paths",True)
        db.commit()
    finally:
        mf.close(); ff.close(); df.close()


def sql_stats(db: sqlite3.Connection, col: str, where: str, params: tuple) -> dict:
    """Exact nearest-rank percentiles; SQLite sorts on disk, rows are streamed."""
    if col not in DETAIL_COLUMNS:
        raise ValueError(col)
    condition=f"({where}) AND {col} IS NOT NULL"
    row=db.execute(f"SELECT COUNT(*),AVG({col}),MIN({col}),MAX({col}) FROM detail WHERE {condition}",params).fetchone()
    n,mean,low,high=row
    result={"n":n,"mean":mean,"min":low,"max":high,**{f"p{q}":None for q in (50,90,95,99)}}
    if not n:
        return result
    targets={q:max(1,math.ceil(q*n/100)) for q in (50,90,95,99)}
    for i,r in enumerate(db.execute(f"SELECT {col} FROM detail WHERE {condition} ORDER BY {col}",params),1):
        for q,index in targets.items():
            if i==index:
                result[f"p{q}"]=r[0]
    return result


def summarize(ex: Extractor, out: Path):
    db,args=ex.db,ex.args
    common={"run_id":ex.run_id,"label":args.label,"degree":args.degree}
    qheaders=["run_id","label","degree","topic","payload_bytes","encoding","receivers","messages",
        "hop_resolved","hop_resolved_fraction","identity_safe_hops","hop_mean_receiver_weighted","hop_mean_message_weighted",
        "hop_p50","hop_p90","hop_p95","hop_max","hop_mean_identity_safe","latency_samples","latency_mean_ms","latency_p95_ms",
        "parent_interval_samples","parent_interval_mean_ms","parent_interval_p50_ms","parent_interval_p90_ms",
        "negative_parent_intervals","unverified_parent_intervals","matched_snapshot_pairs","snapshot_shortest_mean",
        "hop_mean_matched_pairs","snapshot_hop_ratio_mean","shorter_than_snapshot_count","mean_snapshot_age_s"]
    th=["run_id","label","degree","topic","payload_bytes","encoding","hop","receivers","fraction_of_resolved_hops"]
    for prefix in ("latency_ms","parent_interval_ms","rpc_recv_to_app_ms","parent_to_send_accept_ms","send_accept_to_recv_ms"):
        th.extend(prefix+"_"+stat for stat in ("n","mean","p50","p90","p95","p99"))
    th += ["negative_parent_intervals","unverified_parent_intervals","negative_send_accept_to_recv","negative_parent_to_send"]
    qf,qw=csv_writer(out/"hop_quality.csv",qheaders)
    tf,tw=csv_writer(out/"hop_timing.csv",th)
    rf,rw=csv_writer(out/"receiver_paths.csv.gz",["run_id","label","degree"]+DETAIL_COLUMNS)
    try:
        groups=list(db.execute("SELECT DISTINCT topic,payload,encoding FROM detail ORDER BY topic,payload,encoding"))
        for topic,payload,encoding in groups:
            where="topic=? AND payload IS ? AND encoding=?"
            ps=(topic,payload,encoding)
            base={**common,"topic":topic,"payload_bytes":payload,"encoding":encoding}
            n=db.execute("SELECT COUNT(*) FROM detail WHERE "+where,ps).fetchone()[0]
            nmsg=db.execute("SELECT COUNT(DISTINCT mid) FROM detail WHERE "+where,ps).fetchone()[0]
            h=sql_stats(db,"hop",where,ps)
            safe=sql_stats(db,"hop",where+" AND identity_safe=1",ps)
            latency=sql_stats(db,"latency",where,ps)
            delta=sql_stats(db,"parent_delta",where,ps)
            matched=where+" AND hop IS NOT NULL AND shortest>0"
            sh=sql_stats(db,"shortest",matched,ps)
            mh=sql_stats(db,"hop",matched,ps)
            sr=sql_stats(db,"snapshot_ratio",matched,ps)
            age=sql_stats(db,"snapshot_age",matched,ps)
            message_mean=db.execute("SELECT AVG(h) FROM (SELECT AVG(hop) h FROM detail WHERE "+where+" GROUP BY mid)",ps).fetchone()[0]
            extras=db.execute("SELECT SUM(parent_delta_raw<0),SUM(parent_clock='unverified-cross-agent'),SUM(hop<shortest) FROM detail WHERE "+where,ps).fetchone()
            qw.writerow({**base,"receivers":n,"messages":nmsg,"hop_resolved":h["n"],"hop_resolved_fraction":ratio(h["n"],n),
                "identity_safe_hops":safe["n"],"hop_mean_receiver_weighted":h["mean"],"hop_mean_message_weighted":message_mean,
                "hop_p50":h["p50"],"hop_p90":h["p90"],"hop_p95":h["p95"],"hop_max":h["max"],"hop_mean_identity_safe":safe["mean"],
                "latency_samples":latency["n"],"latency_mean_ms":latency["mean"],"latency_p95_ms":latency["p95"],
                "parent_interval_samples":delta["n"],"parent_interval_mean_ms":delta["mean"],"parent_interval_p50_ms":delta["p50"],
                "parent_interval_p90_ms":delta["p90"],"negative_parent_intervals":extras[0] or 0,
                "unverified_parent_intervals":extras[1] or 0,"matched_snapshot_pairs":sh["n"],
                "snapshot_shortest_mean":sh["mean"],"hop_mean_matched_pairs":mh["mean"],"snapshot_hop_ratio_mean":sr["mean"],
                "shorter_than_snapshot_count":extras[2] or 0,"mean_snapshot_age_s":age["mean"]})
            for hoprow in db.execute("SELECT DISTINCT hop FROM detail WHERE "+where+" ORDER BY hop",ps):
                hop=hoprow[0]
                hw=where+" AND hop IS ?"
                hp=ps+(hop,)
                count=db.execute("SELECT COUNT(*) FROM detail WHERE "+hw,hp).fetchone()[0]
                record={**base,"hop":hop if hop is not None else "unresolved","receivers":count,
                        "fraction_of_resolved_hops":ratio(count,h["n"]) if hop is not None else None}
                for prefix,col in (("latency_ms","latency"),("parent_interval_ms","parent_delta"),
                        ("rpc_recv_to_app_ms","recv_to_app"),("parent_to_send_accept_ms","parent_to_send"),("send_accept_to_recv_ms","send_to_recv")):
                    # Signed local parent-to-send is intentional. Forwarding can
                    # happen before the application Subscription.Next receipt.
                    extra=" AND send_to_recv>=0" if col=="send_to_recv" else ""
                    st=sql_stats(db,col,hw+extra,hp)
                    record.update({prefix+"_"+k:v for k,v in st.items()})
                cnt=db.execute("SELECT SUM(parent_delta_raw<0),SUM(parent_clock='unverified-cross-agent'),SUM(send_to_recv<0),SUM(parent_to_send<0) FROM detail WHERE "+hw,hp).fetchone()
                record.update(dict(zip(th[-4:],[v or 0 for v in cnt])))
                tw.writerow(record)
        for row in db.execute("SELECT * FROM detail ORDER BY topic,mid,node"):
            rw.writerow({**common,**dict(row)})
    finally:
        qf.close();tf.close();rf.close()
    af,aw=csv_writer(out/"placement_timing.csv",["run_id","label","degree","topic","payload_bytes","encoding","hop","same_agent",
             "receivers","interval_samples","interval_mean_ms","interval_p50_ms","interval_p90_ms","interval_p95_ms"])
    try:
        for row in db.execute("SELECT topic,payload,encoding,hop,same_agent,COUNT(*) n FROM detail GROUP BY topic,payload,encoding,hop,same_agent"):
            par=(row["topic"],row["payload"],row["encoding"],row["hop"],row["same_agent"])
            stat=sql_stats(db,"parent_delta","topic=? AND payload IS ? AND encoding=? AND hop IS ? AND same_agent IS ?",par)
            aw.writerow({**common,"topic":row["topic"],"payload_bytes":row["payload"],"encoding":row["encoding"],
                "hop":row["hop"] if row["hop"] is not None else "unresolved","same_agent":row["same_agent"],"receivers":row["n"],
                "interval_samples":stat["n"],"interval_mean_ms":stat["mean"],"interval_p50_ms":stat["p50"],
                "interval_p90_ms":stat["p90"],"interval_p95_ms":stat["p95"]})
    finally:
        af.close()
    pf,pw=csv_writer(out/"path_status_summary.csv",["run_id","label","degree","topic","payload_bytes","encoding","hop_status","identity_safe","receivers"])
    try:
        for row in db.execute("SELECT topic,payload,encoding,hop_status,identity_safe,COUNT(*) n FROM detail GROUP BY topic,payload,encoding,hop_status,identity_safe"):
            pw.writerow({**common,"topic":row["topic"],"payload_bytes":row["payload"],"encoding":row["encoding"],
                         "hop_status":row["hop_status"],"identity_safe":row["identity_safe"],"receivers":row["n"]})
    finally:
        pf.close()
    sf,sw=csv_writer(out/"session_quality.csv",["run_id","node_id","session_id","min_sequence","max_sequence","unique_sequences",
        "missing_prefix_slots","gap_slots_between_observed","start_seen","checkpoint_seen","stop_seen","terminated_seen"])
    try:
        for row in db.execute("SELECT node,session,MIN(seq),MAX(seq),COUNT(*) FROM seen_seq GROUP BY node,session ORDER BY node,session"):
            node,session,lo,hi,n=row
            life={r[0]:r[1] for r in db.execute("SELECT kind,COUNT(*) FROM life WHERE node=? AND session=? GROUP BY kind",(node,session))}
            sw.writerow({"run_id":ex.run_id,"node_id":node,"session_id":session,"min_sequence":lo,"max_sequence":hi,
                "unique_sequences":n,"missing_prefix_slots":lo-1,"gap_slots_between_observed":hi-lo+1-n,
                "start_seen":life.get("measurement_start",0),"checkpoint_seen":life.get("measurement_checkpoint",0),
                "stop_seen":life.get("measurement_stop",0),"terminated_seen":life.get("measurement_terminated",0)})
    finally:
        sf.close()
    cf,cw=csv_writer(out/"control_summary.csv",["run_id","event_type","rpc_records","entries","id_references","truncated_records","omitted_ids"])
    try:
        for kind,data in sorted(ex.controls.items()):
            cw.writerow({"run_id":ex.run_id,"event_type":kind,**data})
    finally:
        cf.close()
    if len(ex.subscription_topics)>1:
        ex.warnings.append("Multiple topics observed: snapshot graphs merge topics; shortest hops are not topic-specific.")
    if ex.counts["events_without_dedup_identity"]:
        ex.warnings.append("Some legacy events lack session/sequence and eventId: exact replay deduplication is unavailable for them.")
    if ex.counts["bad_lines"]:
        ex.warnings.append("Malformed JSON lines were explicitly skipped; see source statistics.")
    if not ex.types["publish"]:
        ex.warnings.append("No observed publish events: paths and publication latency cannot be reconstructed.")
    if not ex.types["deliver"]:
        ex.warnings.append("No observed deliver events: no first-reception samples.")
    if not ex.counts["gossipsub_snapshots"]:
        ex.warnings.append("No usable GossipSub snapshot graph; shortest-hop columns are blank.")
    manifest={"tool":"kpl_path_extract","version":VERSION,"created_utc":datetime.now(timezone.utc).isoformat(),
        "source_review_date":"2026-11-14","source_revision":"Public master and wiki as retrieved; no claim of an exact commit pin.",
        "run_id":ex.run_id,"label":args.label,"degree_label":args.degree,"receiver_group_filter":args.group,"topic_filter":args.topic,
        "args":{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
        "counts":dict(ex.counts),"event_type_counts":dict(ex.types),"sources":ex.source_stats,"warnings":ex.warnings,
        "input_experiment":{"id":ex.metadata.get("id"),"name":ex.metadata.get("name"),"controllerVersion":ex.metadata.get("controllerVersion"),
                            "state":ex.metadata.get("state"),"dataState":ex.metadata.get("dataState")},
        "definitions":{
            "receiver_unit":"message × NodeID, earliest timestamp nonlocal application deliver; publisher excluded",
            "hop":"number of parent links to the observed publisher; cycles/missing parents remain NULL",
            "identity_safe":"all selected path nodes avoid multi-session receipts and known parent PeerID mismatch",
            "parent_delta":"child application deliver timestamp minus parent application deliver (or publish root); not pure link/queue delay",
            "time_fields":"event.timestamp is used as already recorded; clockOffsetMs is never applied again",
            "timing_summary":"nonnegative parent intervals with comparable clocks and no known local identity mismatch; signed raw intervals retained",
            "snapshot":"most recent preceding raw GossipSub undirected topic-unioned graph, subject to max age",
            "snapshot_ratio":"diagnostic hop / snapshot shortest hop on matching publisher-receiver pairs; not a temporal shortest-path guarantee",
            "rpc_send":"local outbound queue acceptance callback, NOT stream-write completion",
            "rpc_pairing":"same topic + wire message ID + directed endpoints; only unique endpoint callbacks; no global RPC identity",
            "duplicates":"sender first-tree hop+1 is only a candidate extension, not an observed duplicate packet hop count",
            "percentiles":"exact nearest-rank; no random sampling",
            "reachability":"NOT computed: no fabricated population denominator or session-window reimplementation",
            "quantities_not_recorded":"per-message true write completion, pure queue dwell, validation duration, or physical link delay cannot be extracted from absent fields"
        },"implementation_sources":SOURCES}
    (out/"manifest.json").write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding="utf-8")


def parser() -> argparse.ArgumentParser:
    p=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("input",help="Single Run directory, raw results ZIP, or events.jsonl[.gz]")
    p.add_argument("--observations",nargs="+",help="observations.jsonl[.gz] (overrides automatic directory/ZIP discovery)")
    p.add_argument("--events-extra",nargs="*",default=[],help="Additional JSONL segments from the same Run (any order; deduplicated by ID)")
    p.add_argument("--out",required=True,type=Path,help="Empty output directory")
    p.add_argument("--run-id",default="",help="Analyze only this Run from mixed logs")
    p.add_argument("--label",default="",help="Experiment label to distinguish results, e.g. d6-200kb-r01")
    p.add_argument("--degree",type=int,default=None,help="Target D metadata label (separate from observed degree)")
    p.add_argument("--payload-bytes",type=int,default=None,help="Label used only when the log has no size field; does not estimate wire size")
    p.add_argument("--topic",default=None,help="Analyze only publications on this topic")
    p.add_argument("--group",default=None,help="Receiver group filter; reconstruct parent paths across all groups")
    p.add_argument("--max-snapshot-age",type=float,default=10.0,help="Maximum age in seconds of the snapshot preceding publication (default: 10)")
    p.add_argument("--no-rpc",action="store_true",help="Skip detailed RPC timing indexing")
    p.add_argument("--work-dir",type=Path,help="Directory for temporary SQLite storage; a local SSD is recommended (default: output directory)")
    p.add_argument("--keep-db",action="store_true",help="Keep the working SQLite database after completion")
    p.add_argument("--skip-bad-lines",action="store_true",help="Count and skip malformed JSON lines only; exit with an error by default")
    p.add_argument("--max-line-mib",type=int,default=64,help="JSONL line safety limit in MiB")
    p.add_argument("--quiet",action="store_true")
    p.add_argument("--version",action="version",version=VERSION)
    return p


def main(argv: list[str] | None=None) -> int:
    args=parser().parse_args(argv)
    if not math.isfinite(args.max_snapshot_age) or args.max_snapshot_age<0 or args.max_line_mib<=0:
        print("Error: snapshot age must be finite and nonnegative, and max-line-mib must be positive.",file=sys.stderr)
        return 2
    if args.payload_bytes is not None and args.payload_bytes<0:
        print("Error: --payload-bytes must be nonnegative.",file=sys.stderr)
        return 2
    ex=None
    db_path=None
    success=False
    try:
        events,observations,metadata=locate_inputs(args)
        out=args.out.expanduser().resolve()
        if out.exists() and any(out.iterdir()):
            raise ValueError(f"Output directory is not empty: {out}. Use a new path.")
        out.mkdir(parents=True,exist_ok=True)
        work=(args.work_dir or out).expanduser().resolve()
        work.mkdir(parents=True,exist_ok=True)
        fd,raw=tempfile.mkstemp(prefix="kpl-path-",suffix=".sqlite3",dir=work)
        os.close(fd)
        db_path=Path(raw)
        ex=Extractor(args,db_path)
        ex.metadata=metadata
        # Metadata is an identity hint, not a replacement for events.
        if not ex.run_id and text(metadata.get("id")):
            ex.run_id=metadata["id"]
        ex.ingest_events(events)
        ex.ingest_observations(observations)
        analyze(ex,out)
        summarize(ex,out)
        ex.db.commit()
        ex.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        success=True
        print(f"Completed: {out}\nRun: {ex.run_id}\nMessage IDs: {ex.counts['analyzed_message_ids']:,}\n"
              "Files to inspect first: hop_quality.csv, hop_timing.csv, message_summary.csv")
        if args.keep_db:
            print(f"Working database: {db_path}")
        return 0
    except KeyboardInterrupt:
        print("Interrupted. Input files were not modified.",file=sys.stderr)
        return 130
    except (ValueError,OSError,sqlite3.Error,zipfile.BadZipFile,json.JSONDecodeError) as exc:
        print(f"Error: {exc}",file=sys.stderr)
        if db_path:
            print(f"Working database for diagnostics: {db_path}",file=sys.stderr)
        return 1
    finally:
        if ex is not None:
            ex.db.close()
        if success and db_path and not args.keep_db:
            for suffix in ("","-wal","-shm"):
                Path(str(db_path)+suffix).unlink(missing_ok=True)


if __name__=="__main__":
    sys.exit(main())
