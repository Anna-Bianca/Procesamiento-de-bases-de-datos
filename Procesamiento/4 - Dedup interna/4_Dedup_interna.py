"""Paso 4: auditoría y aplicación de deduplicación interna del JSONL.

Uso: python 4_Dedup_interna.py --mode audit [--resume]
     python 4_Dedup_interna.py --mode apply --review-csv grupos.csv [--resume]
"""

from __future__ import annotations

import argparse
from collections import OrderedDict, defaultdict
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import threading
import time
from typing import Callable

from dedup_core import (
    MIN_PASSAGE_TOKENS, SOURCE_ORDER, VERSION, Match,
    exact_text_key, minhash_bands, numeric_subsequence, passage_anchors, preference, relation,
    remove_spans, safe_trim_spans, similarity, source_rank, substantial_extra,
    safe_to_drop_variant, tokenize,
)


def repository_root(script_path: Path) -> Path:
    """Devuelve la raíz local o, para un montaje plano, la carpeta del script."""
    step_dir = script_path.parent
    if step_dir.parent.name == "Procesamiento":
        return step_dir.parent.parent
    return step_dir


ROOT = repository_root(Path(__file__).resolve())
DEFAULT_INPUT = ROOT / "Base de datos" / "3 - Eliminar ruido" / "sin_ruido.jsonl"
DEFAULT_OUTPUT = ROOT / "Base de datos" / "4 - Dedup interna"
STEP_LABEL = "Paso 4: Dedup interna"
MAX_PLANNED_GROUP = 500
GROUP_COLUMNS = [
    "group_id", "keeper_record_no", "keeper_id", "member_count",
    "proposed_deletions", "proposed_trims", "signature", "decision", "keeper_override",
]
MEMBER_COLUMNS = ["group_id", "record_no", "id", "origin", "words", "preview"]
ACTION_COLUMNS = [
    "group_id", "record_no", "id", "origin", "action", "target_record_no",
    "reason", "start", "end", "preview",
]


CheckpointCallback = Callable[[sqlite3.Connection], None]


class CheckpointConnection(sqlite3.Connection):
    """Conexión que publica un snapshot después de cada commit confirmado."""

    checkpoint_callback: CheckpointCallback | None = None

    def commit(self) -> None:
        super().commit()
        if self.checkpoint_callback is not None:
            self.checkpoint_callback(self)


class CheckpointMirror:
    """Crea copias SQLite consistentes para reanudar fuera del disco local."""

    def __init__(self, directory: Path, minimum_seconds: float = 300.0):
        self.directory = directory
        self.minimum_seconds = minimum_seconds
        self.last_snapshot = 0.0
        self.directory.mkdir(parents=True, exist_ok=True)

    def __call__(self, db: sqlite3.Connection) -> None:
        self.snapshot(db)

    @staticmethod
    def _meta(db: sqlite3.Connection, key: str, default=None):
        row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def snapshot(self, db: sqlite3.Connection, *, force: bool = False) -> bool:
        now = time.monotonic()
        if not force and self.last_snapshot and now - self.last_snapshot < self.minimum_seconds:
            return False
        suffix = f"{os.getpid()}-{threading.get_ident()}"
        temp_db = self.directory / f".latest.sqlite3.tmp-{suffix}"
        latest_db = self.directory / "latest.sqlite3"
        temp_json = self.directory / f".latest.json.tmp-{suffix}"
        latest_json = self.directory / "latest.json"
        try:
            target = sqlite3.connect(temp_db)
            try:
                db.backup(target)
                result = target.execute("PRAGMA quick_check").fetchone()[0]
                if result != "ok":
                    raise sqlite3.DatabaseError(f"Snapshot SQLite inválido: {result}")
            finally:
                target.close()
            os.replace(temp_db, latest_db)
            metadata = {
                "snapshot_version": 1,
                "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "database": latest_db.name,
                "bytes": latest_db.stat().st_size,
                "phase": self._meta(db, "phase"),
                "indexed_count": self._meta(db, "indexed_count", 0),
                "verified_checked": self._meta(db, "verified_checked", 0),
                "apply_count": self._meta(db, "apply_count", 0),
            }
            temp_json.write_text(
                json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            os.replace(temp_json, latest_json)
            self.last_snapshot = now
            print(
                f"[CHECKPOINT] Snapshot persistente: {latest_db} "
                f"({metadata['bytes']:,} bytes)",
                file=sys.stderr,
                flush=True,
            )
            return True
        finally:
            temp_db.unlink(missing_ok=True)
            for sidecar_suffix in ("-journal", "-wal", "-shm"):
                Path(f"{temp_db}{sidecar_suffix}").unlink(missing_ok=True)
            temp_json.unlink(missing_ok=True)


def sha256_file(path: Path, max_bytes: int | None = None,
                progress_callback: Callable[[int], None] | None = None) -> str:
    digest = hashlib.sha256()
    completed = 0
    with path.open("rb") as stream:
        remaining = max_bytes
        while True:
            block = stream.read(min(8 * 1024 * 1024, remaining) if remaining is not None else 8 * 1024 * 1024)
            if not block:
                break
            digest.update(block)
            completed += len(block)
            if progress_callback is not None:
                progress_callback(completed)
            if remaining is not None:
                remaining -= len(block)
                if remaining == 0:
                    break
    return digest.hexdigest()


def quick_identity(path: Path) -> dict:
    """Identidad portable: no depende de ruta, mtime ni sistema operativo."""
    stat = path.stat()
    with path.open("rb") as stream:
        first = stream.read(65536)
        stream.seek(max(0, stat.st_size - 65536))
        last = stream.read(65536)
    return {"version": 2, "size": stat.st_size,
            "sample": hashlib.sha256(first + last).hexdigest()}


def compatible_identity(stored: dict, current: dict) -> bool:
    """Acepta checkpoints v1 locales y v2 portables del mismo archivo."""
    return (stored.get("size") == current.get("size") and
            stored.get("sample") == current.get("sample"))


def json_record(raw: bytes, record_no: int) -> dict:
    try:
        value = json.loads(raw.decode("utf-8-sig" if record_no == 1 else "utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"JSONL inválido en el registro {record_no}") from exc
    if not isinstance(value, dict) or not isinstance(value.get("texto"), str):
        raise ValueError(f"Falta texto válido en el registro {record_no}")
    if not isinstance(value.get("id"), str) or not isinstance(value.get("procesamiento"), list):
        raise ValueError(f"Faltan id o procesamiento en el registro {record_no}")
    return value


def connect(path: Path, checkpoint_callback: CheckpointCallback | None = None) -> CheckpointConnection:
    db = sqlite3.connect(path, factory=CheckpointConnection)
    db.checkpoint_callback = checkpoint_callback
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("PRAGMA cache_size=-131072")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS records (
            record_no INTEGER PRIMARY KEY, offset INTEGER NOT NULL, id TEXT NOT NULL,
            origin TEXT NOT NULL, words INTEGER NOT NULL, norm_hash TEXT NOT NULL,
            text_hash TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS postings (
            kind TEXT NOT NULL, key TEXT NOT NULL, record_no INTEGER NOT NULL,
            PRIMARY KEY(kind,key,record_no)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS candidates (
            a INTEGER NOT NULL, b INTEGER NOT NULL, PRIMARY KEY(a,b)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS verified (
            a INTEGER NOT NULL, b INTEGER NOT NULL, relation TEXT NOT NULL,
            jaccard REAL NOT NULL, coverage_a REAL NOT NULL, coverage_b REAL NOT NULL,
            matches TEXT NOT NULL, PRIMARY KEY(a,b)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS groups (
            group_id TEXT PRIMARY KEY, members TEXT NOT NULL,
            keeper_record_no INTEGER NOT NULL, signature TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS decisions (
            record_no INTEGER PRIMARY KEY, group_id TEXT NOT NULL, action TEXT NOT NULL,
            target_record_no INTEGER, spans TEXT NOT NULL
        );
    """)
    return db


def get_meta(db: sqlite3.Connection, key: str, default=None):
    row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return json.loads(row[0]) if row else default


def put_meta(db: sqlite3.Connection, key: str, value) -> None:
    db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, json.dumps(value)))


def report(phase: str, count: int, interval: int) -> None:
    if interval and count % interval == 0:
        print(f"[{phase}] {count:,}", file=sys.stderr, flush=True)


class AuditHeartbeat:
    """Latido independiente de SQLite; distingue actividad de avance confirmado."""

    def __init__(self, seconds: float):
        self.seconds = seconds
        self.started = time.monotonic()
        self.phase_name = "preparación"
        self.unit = "unidades"
        self.active = 0
        self.confirmed = 0
        self.total: int | None = None
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self) -> None:
        if self.seconds <= 0:
            return
        print(f"[ACTIVO] auditoría iniciada; latido cada {self.seconds:g} s", file=sys.stderr, flush=True)
        self.thread = threading.Thread(target=self._run, name="dedup-audit-heartbeat", daemon=True)
        self.thread.start()

    def phase(self, name: str, *, active: int = 0, confirmed: int = 0,
              total: int | None = None, unit: str = "unidades") -> None:
        with self.lock:
            self.phase_name, self.active, self.confirmed = name, active, confirmed
            self.total, self.unit = total, unit
        if self.seconds > 0:
            print(f"[FASE] {name}", file=sys.stderr, flush=True)

    def update(self, *, active: int | None = None, confirmed: int | None = None) -> None:
        if self.seconds <= 0:
            return
        with self.lock:
            if active is not None:
                self.active = active
            if confirmed is not None:
                self.confirmed = confirmed

    def _run(self) -> None:
        while not self.stop_event.wait(self.seconds):
            with self.lock:
                name, active, confirmed, total, unit = (
                    self.phase_name, self.active, self.confirmed, self.total, self.unit)
            suffix = f"/{total:,}" if total is not None else ""
            elapsed = int(time.monotonic() - self.started)
            print(
                f"[ACTIVO] {name}: en curso {active:,}{suffix} {unit}; "
                f"checkpoint confirmado {confirmed:,}{suffix}; {elapsed:,} s transcurridos",
                file=sys.stderr, flush=True,
            )

    def close(self) -> None:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=max(1.0, self.seconds + 0.1))


def index_records(db: sqlite3.Connection, input_path: Path, batch_size: int,
                  progress: int, limit: int, heartbeat: AuditHeartbeat) -> None:
    offset = get_meta(db, "indexed_offset", 0)
    count = get_meta(db, "indexed_count", 0)
    heartbeat.phase("indexación", active=count, confirmed=count,
                    total=limit or None, unit="registros")
    with input_path.open("rb") as stream:
        stream.seek(offset)
        pending_records: list[tuple] = []
        pending_postings: list[tuple[str, str, int]] = []

        def flush_batch() -> None:
            if not pending_records:
                return
            db.executemany("INSERT INTO records VALUES (?,?,?,?,?,?,?)", pending_records)
            db.executemany("INSERT OR IGNORE INTO postings VALUES (?,?,?)", pending_postings)
            put_meta(db, "indexed_offset", stream.tell())
            put_meta(db, "indexed_count", count)
            db.commit()
            pending_records.clear()
            pending_postings.clear()
            heartbeat.update(active=count, confirmed=count)

        while True:
            if limit and count >= limit:
                break
            start = stream.tell()
            raw = stream.readline()
            if not raw:
                break
            if not raw.strip():
                raise ValueError(f"Línea vacía en byte {start}; el JSONL debe tener una línea por registro")
            record = json_record(raw, count + 1)
            text = record["texto"]
            tokens = tokenize(text)
            count += 1
            exact_key = exact_text_key(text)
            pending_records.append((
                count, start, record["id"], str(record.get("base_de_datos_origen") or ""),
                len(tokens), exact_key, hashlib.sha256(text.encode("utf-8")).hexdigest(),
            ))
            postings = [("E", exact_key, count)] if len(tokens) >= 20 else []
            postings.extend(("M", f"{band:02d}:{key}", count)
                            for band, key in enumerate(minhash_bands(tokens)))
            postings.extend(("A", key, count) for key in passage_anchors(tokens))
            pending_postings.extend(postings)
            if count % 100 == 0:
                heartbeat.update(active=count)
            if len(pending_records) >= batch_size or len(pending_postings) >= 50_000:
                flush_batch()
            report("indexación", count, progress)
        flush_batch()
        put_meta(db, "phase", "indexed")
        db.commit()
        heartbeat.update(active=count, confirmed=count)


def candidate_pairs(db: sqlite3.Connection, max_bucket: int,
                    progress: int, heartbeat: AuditHeartbeat) -> None:
    """Expande grupos pequeños en SQL; los grupos exactos usan una estrella."""
    chunks = ([('A', digit, chr(ord(digit) + 1) if digit != '9' else 'a') for digit in '0123456789abcdef'] +
              [('E', digit, chr(ord(digit) + 1) if digit != '9' else 'a') for digit in '0123456789abcdef'] +
              [('M', f'{band:02d}:', f'{band + 1:02d}:') for band in range(16)])
    # En hexadecimal, después de 'f' se usa 'g' como límite exclusivo.
    chunks = [(kind, low, 'g' if low == 'f' else high) for kind, low, high in chunks]
    start_index = get_meta(db, "posting_chunk", 0)
    heartbeat.phase("generación de pares", active=start_index, confirmed=start_index,
                    total=len(chunks), unit="bloques")
    skipped = get_meta(db, "hot_buckets", 0)
    for index in range(start_index, len(chunks)):
        heartbeat.update(active=index + 1)
        kind, low, high = chunks[index]
        if kind == "E":
            db.execute("""
                INSERT OR IGNORE INTO candidates(a,b)
                SELECT firsts.first_no, p.record_no
                FROM (SELECT key, MIN(record_no) AS first_no FROM postings
                      WHERE kind='E' AND key>=? AND key<? GROUP BY key HAVING COUNT(*)>1) firsts
                JOIN postings p ON p.kind='E' AND p.key=firsts.key AND p.record_no>firsts.first_no
            """, (low, high))
        else:
            counts = db.execute("""
                SELECT COUNT(*) FROM (
                    SELECT key FROM postings WHERE kind=? AND key>=? AND key<?
                    GROUP BY key HAVING COUNT(*)>?
                )
            """, (kind, low, high, max_bucket)).fetchone()[0]
            skipped += counts
            db.execute("""
                INSERT OR IGNORE INTO candidates(a,b)
                SELECT p1.record_no, p2.record_no
                FROM (SELECT key FROM postings WHERE kind=? AND key>=? AND key<?
                      GROUP BY key HAVING COUNT(*) BETWEEN 2 AND ?) active
                JOIN postings p1 ON p1.kind=? AND p1.key=active.key
                JOIN postings p2 ON p2.kind=? AND p2.key=active.key AND p2.record_no>p1.record_no
            """, (kind, low, high, max_bucket, kind, kind))
        put_meta(db, "posting_chunk", index + 1)
        put_meta(db, "hot_buckets", skipped)
        db.commit()
        heartbeat.update(confirmed=index + 1)
        if progress:
            print(f"[candidatos] bloque {index + 1}/{len(chunks)}", file=sys.stderr, flush=True)
    put_meta(db, "phase", "candidates")
    db.commit()


class RecordReader:
    def __init__(self, path: Path, db: sqlite3.Connection, cache_chars: int = 8_000_000):
        self.stream = path.open("rb")
        self.db = db
        self.cache: OrderedDict[int, tuple[dict, list]] = OrderedDict()
        self.cache_chars = cache_chars
        self.used_chars = 0

    def close(self):
        self.stream.close()

    def get(self, record_no: int) -> tuple[dict, list]:
        if record_no in self.cache:
            self.cache.move_to_end(record_no)
            return self.cache[record_no]
        meta = self.db.execute("SELECT offset,text_hash FROM records WHERE record_no=?", (record_no,)).fetchone()
        if meta is None:
            raise ValueError(f"Registro desconocido: {record_no}")
        self.stream.seek(meta["offset"])
        record = json_record(self.stream.readline(), record_no)
        if hashlib.sha256(record["texto"].encode("utf-8")).hexdigest() != meta["text_hash"]:
            raise ValueError(f"Texto de entrada modificado: registro {record_no}")
        value = record, tokenize(record["texto"])
        size = len(record["texto"])
        if size <= self.cache_chars:
            while self.cache and self.used_chars + size > self.cache_chars:
                _old_no, old = self.cache.popitem(last=False)
                self.used_chars -= len(old[0]["texto"])
            self.cache[record_no] = value
            self.used_chars += size
        return value


def verify_pairs(db: sqlite3.Connection, input_path: Path, batch_size: int,
                 progress: int, heartbeat: AuditHeartbeat) -> None:
    last = get_meta(db, "verified_last", [0, 0])
    cursor = db.execute("SELECT a,b FROM candidates WHERE (a,b)>(?,?) ORDER BY a,b", last)
    reader = RecordReader(input_path, db)
    count = get_meta(db, "verified_checked", 0)
    heartbeat.phase("verificación de pares", active=count, confirmed=count, unit="pares")
    try:
        while rows := cursor.fetchmany(batch_size):
            for row in rows:
                a, b = row["a"], row["b"]
                record_a, tokens_a = reader.get(a)
                record_b, tokens_b = reader.get(b)
                if exact_text_key(record_a["texto"]) == exact_text_key(record_b["texto"]) and len(tokens_a) >= 20:
                    jaccard, ca, cb, matches, kind = 1.0, 1.0, 1.0, [], "exacto"
                else:
                    jaccard, ca, cb, matches = similarity(tokens_a, tokens_b)
                    kind = relation(tokens_a, tokens_b, jaccard, ca, cb, matches)
                if kind:
                    db.execute("INSERT OR REPLACE INTO verified VALUES (?,?,?,?,?,?,?)", (
                        a, b, kind, jaccard, ca, cb,
                        json.dumps([[m.a_start, m.a_end, m.b_start, m.b_end] for m in matches]),
                    ))
                count += 1
                last = [a, b]
                if count % 100 == 0:
                    heartbeat.update(active=count)
                report("pares verificados", count, progress)
            put_meta(db, "verified_last", last)
            put_meta(db, "verified_checked", count)
            db.commit()
            heartbeat.update(active=count, confirmed=count)
    finally:
        reader.close()
    put_meta(db, "phase", "verified")
    db.commit()


def group_components(db: sqlite3.Connection) -> list[list[int]]:
    parent: dict[int, int] = {}

    def find(x: int) -> int:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for row in db.execute("SELECT a,b FROM verified ORDER BY a,b"):
        a, b = find(row["a"]), find(row["b"])
        if a != b:
            parent[max(a, b)] = min(a, b)
    groups: dict[int, list[int]] = defaultdict(list)
    for member in parent:
        groups[find(member)].append(member)
    return [sorted(members) for _key, members in sorted(groups.items())]


def pair_data(db: sqlite3.Connection, a: int, b: int):
    row = db.execute("SELECT * FROM verified WHERE a=? AND b=?", (min(a, b), max(a, b))).fetchone()
    if row is None:
        hashes = db.execute("SELECT norm_hash FROM records WHERE record_no IN (?,?)", (a, b)).fetchall()
        if len(hashes) == 2 and hashes[0][0] == hashes[1][0]:
            return "exacto", 1.0, 1.0, 1.0, []
        return None
    matches = [Match(*values) for values in json.loads(row["matches"])]
    if a > b:
        matches = [Match(m.b_start, m.b_end, m.a_start, m.a_end) for m in matches]
        ca, cb = row["coverage_b"], row["coverage_a"]
    else:
        ca, cb = row["coverage_a"], row["coverage_b"]
    return row["relation"], row["jaccard"], ca, cb, matches


def plan_group(db: sqlite3.Connection, reader: RecordReader, members: list[int], override: int | None = None):
    metas = {}
    for start in range(0, len(members), 500):
        chunk = members[start:start + 500]
        for row in db.execute(
            f"SELECT * FROM records WHERE record_no IN ({','.join('?' for _ in chunk)})", chunk):
            metas[row["record_no"]] = dict(row)
    max_words = max(meta["words"] for meta in metas.values())
    order = sorted(members, key=lambda n: preference(metas[n], max_words))
    if override is not None:
        if override not in metas:
            raise ValueError(f"El representante {override} no pertenece al grupo")
        order.remove(override)
        order.insert(0, override)
    keeper = order[0]
    if len(members) > MAX_PLANNED_GROUP:
        # Evita decisiones transitivas masivas en componentes creados por anclas comunes.
        if len({meta["norm_hash"] for meta in metas.values()}) == 1:
            return keeper, [
                {"record_no": n, "action": "eliminar", "target": keeper,
                 "reason": "exacto", "start": "", "end": ""}
                for n in members if n != keeper
            ]
        return keeper, []
    retained: list[int] = []
    drops: dict[int, tuple[int, str]] = {}
    for current in order:
        for other in retained:
            pair = pair_data(db, current, other)
            if pair is None:
                continue
            kind, _jaccard, current_coverage, _other_coverage, matches = pair
            current_words, other_words = metas[current]["words"], metas[other]["words"]
            complete_elsewhere = other_words >= current_words and substantial_extra(other_words, current_words)
            safe_variant = kind == "exacto" or safe_to_drop_variant(reader.get(current)[1], matches, "a")
            if safe_variant and kind != "exacto":
                safe_variant = numeric_subsequence(
                    reader.get(current)[0]["texto"], reader.get(other)[0]["texto"])
            if kind == "exacto" or (
                kind == "contenido" and current_coverage >= 0.95 and safe_variant and
                (complete_elsewhere or not substantial_extra(current_words, other_words))
            ) or (
                kind == "casi_total" and current_coverage >= 0.95 and safe_variant and
                not substantial_extra(current_words, other_words)
            ):
                drops[current] = other, kind
                break
        if current not in drops:
            retained.append(current)
    trims: dict[int, list[tuple[int, int, int, str]]] = defaultdict(list)
    for i, first in enumerate(retained):
        for second in retained[i + 1:]:
            pair = pair_data(db, first, second)
            if pair is None:
                continue
            kind, _jaccard, _ca, _cb, matches = pair
            if not matches:
                continue
            if first == keeper or second == keeper:
                winner = keeper
            else:
                winner = min((first, second), key=lambda n: (
                    source_rank(metas[n]["origin"]), -metas[n]["words"], n))
            loser = second if winner == first else first
            record, tokens = reader.get(loser)
            winner_record, winner_tokens = reader.get(winner)
            side = "b" if loser == second else "a"
            identical_matches = []
            for match in matches:
                loser_start = getattr(match, side + "_start")
                loser_end = getattr(match, side + "_end")
                winner_side = "a" if side == "b" else "b"
                winner_start = getattr(match, winner_side + "_start")
                winner_end = getattr(match, winner_side + "_end")
                loser_text = record["texto"][tokens[loser_start].start:tokens[loser_end - 1].end]
                winner_text = winner_record["texto"][winner_tokens[winner_start].start:winner_tokens[winner_end - 1].end]
                if exact_text_key(loser_text) == exact_text_key(winner_text):
                    identical_matches.append(match)
            for start, end in safe_trim_spans(record["texto"], tokens, identical_matches, side):
                trims[loser].append((start, end, winner, kind))
    actions = []
    for n, (target, reason) in sorted(drops.items()):
        actions.append({"record_no": n, "action": "eliminar", "target": target,
                        "reason": reason, "start": "", "end": ""})
    for n, spans in sorted(trims.items()):
        record, _tokens = reader.get(n)
        spans.sort()
        accepted = []
        for start, end, target, reason in spans:
            if accepted and start < accepted[-1][1]:
                if end > accepted[-1][1]:
                    accepted.append((accepted[-1][1], end, target, reason))
            else:
                accepted.append((start, end, target, reason))
        if not remove_spans(record["texto"], [(x[0], x[1]) for x in accepted]):
            continue
        for start, end, target, reason in accepted:
            actions.append({"record_no": n, "action": "recortar", "target": target,
                            "reason": reason, "start": start, "end": end})
    return keeper, actions


def group_signature(group_id: str, members: list[int], keeper: int) -> str:
    payload = json.dumps([VERSION, group_id, members, keeper], separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def export_groups(db: sqlite3.Connection, input_path: Path, out_dir: Path,
                  progress: int, heartbeat: AuditHeartbeat) -> None:
    heartbeat.phase("agrupación de relaciones")
    groups = group_components(db)
    heartbeat.phase("exportación de grupos", total=len(groups), unit="grupos")
    group_path, action_path = out_dir / "grupos.csv", out_dir / "acciones_propuestas.csv"
    member_path = out_dir / "miembros.csv"
    reader = RecordReader(input_path, db)
    oversized = 0
    db.execute("DELETE FROM groups")
    try:
        with group_path.open("w", encoding="utf-8-sig", newline="") as gf, action_path.open("w", encoding="utf-8-sig", newline="") as af, member_path.open("w", encoding="utf-8-sig", newline="") as mf:
            group_writer = csv.DictWriter(gf, fieldnames=GROUP_COLUMNS)
            action_writer = csv.DictWriter(af, fieldnames=ACTION_COLUMNS)
            member_writer = csv.DictWriter(mf, fieldnames=MEMBER_COLUMNS)
            group_writer.writeheader()
            action_writer.writeheader()
            member_writer.writeheader()
            for index, members in enumerate(groups, 1):
                if len(members) > MAX_PLANNED_GROUP:
                    oversized += 1
                group_id = f"g{members[0]:09d}"
                keeper, actions = plan_group(db, reader, members)
                keeper_id = db.execute("SELECT id FROM records WHERE record_no=?", (keeper,)).fetchone()[0]
                signature = group_signature(group_id, members, keeper)
                group_writer.writerow({
                    "group_id": group_id, "keeper_record_no": keeper, "keeper_id": keeper_id,
                    "member_count": len(members),
                    "proposed_deletions": sum(a["action"] == "eliminar" for a in actions),
                    "proposed_trims": sum(a["action"] == "recortar" for a in actions),
                    "signature": signature, "decision": "", "keeper_override": "",
                })
                db.execute("INSERT INTO groups VALUES (?,?,?,?)", (
                    group_id, json.dumps(members), keeper, signature))
                for n in members:
                    meta = db.execute("SELECT id,origin,words FROM records WHERE record_no=?", (n,)).fetchone()
                    record, _tokens = reader.get(n)
                    member_writer.writerow({
                        "group_id": group_id, "record_no": n, "id": meta["id"],
                        "origin": meta["origin"], "words": meta["words"],
                        "preview": record["texto"][:500],
                    })
                for action in actions:
                    n = action["record_no"]
                    meta = db.execute("SELECT id,origin FROM records WHERE record_no=?", (n,)).fetchone()
                    record, _tokens = reader.get(n)
                    preview = (record["texto"][action["start"]:action["end"]]
                               if action["action"] == "recortar" else record["texto"][:240])
                    action_writer.writerow({
                        "group_id": group_id, "record_no": n, "id": meta["id"],
                        "origin": meta["origin"], "action": action["action"],
                        "target_record_no": action["target"], "reason": action["reason"],
                        "start": action["start"], "end": action["end"], "preview": preview[:500],
                    })
                if index % 1000 == 0:
                    db.commit()
                if index % 100 == 0:
                    heartbeat.update(active=index)
                report("grupos exportados", index, progress)
        db.commit()
        heartbeat.update(active=len(groups), confirmed=len(groups))
    finally:
        reader.close()
    put_meta(db, "phase", "exported")
    put_meta(db, "groups_count", len(groups))
    put_meta(db, "oversized_groups", oversized)
    db.commit()


def build_decisions(db: sqlite3.Connection, input_path: Path, review_csv: Path) -> dict:
    expected = db.execute("SELECT COUNT(*) FROM groups").fetchone()[0]
    reviewed = approved = 0
    seen: set[str] = set()
    reader = RecordReader(input_path, db)
    db.execute("DELETE FROM decisions")
    try:
        with review_csv.open("r", encoding="utf-8-sig", newline="") as stream:
            csv_reader = csv.DictReader(stream)
            if set(csv_reader.fieldnames or ()) != set(GROUP_COLUMNS):
                raise ValueError("Columnas de grupos.csv incompletas o alteradas")
            for row in csv_reader:
                group = db.execute("SELECT * FROM groups WHERE group_id=?", (row["group_id"],)).fetchone()
                if group is None or row["signature"] != group["signature"] or row["group_id"] in seen:
                    raise ValueError(f"Grupo desconocido o modificado: {row['group_id']}")
                seen.add(row["group_id"])
                members = json.loads(group["members"])
                if (row["keeper_record_no"] != str(group["keeper_record_no"]) or
                    row["member_count"] != str(len(members)) or
                    row["keeper_id"] != db.execute("SELECT id FROM records WHERE record_no=?", (group["keeper_record_no"],)).fetchone()[0]):
                    raise ValueError(f"Datos inmutables alterados: {row['group_id']}")
                default_keeper, default_actions = plan_group(db, reader, members)
                if (default_keeper != group["keeper_record_no"] or
                    row["proposed_deletions"] != str(sum(a["action"] == "eliminar" for a in default_actions)) or
                    row["proposed_trims"] != str(sum(a["action"] == "recortar" for a in default_actions))):
                    raise ValueError(f"Propuesta modificada: {row['group_id']}")
                if row["decision"] not in ("", "aprobar", "rechazar"):
                    raise ValueError(f"Decisión inválida: {row['group_id']}")
                override = int(row["keeper_override"]) if row["keeper_override"].strip() else None
                if override is not None and override not in members:
                    raise ValueError(f"Representante fuera del grupo: {row['group_id']}")
                reviewed += 1
                if row["decision"] == "aprobar":
                    approved += 1
                    _keeper, actions = plan_group(db, reader, members, override)
                    per_record: dict[int, dict] = {}
                    for action in actions:
                        n = action["record_no"]
                        item = per_record.setdefault(n, {"action": action["action"],
                                                         "target": action["target"], "spans": []})
                        if item["action"] != action["action"]:
                            raise ValueError(f"Acciones contradictorias en {row['group_id']}")
                        if action["action"] == "recortar":
                            item["spans"].append([action["start"], action["end"]])
                    for n, item in per_record.items():
                        db.execute("INSERT INTO decisions VALUES (?,?,?,?,?)", (
                            n, row["group_id"], item["action"], item["target"], json.dumps(item["spans"])))
                if reviewed % 1000 == 0:
                    db.commit()
        if reviewed != expected:
            raise ValueError(f"Se esperaban {expected} grupos y llegaron {reviewed}")
        db.commit()
        return {"groups_total": expected, "groups_approved": approved,
                "actions": db.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]}
    finally:
        reader.close()


def apply_decisions(db: sqlite3.Connection, input_path: Path, out_dir: Path,
                    review_csv: Path, batch_size: int, progress: int, resume: bool) -> None:
    output_path = out_dir / "deduplicado.jsonl"
    log_path = out_dir / "acciones_aplicadas.csv"
    review_hash = sha256_file(review_csv)
    if get_meta(db, "apply_review_hash") is None:
        if output_path.exists() or log_path.exists():
            raise FileExistsError("Salida existente; use otra carpeta o termine la aplicación anterior")
        stats = build_decisions(db, input_path, review_csv)
        put_meta(db, "apply_review_hash", review_hash)
        put_meta(db, "apply_stats", stats)
        put_meta(db, "apply_input_offset", 0)
        put_meta(db, "apply_output_offset", 0)
        put_meta(db, "apply_log_offset", 0)
        put_meta(db, "apply_count", 0)
        db.commit()
    elif not resume or review_hash != get_meta(db, "apply_review_hash"):
        raise ValueError("Aplicación previa: use --resume con el mismo CSV de revisión")
    in_offset = get_meta(db, "apply_input_offset", 0)
    out_offset = get_meta(db, "apply_output_offset", 0)
    log_offset = get_meta(db, "apply_log_offset", 0)
    count = get_meta(db, "apply_count", 0)
    removed = get_meta(db, "apply_removed", 0)
    trimmed = get_meta(db, "apply_trimmed", 0)
    limit = get_meta(db, "limit", 0)
    if out_offset and (not output_path.exists() or output_path.stat().st_size < out_offset):
        raise ValueError("La salida aplicada falta o es más corta que el checkpoint")
    if log_offset and (not log_path.exists() or log_path.stat().st_size < log_offset):
        raise ValueError("El registro de acciones falta o es más corto que el checkpoint")
    out_mode = "r+b" if output_path.exists() else "w+b"
    log_mode = "r+b" if log_path.exists() else "w+b"
    with input_path.open("rb") as source, output_path.open(out_mode) as target, log_path.open(log_mode) as log:
        source.seek(in_offset)
        target.truncate(out_offset)
        target.seek(out_offset)
        log.truncate(log_offset)
        log.seek(log_offset)
        if not log_offset:
            log.write(b"group_id,record_no,id,action,target_record_no,start,end\n")
        while (not limit or count < limit) and (raw := source.readline()):
            count += 1
            record = json_record(raw, count)
            decision = db.execute("SELECT * FROM decisions WHERE record_no=?", (count,)).fetchone()
            if decision is None:
                if STEP_LABEL not in record["procesamiento"]:
                    record["procesamiento"].append(STEP_LABEL)
                target.write((json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8"))
            else:
                meta = db.execute("SELECT text_hash FROM records WHERE record_no=?", (count,)).fetchone()
                if hashlib.sha256(record["texto"].encode("utf-8")).hexdigest() != meta[0]:
                    raise ValueError(f"Texto cambiado en registro {count}")
                if decision["action"] == "eliminar":
                    removed += 1
                else:
                    spans = json.loads(decision["spans"])
                    record["texto"] = remove_spans(record["texto"], spans)
                    if not record["texto"]:
                        raise ValueError(f"Recorte vació el registro {count}")
                    if STEP_LABEL not in record["procesamiento"]:
                        record["procesamiento"].append(STEP_LABEL)
                    target.write((json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8"))
                    trimmed += 1
                for start, end in json.loads(decision["spans"]) or [["", ""]]:
                    line = [decision["group_id"], count, record["id"], decision["action"],
                            decision["target_record_no"], start, end]
                    buffer = io.StringIO()
                    csv.writer(buffer, lineterminator="\n").writerow(line)
                    log.write(buffer.getvalue().encode("utf-8"))
            if count % batch_size == 0:
                target.flush(); os.fsync(target.fileno())
                log.flush(); os.fsync(log.fileno())
                put_meta(db, "apply_input_offset", source.tell())
                put_meta(db, "apply_output_offset", target.tell())
                put_meta(db, "apply_log_offset", log.tell())
                put_meta(db, "apply_count", count)
                put_meta(db, "apply_removed", removed)
                put_meta(db, "apply_trimmed", trimmed)
                db.commit()
            report("aplicación", count, progress)
        target.flush(); os.fsync(target.fileno())
        log.flush(); os.fsync(log.fileno())
        put_meta(db, "apply_input_offset", source.tell())
        put_meta(db, "apply_output_offset", target.tell())
        put_meta(db, "apply_log_offset", log.tell())
        put_meta(db, "apply_count", count)
        put_meta(db, "apply_removed", removed)
        put_meta(db, "apply_trimmed", trimmed)
        put_meta(db, "phase", "applied")
        db.commit()
    summary = {**get_meta(db, "apply_stats"), "input_records": count,
               "output_records": count - removed, "removed_records": removed,
               "trimmed_records": trimmed, "input_sha256": get_meta(db, "input_sha256"),
               "review_sha256": review_hash, "version": VERSION}
    (out_dir / "aplicacion_resumen.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_audit(db: sqlite3.Connection, input_path: Path, out_dir: Path, args) -> None:
    heartbeat = AuditHeartbeat(args.heartbeat_seconds)
    heartbeat.start()
    try:
        phase = get_meta(db, "phase")
        if phase is None:
            index_records(db, input_path, args.batch_size, args.progress_every,
                          args.max_records, heartbeat)
            phase = "indexed"
        if phase == "indexed":
            candidate_pairs(db, args.max_bucket, args.progress_every, heartbeat)
            phase = "candidates"
        if phase == "candidates":
            verify_pairs(db, input_path, args.batch_size, args.progress_every, heartbeat)
            phase = "verified"
        if phase == "verified":
            scope = get_meta(db, "indexed_offset") if args.max_records else None
            byte_count = scope if scope is not None else input_path.stat().st_size
            heartbeat.phase("SHA-256 de entrada", total=byte_count, unit="bytes")
            fingerprint = sha256_file(input_path, scope,
                                      progress_callback=lambda value: heartbeat.update(active=value))
            put_meta(db, "input_sha256", fingerprint)
            db.commit()
            heartbeat.update(active=byte_count, confirmed=byte_count)
            export_groups(db, input_path, out_dir, args.progress_every, heartbeat)
        heartbeat.phase("resumen final")
        summary = {"version": VERSION, "input_records": get_meta(db, "indexed_count"),
                   "input_sha256": get_meta(db, "input_sha256"),
                   "pilot_records": args.max_records,
                   "candidate_pairs": db.execute("SELECT COUNT(*) FROM candidates").fetchone()[0],
                   "verified_pairs": db.execute("SELECT COUNT(*) FROM verified").fetchone()[0],
                   "groups": get_meta(db, "groups_count"),
                   "oversized_groups": get_meta(db, "oversized_groups", 0),
                   "hot_buckets_skipped": get_meta(db, "hot_buckets", 0),
                   "source_preference": list(SOURCE_ORDER),
                   "minimum_passage_words": MIN_PASSAGE_TOKENS}
        (out_dir / "auditoria_resumen.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"Auditoría terminada: {out_dir / 'grupos.csv'}")
    finally:
        heartbeat.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("audit", "apply"), required=True)
    parser.add_argument("--input-file", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--state-dir", type=Path,
        help="Carpeta para SQLite; por defecto coincide con --output-dir",
    )
    parser.add_argument(
        "--checkpoint-mirror-dir", type=Path,
        help="Destino persistente para latest.sqlite3 y latest.json",
    )
    parser.add_argument(
        "--checkpoint-mirror-seconds", type=float, default=300.0,
        help="Intervalo mínimo entre snapshots persistentes; 0 guarda cada commit",
    )
    parser.add_argument("--review-csv", type=Path)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--progress-every", type=int, default=10000)
    parser.add_argument("--heartbeat-seconds", type=float, default=15.0,
                        help="Intervalo del latido de auditoría en segundos; 0 lo desactiva")
    parser.add_argument("--max-bucket", type=int, default=64)
    parser.add_argument("--max-records", type=int, default=0, help="Solo para pilotos; procesa un prefijo del JSONL")
    args = parser.parse_args(argv)
    if (args.batch_size < 1 or args.max_bucket < 2 or args.max_records < 0 or
            args.heartbeat_seconds < 0 or args.checkpoint_mirror_seconds < 0):
        parser.error(
            "batch-size y max-bucket deben ser positivos; max-records, "
            "heartbeat-seconds y checkpoint-mirror-seconds no negativos"
        )
    input_path = args.input_file.resolve()
    out_dir = args.output_dir.resolve()
    state_dir = (args.state_dir or out_dir).resolve()
    if not input_path.is_file():
        parser.error(f"No existe la entrada: {input_path}")
    if out_dir == input_path.parent or out_dir in input_path.parents:
        parser.error("La carpeta de salida debe ser distinta de la entrada")
    if state_dir == input_path.parent or state_dir in input_path.parents:
        parser.error("La carpeta de estado debe ser distinta de la entrada")
    out_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    db_path = state_dir / "dedup_estado.sqlite3"
    mirror = None
    if args.checkpoint_mirror_dir is not None:
        mirror_dir = args.checkpoint_mirror_dir.resolve()
        if mirror_dir == state_dir:
            parser.error("El espejo de checkpoints debe ser distinto de --state-dir")
        mirror = CheckpointMirror(mirror_dir, args.checkpoint_mirror_seconds)
    if db_path.exists() and not args.resume and args.mode == "audit":
        parser.error("Ya existe una auditoría; use --resume u otra carpeta de estado")
    if args.mode == "audit" and not db_path.exists() and any(
        (out_dir / name).exists() for name in (
            "grupos.csv", "miembros.csv", "acciones_propuestas.csv", "auditoria_resumen.json",
            "deduplicado.jsonl", "acciones_aplicadas.csv")):
        parser.error("La carpeta ya contiene salidas del paso 4; use otra carpeta")
    if args.mode == "apply" and not db_path.exists():
        parser.error("Primero hay que completar --mode audit")
    db = connect(db_path, checkpoint_callback=mirror)
    checkpoint_ready = False
    try:
        identity = quick_identity(input_path)
        initial = get_meta(db, "input_identity")
        if initial is None:
            put_meta(db, "input_identity", identity)
            put_meta(db, "input_path", str(input_path))
            put_meta(db, "limit", args.max_records)
            put_meta(db, "max_bucket", args.max_bucket)
            db.commit()
        elif (not compatible_identity(initial, identity) or
              (args.mode == "audit" and (get_meta(db, "limit") != args.max_records or
                                           get_meta(db, "max_bucket") != args.max_bucket))):
            raise ValueError("Entrada o configuración incompatible con la auditoría existente")
        elif initial != identity or get_meta(db, "input_path") != str(input_path):
            put_meta(db, "input_identity", identity)
            put_meta(db, "input_path", str(input_path))
            db.commit()
        checkpoint_ready = True
        phase = get_meta(db, "phase")
        if args.mode == "audit":
            run_audit(db, input_path, out_dir, args)
        else:
            if phase not in ("exported", "applied"):
                raise ValueError("La auditoría aún no terminó")
            scope = get_meta(db, "indexed_offset") if get_meta(db, "limit", 0) else None
            if sha256_file(input_path, scope) != get_meta(db, "input_sha256"):
                raise ValueError("La entrada cambió desde la auditoría")
            review_csv = (args.review_csv or out_dir / "grupos.csv").resolve()
            if not review_csv.is_file():
                raise FileNotFoundError(review_csv)
            apply_decisions(db, input_path, out_dir, review_csv,
                            args.batch_size, args.progress_every, args.resume)
            print(f"Corpus deduplicado: {out_dir / 'deduplicado.jsonl'}")
        if mirror is not None:
            mirror.snapshot(db, force=True)
        return 0
    except BaseException:
        if mirror is not None and checkpoint_ready:
            try:
                mirror.snapshot(db, force=True)
            except (OSError, sqlite3.Error) as snapshot_error:
                print(
                    f"Advertencia: no se pudo guardar el snapshot final: {snapshot_error}",
                    file=sys.stderr,
                    flush=True,
                )
        raise
    finally:
        db.close()

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, sqlite3.Error) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
