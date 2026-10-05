"""Revisión persistente de candidatos, sin dependencias de interfaz ni pandas.

SQLite es el checkpoint: filas, posición de importación y contadores se confirman
en la misma transacción. El CSV auditado nunca se modifica.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import importlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from typing import Callable, Iterator, Mapping

workflow = importlib.import_module("3_Eliminar_ruido")
from ruido_core import AUDIT_VERSION, DELETE_RECORD, DELETE_SPAN, build_candidate_id, sha256_text

SCHEMA_VERSION = 1
DEFAULT_CSV = workflow.DEFAULT_OUTPUT_DIR / "candidatos_ruido.csv"
DECISIONS = ("", "eliminar", "conservar")
ProgressCallback = Callable[[int, int, str], None]


class ReviewConflictError(ValueError):
    """Otra pestaña editó este candidato desde que se mostró."""


def connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA cache_size=-32768")
    connection.execute("PRAGMA temp_store=FILE")
    return connection


def get_metadata(connection: sqlite3.Connection) -> dict[str, object]:
    return {row["key"]: json.loads(row["value"]) for row in connection.execute("SELECT * FROM metadata")}


def set_metadata(connection: sqlite3.Connection, values: Mapping[str, object]) -> None:
    connection.executemany(
        "INSERT OR REPLACE INTO metadata(key, value) VALUES (?, ?)",
        ((key, json.dumps(value, ensure_ascii=False, sort_keys=True)) for key, value in values.items()),
    )


def read_manifest(source_csv: Path) -> tuple[dict[str, object], str]:
    summary_path = source_csv.with_name("auditoria_resumen.json")
    with summary_path.open("r", encoding="utf-8") as summary_file:
        summary = json.load(summary_file)
    if summary.get("version_auditoria") != AUDIT_VERSION:
        raise ValueError("La versión del resumen no es compatible con esta revisión.")
    fingerprint = summary.get("fingerprint_entrada")
    candidate_set = summary.get("conjunto_inmutable_candidatos")
    if not isinstance(fingerprint, dict) or not isinstance(fingerprint.get("sha256"), str):
        raise ValueError("El resumen no contiene el SHA-256 de la entrada.")
    if not isinstance(candidate_set, dict) or candidate_set.get("count") != summary.get("total_candidatos"):
        raise ValueError("El resumen no contiene un conjunto completo de candidatos válido.")
    if not isinstance(summary.get("total_candidatos"), int) or summary["total_candidatos"] < 0:
        raise ValueError("El total de candidatos del resumen no es válido.")
    checkpoint_path = source_csv.with_name("auditoria.checkpoint.json")
    if checkpoint_path.exists():
        with checkpoint_path.open("r", encoding="utf-8") as checkpoint_file:
            checkpoint = json.load(checkpoint_file)
        if checkpoint.get("completed") is not True:
            raise ValueError("La auditoría todavía no terminó. Finalizala antes de revisar.")
    return summary, workflow.full_sha256(summary_path)


def validate_candidate(row: dict[str, str], input_sha256: str) -> None:
    if row["decision"] not in DECISIONS:
        raise ValueError(f"Decisión desconocida: {row['decision']!r}.")
    if row["version_auditoria"] != AUDIT_VERSION or row["fingerprint_entrada_sha256"] != input_sha256:
        raise ValueError("Un candidato no corresponde a esta auditoría.")
    start, end = int(row["inicio_caracter"]), int(row["fin_caracter"])
    if int(row["numero_registro"]) <= 0 or start < 0 or end < start:
        raise ValueError("Número de registro u offsets inválidos.")
    fragment = row["texto_detectado"]
    if len(fragment) != end - start or sha256_text(fragment) != row["hash_fragmento"]:
        raise ValueError("El fragmento fue modificado o no coincide con sus offsets.")
    if int(row["longitud_texto_detectado"]) != len(fragment):
        raise ValueError("La longitud declarada no coincide con el fragmento.")
    if row["accion_propuesta"] not in (DELETE_SPAN, DELETE_RECORD):
        raise ValueError("Acción propuesta desconocida.")
    expected_id = build_candidate_id(
        input_sha256=input_sha256,
        stable_record_key=row["clave_estable_registro"],
        reason_codes=tuple(filter(None, row["codigos_motivo"].split("|"))),
        start=start,
        end=end,
        fragment_hash=row["hash_fragmento"],
    )
    if row["candidate_id"] != expected_id:
        raise ValueError("candidate_id alterado o inconsistente.")
    if not isinstance(json.loads(row["metricas"]), dict):
        raise ValueError("Las métricas deben ser un objeto JSON.")


class BinaryCsvLines:
    """No hace read-ahead: tell() es un offset seguro incluso con celdas multilínea."""

    def __init__(self, binary_file, *, initial: bool):
        self.binary_file = binary_file
        self.initial = initial

    def __iter__(self) -> Iterator[str]:
        return self

    def __next__(self) -> str:
        raw_line = self.binary_file.readline()
        if not raw_line:
            raise StopIteration
        encoding = "utf-8-sig" if self.initial else "utf-8"
        self.initial = False
        return raw_line.decode(encoding, errors="strict")


def prepare_store(
    source_csv: Path,
    state_db: Path,
    *,
    resume: bool = False,
    batch_size: int = 500,
    max_batch_bytes: int = 8 * 1024 * 1024,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    """Importa una vez o retoma el último lote; nunca reinicia decisiones existentes."""
    source_csv, state_db = source_csv.resolve(), state_db.resolve()
    if batch_size < 1 or max_batch_bytes < 1:
        raise ValueError("El tamaño de lote debe ser positivo.")
    if state_db.suffix != ".sqlite3" or state_db == source_csv:
        raise ValueError("El estado debe ser un archivo .sqlite3 separado del CSV.")
    reserved = {
        source_csv.with_name("auditoria_global.sqlite3"),
        source_csv.with_name("aplicacion_decisiones.sqlite3"),
    }
    if state_db in reserved:
        raise ValueError("No se puede usar una base de auditoría o aplicación como estado de revisión.")
    exists = state_db.exists()
    if exists and not resume:
        raise FileExistsError("Ya existe una revisión. Volvé a arrancar agregando --resume; no se borró nada.")
    if resume and not exists:
        raise FileNotFoundError("No existe una revisión para retomar. La primera vez ejecutá sin --resume.")
    summary, summary_sha256 = read_manifest(source_csv)
    total = int(summary["total_candidatos"])
    if progress:
        progress(0, total, "Verificando el CSV completo")
    source_fingerprint = workflow.quick_fingerprint(source_csv)
    csv_sha256 = workflow.full_sha256(source_csv)
    if workflow.quick_fingerprint(source_csv) != source_fingerprint:
        raise ValueError("El CSV cambió durante la verificación.")
    state_db.parent.mkdir(parents=True, exist_ok=True)
    connection = connect(state_db)
    try:
        if not exists:
            connection.executescript(
                """
                CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE candidates (
                    seq INTEGER PRIMARY KEY,
                    candidate_id TEXT NOT NULL UNIQUE,
                    origin TEXT NOT NULL,
                    row_json TEXT NOT NULL,
                    decision TEXT NOT NULL CHECK(decision IN ('', 'eliminar', 'conservar')),
                    notes TEXT NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT
                );
                CREATE INDEX idx_candidates_decision ON candidates(decision, seq);
                CREATE INDEX idx_candidates_origin ON candidates(origin, decision, seq);
                CREATE TABLE candidate_reasons (
                    reason TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    PRIMARY KEY(reason, seq)
                ) WITHOUT ROWID;
                """
            )
            with connection:
                set_metadata(connection, {
                    "schema_version": SCHEMA_VERSION,
                    "source_csv": str(source_csv),
                    "source_fingerprint": source_fingerprint,
                    "csv_sha256": csv_sha256,
                    "summary_sha256": summary_sha256,
                    "input_sha256": summary["fingerprint_entrada"]["sha256"],
                    "input_path": summary["fingerprint_entrada"].get("path", str(workflow.DEFAULT_INPUT_PATH)),
                    "expected_set": summary["conjunto_inmutable_candidatos"],
                    "candidate_set": workflow.fresh_candidate_set_accumulator(),
                    "phase": "importing", "input_offset": 0,
                    "counts": {"total": 0, "pending": 0, "eliminar": 0, "conservar": 0},
                    "revision": 0, "last_seq": 1,
                    "export_revision": -1, "export_sha256": "", "export_path": "",
                })
        metadata = get_metadata(connection)
        if metadata.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("Versión de estado de revisión incompatible.")
        if metadata.get("source_csv") != str(source_csv) or metadata.get("csv_sha256") != csv_sha256:
            raise ValueError("El CSV cambió o es de otra auditoría. No se puede retomar esta revisión.")
        if metadata.get("summary_sha256") != summary_sha256:
            raise ValueError("El resumen de auditoría cambió desde el inicio de la revisión.")
        if metadata["phase"] == "ready":
            return metadata
        _import_candidates(connection, source_csv, metadata, total, batch_size, max_batch_bytes, progress)
        if workflow.quick_fingerprint(source_csv) != source_fingerprint:
            raise ValueError("El CSV cambió durante la importación.")
        metadata = get_metadata(connection)
        if metadata["candidate_set"] != metadata["expected_set"]:
            raise ValueError("El CSV no contiene exactamente todos los candidatos. No uses la muestra.")
        with connection:
            set_metadata(connection, {
                "phase": "ready",
                "origins": [row[0] for row in connection.execute("SELECT DISTINCT origin FROM candidates ORDER BY origin")],
                "reasons": [row[0] for row in connection.execute("SELECT DISTINCT reason FROM candidate_reasons ORDER BY reason")],
            })
        return get_metadata(connection)
    finally:
        connection.close()


def _import_candidates(connection, source_csv, metadata, total, batch_size, max_batch_bytes, progress):
    workflow.allow_large_csv_fields()
    offset = int(metadata["input_offset"])
    counts = dict(metadata["counts"])
    accumulator = dict(metadata["candidate_set"])
    if progress:
        progress(counts["total"], total, "Importando candidatos a SQLite")
    with source_csv.open("rb") as source_file:
        source_file.seek(offset)
        lines = BinaryCsvLines(source_file, initial=offset == 0)
        reader = csv.DictReader(lines, fieldnames=None if offset == 0 else workflow.CSV_COLUMNS)
        if reader.fieldnames != workflow.CSV_COLUMNS:
            raise ValueError("Las columnas del CSV no coinciden con el formato completo de audit.")
        batch_count, batch_start = 0, source_file.tell()
        try:
            for raw_row in reader:
                if None in raw_row or any(value is None for value in raw_row.values()):
                    raise ValueError("Fila CSV con columnas faltantes o sobrantes.")
                row = dict(raw_row)
                validate_candidate(row, str(metadata["input_sha256"]))
                seq = counts["total"] + 1
                try:
                    connection.execute(
                        "INSERT INTO candidates(seq, candidate_id, origin, row_json, decision, notes) VALUES (?, ?, ?, ?, ?, ?)",
                        (seq, row["candidate_id"], row["base_de_datos_origen"],
                         json.dumps(row, ensure_ascii=False), row["decision"], row["notas_revision"]),
                    )
                except sqlite3.IntegrityError as error:
                    raise ValueError(f"candidate_id duplicado: {row['candidate_id']}.") from error
                connection.executemany(
                    "INSERT INTO candidate_reasons(reason, seq) VALUES (?, ?)",
                    ((reason, seq) for reason in sorted(set(filter(None, row["codigos_motivo"].split("|"))))),
                )
                counts["total"] += 1
                counts[row["decision"] or "pending"] += 1
                workflow.update_candidate_set_accumulator(accumulator, (row["candidate_id"],))
                batch_count += 1
                if batch_count >= batch_size or source_file.tell() - batch_start >= max_batch_bytes:
                    set_metadata(connection, {"input_offset": source_file.tell(), "counts": counts, "candidate_set": accumulator})
                    connection.commit()
                    batch_count, batch_start = 0, source_file.tell()
                    if progress:
                        progress(counts["total"], total, "Importando candidatos a SQLite")
            set_metadata(connection, {"input_offset": source_file.tell(), "counts": counts, "candidate_set": accumulator})
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
    if progress:
        progress(counts["total"], total, "Importación completa")


class ReviewStore:
    def __init__(self, state_db: Path):
        self.state_db = state_db.resolve()

    def metadata(self) -> dict[str, object]:
        connection = connect(self.state_db)
        try:
            metadata = get_metadata(connection)
            if metadata.get("phase") != "ready":
                raise ValueError("La importación no está completa. Volvé a arrancar con --resume.")
            return metadata
        finally:
            connection.close()

    @staticmethod
    def _filters(status: str | None, origin: str | None, reason: str | None) -> tuple[str, list[str]]:
        clauses, values = [], []
        if status is not None:
            if status not in DECISIONS:
                raise ValueError("Filtro de decisión desconocido.")
            clauses.append("c.decision = ?")
            values.append(status)
        if origin is not None:
            clauses.append("c.origin = ?")
            values.append(origin)
        if reason is not None:
            clauses.append("EXISTS (SELECT 1 FROM candidate_reasons r WHERE r.seq = c.seq AND r.reason = ?)")
            values.append(reason)
        return (" AND ".join(clauses) or "1"), values

    def count(self, *, status=None, origin=None, reason=None) -> int:
        where, values = self._filters(status, origin, reason)
        connection = connect(self.state_db)
        try:
            return int(connection.execute(f"SELECT COUNT(*) FROM candidates c WHERE {where}", values).fetchone()[0])
        finally:
            connection.close()

    def candidate(self, seq: int) -> dict[str, object] | None:
        connection = connect(self.state_db)
        try:
            row = connection.execute("SELECT * FROM candidates WHERE seq = ?", (seq,)).fetchone()
            if row is None:
                return None
            result = json.loads(row["row_json"])
            result.update({"decision": row["decision"], "notas_revision": row["notes"],
                           "seq": row["seq"], "revision": row["revision"]})
            return result
        finally:
            connection.close()

    def find(self, *, after: int = 0, direction: int = 1, status=None, origin=None, reason=None, wrap=True) -> int | None:
        where, values = self._filters(status, origin, reason)
        comparison, order = (">", "ASC") if direction >= 0 else ("<", "DESC")
        connection = connect(self.state_db)
        try:
            row = connection.execute(
                f"SELECT c.seq FROM candidates c WHERE {where} AND c.seq {comparison} ? ORDER BY c.seq {order} LIMIT 1",
                [*values, after],
            ).fetchone()
            if row is None and wrap:
                row = connection.execute(
                    f"SELECT c.seq FROM candidates c WHERE {where} ORDER BY c.seq {order} LIMIT 1", values,
                ).fetchone()
            return None if row is None else int(row[0])
        finally:
            connection.close()

    def remember(self, seq: int | None) -> None:
        if seq is None:
            return
        connection = connect(self.state_db)
        try:
            with connection:
                set_metadata(connection, {"last_seq": seq})
        finally:
            connection.close()

    def save(self, seq: int, decision: str, notes: str, *, expected_revision: int) -> None:
        if decision not in DECISIONS:
            raise ValueError("La decisión debe ser eliminar, conservar o pendiente.")
        connection = connect(self.state_db)
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT decision, revision FROM candidates WHERE seq = ?", (seq,)).fetchone()
            if row is None:
                raise ValueError("El candidato no existe.")
            if row["revision"] != expected_revision:
                raise ReviewConflictError("Otra pestaña cambió este candidato. Recargá antes de guardar.")
            metadata = get_metadata(connection)
            if metadata.get("phase") != "ready":
                raise ValueError("La revisión todavía no está lista.")
            counts = dict(metadata["counts"])
            counts[row["decision"] or "pending"] -= 1
            counts[decision or "pending"] += 1
            connection.execute(
                "UPDATE candidates SET decision = ?, notes = ?, revision = revision + 1, updated_at = ? WHERE seq = ?",
                (decision, notes, datetime.now(timezone.utc).isoformat(), seq),
            )
            set_metadata(connection, {"counts": counts, "revision": int(metadata["revision"]) + 1, "last_seq": seq})
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def export(self, output_csv: Path, *, progress: ProgressCallback | None = None) -> dict[str, object]:
        """Snapshot completo y atómico. Las únicas columnas editadas son decisión/notas."""
        output_csv = output_csv.resolve()
        connection = connect(self.state_db)
        temporary_path = None
        try:
            connection.execute("BEGIN IMMEDIATE")
            metadata = get_metadata(connection)
            if metadata.get("phase") != "ready":
                raise ValueError("No se puede exportar una importación incompleta.")
            source_csv = Path(str(metadata["source_csv"]))
            if output_csv == source_csv or output_csv.parent != source_csv.parent or output_csv.suffix != ".csv":
                raise ValueError("Exportá a otro .csv en la misma carpeta del CSV original y su resumen.")
            if output_csv.name in {"muestra_revision_ruido.csv", "eliminaciones_aplicadas.csv"}:
                raise ValueError("No se puede sobrescribir la muestra ni el log de aplicación.")
            summary, summary_sha = read_manifest(source_csv)
            if summary_sha != metadata["summary_sha256"] or workflow.full_sha256(source_csv) != metadata["csv_sha256"]:
                raise ValueError("El CSV original o su resumen cambiaron. No se exportó nada.")
            if output_csv.exists():
                previous_hash = workflow.full_sha256(output_csv)
                known_hashes = [metadata.get("export_sha256"), metadata.get("pending_export_sha256")]
                known_paths = [metadata.get("export_path"), metadata.get("pending_export_path")]
                if str(output_csv) not in known_paths or previous_hash not in known_hashes:
                    raise FileExistsError("El archivo de salida existe y no es una exportación intacta de esta revisión. Elegí otro --export-csv.")
            total = int(metadata["counts"]["total"])
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8-sig", newline="", dir=output_csv.parent,
                                             prefix=output_csv.name + ".", suffix=".tmp", delete=False) as output_file:
                temporary_path = Path(output_file.name)
                writer = csv.DictWriter(output_file, fieldnames=workflow.CSV_COLUMNS, lineterminator="\n")
                writer.writeheader()
                accumulator = workflow.fresh_candidate_set_accumulator()
                for seq, row in enumerate(connection.execute("SELECT * FROM candidates ORDER BY seq"), start=1):
                    csv_row = json.loads(row["row_json"])
                    csv_row["decision"], csv_row["notas_revision"] = row["decision"], row["notes"]
                    writer.writerow(csv_row)
                    workflow.update_candidate_set_accumulator(accumulator, (csv_row["candidate_id"],))
                    if progress and (seq % 1000 == 0 or seq == total):
                        progress(seq, total, "Exportando CSV completo")
                if accumulator != metadata["expected_set"]:
                    raise ValueError("El estado no contiene exactamente el conjunto original de candidatos.")
                output_file.flush()
                os.fsync(output_file.fileno())
            export_sha = workflow.full_sha256(temporary_path)
            # Este marcador permite recuperar un cierre justo después de os.replace.
            set_metadata(connection, {"pending_export_path": str(output_csv), "pending_export_sha256": export_sha})
            connection.commit()
            connection.execute("BEGIN IMMEDIATE")
            if int(get_metadata(connection)["revision"]) != int(metadata["revision"]):
                raise ReviewConflictError("Se guardaron decisiones durante la exportación. Volvé a exportar.")
            os.replace(temporary_path, output_csv)
            temporary_path = None
            set_metadata(connection, {"export_path": str(output_csv), "export_sha256": export_sha,
                                      "export_revision": metadata["revision"], "pending_export_path": "", "pending_export_sha256": ""})
            connection.commit()
            return {"path": str(output_csv), "sha256": export_sha, "revision": metadata["revision"],
                    "counts": metadata["counts"], "input_path": summary["fingerprint_entrada"].get("path")}
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
            if temporary_path is not None and temporary_path.exists():
                temporary_path.unlink()


def apply_command(input_path: Path, review_csv: Path, *, resume: bool = False) -> str:
    script = Path(workflow.__file__).resolve()
    paths = [str(script), str(input_path.resolve()), str(review_csv.resolve()), str(review_csv.resolve().parent)]
    if any('"' in value or "\n" in value or "\r" in value for value in paths):
        raise ValueError("Ruta no válida para generar un comando de PowerShell.")
    # Comillas simples: rutas con $ o backticks no se interpretan en PowerShell.
    quoted = ["'" + value.replace("'", "''") + "'" for value in paths]
    return (f"python {quoted[0]} `\n  --mode apply `\n  --input-file {quoted[1]} `\n"
            f"  --review-csv {quoted[2]} `\n  --output-dir {quoted[3]}" + (" `\n  --resume" if resume else ""))
