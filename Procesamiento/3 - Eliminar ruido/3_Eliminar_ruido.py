from __future__ import annotations

import argparse
from collections import Counter
import csv
from dataclasses import dataclass
import functools
import hashlib
import io
import json
import os
from pathlib import Path
import random
import sqlite3
import sys
import time
from typing import BinaryIO, Iterable, Iterator, Mapping, Sequence

from ruido_core import (
    AUDIT_VERSION,
    DELETE_RECORD,
    DELETE_SPAN,
    ConsolidatedResult,
    DetectionContext,
    Thresholds,
    block_spans,
    build_candidate_id,
    build_detectors,
    consolidate_results,
    detector_catalog,
    normalized_block_hash,
    record_stable_key,
    sha256_text,
    thresholds_as_dict,
    unicode_words,
)


PASO_PROCESAMIENTO = "Paso 3: Eliminacion de ruido"
CHECKPOINT_VERSION = 1
CSV_BOM = b"\xef\xbb\xbf"
CONTEXT_CHARACTERS = 180

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_PATH = REPO_ROOT / "Base de datos" / "2 - Normalizacion" / "normalizado.jsonl"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "Base de datos" / "3 - Eliminar ruido"

CSV_COLUMNS = [
    "candidate_id",
    "numero_registro",
    "id",
    "base_de_datos_origen",
    "archivo_origen",
    "ruta_relativa_origen",
    "tipo_deteccion",
    "codigos_motivo",
    "nivel",
    "confianza",
    "inicio_caracter",
    "fin_caracter",
    "texto_detectado",
    "contexto_antes",
    "contexto_despues",
    "longitud_texto_detectado",
    "metricas",
    "accion_propuesta",
    "version_auditoria",
    "fingerprint_entrada_sha256",
    "hash_registro",
    "hash_fragmento",
    "clave_estable_registro",
    "decision",
    "notas_revision",
]
APPLIED_CSV_COLUMNS = CSV_COLUMNS + ["resultado_aplicacion"]
REQUIRED_REVIEW_COLUMNS = set(CSV_COLUMNS)


@dataclass(frozen=True)
class InputRecord:
    record_number: int
    physical_line_number: int
    raw_line: bytes
    end_offset: int
    record: dict[str, object]


@dataclass(frozen=True)
class AuditPaths:
    candidates_csv: Path
    sample_csv: Path
    summary_json: Path
    checkpoint_json: Path
    state_db: Path


@dataclass(frozen=True)
class ApplyPaths:
    output_jsonl: Path
    applied_csv: Path
    summary_json: Path
    checkpoint_json: Path
    state_db: Path


def quick_fingerprint(path: Path) -> dict[str, int | str]:
    stat = path.stat()
    sample_size = 64 * 1024
    digest = hashlib.sha256()
    with path.open("rb") as input_file:
        digest.update(input_file.read(sample_size))
        if stat.st_size > sample_size:
            input_file.seek(max(0, stat.st_size - sample_size))
            digest.update(input_file.read(sample_size))
    return {
        "path": str(path.resolve()),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "sample_sha256": digest.hexdigest(),
    }


def full_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as input_file:
        while chunk := input_file.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def write_json_atomic(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    with temporary_path.open("w", encoding="utf-8", newline="\n") as output_file:
        json.dump(value, output_file, ensure_ascii=False, indent=2, sort_keys=True)
        output_file.write("\n")
        output_file.flush()
        os.fsync(output_file.fileno())
    os.replace(temporary_path, path)


def read_checkpoint(path: Path, expected_mode: str) -> dict[str, object]:
    try:
        with path.open("r", encoding="utf-8") as checkpoint_file:
            state = json.load(checkpoint_file)
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"No se pudo leer el checkpoint: {path}") from error
    if (
        not isinstance(state, dict)
        or state.get("checkpoint_version") != CHECKPOINT_VERSION
        or state.get("mode") != expected_mode
    ):
        raise ValueError(f"Checkpoint incompatible: {path}")
    return state


def decode_json_record(raw_line: bytes, physical_line_number: int) -> dict[str, object]:
    try:
        line = raw_line.decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise ValueError(
            f"La linea fisica {physical_line_number:,} no es UTF-8 valido "
            f"(byte {error.start})."
        ) from error
    if physical_line_number == 1:
        line = line.removeprefix("\ufeff")
    try:
        record = json.loads(line)
    except json.JSONDecodeError as error:
        raise ValueError(
            f"JSON invalido en la linea fisica {physical_line_number:,}, "
            f"columna {error.colno}: {error.msg}."
        ) from error
    if not isinstance(record, dict):
        raise ValueError(
            f"La linea fisica {physical_line_number:,} no contiene un objeto JSON."
        )
    text = record.get("texto")
    if not isinstance(text, str):
        raise ValueError(
            f"El campo 'texto' de la linea fisica {physical_line_number:,} no es texto."
        )
    processing = record.get("procesamiento")
    if not isinstance(processing, list) or not all(isinstance(item, str) for item in processing):
        raise ValueError(
            f"El campo 'procesamiento' de la linea fisica {physical_line_number:,} "
            "no es una lista de textos."
        )
    return record


def iter_input_records(
    input_file: BinaryIO,
    *,
    first_record_number: int = 1,
    first_physical_line_number: int = 1,
) -> Iterator[InputRecord]:
    record_number = first_record_number - 1
    physical_line_number = first_physical_line_number - 1
    while True:
        raw_line = input_file.readline()
        if not raw_line:
            return
        physical_line_number += 1
        end_offset = input_file.tell()
        if not raw_line.strip():
            continue
        record_number += 1
        yield InputRecord(
            record_number=record_number,
            physical_line_number=physical_line_number,
            raw_line=raw_line,
            end_offset=end_offset,
            record=decode_json_record(raw_line, physical_line_number),
        )


def iter_batches(
    records: Iterable[InputRecord], batch_size: int, max_batch_bytes: int
) -> Iterator[list[InputRecord]]:
    batch: list[InputRecord] = []
    batch_bytes = 0
    for record in records:
        size = len(record.raw_line)
        if batch and (len(batch) >= batch_size or batch_bytes + size > max_batch_bytes):
            yield batch
            batch = []
            batch_bytes = 0
        batch.append(record)
        batch_bytes += size
    if batch:
        yield batch


def open_csv_writer(
    path: Path,
    columns: Sequence[str],
    *,
    resume_offset: int | None = None,
) -> tuple[BinaryIO, io.TextIOWrapper, csv.DictWriter]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if resume_offset is None:
        binary_file = path.open("w+b")
        binary_file.write(CSV_BOM)
    else:
        binary_file = path.open("r+b")
        if binary_file.seek(0, os.SEEK_END) < resume_offset:
            binary_file.close()
            raise ValueError(f"El CSV parcial es mas corto que el checkpoint: {path}")
        binary_file.truncate(resume_offset)
        binary_file.seek(resume_offset)
    text_file = io.TextIOWrapper(binary_file, encoding="utf-8", newline="", write_through=False)
    writer = csv.DictWriter(
        text_file,
        fieldnames=list(columns),
        extrasaction="ignore",
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n",
    )
    if resume_offset is None:
        writer.writeheader()
        text_file.flush()
        os.fsync(binary_file.fileno())
    return binary_file, text_file, writer


def csv_byte_offset(binary_file: BinaryIO, text_file: io.TextIOWrapper) -> int:
    text_file.flush()
    return binary_file.tell()


def fsync_csv(binary_file: BinaryIO, text_file: io.TextIOWrapper) -> int:
    offset = csv_byte_offset(binary_file, text_file)
    os.fsync(binary_file.fileno())
    return offset


def connect_sqlite(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA temp_store=FILE")
    connection.row_factory = sqlite3.Row
    return connection


def initialize_audit_database(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS documents (
            record_number INTEGER PRIMARY KEY,
            stable_key TEXT NOT NULL,
            origin TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS block_documents (
            block_hash TEXT NOT NULL,
            record_number INTEGER NOT NULL,
            PRIMARY KEY (block_hash, record_number)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS block_stats (
            block_hash TEXT PRIMARY KEY,
            document_count INTEGER NOT NULL,
            start_count INTEGER NOT NULL,
            end_count INTEGER NOT NULL,
            middle_count INTEGER NOT NULL,
            character_count INTEGER NOT NULL,
            word_count INTEGER NOT NULL,
            sample TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS block_origins (
            block_hash TEXT NOT NULL,
            origin TEXT NOT NULL,
            document_count INTEGER NOT NULL,
            PRIMARY KEY (block_hash, origin)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS idx_block_documents_record
            ON block_documents(record_number);
        """
    )
    connection.commit()


def index_record_blocks(connection: sqlite3.Connection, item: InputRecord) -> None:
    record = item.record
    origin = str(record.get("base_de_datos_origen") or "")
    inserted = connection.execute(
        "INSERT OR IGNORE INTO documents(record_number, stable_key, origin) VALUES (?, ?, ?)",
        (item.record_number, record_stable_key(record, item.record_number), origin),
    ).rowcount
    if not inserted:
        return
    text = str(record["texto"])
    per_hash: dict[str, dict[str, object]] = {}
    blocks = block_spans(text)
    for block_index, block in enumerate(blocks):
        block_hash = normalized_block_hash(block.text)
        values = per_hash.setdefault(
            block_hash,
            {
                "start": False,
                "end": False,
                "middle": False,
                "character_count": len(block.text),
                "word_count": len(unicode_words(block.text)),
                "sample": block.text[:500],
            },
        )
        values["start"] = bool(values["start"] or block_index == 0)
        values["end"] = bool(values["end"] or block_index == len(blocks) - 1)
        values["middle"] = bool(
            values["middle"] or (block_index not in {0, len(blocks) - 1})
        )

    for block_hash, values in per_hash.items():
        connection.execute(
            "INSERT INTO block_documents(block_hash, record_number) VALUES (?, ?)",
            (block_hash, item.record_number),
        )
        connection.execute(
            """
            INSERT INTO block_stats(
                block_hash, document_count, start_count, end_count, middle_count,
                character_count, word_count, sample
            ) VALUES (?, 1, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(block_hash) DO UPDATE SET
                document_count = document_count + 1,
                start_count = start_count + excluded.start_count,
                end_count = end_count + excluded.end_count,
                middle_count = middle_count + excluded.middle_count
            """,
            (
                block_hash,
                int(bool(values["start"])),
                int(bool(values["end"])),
                int(bool(values["middle"])),
                int(values["character_count"]),
                int(values["word_count"]),
                str(values["sample"]),
            ),
        )
        connection.execute(
            """
            INSERT INTO block_origins(block_hash, origin, document_count)
            VALUES (?, ?, 1)
            ON CONFLICT(block_hash, origin) DO UPDATE SET
                document_count = document_count + 1
            """,
            (block_hash, origin),
        )


def audit_configuration(
    thresholds: Thresholds,
    detectors: Sequence[object],
    review_sample_size: int,
    sampling_seed: int,
) -> dict[str, object]:
    return {
        "audit_version": AUDIT_VERSION,
        "thresholds": thresholds_as_dict(thresholds),
        "enabled_detectors": [getattr(detector, "reason_code") for detector in detectors],
        "review_sample_size": review_sample_size,
        "sampling_seed": sampling_seed,
    }


def fresh_audit_counters() -> dict[str, object]:
    return {
        "records_processed": 0,
        "records_with_candidates": 0,
        "total_candidates": 0,
        "candidates_by_reason": {},
        "candidates_by_origin": {},
        "candidates_by_level": {},
        "confidence_distribution": {"baja": 0, "media": 0, "alta": 0},
        "proposed_characters": 0,
        "full_records_proposed": 0,
        "metric_distributions": {},
        "errors": 0,
    }


def fresh_candidate_set_accumulator() -> dict[str, object]:
    return {"count": 0, "xor_sha256": "0" * 64, "sum_sha256": "0" * 64}


def update_candidate_set_accumulator(
    accumulator: dict[str, object], candidate_ids: Iterable[str]
) -> None:
    xor_value = int(str(accumulator["xor_sha256"]), 16)
    sum_value = int(str(accumulator["sum_sha256"]), 16)
    count = int(accumulator["count"])
    modulus = 1 << 256
    for candidate_id in candidate_ids:
        numeric_id = int(candidate_id, 16)
        xor_value ^= numeric_id
        sum_value = (sum_value + numeric_id) % modulus
        count += 1
    accumulator["count"] = count
    accumulator["xor_sha256"] = f"{xor_value:064x}"
    accumulator["sum_sha256"] = f"{sum_value:064x}"


def _increment(mapping: dict[str, int], key: str, amount: int = 1) -> None:
    mapping[key] = mapping.get(key, 0) + amount


def _numeric_metrics(value: object, prefix: str = "") -> Iterator[tuple[str, float]]:
    if isinstance(value, bool):
        return
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if number == number and number not in {float("inf"), float("-inf")}:
            yield prefix, number
        return
    if isinstance(value, dict):
        for key in sorted(value):
            nested_prefix = f"{prefix}.{key}" if prefix else str(key)
            yield from _numeric_metrics(value[key], nested_prefix)


def update_audit_counters(
    counters: dict[str, object],
    record: Mapping[str, object],
    candidates: Sequence[ConsolidatedResult],
) -> None:
    counters["records_processed"] = int(counters["records_processed"]) + 1
    if candidates:
        counters["records_with_candidates"] = int(counters["records_with_candidates"]) + 1
    origin = str(record.get("base_de_datos_origen") or "")
    by_reason = counters["candidates_by_reason"]
    by_origin = counters["candidates_by_origin"]
    by_level = counters["candidates_by_level"]
    confidence_distribution = counters["confidence_distribution"]
    distributions = counters["metric_distributions"]
    assert isinstance(by_reason, dict) and isinstance(by_origin, dict)
    assert isinstance(by_level, dict) and isinstance(confidence_distribution, dict)
    assert isinstance(distributions, dict)
    for candidate in candidates:
        counters["total_candidates"] = int(counters["total_candidates"]) + 1
        _increment(by_origin, origin)
        _increment(by_level, candidate.level)
        for reason in candidate.reason_codes:
            _increment(by_reason, reason)
        counters["proposed_characters"] = int(counters["proposed_characters"]) + (
            candidate.end - candidate.start
        )
        if candidate.proposed_action == DELETE_RECORD:
            counters["full_records_proposed"] = int(counters["full_records_proposed"]) + 1
        bucket = "alta" if candidate.confidence >= 0.80 else "media" if candidate.confidence >= 0.60 else "baja"
        _increment(confidence_distribution, bucket)
        for key, number in _numeric_metrics(candidate.metrics):
            stats = distributions.setdefault(
                key,
                {"count": 0, "minimum": number, "maximum": number, "sum": 0.0},
            )
            stats["count"] += 1
            stats["minimum"] = min(stats["minimum"], number)
            stats["maximum"] = max(stats["maximum"], number)
            stats["sum"] += number


def candidate_row(
    *,
    candidate: ConsolidatedResult,
    record: Mapping[str, object],
    record_number: int,
    input_sha256: str,
) -> dict[str, object]:
    text = str(record["texto"])
    fragment = text[candidate.start:candidate.end]
    fragment_hash = sha256_text(fragment)
    stable_key = record_stable_key(record, record_number)
    candidate_id = build_candidate_id(
        input_sha256=input_sha256,
        stable_record_key=stable_key,
        reason_codes=candidate.reason_codes,
        start=candidate.start,
        end=candidate.end,
        fragment_hash=fragment_hash,
    )
    return {
        "candidate_id": candidate_id,
        "numero_registro": record_number,
        "id": record.get("id", ""),
        "base_de_datos_origen": record.get("base_de_datos_origen", ""),
        "archivo_origen": record.get("archivo_origen", ""),
        "ruta_relativa_origen": record.get("ruta_relativa_origen", ""),
        "tipo_deteccion": " | ".join(candidate.detector_names),
        "codigos_motivo": "|".join(candidate.reason_codes),
        "nivel": candidate.level,
        "confianza": f"{candidate.confidence:.6f}",
        "inicio_caracter": candidate.start,
        "fin_caracter": candidate.end,
        "texto_detectado": fragment,
        "contexto_antes": text[max(0, candidate.start - CONTEXT_CHARACTERS):candidate.start],
        "contexto_despues": text[candidate.end:candidate.end + CONTEXT_CHARACTERS],
        "longitud_texto_detectado": len(fragment),
        "metricas": json.dumps(
            candidate.metrics,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        "accion_propuesta": candidate.proposed_action,
        "version_auditoria": AUDIT_VERSION,
        "fingerprint_entrada_sha256": input_sha256,
        "hash_registro": sha256_text(text),
        "hash_fragmento": fragment_hash,
        "clave_estable_registro": stable_key,
        "decision": "",
        "notas_revision": "",
    }


def allocate_balanced_quotas(counts: Mapping[str, int], sample_size: int) -> dict[str, int]:
    quotas = {category: 0 for category in sorted(counts) if counts[category] > 0}
    remaining = min(sample_size, sum(counts.values()))
    while remaining > 0:
        eligible = [category for category in quotas if quotas[category] < counts[category]]
        if not eligible:
            break
        for category in eligible:
            if remaining <= 0:
                break
            quotas[category] += 1
            remaining -= 1
    return quotas


def write_balanced_sample(
    input_csv: Path,
    output_csv: Path,
    *,
    sample_size: int,
    seed: int,
) -> int:
    category_counts: Counter[str] = Counter()
    with input_csv.open("r", encoding="utf-8-sig", newline="") as input_file:
        reader = csv.DictReader(input_file)
        for row in reader:
            category_counts[row["tipo_deteccion"]] += 1
    quotas = allocate_balanced_quotas(category_counts, sample_size)
    reservoirs: dict[str, list[dict[str, str]]] = {category: [] for category in quotas}
    seen: Counter[str] = Counter()
    randomizers = {
        category: random.Random(f"{seed}:{category}") for category in quotas
    }
    with input_csv.open("r", encoding="utf-8-sig", newline="") as input_file:
        reader = csv.DictReader(input_file)
        for row in reader:
            category = row["tipo_deteccion"]
            quota = quotas.get(category, 0)
            if quota <= 0:
                continue
            seen[category] += 1
            reservoir = reservoirs[category]
            if len(reservoir) < quota:
                reservoir.append(row)
            else:
                selected = randomizers[category].randrange(seen[category])
                if selected < quota:
                    reservoir[selected] = row
    selected_rows = [
        row for category in sorted(reservoirs) for row in reservoirs[category]
    ]
    selected_rows.sort(key=lambda row: row["candidate_id"])
    binary_file, text_file, writer = open_csv_writer(output_csv, CSV_COLUMNS)
    try:
        writer.writerows(selected_rows)
        fsync_csv(binary_file, text_file)
    finally:
        text_file.close()
    return len(selected_rows)


def finalized_metric_distributions(raw: object) -> dict[str, object]:
    if not isinstance(raw, dict):
        return {}
    result: dict[str, object] = {}
    for key in sorted(raw):
        stats = raw[key]
        if not isinstance(stats, dict) or not stats.get("count"):
            continue
        result[key] = {
            "count": stats["count"],
            "minimum": stats["minimum"],
            "maximum": stats["maximum"],
            "mean": stats["sum"] / stats["count"],
        }
    return result


def build_audit_summary(
    *,
    state: Mapping[str, object],
    configuration: Mapping[str, object],
    input_path: Path,
    sample_count: int,
) -> dict[str, object]:
    counters = state["counters"]
    assert isinstance(counters, dict)
    return {
        "version_auditoria": AUDIT_VERSION,
        "total_registros_procesados": counters["records_processed"],
        "total_candidatos": counters["total_candidates"],
        "registros_con_al_menos_un_candidato": counters["records_with_candidates"],
        "candidatos_por_codigo_motivo": counters["candidates_by_reason"],
        "candidatos_por_base_origen": counters["candidates_by_origin"],
        "candidatos_por_nivel": counters["candidates_by_level"],
        "caracteres_propuestos_para_eliminar": counters["proposed_characters"],
        "registros_completos_propuestos_para_eliminar": counters["full_records_proposed"],
        "distribucion_confianza": counters["confidence_distribution"],
        "distribuciones_metricas": finalized_metric_distributions(
            counters["metric_distributions"]
        ),
        "umbrales": configuration["thresholds"],
        "detectores_habilitados": configuration["enabled_detectors"],
        "semilla_muestreo": configuration["sampling_seed"],
        "tamano_muestra_solicitado": configuration["review_sample_size"],
        "tamano_muestra_generado": sample_count,
        "fingerprint_entrada": {
            **quick_fingerprint(input_path),
            "sha256": state["input_sha256"],
        },
        "duracion_fases_segundos": state["durations"],
        "errores": counters["errors"],
        "sqlite_estadisticas_globales": state["state_db"],
        "conjunto_inmutable_candidatos": state["candidate_set"],
    }


def run_audit(
    *,
    input_path: Path,
    paths: AuditPaths,
    thresholds: Thresholds,
    disabled_detectors: Sequence[str],
    review_sample_size: int,
    sampling_seed: int,
    batch_size: int,
    max_batch_bytes: int,
    progress_every: int,
    resume: bool,
    overwrite: bool,
) -> dict[str, object]:
    thresholds.validate()
    detectors = build_detectors(disabled_detectors)
    configuration = audit_configuration(
        thresholds, detectors, review_sample_size, sampling_seed
    )
    targets = [
        paths.candidates_csv, paths.sample_csv, paths.summary_json,
        paths.checkpoint_json, paths.state_db,
    ]
    if resume and overwrite:
        raise ValueError("Use --resume o --overwrite, pero no ambos.")

    if resume:
        if not paths.checkpoint_json.exists():
            raise FileNotFoundError(f"No existe el checkpoint: {paths.checkpoint_json}")
        state = read_checkpoint(paths.checkpoint_json, "audit")
        if state.get("input_quick_fingerprint") != quick_fingerprint(input_path):
            raise ValueError("La entrada cambio desde el checkpoint de auditoria.")
        if state.get("configuration") != configuration:
            raise ValueError("La configuracion no coincide con la auditoria reanudada.")
        if state.get("completed") is True:
            if not paths.summary_json.exists():
                raise FileNotFoundError("El checkpoint esta completo pero falta el resumen.")
            with paths.summary_json.open("r", encoding="utf-8") as summary_file:
                return json.load(summary_file)
    else:
        existing = [path for path in targets if path.exists()]
        if existing and not overwrite:
            raise FileExistsError(
                f"Ya existen archivos de auditoria ({existing[0]}). Use --overwrite o --resume."
            )
        if overwrite:
            for path in targets:
                if path.exists():
                    path.unlink()
            for suffix in ("-wal", "-shm"):
                sidecar = Path(str(paths.state_db) + suffix)
                if sidecar.exists():
                    sidecar.unlink()
        state = {
            "checkpoint_version": CHECKPOINT_VERSION,
            "mode": "audit",
            "audit_version": AUDIT_VERSION,
            "input_quick_fingerprint": quick_fingerprint(input_path),
            "configuration": configuration,
            "phase": "index",
            "index_input_bytes": 0,
            "index_physical_lines": 0,
            "records_indexed": 0,
            "detect_input_bytes": 0,
            "detect_physical_lines": 0,
            "candidate_csv_bytes": 0,
            "input_sha256": None,
            "counters": fresh_audit_counters(),
            "candidate_set": fresh_candidate_set_accumulator(),
            "durations": {"indexacion_global": 0.0, "fingerprint": 0.0, "deteccion": 0.0, "muestreo": 0.0},
            "state_db": str(paths.state_db),
            "completed": False,
        }
        write_json_atomic(paths.checkpoint_json, state)

    connection = connect_sqlite(paths.state_db)
    initialize_audit_database(connection)
    try:
        if state["phase"] == "index":
            phase_started = time.perf_counter()
            previous_duration = float(state["durations"]["indexacion_global"])
            with input_path.open("rb") as input_file:
                input_file.seek(int(state["index_input_bytes"]))
                records = iter_input_records(
                    input_file,
                    first_record_number=int(state["records_indexed"]) + 1,
                    first_physical_line_number=int(state["index_physical_lines"]) + 1,
                )
                for batch in iter_batches(records, batch_size, max_batch_bytes):
                    try:
                        with connection:
                            for item in batch:
                                index_record_blocks(connection, item)
                    except (OSError, ValueError, sqlite3.Error):
                        state["counters"]["errors"] += 1
                        write_json_atomic(paths.checkpoint_json, state)
                        raise
                    state["records_indexed"] = batch[-1].record_number
                    state["index_input_bytes"] = batch[-1].end_offset
                    state["index_physical_lines"] = batch[-1].physical_line_number
                    state["durations"]["indexacion_global"] = previous_duration + (
                        time.perf_counter() - phase_started
                    )
                    write_json_atomic(paths.checkpoint_json, state)
                    if progress_every and batch[-1].record_number % progress_every < len(batch):
                        print(
                            f"Registros indexados: {batch[-1].record_number:,}",
                            file=sys.stderr, flush=True,
                        )
            total_documents = connection.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
            if total_documents != int(state["records_indexed"]):
                raise ValueError("La base SQLite global no coincide con el checkpoint.")
            fingerprint_started = time.perf_counter()
            state["input_sha256"] = full_sha256(input_path)
            state["durations"]["fingerprint"] = time.perf_counter() - fingerprint_started
            state["phase"] = "detect"
            state["detect_input_bytes"] = 0
            state["detect_physical_lines"] = 0
            state["counters"] = fresh_audit_counters()
            state["candidate_set"] = fresh_candidate_set_accumulator()
            binary_file, text_file, _writer = open_csv_writer(paths.candidates_csv, CSV_COLUMNS)
            try:
                state["candidate_csv_bytes"] = fsync_csv(binary_file, text_file)
            finally:
                text_file.close()
            write_json_atomic(paths.checkpoint_json, state)

        if state["phase"] == "detect":
            total_documents = connection.execute("SELECT COUNT(*) FROM documents").fetchone()[0]

            @functools.lru_cache(maxsize=20_000)
            def global_lookup(block_hash: str) -> Mapping[str, object]:
                row = connection.execute(
                    "SELECT * FROM block_stats WHERE block_hash = ?", (block_hash,)
                ).fetchone()
                if row is None:
                    return {}
                origins = connection.execute(
                    "SELECT origin, document_count FROM block_origins WHERE block_hash = ? ORDER BY origin",
                    (block_hash,),
                ).fetchall()
                return {
                    **dict(row),
                    "origin_counts": {origin["origin"]: origin["document_count"] for origin in origins},
                }

            phase_started = time.perf_counter()
            previous_duration = float(state["durations"]["deteccion"])
            binary_file, text_file, writer = open_csv_writer(
                paths.candidates_csv,
                CSV_COLUMNS,
                resume_offset=int(state["candidate_csv_bytes"]),
            )
            try:
                with input_path.open("rb") as input_file:
                    input_file.seek(int(state["detect_input_bytes"]))
                    records = iter_input_records(
                        input_file,
                        first_record_number=int(state["counters"]["records_processed"]) + 1,
                        first_physical_line_number=int(state["detect_physical_lines"]) + 1,
                    )
                    for batch in iter_batches(records, batch_size, max_batch_bytes):
                        batch_rows: list[dict[str, object]] = []
                        batch_candidates: list[tuple[Mapping[str, object], list[ConsolidatedResult]]] = []
                        try:
                            for item in batch:
                                text = str(item.record["texto"])
                                context = DetectionContext(
                                    record_number=item.record_number,
                                    record=item.record,
                                    total_documents=total_documents,
                                    thresholds=thresholds,
                                    global_block_lookup=global_lookup,
                                )
                                raw_results = [
                                    result
                                    for detector in detectors
                                    for result in detector.detect(text, context)
                                ]
                                consolidated = consolidate_results(raw_results)
                                batch_candidates.append((item.record, consolidated))
                                batch_rows.extend(
                                    candidate_row(
                                        candidate=candidate,
                                        record=item.record,
                                        record_number=item.record_number,
                                        input_sha256=str(state["input_sha256"]),
                                    )
                                    for candidate in consolidated
                                )
                        except (OSError, ValueError, sqlite3.Error):
                            state["counters"]["errors"] += 1
                            write_json_atomic(paths.checkpoint_json, state)
                            raise
                        writer.writerows(batch_rows)
                        state["candidate_csv_bytes"] = fsync_csv(binary_file, text_file)
                        update_candidate_set_accumulator(
                            state["candidate_set"],
                            (str(row["candidate_id"]) for row in batch_rows),
                        )
                        for record, consolidated in batch_candidates:
                            update_audit_counters(state["counters"], record, consolidated)
                        state["detect_input_bytes"] = batch[-1].end_offset
                        state["detect_physical_lines"] = batch[-1].physical_line_number
                        state["durations"]["deteccion"] = previous_duration + (
                            time.perf_counter() - phase_started
                        )
                        write_json_atomic(paths.checkpoint_json, state)
                        if progress_every and batch[-1].record_number % progress_every < len(batch):
                            print(
                                f"Registros auditados: {batch[-1].record_number:,}",
                                file=sys.stderr, flush=True,
                            )
            finally:
                text_file.close()
            state["phase"] = "sample"
            write_json_atomic(paths.checkpoint_json, state)

        sample_started = time.perf_counter()
        sample_count = write_balanced_sample(
            paths.candidates_csv,
            paths.sample_csv,
            sample_size=review_sample_size,
            seed=sampling_seed,
        )
        state["durations"]["muestreo"] = time.perf_counter() - sample_started
        state["phase"] = "complete"
        state["completed"] = True
        summary = build_audit_summary(
            state=state,
            configuration=configuration,
            input_path=input_path,
            sample_count=sample_count,
        )
        write_json_atomic(paths.summary_json, summary)
        write_json_atomic(paths.checkpoint_json, state)
        return summary
    finally:
        connection.close()


def initialize_application_database(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS review_candidates (
            candidate_id TEXT PRIMARY KEY,
            record_number INTEGER NOT NULL,
            decision TEXT NOT NULL,
            start_offset INTEGER NOT NULL,
            end_offset INTEGER NOT NULL,
            proposed_action TEXT NOT NULL,
            input_sha256 TEXT NOT NULL,
            record_hash TEXT NOT NULL,
            fragment_hash TEXT NOT NULL,
            stable_key TEXT NOT NULL,
            row_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_review_candidates_record
            ON review_candidates(record_number);
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )
    connection.commit()


def import_review_csv(
    connection: sqlite3.Connection,
    review_csv: Path,
    *,
    input_sha256: str,
) -> tuple[dict[str, int], dict[str, object]]:
    counts = {"pending": 0, "conservar": 0, "eliminar": 0, "total": 0}
    candidate_set = fresh_candidate_set_accumulator()
    with review_csv.open("r", encoding="utf-8-sig", newline="") as review_file:
        reader = csv.DictReader(review_file)
        missing = REQUIRED_REVIEW_COLUMNS - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                "El CSV de revision no contiene las columnas requeridas: "
                + ", ".join(sorted(missing))
            )
        with connection:
            for row_number, row in enumerate(reader, start=2):
                decision = row["decision"].strip()
                if decision not in {"", "eliminar", "conservar"}:
                    raise ValueError(
                        f"Decision desconocida en la fila CSV {row_number}: {decision!r}."
                    )
                if row["fingerprint_entrada_sha256"] != input_sha256:
                    raise ValueError(
                        f"El candidato de la fila {row_number} pertenece a otra entrada."
                    )
                if row["version_auditoria"] != AUDIT_VERSION:
                    raise ValueError(
                        f"Version de auditoria incompatible en la fila {row_number}."
                    )
                try:
                    record_number = int(row["numero_registro"])
                    start = int(row["inicio_caracter"])
                    end = int(row["fin_caracter"])
                except ValueError as error:
                    raise ValueError(
                        f"Offsets o numero de registro invalidos en la fila {row_number}."
                    ) from error
                if record_number <= 0 or start < 0 or end < start:
                    raise ValueError(f"Span invalido en la fila CSV {row_number}.")
                canonical_row = {column: row.get(column, "") for column in CSV_COLUMNS}
                try:
                    connection.execute(
                        """
                        INSERT INTO review_candidates(
                            candidate_id, record_number, decision, start_offset,
                            end_offset, proposed_action, input_sha256, record_hash,
                            fragment_hash, stable_key, row_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            row["candidate_id"], record_number, decision, start, end,
                            row["accion_propuesta"], row["fingerprint_entrada_sha256"],
                            row["hash_registro"], row["hash_fragmento"],
                            row["clave_estable_registro"],
                            json.dumps(canonical_row, ensure_ascii=False, sort_keys=True),
                        ),
                    )
                except sqlite3.IntegrityError as error:
                    raise ValueError(
                        f"candidate_id duplicado en la fila CSV {row_number}: "
                        f"{row['candidate_id']}"
                    ) from error
                counts[decision or "pending"] += 1
                counts["total"] += 1
                update_candidate_set_accumulator(candidate_set, (row["candidate_id"],))
    return counts, candidate_set


def verify_review_manifest(
    review_csv: Path,
    input_sha256: str,
    candidate_set: Mapping[str, object],
) -> None:
    summary_path = review_csv.with_name("auditoria_resumen.json")
    if not summary_path.exists():
        raise ValueError(
            "Falta auditoria_resumen.json; no se puede verificar que el CSV "
            "contenga exactamente todos los candidatos auditados."
        )
    with summary_path.open("r", encoding="utf-8") as summary_file:
        summary = json.load(summary_file)
    fingerprint = summary.get("fingerprint_entrada", {})
    if not isinstance(fingerprint, dict) or fingerprint.get("sha256") != input_sha256:
        raise ValueError("El resumen de auditoria no corresponde al JSONL de entrada.")
    expected_set = summary.get("conjunto_inmutable_candidatos")
    if expected_set != dict(candidate_set):
        raise ValueError(
            "El CSV revisado no contiene exactamente el conjunto de candidatos "
            "producido por la auditoria."
        )


def verify_candidate_row(
    row: sqlite3.Row,
    record: Mapping[str, object],
    record_number: int,
    input_sha256: str,
) -> dict[str, str]:
    csv_row = json.loads(row["row_json"])
    text = str(record["texto"])
    start = int(row["start_offset"])
    end = int(row["end_offset"])
    candidate_id = str(row["candidate_id"])
    if row["input_sha256"] != input_sha256:
        raise ValueError(f"Candidato {candidate_id} asociado a otra entrada.")
    if row["record_hash"] != sha256_text(text):
        raise ValueError(f"Candidato obsoleto {candidate_id}: cambio el texto del registro.")
    stable_key = record_stable_key(record, record_number)
    if row["stable_key"] != stable_key:
        raise ValueError(f"Candidato obsoleto {candidate_id}: cambio la identidad del registro.")
    if start < 0 or end > len(text) or end < start:
        raise ValueError(f"Candidato obsoleto {candidate_id}: span fuera de rango.")
    fragment = text[start:end]
    if fragment != csv_row["texto_detectado"] or sha256_text(fragment) != row["fragment_hash"]:
        raise ValueError(f"Candidato obsoleto {candidate_id}: no coincide el fragmento exacto.")
    reason_codes = tuple(filter(None, csv_row["codigos_motivo"].split("|")))
    expected_id = build_candidate_id(
        input_sha256=input_sha256,
        stable_record_key=stable_key,
        reason_codes=reason_codes,
        start=start,
        end=end,
        fragment_hash=str(row["fragment_hash"]),
    )
    if expected_id != candidate_id:
        raise ValueError(f"candidate_id alterado o inconsistente: {candidate_id}.")
    return csv_row


def merge_intervals(intervals: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for start, end in sorted(intervals):
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged


def cleanup_removal_boundaries(text: str) -> str:
    # El paso 2 ya garantiza espacios simples. Estas reglas solo colapsan los
    # huecos que pueden aparecer al unir los lados de un span eliminado.
    text = text.replace(" \n", "\n").replace("\n ", "\n")
    while "  " in text:
        text = text.replace("  ", " ")
    while "\n\n\n" in text:
        text = text.replace("\n\n\n", "\n\n")
    return text.strip()


def remove_exact_spans(text: str, intervals: Sequence[tuple[int, int]]) -> str:
    transformed = text
    for start, end in reversed(merge_intervals(intervals)):
        transformed = transformed[:start] + transformed[end:]
    return cleanup_removal_boundaries(transformed)


def initial_apply_state(
    *,
    input_path: Path,
    input_sha256: str,
    review_csv: Path,
    review_sha256: str,
    paths: ApplyPaths,
    decision_counts: Mapping[str, int],
) -> dict[str, object]:
    return {
        "checkpoint_version": CHECKPOINT_VERSION,
        "mode": "apply",
        "audit_version": AUDIT_VERSION,
        "input_quick_fingerprint": quick_fingerprint(input_path),
        "input_sha256": input_sha256,
        "review_csv": str(review_csv),
        "review_sha256": review_sha256,
        "state_db": str(paths.state_db),
        "input_bytes": 0,
        "physical_lines": 0,
        "output_jsonl_bytes": 0,
        "applied_csv_bytes": 0,
        "records_processed": 0,
        "records_written": 0,
        "records_removed": 0,
        "records_with_partial_removals": 0,
        "candidates_verified": 0,
        "candidates_applied": 0,
        "characters_removed": 0,
        "decision_counts": dict(decision_counts),
        "errors": 0,
        "elapsed_seconds": 0.0,
        "completed": False,
    }


def run_apply(
    *,
    input_path: Path,
    review_csv: Path,
    paths: ApplyPaths,
    batch_size: int,
    max_batch_bytes: int,
    progress_every: int,
    resume: bool,
    overwrite: bool,
) -> dict[str, object]:
    if not review_csv.exists():
        raise FileNotFoundError(f"No existe el CSV revisado: {review_csv}")
    if resume and overwrite:
        raise ValueError("Use --resume o --overwrite, pero no ambos.")

    fingerprint_started = time.perf_counter()
    input_sha256 = full_sha256(input_path)
    fingerprint_duration = time.perf_counter() - fingerprint_started
    review_sha256 = full_sha256(review_csv)
    targets = [
        paths.output_jsonl, paths.applied_csv, paths.summary_json,
        paths.checkpoint_json, paths.state_db,
    ]
    if resume:
        if not paths.checkpoint_json.exists():
            raise FileNotFoundError(f"No existe el checkpoint: {paths.checkpoint_json}")
        state = read_checkpoint(paths.checkpoint_json, "apply")
        if state.get("input_quick_fingerprint") != quick_fingerprint(input_path):
            raise ValueError("La entrada cambio desde el checkpoint de aplicacion.")
        if state.get("input_sha256") != input_sha256:
            raise ValueError("El SHA-256 de la entrada cambio durante la aplicacion.")
        if state.get("review_sha256") != review_sha256:
            raise ValueError("El CSV revisado cambio desde el inicio de la aplicacion.")
        if state.get("completed") is True:
            if not paths.summary_json.exists():
                raise FileNotFoundError("El checkpoint esta completo pero falta el resumen.")
            with paths.summary_json.open("r", encoding="utf-8") as summary_file:
                return json.load(summary_file)
    else:
        existing = [path for path in targets if path.exists()]
        if existing and not overwrite:
            raise FileExistsError(
                f"Ya existe una salida de aplicacion ({existing[0]}). Use --overwrite o --resume."
            )
        if overwrite:
            for path in targets:
                if path.exists():
                    path.unlink()
            for suffix in ("-wal", "-shm"):
                sidecar = Path(str(paths.state_db) + suffix)
                if sidecar.exists():
                    sidecar.unlink()

    connection = connect_sqlite(paths.state_db)
    initialize_application_database(connection)
    try:
        if not resume:
            decision_counts, candidate_set = import_review_csv(
                connection, review_csv, input_sha256=input_sha256
            )
            verify_review_manifest(review_csv, input_sha256, candidate_set)
            with connection:
                connection.execute(
                    "INSERT OR REPLACE INTO metadata(key, value) VALUES ('input_sha256', ?)",
                    (input_sha256,),
                )
                connection.execute(
                    "INSERT OR REPLACE INTO metadata(key, value) VALUES ('review_sha256', ?)",
                    (review_sha256,),
                )
            paths.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
            with paths.output_jsonl.open("wb") as output_file:
                output_file.flush()
                os.fsync(output_file.fileno())
            binary_csv, text_csv, _writer = open_csv_writer(
                paths.applied_csv, APPLIED_CSV_COLUMNS
            )
            try:
                applied_offset = fsync_csv(binary_csv, text_csv)
            finally:
                text_csv.close()
            state = initial_apply_state(
                input_path=input_path,
                input_sha256=input_sha256,
                review_csv=review_csv,
                review_sha256=review_sha256,
                paths=paths,
                decision_counts=decision_counts,
            )
            state["applied_csv_bytes"] = applied_offset
            write_json_atomic(paths.checkpoint_json, state)

        started = time.perf_counter()
        previous_elapsed = float(state["elapsed_seconds"])
        output_file = paths.output_jsonl.open("r+b")
        output_file.truncate(int(state["output_jsonl_bytes"]))
        output_file.seek(int(state["output_jsonl_bytes"]))
        binary_csv, text_csv, applied_writer = open_csv_writer(
            paths.applied_csv,
            APPLIED_CSV_COLUMNS,
            resume_offset=int(state["applied_csv_bytes"]),
        )
        try:
            with input_path.open("rb") as input_file:
                input_file.seek(int(state["input_bytes"]))
                records = iter_input_records(
                    input_file,
                    first_record_number=int(state["records_processed"]) + 1,
                    first_physical_line_number=int(state["physical_lines"]) + 1,
                )
                for batch in iter_batches(records, batch_size, max_batch_bytes):
                    batch_output: list[bytes] = []
                    batch_applied_rows: list[dict[str, object]] = []
                    batch_written = batch_removed = batch_partial = 0
                    batch_verified = batch_applied = batch_characters = 0
                    try:
                        for item in batch:
                            record = item.record
                            text = str(record["texto"])
                            rows = connection.execute(
                                "SELECT * FROM review_candidates WHERE record_number = ? ORDER BY start_offset, end_offset, candidate_id",
                                (item.record_number,),
                            ).fetchall()
                            verified_rows: list[tuple[sqlite3.Row, dict[str, str]]] = []
                            for row in rows:
                                csv_row = verify_candidate_row(
                                    row, record, item.record_number, input_sha256
                                )
                                verified_rows.append((row, csv_row))
                                batch_verified += 1
                            approved = [
                                (row, csv_row)
                                for row, csv_row in verified_rows
                                if row["decision"] == "eliminar"
                            ]
                            full_approved = [
                                pair for pair in approved if pair[0]["proposed_action"] == DELETE_RECORD
                            ]
                            if full_approved:
                                batch_removed += 1
                                batch_applied += len(approved)
                                batch_characters += len(text)
                                for row, csv_row in approved:
                                    applied = dict(csv_row)
                                    applied["resultado_aplicacion"] = (
                                        "eliminacion_registro_aplicada"
                                        if row["proposed_action"] == DELETE_RECORD
                                        else "subsumida_por_eliminacion_registro"
                                    )
                                    batch_applied_rows.append(applied)
                                continue

                            span_approved = [
                                pair for pair in approved if pair[0]["proposed_action"] == DELETE_SPAN
                            ]
                            intervals = [
                                (int(row["start_offset"]), int(row["end_offset"]))
                                for row, _csv_row in span_approved
                            ]
                            transformed = remove_exact_spans(text, intervals) if intervals else text
                            if intervals:
                                batch_partial += 1
                                batch_applied += len(span_approved)
                                batch_characters += len(text) - len(transformed)
                                for _row, csv_row in span_approved:
                                    applied = dict(csv_row)
                                    applied["resultado_aplicacion"] = "eliminacion_fragmento_aplicada"
                                    batch_applied_rows.append(applied)
                            record["texto"] = transformed
                            processing = record["procesamiento"]
                            assert isinstance(processing, list)
                            if PASO_PROCESAMIENTO not in processing:
                                processing.append(PASO_PROCESAMIENTO)
                            batch_output.append(
                                (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8")
                            )
                            batch_written += 1
                    except (OSError, ValueError, sqlite3.Error):
                        state["errors"] = int(state["errors"]) + 1
                        state["elapsed_seconds"] = previous_elapsed + (
                            time.perf_counter() - started
                        )
                        write_json_atomic(paths.checkpoint_json, state)
                        raise

                    for output_line in batch_output:
                        output_file.write(output_line)
                    applied_writer.writerows(batch_applied_rows)
                    output_file.flush()
                    os.fsync(output_file.fileno())
                    state["applied_csv_bytes"] = fsync_csv(binary_csv, text_csv)
                    state["records_processed"] = batch[-1].record_number
                    state["records_written"] = int(state["records_written"]) + batch_written
                    state["records_removed"] = int(state["records_removed"]) + batch_removed
                    state["records_with_partial_removals"] = int(
                        state["records_with_partial_removals"]
                    ) + batch_partial
                    state["candidates_verified"] = int(state["candidates_verified"]) + batch_verified
                    state["candidates_applied"] = int(state["candidates_applied"]) + batch_applied
                    state["characters_removed"] = int(state["characters_removed"]) + batch_characters
                    state["input_bytes"] = batch[-1].end_offset
                    state["physical_lines"] = batch[-1].physical_line_number
                    state["output_jsonl_bytes"] = output_file.tell()
                    state["elapsed_seconds"] = previous_elapsed + (
                        time.perf_counter() - started
                    )
                    write_json_atomic(paths.checkpoint_json, state)
                    if progress_every and batch[-1].record_number % progress_every < len(batch):
                        print(
                            f"Registros aplicados: {batch[-1].record_number:,}",
                            file=sys.stderr, flush=True,
                        )
        finally:
            output_file.close()
            text_csv.close()

        total_review_candidates = connection.execute(
            "SELECT COUNT(*) FROM review_candidates"
        ).fetchone()[0]
        if int(state["candidates_verified"]) != total_review_candidates:
            state["errors"] = int(state["errors"]) + 1
            write_json_atomic(paths.checkpoint_json, state)
            raise ValueError(
                "Hay candidatos obsoletos: su numero_registro no existe en la entrada."
            )
        state["completed"] = True
        summary = {
            "version_auditoria": AUDIT_VERSION,
            "registros_entrada": state["records_processed"],
            "registros_escritos": state["records_written"],
            "registros_eliminados_completos": state["records_removed"],
            "registros_con_eliminaciones_parciales": state["records_with_partial_removals"],
            "candidatos_verificados": state["candidates_verified"],
            "candidatos_aplicados": state["candidates_applied"],
            "candidatos_pendientes": state["decision_counts"]["pending"],
            "candidatos_conservar": state["decision_counts"]["conservar"],
            "decisiones_eliminar": state["decision_counts"]["eliminar"],
            "caracteres_eliminados": state["characters_removed"],
            "candidatos_obsoletos": 0,
            "fingerprint_entrada": {
                **quick_fingerprint(input_path),
                "sha256": input_sha256,
            },
            "fingerprint_csv_revision_sha256": review_sha256,
            "duracion_fingerprint_segundos": fingerprint_duration,
            "duracion_aplicacion_segundos": state["elapsed_seconds"],
            "errores": state["errors"],
        }
        write_json_atomic(paths.summary_json, summary)
        write_json_atomic(paths.checkpoint_json, state)
        return summary
    finally:
        connection.close()


def threshold_arguments(parser: argparse.ArgumentParser) -> None:
    defaults = Thresholds()
    parser.add_argument("--short-line-max-chars", type=int, default=defaults.short_line_max_chars)
    parser.add_argument("--peripheral-ratio", type=float, default=defaults.peripheral_ratio)
    parser.add_argument("--navigation-min-lines", type=int, default=defaults.navigation_min_lines)
    parser.add_argument("--navigation-min-phrases", type=int, default=defaults.navigation_min_phrases)
    parser.add_argument("--cookie-min-signals", type=int, default=defaults.cookie_min_signals)
    parser.add_argument("--repeated-header-min-docs", type=int, default=defaults.repeated_header_min_docs)
    parser.add_argument("--repeated-header-min-ratio", type=float, default=defaults.repeated_header_min_ratio)
    parser.add_argument("--repeated-header-max-chars", type=int, default=defaults.repeated_header_max_chars)
    parser.add_argument("--link-list-min-urls", type=int, default=defaults.link_list_min_urls)
    parser.add_argument("--advertising-min-signals", type=int, default=defaults.advertising_min_signals)
    parser.add_argument("--http-error-max-chars", type=int, default=defaults.http_error_max_chars)
    parser.add_argument("--repeated-letter-min", type=int, default=defaults.repeated_letter_min)
    parser.add_argument("--repeated-digit-min", type=int, default=defaults.repeated_digit_min)
    parser.add_argument("--repeated-punctuation-min", type=int, default=defaults.repeated_punctuation_min)
    parser.add_argument("--repeated-whitespace-min", type=int, default=defaults.repeated_whitespace_min)
    parser.add_argument("--code-min-signals", type=int, default=defaults.code_min_signals)
    parser.add_argument("--near-empty-max-visible", type=int, default=defaults.near_empty_max_visible)
    parser.add_argument("--near-empty-max-words", type=int, default=defaults.near_empty_max_words)
    parser.add_argument("--automatic-index-min-lines", type=int, default=defaults.automatic_index_min_lines)
    parser.add_argument(
        "--internal-repetition-min-occurrences",
        type=int,
        default=defaults.internal_repetition_min_occurrences,
    )
    parser.add_argument("--internal-repetition-min-chars", type=int, default=defaults.internal_repetition_min_chars)


def thresholds_from_args(args: argparse.Namespace) -> Thresholds:
    return Thresholds(
        short_line_max_chars=args.short_line_max_chars,
        peripheral_ratio=args.peripheral_ratio,
        navigation_min_lines=args.navigation_min_lines,
        navigation_min_phrases=args.navigation_min_phrases,
        cookie_min_signals=args.cookie_min_signals,
        repeated_header_min_docs=args.repeated_header_min_docs,
        repeated_header_min_ratio=args.repeated_header_min_ratio,
        repeated_header_max_chars=args.repeated_header_max_chars,
        link_list_min_urls=args.link_list_min_urls,
        advertising_min_signals=args.advertising_min_signals,
        http_error_max_chars=args.http_error_max_chars,
        repeated_letter_min=args.repeated_letter_min,
        repeated_digit_min=args.repeated_digit_min,
        repeated_punctuation_min=args.repeated_punctuation_min,
        repeated_whitespace_min=args.repeated_whitespace_min,
        code_min_signals=args.code_min_signals,
        near_empty_max_visible=args.near_empty_max_visible,
        near_empty_max_words=args.near_empty_max_words,
        automatic_index_min_lines=args.automatic_index_min_lines,
        internal_repetition_min_occurrences=args.internal_repetition_min_occurrences,
        internal_repetition_min_chars=args.internal_repetition_min_chars,
    )


def parse_args() -> argparse.Namespace:
    catalog = detector_catalog()
    parser = argparse.ArgumentParser(
        description=(
            "Audita ruido heuristico sin eliminar por defecto, o aplica exclusivamente "
            "decisiones explicitas de un CSV revisado."
        )
    )
    parser.add_argument("--mode", choices=("audit", "apply"), required=True)
    parser.add_argument("--input-file", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--review-csv", type=Path)
    parser.add_argument("--review-sample-size", type=int, default=110)
    parser.add_argument("--sampling-seed", type=int, default=1729)
    parser.add_argument(
        "--disable-detector",
        action="append",
        default=[],
        choices=sorted(catalog),
        help="Codigo estable de detector a desactivar. Puede repetirse.",
    )
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--batch-max-mb", type=float, default=64.0)
    parser.add_argument("--progress-every", type=int, default=1_000)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    threshold_arguments(parser)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = args.input_file.resolve()
    output_dir = args.output_dir.resolve()
    if not input_path.exists() or not input_path.is_file():
        print(f"No existe el JSONL de entrada: {input_path}", file=sys.stderr)
        return 1
    if args.batch_size <= 0 or args.batch_max_mb <= 0:
        print("Los limites de lote deben ser mayores que 0.", file=sys.stderr)
        return 1
    if args.progress_every < 0 or args.review_sample_size < 0:
        print("El progreso y el tamano de muestra no pueden ser negativos.", file=sys.stderr)
        return 1
    max_batch_bytes = max(1, int(args.batch_max_mb * 1024 * 1024))

    try:
        if args.mode == "audit":
            if args.review_csv is not None:
                raise ValueError("--review-csv solo se usa con --mode apply.")
            paths = AuditPaths(
                candidates_csv=output_dir / "candidatos_ruido.csv",
                sample_csv=output_dir / "muestra_revision_ruido.csv",
                summary_json=output_dir / "auditoria_resumen.json",
                checkpoint_json=output_dir / "auditoria.checkpoint.json",
                state_db=output_dir / "auditoria_global.sqlite3",
            )
            summary = run_audit(
                input_path=input_path,
                paths=paths,
                thresholds=thresholds_from_args(args),
                disabled_detectors=args.disable_detector,
                review_sample_size=args.review_sample_size,
                sampling_seed=args.sampling_seed,
                batch_size=args.batch_size,
                max_batch_bytes=max_batch_bytes,
                progress_every=args.progress_every,
                resume=args.resume,
                overwrite=args.overwrite,
            )
            print("Auditoria de ruido terminada; no se elimino contenido.")
            print(f"Registros procesados: {summary['total_registros_procesados']:,}")
            print(f"Candidatos: {summary['total_candidatos']:,}")
            print(f"CSV completo: {paths.candidates_csv}")
            print(f"Muestra: {paths.sample_csv}")
            print(f"Resumen: {paths.summary_json}")
        else:
            review_csv = (
                args.review_csv.resolve()
                if args.review_csv
                else output_dir / "candidatos_ruido.csv"
            )
            paths = ApplyPaths(
                output_jsonl=output_dir / "sin_ruido.jsonl",
                applied_csv=output_dir / "eliminaciones_aplicadas.csv",
                summary_json=output_dir / "aplicacion_resumen.json",
                checkpoint_json=output_dir / "aplicacion.checkpoint.json",
                state_db=output_dir / "aplicacion_decisiones.sqlite3",
            )
            summary = run_apply(
                input_path=input_path,
                review_csv=review_csv,
                paths=paths,
                batch_size=args.batch_size,
                max_batch_bytes=max_batch_bytes,
                progress_every=args.progress_every,
                resume=args.resume,
                overwrite=args.overwrite,
            )
            print("Aplicacion de decisiones terminada.")
            print(f"Registros escritos: {summary['registros_escritos']:,}")
            print(f"Registros eliminados: {summary['registros_eliminados_completos']:,}")
            print(f"Candidatos pendientes: {summary['candidatos_pendientes']:,}")
            print(f"Corpus resultante: {paths.output_jsonl}")
            print(f"Eliminaciones aplicadas: {paths.applied_csv}")
    except KeyboardInterrupt:
        print("\nProceso interrumpido. Use --resume para continuar.", file=sys.stderr)
        return 130
    except (FileExistsError, FileNotFoundError, OSError, ValueError, sqlite3.Error) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
