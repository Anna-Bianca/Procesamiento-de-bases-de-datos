"""Adaptador de Hugging Face Jobs para el paso 4 de deduplicación.

Mantiene SQLite en disco temporal, restaura el último snapshot consistente y
escribe resultados y checkpoints en un bucket montado.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import traceback


HERE = Path(__file__).resolve().parent
STEP_DIR = HERE.parent
WORKFLOW_PATH = STEP_DIR / "4_Dedup_interna.py"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_workflow():
    sys.path.insert(0, str(STEP_DIR))
    spec = importlib.util.spec_from_file_location("dedup_step4_hf", WORKFLOW_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar {WORKFLOW_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def restore_checkpoint(checkpoint_dir: Path, state_dir: Path) -> bool:
    """Restaura únicamente snapshots creados con sqlite3.Connection.backup."""
    source = checkpoint_dir / "latest.sqlite3"
    destination = state_dir / "dedup_estado.sqlite3"
    state_dir.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        return False
    if not source.is_file():
        return False
    temporary = destination.with_name(f".{destination.name}.restore-{os.getpid()}")
    try:
        shutil.copy2(source, temporary)
        connection = sqlite3.connect(temporary)
        try:
            result = connection.execute("PRAGMA quick_check").fetchone()[0]
        finally:
            connection.close()
        if result != "ok":
            raise sqlite3.DatabaseError(f"Checkpoint remoto inválido: {result}")
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    print(f"[HF] Checkpoint restaurado: {source}", file=sys.stderr, flush=True)
    return True


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("audit", "apply"), default="audit")
    parser.add_argument("--input-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--checkpoint-dir", type=Path, required=True)
    parser.add_argument("--scratch-dir", type=Path, default=Path("/tmp/dedup"))
    parser.add_argument("--review-csv", type=Path)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--progress-every", type=int, default=10000)
    parser.add_argument("--heartbeat-seconds", type=float, default=15.0)
    parser.add_argument("--checkpoint-seconds", type=float, default=300.0)
    parser.add_argument("--max-bucket", type=int, default=64)
    parser.add_argument("--max-records", type=int, default=0)
    return parser.parse_args(argv)


def run(argv=None) -> int:
    args = parse_args(argv)
    input_path = args.input_file.resolve()
    output_dir = args.output_dir.resolve()
    checkpoint_dir = args.checkpoint_dir.resolve()
    scratch_dir = args.scratch_dir.resolve()
    state_dir = scratch_dir / "state"
    status_path = output_dir / "run.json"
    if not input_path.is_file():
        raise FileNotFoundError(input_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    if args.resume:
        restore_checkpoint(checkpoint_dir, state_dir)
    elif (checkpoint_dir / "latest.sqlite3").exists():
        raise ValueError("Ya existe un checkpoint remoto; use --resume u otro run-id")

    status = {
        "schema_version": 1,
        "status": "running",
        "started_at_utc": utc_now(),
        "updated_at_utc": utc_now(),
        "job_id": os.environ.get("JOB_ID"),
        "input_file": str(input_path),
        "output_dir": str(output_dir),
        "checkpoint_dir": str(checkpoint_dir),
        "mode": args.mode,
        "max_records": args.max_records,
        "max_bucket": args.max_bucket,
        "batch_size": args.batch_size,
    }
    atomic_json(status_path, status)
    workflow_args = [
        "--mode", args.mode,
        "--input-file", str(input_path),
        "--output-dir", str(output_dir),
        "--state-dir", str(state_dir),
        "--checkpoint-mirror-dir", str(checkpoint_dir),
        "--checkpoint-mirror-seconds", str(args.checkpoint_seconds),
        "--batch-size", str(args.batch_size),
        "--progress-every", str(args.progress_every),
        "--heartbeat-seconds", str(args.heartbeat_seconds),
        "--max-bucket", str(args.max_bucket),
        "--max-records", str(args.max_records),
    ]
    if args.review_csv is not None:
        workflow_args.extend(("--review-csv", str(args.review_csv.resolve())))
    if args.resume:
        workflow_args.append("--resume")
    try:
        result = load_workflow().main(workflow_args)
    except BaseException as error:
        status.update({
            "status": "failed",
            "updated_at_utc": utc_now(),
            "error_type": type(error).__name__,
            "error": str(error),
            "traceback": traceback.format_exc(),
        })
        atomic_json(status_path, status)
        raise
    status.update({"status": "completed", "updated_at_utc": utc_now()})
    atomic_json(status_path, status)
    return result


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except (ValueError, OSError, sqlite3.Error) as error:
        print(f"Error: {error}", file=sys.stderr, flush=True)
        raise SystemExit(1)
