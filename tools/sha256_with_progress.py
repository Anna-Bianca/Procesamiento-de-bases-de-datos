"""Calcula SHA-256 en streaming y muestra progreso para archivos grandes."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import sys
import time


def format_duration(seconds: float) -> str:
    if seconds == float("inf"):
        return "calculando"
    seconds = max(0, round(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours:d}h {minutes:02d}m {seconds:02d}s"
    return f"{minutes:d}m {seconds:02d}s"


def calculate(path: Path, chunk_size: int = 16 * 1024 * 1024) -> str:
    total = path.stat().st_size
    processed = 0
    digest = hashlib.sha256()
    started = time.monotonic()
    last_report = 0.0

    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
            processed += len(chunk)
            now = time.monotonic()
            if now - last_report >= 0.5 or processed == total:
                elapsed = max(now - started, 1e-9)
                speed = processed / elapsed
                percent = processed / total * 100 if total else 100.0
                eta = (total - processed) / speed if speed else float("inf")
                print(
                    f"\r{percent:6.2f}% | {speed / 1024 / 1024:7.1f} MiB/s "
                    f"| ETA {format_duration(eta)}",
                    end="",
                    flush=True,
                )
                last_report = now
    print()
    return digest.hexdigest().upper()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_file", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    input_path = args.input_file.resolve()
    if not input_path.is_file():
        parser.error(f"No existe el archivo: {input_path}")

    try:
        value = calculate(input_path)
    except KeyboardInterrupt:
        print("\nCálculo cancelado; el archivo de entrada no fue modificado.", file=sys.stderr)
        return 130

    print(f"SHA256: {value}")
    if args.output:
        output_path = args.output.resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(value + "\n", encoding="utf-8")
        print(f"Guardado en: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())