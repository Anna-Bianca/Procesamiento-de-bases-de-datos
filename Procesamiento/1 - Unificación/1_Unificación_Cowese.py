from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterator, TextIO


BASE_DE_DATOS_ORIGEN = "CoWeSe"
DOI = "10.5281/zenodo.5513237"
LINK = "https://zenodo.org/records/5513237"
PASO_PROCESAMIENTO = "Paso 1: Unificacion"

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_PATH = (
    REPO_ROOT / "Base de datos" / "0 - Crudo" / "Cowese" / "CoWeSe.txt"
)
DEFAULT_OUTPUT_PATH = REPO_ROOT / "Base de datos" / "1 - Unificado" / "unificado.jsonl"


def iter_text_blocks(input_file: TextIO) -> Iterator[str]:
    """Yield blocks separated by blank lines without loading the full corpus."""
    current_lines: list[str] = []

    for line in input_file:
        if line.strip():
            current_lines.append(line.rstrip())
        elif current_lines:
            yield "\n".join(current_lines)
            current_lines = []

    if current_lines:
        yield "\n".join(current_lines)


def iter_cowese_records(
    input_path: Path,
    *,
    encoding: str,
) -> Iterator[dict[str, object]]:
    source_filename = input_path.name

    with input_path.open("r", encoding=encoding, errors="replace", newline="") as file:
        for block_number, raw_block in enumerate(
            iter_text_blocks(file),
            start=1,
        ):
            yield {
                "id": f"{source_filename}__bloque_{block_number:09d}",
                "texto": raw_block,
                "base_de_datos_origen": BASE_DE_DATOS_ORIGEN,
                "doi": DOI,
                "link": LINK,
                "url_path_original": str(input_path),
                "archivo_origen": source_filename,
                "ruta_relativa_origen": source_filename,
                "carpeta_origen": ".",
                "tipo_unidad_textual": "bloque",
                "numero_unidad_textual": block_number,
                "archivo_guia_segmentacion": None,
                "url_path_guia_segmentacion": None,
                "procesamiento": [PASO_PROCESAMIENTO],
            }


def write_jsonl(
    records: Iterator[dict[str, object]],
    output_path: Path,
    *,
    progress_every: int,
    overwrite: bool,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total_records = 0
    mode = "w" if overwrite else "a"

    with output_path.open(mode, encoding="utf-8", newline="\n") as output_file:
        for record in records:
            line = json.dumps(record, ensure_ascii=False) + "\n"
            output_file.write(line)
            total_records += 1

            if progress_every and total_records % progress_every == 0:
                print(f"Registros escritos: {total_records:,}", file=sys.stderr)

    return total_records


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Unifica CoWeSe desde un TXT grande hacia archivos JSONL con metadata."
        )
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT_PATH,
        help=f"Ruta del TXT crudo. Default: {DEFAULT_INPUT_PATH}",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Archivo JSONL unificado de salida. Default: {DEFAULT_OUTPUT_PATH}",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Encoding para leer el TXT. Default: utf-8",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100_000,
        help="Muestra progreso cada N registros. Use 0 para silenciar. Default: 100000",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Recrea el archivo de salida en vez de agregar registros al final.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = args.input.resolve()
    output_file = args.output_file.resolve()

    if not input_path.exists():
        print(f"No existe el archivo de entrada: {input_path}", file=sys.stderr)
        return 1

    records = iter_cowese_records(
        input_path,
        encoding=args.encoding,
    )
    total_records = write_jsonl(
        records,
        output_file,
        progress_every=args.progress_every,
        overwrite=args.overwrite,
    )

    print("Unificacion CoWeSe terminada.")
    print(f"Registros escritos: {total_records:,}")
    print(f"Archivo JSONL unificado: {output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
