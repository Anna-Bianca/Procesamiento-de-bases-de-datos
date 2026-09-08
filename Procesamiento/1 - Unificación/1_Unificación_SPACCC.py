from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterator, TextIO


BASE_DE_DATOS_ORIGEN = "SPACCC"
DOI = "10.5281/zenodo.2560316"
LINK = "https://doi.org/10.5281/zenodo.2560316"
PASO_PROCESAMIENTO = "Paso 1: Unificacion"

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_DIR = (
    REPO_ROOT / "Base de datos" / "0 - Crudo" / "SPACC" / "SPACCC" / "corpus"
)
DEFAULT_OUTPUT_PATH = REPO_ROOT / "Base de datos" / "1 - Unificado" / "unificado.jsonl"


def iter_input_files(input_dir: Path, pattern: str) -> Iterator[Path]:
    yield from sorted(path for path in input_dir.glob(pattern) if path.is_file())


def iter_spaccc_records(
    input_dir: Path,
    *,
    pattern: str,
    encoding: str,
) -> Iterator[dict[str, object]]:
    for file_number, input_path in enumerate(
        iter_input_files(input_dir, pattern),
        start=1,
    ):
        source_filename = input_path.name
        raw_text = input_path.read_text(encoding=encoding, errors="replace")

        yield {
            "id": source_filename,
            "texto": raw_text,
            "base_de_datos_origen": BASE_DE_DATOS_ORIGEN,
            "doi": DOI,
            "link": LINK,
            "url_path_original": str(input_path),
            "archivo_origen": source_filename,
            "ruta_relativa_origen": source_filename,
            "carpeta_origen": ".",
            "tipo_unidad_textual": "archivo",
            "numero_unidad_textual": file_number,
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
            "Unifica SPACCC desde una carpeta de TXT hacia archivos JSONL con metadata."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"Carpeta con TXT crudos. Default: {DEFAULT_INPUT_DIR}",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Archivo JSONL unificado de salida. Default: {DEFAULT_OUTPUT_PATH}",
    )
    parser.add_argument(
        "--pattern",
        default="*.txt",
        help="Patron de archivos a procesar dentro de input-dir. Default: *.txt",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Encoding para leer los TXT. Default: utf-8",
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
    input_dir = args.input_dir.resolve()
    output_file = args.output_file.resolve()

    if not input_dir.exists():
        print(f"No existe la carpeta de entrada: {input_dir}", file=sys.stderr)
        return 1
    if not input_dir.is_dir():
        print(f"La ruta de entrada no es una carpeta: {input_dir}", file=sys.stderr)
        return 1

    records = iter_spaccc_records(
        input_dir,
        pattern=args.pattern,
        encoding=args.encoding,
    )
    total_records = write_jsonl(
        records,
        output_file,
        progress_every=args.progress_every,
        overwrite=args.overwrite,
    )

    print("Unificacion SPACCC terminada.")
    print(f"Registros escritos: {total_records:,}")
    print(f"Archivo JSONL unificado: {output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
