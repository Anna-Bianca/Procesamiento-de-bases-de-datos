from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from pathlib import Path
from typing import Iterator, TextIO


BASE_DE_DATOS_ORIGEN = "SciELO biomedico en espanol"
DOI = "10.5281/zenodo.5902835"
LINK = "https://zenodo.org/records/5902835"
PASO_PROCESAMIENTO = "Paso 1: Unificacion"

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_DIR = REPO_ROOT / "Base de datos" / "0 - Crudo" / "SciELO Crawled"
DEFAULT_RAW_PATH = DEFAULT_INPUT_DIR / "scielo_raw.txt"
DEFAULT_BOUNDARIES_PATH = DEFAULT_INPUT_DIR / "scielo_numbers.txt"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "Base de datos" / "1 - Unificado" / "unificado.jsonl"

NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def normalize_for_alignment(text: str) -> str:
    text = text.lower()
    text = "".join(
        char
        for char in unicodedata.normalize("NFD", text)
        if unicodedata.category(char) != "Mn"
    )
    text = NON_ALNUM_RE.sub(" ", text)
    return " ".join(text.split())


def join_with_single_space(parts: list[str]) -> str:
    return " ".join(part for part in parts if part)


def iter_scielo_raw_documents(
    raw_file: TextIO,
    boundaries_file: TextIO,
) -> Iterator[tuple[int, str]]:
    """Use scielo_numbers.txt lines as document boundaries for scielo_raw.txt."""
    raw_lines: list[str] = []
    normalized_parts: list[str] = []
    normalized_length = 0

    boundary_iterator = enumerate(boundaries_file, start=1)
    try:
        document_number, boundary_line = next(boundary_iterator)
    except StopIteration:
        return

    boundary_text: str | None = normalize_for_alignment(boundary_line)

    for raw_line_number, raw_line in enumerate(raw_file, start=1):
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        if boundary_text is None:
            raise ValueError(
                "scielo_raw.txt contiene lineas adicionales despues del ultimo "
                f"documento guia, cerca de la linea raw {raw_line_number}."
            )

        raw_lines.append(raw_line)
        normalized_raw_line = normalize_for_alignment(raw_line)
        if normalized_raw_line:
            normalized_parts.append(normalized_raw_line)
            normalized_length += len(normalized_raw_line)
            if len(normalized_parts) > 1:
                normalized_length += 1

        if normalized_length < len(boundary_text):
            continue

        current_text = join_with_single_space(normalized_parts)
        if current_text != boundary_text:
            raise ValueError(
                "No se pudo alinear scielo_raw.txt con scielo_numbers.txt "
                f"en el documento {document_number}, cerca de la linea raw "
                f"{raw_line_number}."
            )

        yield document_number, " ".join(raw_lines)
        raw_lines = []
        normalized_parts = []
        normalized_length = 0

        try:
            document_number, boundary_line = next(boundary_iterator)
        except StopIteration:
            boundary_text = None
            continue
        boundary_text = normalize_for_alignment(boundary_line)

    remaining_boundaries = 1 if boundary_text is not None else 0
    remaining_boundaries += sum(1 for _ in boundary_iterator)

    if raw_lines or remaining_boundaries:
        raise ValueError(
            "La alineacion termino con datos pendientes: "
            f"{len(raw_lines)} lineas raw acumuladas y "
            f"{remaining_boundaries} documentos guia pendientes."
        )


def iter_scielo_records(
    raw_path: Path,
    boundaries_path: Path,
    *,
    encoding: str,
) -> Iterator[dict[str, object]]:
    raw_filename = raw_path.name
    boundaries_filename = boundaries_path.name

    with raw_path.open("r", encoding=encoding, errors="replace", newline="") as raw_file:
        with boundaries_path.open(
            "r",
            encoding=encoding,
            errors="replace",
            newline="",
        ) as boundaries_file:
            for document_number, raw_text in iter_scielo_raw_documents(
                raw_file,
                boundaries_file,
            ):
                yield {
                    "id": f"{raw_filename}__documento_{document_number:09d}",
                    "texto": raw_text,
                    "base_de_datos_origen": BASE_DE_DATOS_ORIGEN,
                    "doi": DOI,
                    "link": LINK,
                    "url_path_original": str(raw_path),
                    "archivo_origen": raw_filename,
                    "ruta_relativa_origen": raw_filename,
                    "carpeta_origen": ".",
                    "tipo_unidad_textual": "documento",
                    "numero_unidad_textual": document_number,
                    "archivo_guia_segmentacion": boundaries_filename,
                    "url_path_guia_segmentacion": str(boundaries_path),
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
            "Unifica SciELO desde scielo_raw.txt y scielo_numbers.txt hacia JSONL."
        )
    )
    parser.add_argument(
        "--raw",
        type=Path,
        default=DEFAULT_RAW_PATH,
        help=f"TXT crudo con texto original. Default: {DEFAULT_RAW_PATH}",
    )
    parser.add_argument(
        "--boundaries",
        type=Path,
        default=DEFAULT_BOUNDARIES_PATH,
        help=(
            "TXT guia con un documento por linea. "
            f"Default: {DEFAULT_BOUNDARIES_PATH}"
        ),
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
        help="Encoding para leer los TXT. Default: utf-8",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=1_000,
        help="Muestra progreso cada N registros. Use 0 para silenciar. Default: 1000",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Recrea el archivo de salida en vez de agregar registros al final.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    raw_path = args.raw.resolve()
    boundaries_path = args.boundaries.resolve()
    output_file = args.output_file.resolve()

    if not raw_path.exists():
        print(f"No existe el archivo raw: {raw_path}", file=sys.stderr)
        return 1
    if not boundaries_path.exists():
        print(f"No existe el archivo guia: {boundaries_path}", file=sys.stderr)
        return 1

    records = iter_scielo_records(
        raw_path,
        boundaries_path,
        encoding=args.encoding,
    )
    total_records = write_jsonl(
        records,
        output_file,
        progress_every=args.progress_every,
        overwrite=args.overwrite,
    )

    print("Unificacion SciELO terminada.")
    print(f"Registros escritos: {total_records:,}")
    print(f"Archivo JSONL unificado: {output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
