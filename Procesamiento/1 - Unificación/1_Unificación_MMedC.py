from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Iterator


BASE_DE_DATOS_ORIGEN = "MMedC"
DOI = "arXiv:2402.13963"
LINK = "https://huggingface.co/datasets/Henrychur/MMedC"
PASO_PROCESAMIENTO = "Paso 1: Unificacion"

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_DIR = (
    REPO_ROOT / "Base de datos" / "0 - Crudo" / "MMedC - Spanish - 3"
)
DEFAULT_OUTPUT_PATH = REPO_ROOT / "Base de datos" / "1 - Unificado" / "unificado.jsonl"


def should_skip_dir(dir_path: Path, exclude_dir_fragments: tuple[str, ...]) -> bool:
    normalized_path = str(dir_path).lower()
    return any(fragment.lower() in normalized_path for fragment in exclude_dir_fragments)


def iter_input_files(
    input_dir: Path,
    *,
    extension: str,
    exclude_dir_fragments: tuple[str, ...],
) -> Iterator[Path]:
    """Yield TXT files recursively without deciding internal document boundaries."""
    for root, dirs, files in os.walk(input_dir):
        root_path = Path(root)

        dirs[:] = [
            dirname
            for dirname in dirs
            if not should_skip_dir(root_path / dirname, exclude_dir_fragments)
        ]

        if should_skip_dir(root_path, exclude_dir_fragments):
            continue

        for filename in sorted(files):
            if filename.lower().endswith(extension.lower()):
                yield root_path / filename


def iter_mmedc_records(
    input_dir: Path,
    *,
    extension: str,
    encoding: str,
    exclude_dir_fragments: tuple[str, ...],
    progress_every: int,
    resume_after: str | None,
) -> Iterator[dict[str, object]]:
    resume_after_normalized = (
        resume_after.replace("\\", "/").strip() if resume_after else None
    )
    resume_point_found = resume_after_normalized is None

    for file_number, input_path in enumerate(
        iter_input_files(
            input_dir,
            extension=extension,
            exclude_dir_fragments=exclude_dir_fragments,
        ),
        start=1,
    ):
        source_filename = input_path.name
        relative_path = input_path.relative_to(input_dir)
        relative_path_string = str(relative_path).replace("\\", "/")

        if not resume_point_found:
            if relative_path_string == resume_after_normalized:
                resume_point_found = True
                print(
                    "Punto de reanudacion encontrado en MMedC: "
                    f"{relative_path_string}. Continuando desde el siguiente archivo.",
                    file=sys.stderr,
                    flush=True,
                )
            elif progress_every and file_number % progress_every == 0:
                print(
                    "Buscando punto de reanudacion MMedC. "
                    f"Archivos revisados: {file_number:,}",
                    file=sys.stderr,
                    flush=True,
                )
            continue

        raw_text = input_path.read_text(encoding=encoding, errors="replace")

        yield {
            "id": relative_path_string,
            "texto": raw_text,
            "base_de_datos_origen": BASE_DE_DATOS_ORIGEN,
            "doi": DOI,
            "link": LINK,
            "url_path_original": str(input_path),
            "archivo_origen": source_filename,
            "ruta_relativa_origen": relative_path_string,
            "carpeta_origen": str(relative_path.parent).replace("\\", "/"),
            "tipo_unidad_textual": "archivo",
            "numero_unidad_textual": file_number,
            "archivo_guia_segmentacion": None,
            "url_path_guia_segmentacion": None,
            "procesamiento": [PASO_PROCESAMIENTO],
        }

    if resume_after_normalized and not resume_point_found:
        raise ValueError(
            "No se encontro el archivo indicado en --resume-after dentro de "
            f"{input_dir}: {resume_after_normalized}"
        )


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
            "Unifica MMedC Spanish desde TXT recursivos hacia JSONL con metadata. "
            "Cada TXT se conserva como un registro crudo."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_DIR,
        help=f"Carpeta raiz con TXT crudos. Default: {DEFAULT_INPUT_DIR}",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Archivo JSONL unificado de salida. Default: {DEFAULT_OUTPUT_PATH}",
    )
    parser.add_argument(
        "--extension",
        default=".txt",
        help="Extension de archivos a procesar recursivamente. Default: .txt",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Encoding para leer los TXT. Default: utf-8",
    )
    parser.add_argument(
        "--exclude-dir-fragment",
        action="append",
        default=[],
        help=(
            "Fragmento de ruta a excluir. Puede repetirse. "
            "Ejemplo: --exclude-dir-fragment cultural_filtered_data_used"
        ),
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
    parser.add_argument(
        "--resume-after",
        help=(
            "Ruta relativa del ultimo TXT ya escrito. El script saltea hasta "
            "ese archivo y continua desde el siguiente, sin duplicarlo."
        ),
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
    if not args.extension.startswith("."):
        print("La extension debe empezar con punto, por ejemplo: .txt", file=sys.stderr)
        return 1

    records = iter_mmedc_records(
        input_dir,
        extension=args.extension,
        encoding=args.encoding,
        exclude_dir_fragments=tuple(args.exclude_dir_fragment),
        progress_every=args.progress_every,
        resume_after=args.resume_after,
    )
    total_records = write_jsonl(
        records,
        output_file,
        progress_every=args.progress_every,
        overwrite=args.overwrite,
    )

    print("Unificacion MMedC Spanish terminada.")
    print(f"Registros escritos: {total_records:,}")
    print(f"Archivo JSONL unificado: {output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
