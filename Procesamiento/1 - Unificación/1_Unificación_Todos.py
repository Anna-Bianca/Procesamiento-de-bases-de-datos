from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_PATH = REPO_ROOT / "Base de datos" / "1 - Unificado" / "unificado.jsonl"

SCRIPT_SUFFIXES = (
    "_Cowese.py",
    "_MMedC.py",
    "_SciELO.py",
    "_SPACCC.py",
)

SCRIPT_SEQUENCE = (
    ("CoWeSe", "_Cowese.py"),
    ("MMedC", "_MMedC.py"),
    ("SciELO biomedico en espanol", "_SciELO.py"),
    ("SPACCC", "_SPACCC.py"),
)

START_AT_ALIASES = {
    "cowese": "CoWeSe",
    "mmedc": "MMedC",
    "scielo": "SciELO biomedico en espanol",
    "spaccc": "SPACCC",
}


def find_script(script_dir: Path, suffix: str) -> Path:
    matches = sorted(script_dir.glob(f"*{suffix}"))
    if not matches:
        raise FileNotFoundError(f"No se encontro el script con sufijo {suffix}")
    if len(matches) > 1:
        raise RuntimeError(f"Hay mas de un script con sufijo {suffix}: {matches}")
    return matches[0]


def run_script(
    script_path: Path,
    *,
    output_file: Path,
    progress_every: int,
    extra_args: list[str] | None = None,
) -> None:
    command = [
        sys.executable,
        str(script_path),
        "--output-file",
        str(output_file),
        "--progress-every",
        str(progress_every),
    ]
    if extra_args:
        command.extend(extra_args)

    print(f"\nEjecutando: {script_path.name}")
    subprocess.run(command, check=True)


def read_last_nonempty_line(path: Path) -> str:
    chunk_size = 1024 * 1024
    buffer = b""

    with path.open("rb") as file:
        file.seek(0, 2)
        position = file.tell()

        while position > 0:
            bytes_to_read = min(chunk_size, position)
            position -= bytes_to_read
            file.seek(position)
            buffer = file.read(bytes_to_read) + buffer
            lines = buffer.splitlines()

            if position == 0 or len(lines) >= 2:
                for line in reversed(lines):
                    if line.strip():
                        return line.decode("utf-8")

    raise ValueError(f"No se encontro ninguna linea escrita en {path}")


def read_last_record(output_file: Path) -> dict[str, object]:
    try:
        return json.loads(read_last_nonempty_line(output_file))
    except json.JSONDecodeError as error:
        raise ValueError(
            "La ultima linea del JSONL no se pudo leer como JSON. "
            "Puede estar truncada por un corte durante la escritura."
        ) from error


def build_resume_plan(
    script_dir: Path,
    output_file: Path,
) -> list[tuple[Path, list[str]]]:
    if not output_file.exists():
        raise FileNotFoundError(f"No existe el archivo de salida: {output_file}")

    last_record = read_last_record(output_file)
    last_origin = last_record.get("base_de_datos_origen")

    if last_origin == "MMedC":
        resume_after = last_record.get("ruta_relativa_origen") or last_record.get("id")
        if not isinstance(resume_after, str) or not resume_after:
            raise ValueError(
                "La ultima linea de MMedC no tiene ruta_relativa_origen ni id."
            )

        print("Reanudando desde la ultima linea escrita:")
        print(f"Base: {last_origin}")
        print(f"Ultimo archivo MMedC: {resume_after}")

        return [
            (
                find_script(script_dir, "_MMedC.py"),
                ["--resume-after", resume_after],
            ),
            (find_script(script_dir, "_SciELO.py"), []),
            (find_script(script_dir, "_SPACCC.py"), []),
        ]

    origins = [origin for origin, _suffix in SCRIPT_SEQUENCE]
    if last_origin in origins:
        next_index = origins.index(last_origin) + 1
        print("Reanudando desde la ultima linea escrita:")
        print(f"Base: {last_origin}")
        return [
            (find_script(script_dir, suffix), [])
            for _origin, suffix in SCRIPT_SEQUENCE[next_index:]
        ]

    raise ValueError(
        "No se reconoce base_de_datos_origen en la ultima linea: "
        f"{last_origin!r}"
    )


def build_start_at_plan(
    script_dir: Path,
    start_at: str,
) -> list[tuple[Path, list[str]]]:
    normalized_start_at = START_AT_ALIASES.get(start_at.lower())
    if not normalized_start_at:
        valid_options = ", ".join(sorted(START_AT_ALIASES))
        raise ValueError(
            f"--start-at debe ser una de estas opciones: {valid_options}"
        )

    origins = [origin for origin, _suffix in SCRIPT_SEQUENCE]
    start_index = origins.index(normalized_start_at)

    print(f"Ejecutando desde: {normalized_start_at}")
    return [
        (find_script(script_dir, suffix), [])
        for _origin, suffix in SCRIPT_SEQUENCE[start_index:]
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ejecuta los 4 scripts de unificacion, uno despues del otro."
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help=f"Archivo JSONL unificado de salida. Default: {DEFAULT_OUTPUT_PATH}",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100_000,
        help="Muestra progreso cada N registros. Use 0 para silenciar. Default: 100000",
    )
    parser.add_argument(
        "--resume-from-output",
        action="store_true",
        help=(
            "Lee la ultima linea del JSONL de salida y continua desde la "
            "siguiente etapa pendiente. Si quedo en MMedC, continua desde el "
            "siguiente archivo de MMedC y luego corre SciELO y SPACCC."
        ),
    )
    parser.add_argument(
        "--start-at",
        choices=sorted(START_AT_ALIASES),
        help=(
            "Ejecuta desde una base especifica y continua con las siguientes. "
            "Ejemplo: --start-at scielo agrega SciELO y SPACCC."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    output_file = args.output_file.resolve()

    if args.resume_from_output and args.start_at:
        print(
            "Use --resume-from-output o --start-at, pero no ambos.",
            file=sys.stderr,
        )
        return 1

    if args.resume_from_output:
        script_plan = build_resume_plan(script_dir, output_file)
    elif args.start_at:
        script_plan = build_start_at_plan(script_dir, args.start_at)
    else:
        script_plan = [
            (find_script(script_dir, suffix), []) for suffix in SCRIPT_SUFFIXES
        ]

    for script_path, extra_args in script_plan:
        run_script(
            script_path,
            output_file=output_file,
            progress_every=args.progress_every,
            extra_args=extra_args,
        )

    print("\nUnificacion completa terminada.")
    print(f"Archivo JSONL unificado: {output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
