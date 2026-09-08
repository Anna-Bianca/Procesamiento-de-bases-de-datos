from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
import hashlib
import html
from html import entities as html_entities
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import BinaryIO, Callable, Iterator, Mapping
import unicodedata


PASO_PROCESAMIENTO = "Paso 2: Normalizacion"
CHECKPOINT_VERSION = 2
NORMALIZATION_VERSION = "2.0"

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_PATH = REPO_ROOT / "Base de datos" / "1 - Unificado" / "unificado.jsonl"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "Base de datos" / "2 - Normalizacion" / "normalizado.jsonl"
DEFAULT_SUMMARY_PATH = REPO_ROOT / "Base de datos" / "2 - Normalizacion" / "normalizacion_resumen.json"

KNOWN_HTML_TAGS = {
    "a", "address", "area", "article", "aside", "b", "base", "blockquote",
    "body", "br", "canvas", "code", "col", "dd", "div", "dl", "dt",
    "em", "embed", "figcaption", "figure", "footer", "h1", "h2", "h3",
    "h4", "h5", "h6", "head", "header", "hr", "html", "i", "img",
    "input", "li", "link", "main", "menu", "meta", "nav", "noscript",
    "ol", "p", "param", "pre", "script", "section", "source", "span",
    "strong", "style", "sub", "sup", "svg", "table", "tbody", "td",
    "template", "tfoot", "th", "thead", "title", "tr", "track", "u", "ul",
    "wbr",
}
KNOWN_HTML_TAG_RE = re.compile(
    r"<\s*/?\s*(?:" + "|".join(sorted(KNOWN_HTML_TAGS)) + r")\b[^>]*>",
    flags=re.IGNORECASE,
)
WORD_RE = re.compile(r"[^\W\d_]+", flags=re.UNICODE)
HARD_HYPHEN_LINE_BREAK_RE = re.compile(
    r"(?P<left>[^\W\d_]{2,})-[ \t]*\n[ \t]*(?P<right>[^\W\d_]{2,})",
    flags=re.UNICODE,
)
SOFT_HYPHEN_LINE_BREAK_RE = re.compile(
    r"(?<=[^\W\d_])\u00ad[ \t]*\n[ \t]*(?=[^\W\d_])",
    flags=re.UNICODE,
)
STRICT_HTML_ENTITY_RE = re.compile(
    r"&(?:#[0-9]+|#[xX][0-9A-Fa-f]+|[A-Za-z][A-Za-z0-9]+);"
)
POSSIBLE_MOJIBAKE_RE = re.compile(
    r"(?:Ã[\x80-\xBF]|Â[\x80-\xBF]|â(?:€|™|œ|ž|€“|€”|€¦)|ðŸ|�)"
)
REMOVABLE_INVISIBLE_FORMAT_CHARACTERS = {"\u00ad", "\u200b", "\ufeff"}


@dataclass(frozen=True)
class TransformationResult:
    text: str
    changed: bool
    substitutions: int


@dataclass(frozen=True)
class NormalizedRecord:
    output_line: bytes
    text_changed: bool
    operation_stats: dict[str, dict[str, int]]
    possible_mojibake: bool


@dataclass
class ProcessingResult:
    records_processed: int
    records_changed: int
    errors: int
    operation_stats: dict[str, dict[str, int]]
    possible_mojibake_records: int
    elapsed_seconds: float
    input_fingerprint: dict[str, int | str]
    configuration: dict[str, object]


class HTMLTextExtractor(HTMLParser):
    """Extrae texto conservadoramente y descarta solo regiones inequívocas.

    La pila de ignorados contiene cada etiqueta no-void abierta dentro de una
    región descartable. Un cierre solo modifica la pila cuando coincide con su
    cima. Si la región queda ambigua al terminar el documento, se recupera su
    texto visible en vez de perderlo.
    """

    BLOCK_TAGS = {
        "address", "article", "aside", "blockquote", "dd", "div", "dl", "dt",
        "figcaption", "figure", "footer", "header", "h1", "h2", "h3", "h4",
        "h5", "h6", "hr", "li", "main", "ol", "p", "pre", "section", "table",
        "tbody", "td", "tfoot", "th", "thead", "tr", "ul",
    }
    ALWAYS_IGNORED_TAGS = {"menu", "nav", "script", "style", "template"}
    VOID_TAGS = {
        "area", "base", "br", "col", "embed", "hr", "img", "input", "link",
        "meta", "param", "source", "track", "wbr",
    }

    def __init__(self) -> None:
        # Las entidades válidas con ';' ya se decodificaron antes. Desactivar
        # convert_charrefs preserva variantes sin punto y coma.
        super().__init__(convert_charrefs=False)
        self.parts: list[str] = []
        self.ignored_stack: list[str] = []
        self.ignored_text: list[str] = []
        self.ignored_insert_index: int | None = None

    @staticmethod
    def _has_navigation_role(attrs: list[tuple[str, str | None]]) -> bool:
        for name, value in attrs:
            if name.casefold() == "role" and value:
                if "navigation" in {role.casefold() for role in value.split()}:
                    return True
        return False

    @staticmethod
    def _alternative_text(
        tag: str, attrs: list[tuple[str, str | None]]
    ) -> str | None:
        if tag not in {"area", "img"}:
            return None
        for name, value in attrs:
            if name.casefold() == "alt" and value and value.strip():
                return value.strip()
        return None

    def _begin_ignored(self, tag: str) -> None:
        self.ignored_stack = [tag]
        self.ignored_text = []
        self.ignored_insert_index = len(self.parts)

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        tag = tag.casefold()
        if self.ignored_stack:
            if tag not in self.VOID_TAGS:
                self.ignored_stack.append(tag)
            return
        if tag not in KNOWN_HTML_TAGS:
            self.parts.append(self.get_starttag_text() or f"<{tag}>")
            return
        if tag in self.ALWAYS_IGNORED_TAGS or self._has_navigation_role(attrs):
            if tag not in self.VOID_TAGS:
                self._begin_ignored(tag)
            return
        alternative_text = self._alternative_text(tag, attrs)
        if alternative_text:
            self.parts.append(f" {alternative_text} ")
        if tag == "br":
            self.parts.append("\n")
        elif tag in self.BLOCK_TAGS:
            self.parts.append("\n\n")

    def handle_startendtag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        tag = tag.casefold()
        if self.ignored_stack:
            return
        if tag not in KNOWN_HTML_TAGS:
            self.parts.append(self.get_starttag_text() or f"<{tag}/>")
            return
        if tag in self.ALWAYS_IGNORED_TAGS or self._has_navigation_role(attrs):
            return
        alternative_text = self._alternative_text(tag, attrs)
        if alternative_text:
            self.parts.append(f" {alternative_text} ")
        if tag in {"br", "hr"}:
            self.parts.append("\n" if tag == "br" else "\n\n")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.casefold()
        if self.ignored_stack:
            # Un cierre ajeno no reduce la profundidad del ignorado.
            if tag != self.ignored_stack[-1]:
                return
            self.ignored_stack.pop()
            if not self.ignored_stack:
                self.ignored_text = []
                self.ignored_insert_index = None
            return
        if tag not in KNOWN_HTML_TAGS:
            self.parts.append(f"</{tag}>")
        elif tag in self.BLOCK_TAGS:
            self.parts.append("\n\n")

    def handle_data(self, data: str) -> None:
        (self.ignored_text if self.ignored_stack else self.parts).append(data)

    def handle_entityref(self, name: str) -> None:
        target = self.ignored_text if self.ignored_stack else self.parts
        target.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        target = self.ignored_text if self.ignored_stack else self.parts
        target.append(f"&#{name};")

    def get_text(self) -> str:
        parts = list(self.parts)
        if self.ignored_stack and self.ignored_insert_index is not None:
            # HTML ambiguo: conservar el texto visible de una región no cerrada.
            parts.insert(self.ignored_insert_index, "".join(self.ignored_text))
        return "".join(parts)


def _result(original: str, transformed: str, substitutions: int) -> TransformationResult:
    return TransformationResult(
        text=transformed,
        changed=transformed != original,
        substitutions=substitutions if transformed != original else 0,
    )


def transform_line_endings(text: str) -> TransformationResult:
    crlf_count = text.count("\r\n")
    cr_count = text.count("\r") - crlf_count
    transformed = text.replace("\r\n", "\n").replace("\r", "\n")
    return _result(text, transformed, crlf_count + cr_count)


def transform_soft_hyphen_line_breaks(text: str) -> TransformationResult:
    transformed, line_break_count = SOFT_HYPHEN_LINE_BREAK_RE.subn("", text)
    residual_count = transformed.count("\u00ad")
    transformed = transformed.replace("\u00ad", "")
    return _result(text, transformed, line_break_count + residual_count)


def transform_evidenced_hyphenation(text: str) -> TransformationResult:
    intact_words = {word.casefold() for word in WORD_RE.findall(text)}
    substitutions = 0

    def replace(match: re.Match[str]) -> str:
        nonlocal substitutions
        joined = match.group("left") + match.group("right")
        if joined.casefold() in intact_words:
            substitutions += 1
            return joined
        return match.group(0)

    transformed = HARD_HYPHEN_LINE_BREAK_RE.sub(replace, text)
    return _result(text, transformed, substitutions)


def transform_invisible_controls(text: str) -> TransformationResult:
    cleaned: list[str] = []
    substitutions = 0
    for character in text:
        if character in {"\n", "\t"}:
            cleaned.append(character)
            continue
        category = unicodedata.category(character)
        if category in {"Zl", "Zp"}:
            cleaned.append("\n")
            substitutions += 1
        elif category == "Cc" or character in REMOVABLE_INVISIBLE_FORMAT_CHARACTERS:
            substitutions += 1
        else:
            cleaned.append(character)
    return _result(text, "".join(cleaned), substitutions)


def transform_unicode_nfc(text: str) -> TransformationResult:
    transformed = unicodedata.normalize("NFC", text)
    substitutions = max(1, abs(len(text) - len(transformed))) if transformed != text else 0
    return _result(text, transformed, substitutions)


def transform_html_entities(text: str) -> TransformationResult:
    # Alcanzar el punto fijo conserva idempotencia con codificaciones múltiples.
    transformed = text
    substitutions = 0
    while True:
        pass_substitutions = 0

        def decode(match: re.Match[str]) -> str:
            nonlocal pass_substitutions
            entity = match.group(0)
            if entity.startswith("&#"):
                digits = entity[2:-1]
                base = 10
                if digits[:1].casefold() == "x":
                    digits = digits[1:]
                    base = 16
                codepoint = int(digits, base)
                if (
                    codepoint == 0
                    or codepoint > 0x10FFFF
                    or 0xD800 <= codepoint <= 0xDFFF
                ):
                    return entity
            elif entity[1:] not in html_entities.html5:
                return entity
            decoded = html.unescape(entity)
            if decoded != entity:
                pass_substitutions += 1
            return decoded

        decoded_text = STRICT_HTML_ENTITY_RE.sub(decode, transformed)
        substitutions += pass_substitutions
        if decoded_text == transformed:
            break
        transformed = decoded_text
    return _result(text, transformed, substitutions)


def transform_residual_html(text: str) -> TransformationResult:
    if not KNOWN_HTML_TAG_RE.search(text):
        return _result(text, text, 0)
    parser = HTMLTextExtractor()
    try:
        parser.feed(text)
        parser.close()
    except (AssertionError, ValueError):
        return _result(text, text, 0)
    transformed = parser.get_text()
    tag_count = len(KNOWN_HTML_TAG_RE.findall(text))
    return _result(text, transformed, max(tag_count, abs(len(text) - len(transformed))))


def transform_horizontal_whitespace(text: str) -> TransformationResult:
    substitutions = 0
    output_lines: list[str] = []
    for line in text.split("\n"):
        collapsed, count = re.subn(r"[ \t]+", " ", line)
        stripped = collapsed.strip(" ")
        substitutions += count + int(stripped != collapsed)
        output_lines.append(stripped)
    joined = "\n".join(output_lines)
    transformed = joined.strip("\n")
    if transformed != joined:
        substitutions += 1
    return _result(text, transformed, substitutions)


def transform_blank_lines(text: str) -> TransformationResult:
    transformed, substitutions = re.subn(r"\n{3,}", "\n\n", text)
    return _result(text, transformed, substitutions)


TRANSFORMATIONS: tuple[tuple[str, Callable[[str], TransformationResult]], ...] = (
    ("finales_linea_iniciales", transform_line_endings),
    ("guiones_blandos", transform_soft_hyphen_line_breaks),
    ("guiones_visibles_con_evidencia", transform_evidenced_hyphenation),
    ("controles_e_invisibles", transform_invisible_controls),
    ("unicode_nfc_inicial", transform_unicode_nfc),
    ("entidades_html_estrictas", transform_html_entities),
    ("html_residual", transform_residual_html),
    ("finales_linea_posteriores_html", transform_line_endings),
    ("espacios_horizontales", transform_horizontal_whitespace),
    ("lineas_vacias", transform_blank_lines),
    ("unicode_nfc_final", transform_unicode_nfc),
)
TRANSFORMATION_NAMES = tuple(name for name, _function in TRANSFORMATIONS)


# Compatibilidad para usos directos de las funciones de la versión anterior.
def normalize_line_endings(text: str) -> str:
    return transform_line_endings(text).text


def repair_unequivocal_hyphenation(text: str) -> str:
    return transform_evidenced_hyphenation(transform_soft_hyphen_line_breaks(text).text).text


def remove_invisible_controls(text: str) -> str:
    return transform_invisible_controls(text).text


def decode_html_entities(text: str) -> str:
    return transform_html_entities(text).text


def remove_residual_html(text: str) -> str:
    return transform_residual_html(text).text


def normalize_whitespace(text: str) -> str:
    return transform_blank_lines(transform_horizontal_whitespace(text).text).text


def normalize_text_with_stats(text: str) -> tuple[str, dict[str, dict[str, int]]]:
    operation_stats: dict[str, dict[str, int]] = {}
    for name, transformation in TRANSFORMATIONS:
        result = transformation(text)
        operation_stats[name] = {
            "records_affected": int(result.changed),
            "substitutions": result.substitutions,
        }
        text = result.text
    return text, operation_stats


def normalize_text(text: str) -> str:
    return normalize_text_with_stats(text)[0]


def normalize_record(item: tuple[int, bytes] | tuple[int, bytes, int]) -> NormalizedRecord:
    physical_line_number, raw_line = item[0], item[1]
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

    raw_text = record.get("texto")
    if not isinstance(raw_text, str):
        raise ValueError(
            f"El campo 'texto' de la linea fisica {physical_line_number:,} no es texto."
        )
    processing = record.get("procesamiento")
    if not isinstance(processing, list) or not all(
        isinstance(step, str) for step in processing
    ):
        raise ValueError(
            f"El campo 'procesamiento' de la linea fisica {physical_line_number:,} "
            "no es una lista de textos."
        )

    normalized_text, operation_stats = normalize_text_with_stats(raw_text)
    record["texto"] = normalized_text
    if PASO_PROCESAMIENTO not in processing:
        processing.append(PASO_PROCESAMIENTO)
    output_line = (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8")
    return NormalizedRecord(
        output_line=output_line,
        text_changed=normalized_text != raw_text,
        operation_stats=operation_stats,
        possible_mojibake=bool(POSSIBLE_MOJIBAKE_RE.search(raw_text)),
    )


def iter_nonempty_lines(
    input_file: BinaryIO, *, first_physical_line_number: int = 1
) -> Iterator[tuple[int, bytes, int]]:
    physical_line_number = first_physical_line_number - 1
    while True:
        raw_line = input_file.readline()
        if not raw_line:
            return
        physical_line_number += 1
        end_offset = input_file.tell()
        if raw_line.strip():
            yield physical_line_number, raw_line, end_offset


def iter_batches(
    records: Iterator[tuple[int, bytes, int]],
    batch_size: int,
    max_batch_bytes: int,
) -> Iterator[list[tuple[int, bytes, int]]]:
    batch: list[tuple[int, bytes, int]] = []
    batch_bytes = 0
    for record in records:
        record_bytes = len(record[1])
        if batch and (
            len(batch) >= batch_size or batch_bytes + record_bytes > max_batch_bytes
        ):
            yield batch
            batch = []
            batch_bytes = 0
        batch.append(record)
        batch_bytes += record_bytes
    if batch:
        yield batch


def default_checkpoint_path(output_path: Path) -> Path:
    return output_path.with_suffix(output_path.suffix + ".checkpoint.json")


def input_fingerprint(input_path: Path) -> dict[str, int | str]:
    stat = input_path.stat()
    sample_size = 64 * 1024
    digest = hashlib.sha256()
    with input_path.open("rb") as input_file:
        digest.update(input_file.read(sample_size))
        if stat.st_size > sample_size:
            input_file.seek(max(0, stat.st_size - sample_size))
            digest.update(input_file.read(sample_size))
    return {
        "path": str(input_path.resolve()),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "sample_sha256": digest.hexdigest(),
    }


def write_json_atomic(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    with temporary_path.open("w", encoding="utf-8", newline="\n") as output_file:
        json.dump(value, output_file, ensure_ascii=False, indent=2, sort_keys=True)
        output_file.write("\n")
        output_file.flush()
        os.fsync(output_file.fileno())
    os.replace(temporary_path, path)


def write_checkpoint(checkpoint_path: Path, state: dict[str, object]) -> None:
    write_json_atomic(checkpoint_path, state)


def read_checkpoint(checkpoint_path: Path) -> dict[str, object]:
    try:
        with checkpoint_path.open("r", encoding="utf-8") as checkpoint_file:
            state = json.load(checkpoint_file)
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"No se pudo leer el checkpoint: {checkpoint_path}") from error
    if not isinstance(state, dict) or state.get("version") != CHECKPOINT_VERSION:
        raise ValueError(f"Checkpoint incompatible: {checkpoint_path}")
    return state


def empty_operation_stats() -> dict[str, dict[str, int]]:
    return {
        name: {"records_affected": 0, "substitutions": 0}
        for name in TRANSFORMATION_NAMES
    }


def merge_operation_stats(
    target: dict[str, dict[str, int]],
    source: Mapping[str, Mapping[str, int]],
) -> None:
    for name in TRANSFORMATION_NAMES:
        values = source.get(name, {})
        target[name]["records_affected"] += int(values.get("records_affected", 0))
        target[name]["substitutions"] += int(values.get("substitutions", 0))


def _validated_nonnegative_int(state: Mapping[str, object], field_name: str) -> int:
    value = state.get(field_name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"El checkpoint tiene un valor invalido para {field_name}.")
    return value


def validate_operation_stats(value: object) -> dict[str, dict[str, int]]:
    if not isinstance(value, dict):
        raise ValueError("El checkpoint no contiene estadisticas de operaciones validas.")
    result = empty_operation_stats()
    for name in TRANSFORMATION_NAMES:
        operation = value.get(name)
        if not isinstance(operation, dict):
            raise ValueError(f"Faltan estadisticas de la operacion {name}.")
        for metric in ("records_affected", "substitutions"):
            metric_value = operation.get(metric)
            if (
                not isinstance(metric_value, int)
                or isinstance(metric_value, bool)
                or metric_value < 0
            ):
                raise ValueError(f"Estadistica invalida: {name}.{metric}.")
            result[name][metric] = metric_value
    return result


def normalization_configuration(
    *, batch_size: int, max_batch_bytes: int, workers: int
) -> dict[str, object]:
    return {
        "normalization_version": NORMALIZATION_VERSION,
        "transformations": list(TRANSFORMATION_NAMES),
        "unicode_normalization": "NFC",
        "html_entity_policy": "validas_y_terminadas_en_punto_y_coma",
        "mojibake_repair": False,
        "batch_size": batch_size,
        "max_batch_bytes": max_batch_bytes,
        "workers": workers,
    }


def build_checkpoint_state(
    *,
    input_path: Path,
    output_path: Path,
    configuration: Mapping[str, object],
    records_processed: int,
    records_changed: int,
    operation_stats: Mapping[str, Mapping[str, int]],
    possible_mojibake_records: int,
    errors: int,
    elapsed_seconds: float,
    input_bytes: int,
    physical_lines_read: int,
    output_bytes: int,
    completed: bool,
) -> dict[str, object]:
    return {
        "version": CHECKPOINT_VERSION,
        "input": input_fingerprint(input_path),
        "output_path": str(output_path),
        "configuration": dict(configuration),
        "records_processed": records_processed,
        "records_changed": records_changed,
        "operation_stats": {name: dict(values) for name, values in operation_stats.items()},
        "possible_mojibake_records": possible_mojibake_records,
        "errors": errors,
        "elapsed_seconds": round(elapsed_seconds, 6),
        "input_bytes": input_bytes,
        "physical_lines_read": physical_lines_read,
        "output_bytes": output_bytes,
        "completed": completed,
    }


def validate_resume_state(
    state: dict[str, object], *, input_path: Path, output_path: Path
) -> dict[str, object]:
    if state.get("input") != input_fingerprint(input_path):
        raise ValueError("El archivo de entrada cambio desde la creacion del checkpoint.")
    if state.get("output_path") != str(output_path):
        raise ValueError("El checkpoint pertenece a otro archivo de salida.")
    numeric_fields = {
        field_name: _validated_nonnegative_int(state, field_name)
        for field_name in (
            "records_processed", "records_changed", "possible_mojibake_records",
            "errors", "input_bytes", "physical_lines_read", "output_bytes",
        )
    }
    elapsed_seconds = state.get("elapsed_seconds")
    if not isinstance(elapsed_seconds, (int, float)) or elapsed_seconds < 0:
        raise ValueError("El checkpoint tiene una duracion invalida.")
    operation_stats = validate_operation_stats(state.get("operation_stats"))
    if not output_path.exists():
        raise FileNotFoundError(f"No existe el archivo parcial: {output_path}")
    if output_path.stat().st_size < numeric_fields["output_bytes"]:
        raise ValueError("El archivo parcial es mas corto que lo registrado.")
    if numeric_fields["input_bytes"] > input_path.stat().st_size:
        raise ValueError("El checkpoint apunta fuera del archivo de entrada.")
    return {
        **numeric_fields,
        "elapsed_seconds": float(elapsed_seconds),
        "operation_stats": operation_stats,
        "completed": state.get("completed") is True,
        "configuration": state.get("configuration"),
    }


def automatic_worker_count() -> int:
    available = os.cpu_count() or 2
    return max(1, min(8, available - 1))


def build_summary(result: ProcessingResult) -> dict[str, object]:
    return {
        "version_checkpoint": CHECKPOINT_VERSION,
        "version_normalizacion": NORMALIZATION_VERSION,
        "registros_procesados": result.records_processed,
        "registros_modificados": result.records_changed,
        "registros_sin_cambios": result.records_processed - result.records_changed,
        "registros_posible_mojibake_sin_reparar": result.possible_mojibake_records,
        "transformaciones": result.operation_stats,
        "errores": result.errors,
        "duracion_total_segundos": round(result.elapsed_seconds, 6),
        "configuracion": result.configuration,
        "fingerprint_entrada": result.input_fingerprint,
    }


def process_file(
    *,
    input_path: Path,
    output_path: Path,
    checkpoint_path: Path,
    summary_path: Path | None = None,
    batch_size: int,
    max_batch_bytes: int,
    workers: int,
    progress_every: int,
    resume: bool,
    overwrite: bool,
) -> ProcessingResult:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path = summary_path or output_path.with_name("normalizacion_resumen.json")
    configuration = normalization_configuration(
        batch_size=batch_size, max_batch_bytes=max_batch_bytes, workers=workers
    )

    if resume:
        if overwrite:
            raise ValueError("Use --resume o --overwrite, pero no ambos.")
        if not checkpoint_path.exists():
            raise FileNotFoundError(
                f"No existe el checkpoint necesario para reanudar: {checkpoint_path}"
            )
        state = read_checkpoint(checkpoint_path)
        resume_state = validate_resume_state(
            state, input_path=input_path, output_path=output_path
        )
        processed = int(resume_state["records_processed"])
        changed = int(resume_state["records_changed"])
        possible_mojibake_records = int(resume_state["possible_mojibake_records"])
        errors = int(resume_state["errors"])
        input_offset = int(resume_state["input_bytes"])
        physical_lines_read = int(resume_state["physical_lines_read"])
        output_offset = int(resume_state["output_bytes"])
        previous_elapsed = float(resume_state["elapsed_seconds"])
        operation_stats = resume_state["operation_stats"]
        assert isinstance(operation_stats, dict)
        if resume_state["completed"]:
            result = ProcessingResult(
                records_processed=processed,
                records_changed=changed,
                errors=errors,
                operation_stats=operation_stats,
                possible_mojibake_records=possible_mojibake_records,
                elapsed_seconds=previous_elapsed,
                input_fingerprint=input_fingerprint(input_path),
                configuration=(
                    resume_state["configuration"]
                    if isinstance(resume_state["configuration"], dict)
                    else configuration
                ),
            )
            if not summary_path.exists():
                write_json_atomic(summary_path, build_summary(result))
            print("El checkpoint indica que la normalizacion ya esta completa.")
            return result
        output_mode = "r+b"
    else:
        if output_path.exists() and not overwrite:
            raise FileExistsError(
                f"Ya existe {output_path}. Use --overwrite para recrearlo o "
                "--resume para continuar un proceso interrumpido."
            )
        if summary_path.exists() and not overwrite:
            raise FileExistsError(f"Ya existe {summary_path}. Use --overwrite para recrearlo.")
        processed = changed = possible_mojibake_records = errors = 0
        input_offset = physical_lines_read = output_offset = 0
        previous_elapsed = 0.0
        operation_stats = empty_operation_stats()
        output_mode = "wb"

    run_started = time.perf_counter()

    def current_elapsed() -> float:
        return previous_elapsed + (time.perf_counter() - run_started)

    with input_path.open("rb") as input_file:
        if processed:
            print(
                f"Reanudando despues de {processed:,} registros confirmados.",
                file=sys.stderr,
                flush=True,
            )
        input_file.seek(input_offset)
        records = iter_nonempty_lines(
            input_file, first_physical_line_number=physical_lines_read + 1
        )
        with output_path.open(output_mode) as output_file:
            if resume:
                output_file.truncate(output_offset)
                output_file.seek(output_offset)
            else:
                write_checkpoint(
                    checkpoint_path,
                    build_checkpoint_state(
                        input_path=input_path, output_path=output_path,
                        configuration=configuration, records_processed=0,
                        records_changed=0, operation_stats=operation_stats,
                        possible_mojibake_records=0, errors=0, elapsed_seconds=0.0,
                        input_bytes=0, physical_lines_read=0, output_bytes=0,
                        completed=False,
                    ),
                )

            next_progress = (
                ((processed // progress_every) + 1) * progress_every
                if progress_every else None
            )
            executor = ProcessPoolExecutor(max_workers=workers) if workers > 1 else None
            try:
                for batch in iter_batches(records, batch_size, max_batch_bytes):
                    try:
                        if executor is None:
                            normalized_results = [normalize_record(item) for item in batch]
                        else:
                            chunksize = max(1, len(batch) // (workers * 4))
                            normalized_results = list(
                                executor.map(normalize_record, batch, chunksize=chunksize)
                            )
                    except (OSError, ValueError):
                        errors += 1
                        write_checkpoint(
                            checkpoint_path,
                            build_checkpoint_state(
                                input_path=input_path, output_path=output_path,
                                configuration=configuration, records_processed=processed,
                                records_changed=changed, operation_stats=operation_stats,
                                possible_mojibake_records=possible_mojibake_records,
                                errors=errors, elapsed_seconds=current_elapsed(),
                                input_bytes=input_offset,
                                physical_lines_read=physical_lines_read,
                                output_bytes=output_offset, completed=False,
                            ),
                        )
                        raise

                    batch_operation_stats = empty_operation_stats()
                    batch_changed = batch_mojibake = 0
                    for normalized in normalized_results:
                        output_file.write(normalized.output_line)
                        batch_changed += int(normalized.text_changed)
                        batch_mojibake += int(normalized.possible_mojibake)
                        merge_operation_stats(batch_operation_stats, normalized.operation_stats)
                    output_file.flush()
                    os.fsync(output_file.fileno())
                    processed += len(normalized_results)
                    changed += batch_changed
                    possible_mojibake_records += batch_mojibake
                    merge_operation_stats(operation_stats, batch_operation_stats)
                    input_offset = batch[-1][2]
                    physical_lines_read = batch[-1][0]
                    output_offset = output_file.tell()
                    write_checkpoint(
                        checkpoint_path,
                        build_checkpoint_state(
                            input_path=input_path, output_path=output_path,
                            configuration=configuration, records_processed=processed,
                            records_changed=changed, operation_stats=operation_stats,
                            possible_mojibake_records=possible_mojibake_records,
                            errors=errors, elapsed_seconds=current_elapsed(),
                            input_bytes=input_offset,
                            physical_lines_read=physical_lines_read,
                            output_bytes=output_offset, completed=False,
                        ),
                    )
                    while next_progress is not None and processed >= next_progress:
                        print(
                            f"Registros normalizados: {processed:,}",
                            file=sys.stderr, flush=True,
                        )
                        next_progress += progress_every
            finally:
                if executor is not None:
                    executor.shutdown(wait=True, cancel_futures=True)
            output_file.flush()
            os.fsync(output_file.fileno())
            output_offset = output_file.tell()
        input_offset = input_file.tell()

    elapsed_seconds = current_elapsed()
    write_checkpoint(
        checkpoint_path,
        build_checkpoint_state(
            input_path=input_path, output_path=output_path,
            configuration=configuration, records_processed=processed,
            records_changed=changed, operation_stats=operation_stats,
            possible_mojibake_records=possible_mojibake_records,
            errors=errors, elapsed_seconds=elapsed_seconds, input_bytes=input_offset,
            physical_lines_read=physical_lines_read, output_bytes=output_offset,
            completed=True,
        ),
    )
    result = ProcessingResult(
        records_processed=processed, records_changed=changed, errors=errors,
        operation_stats=operation_stats,
        possible_mojibake_records=possible_mojibake_records,
        elapsed_seconds=elapsed_seconds, input_fingerprint=input_fingerprint(input_path),
        configuration=configuration,
    )
    write_json_atomic(summary_path, build_summary(result))
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normaliza el campo 'texto' de unificado.jsonl y conserva sus metadatos."
    )
    parser.add_argument("--input-file", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output-file", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument(
        "--checkpoint-file", type=Path,
        help="Checkpoint de reanudacion. Default: <output-file>.checkpoint.json",
    )
    parser.add_argument("--summary-file", type=Path, default=DEFAULT_SUMMARY_PATH)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--batch-max-mb", type=float, default=64.0)
    parser.add_argument("--workers", type=int, default=0)
    parser.add_argument("--progress-every", type=int, default=500)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = args.input_file.resolve()
    output_path = args.output_file.resolve()
    checkpoint_path = (
        args.checkpoint_file.resolve()
        if args.checkpoint_file else default_checkpoint_path(output_path)
    )
    summary_path = args.summary_file.resolve()
    if not input_path.exists():
        print(f"No existe el archivo de entrada: {input_path}", file=sys.stderr)
        return 1
    if not input_path.is_file():
        print(f"La entrada no es un archivo: {input_path}", file=sys.stderr)
        return 1
    if input_path in {output_path, checkpoint_path, summary_path}:
        print("La entrada y los archivos de salida deben ser distintos.", file=sys.stderr)
        return 1
    if args.batch_size <= 0 or args.batch_max_mb <= 0:
        print("El tamano y el limite de lote deben ser mayores que 0.", file=sys.stderr)
        return 1
    if args.workers < 0 or args.progress_every < 0:
        print("--workers y --progress-every no pueden ser negativos.", file=sys.stderr)
        return 1

    workers = args.workers or automatic_worker_count()
    max_batch_bytes = max(1, int(args.batch_max_mb * 1024 * 1024))
    print(f"Procesos de normalizacion: {workers}", file=sys.stderr)
    print(f"Tamano de lote: {args.batch_size:,}", file=sys.stderr)
    print(f"Limite aproximado por lote: {args.batch_max_mb:g} MiB", file=sys.stderr)
    try:
        result = process_file(
            input_path=input_path, output_path=output_path,
            checkpoint_path=checkpoint_path, summary_path=summary_path,
            batch_size=args.batch_size, max_batch_bytes=max_batch_bytes,
            workers=workers, progress_every=args.progress_every,
            resume=args.resume, overwrite=args.overwrite,
        )
    except KeyboardInterrupt:
        print(
            "\nNormalizacion interrumpida. Use --resume para continuar desde "
            "el ultimo lote confirmado.", file=sys.stderr,
        )
        return 130
    except (FileExistsError, FileNotFoundError, OSError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1

    print("Normalizacion terminada.")
    print(f"Registros procesados: {result.records_processed:,}")
    print(f"Registros cuyo texto cambio: {result.records_changed:,}")
    print(f"Archivo JSONL normalizado: {output_path}")
    print(f"Resumen: {summary_path}")
    print(f"Checkpoint: {checkpoint_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
