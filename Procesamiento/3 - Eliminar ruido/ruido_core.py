from __future__ import annotations

from abc import ABC, abstractmethod
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
import hashlib
import json
import re
import statistics
from typing import Callable, Iterable, Iterator, Mapping, Sequence
import unicodedata


AUDIT_VERSION = "3.0"
DELETE_SPAN = "eliminar_fragmento"
DELETE_RECORD = "eliminar_registro"

WORD_RE = re.compile(r"[^\W_]+(?:[-'][^\W_]+)*", flags=re.UNICODE)
URL_RE = re.compile(
    r"(?i)\b(?:https?://|www\.)[^\s<>()]+|"
    r"\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+(?:com|org|net|edu|gov|"
    r"int|io|es|ar|mx|cl|co|uy|ve|info)(?:/[^\s<>()]*)?"
)
DOI_RE = re.compile(r"(?i)\b(?:doi\s*:\s*|https?://doi\.org/)?10\.\d{4,9}/\S+")
PUBMED_RE = re.compile(r"(?i)\b(?:pubmed|pmid|pmcid)\s*:?")
REFERENCE_RE = re.compile(
    r"(?im)^\s*(?:referencias|bibliograf[ií]a)\b|"
    r"\b(?:vol\.?|doi|et\s+al\.|pmid|pmcid)\b|\(?(?:19|20)\d{2}\)?[.;]"
)
BIOMEDICAL_RE = re.compile(
    r"(?i)\b(?:paciente|diagn[oó]stic[oa]|tratamiento|ensayo|estudio|resultados?|"
    r"m[eé]todo|muestra|cl[ií]nic[oa]|enfermedad|s[ií]ndrome|prote[ií]na|gen(?:es)?|"
    r"c[eé]lula|terapia|dosis|prevalencia|incidencia|mortalidad|hip[oó]tesis|"
    r"intervalo de confianza|p\s*[<=>]\s*0[.,])\b"
)


@dataclass(frozen=True)
class Span:
    start: int
    end: int
    text: str


@dataclass(frozen=True)
class Thresholds:
    short_line_max_chars: int = 45
    peripheral_ratio: float = 0.18
    navigation_min_lines: int = 3
    navigation_min_phrases: int = 2
    cookie_min_signals: int = 2
    repeated_header_min_docs: int = 3
    repeated_header_min_ratio: float = 0.001
    repeated_header_max_chars: int = 400
    link_list_min_urls: int = 3
    advertising_min_signals: int = 2
    http_error_max_chars: int = 900
    repeated_letter_min: int = 12
    repeated_digit_min: int = 14
    repeated_punctuation_min: int = 20
    repeated_whitespace_min: int = 30
    code_min_signals: int = 3
    near_empty_max_visible: int = 40
    near_empty_max_words: int = 3
    automatic_index_min_lines: int = 5
    internal_repetition_min_occurrences: int = 3
    internal_repetition_min_chars: int = 20

    def validate(self) -> None:
        integer_fields = (
            "short_line_max_chars", "navigation_min_lines",
            "navigation_min_phrases", "cookie_min_signals",
            "repeated_header_min_docs", "repeated_header_max_chars",
            "link_list_min_urls", "advertising_min_signals",
            "http_error_max_chars", "repeated_letter_min", "repeated_digit_min",
            "repeated_punctuation_min", "repeated_whitespace_min",
            "code_min_signals", "near_empty_max_visible", "near_empty_max_words",
            "automatic_index_min_lines", "internal_repetition_min_occurrences",
            "internal_repetition_min_chars",
        )
        for name in integer_fields:
            if getattr(self, name) < 0:
                raise ValueError(f"El umbral {name} no puede ser negativo.")
        for name in ("peripheral_ratio", "repeated_header_min_ratio"):
            value = getattr(self, name)
            if not 0 <= value <= 1:
                raise ValueError(f"El umbral {name} debe estar entre 0 y 1.")


@dataclass(frozen=True)
class DetectionContext:
    record_number: int
    record: Mapping[str, object]
    total_documents: int
    thresholds: Thresholds
    global_block_lookup: Callable[[str], Mapping[str, object]]


@dataclass(frozen=True)
class DetectorResult:
    reason_code: str
    detector_name: str
    level: str
    start: int
    end: int
    confidence: float
    metrics: Mapping[str, object]
    proposed_action: str

    def __post_init__(self) -> None:
        if self.start < 0 or self.end < self.start:
            raise ValueError("Span de deteccion invalido.")
        if not 0 <= self.confidence <= 1:
            raise ValueError("La confianza debe estar entre 0 y 1.")


@dataclass(frozen=True)
class ConsolidatedResult:
    reason_codes: tuple[str, ...]
    detector_names: tuple[str, ...]
    level: str
    start: int
    end: int
    confidence: float
    metrics: Mapping[str, object]
    proposed_action: str


def safe_ratio(numerator: int | float, denominator: int | float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def unicode_words(text: str) -> list[str]:
    return WORD_RE.findall(text)


def url_matches(text: str) -> list[re.Match[str]]:
    return list(URL_RE.finditer(text))


def line_spans(text: str) -> list[Span]:
    spans: list[Span] = []
    start = 0
    for line in text.splitlines(keepends=True):
        content = line.rstrip("\r\n")
        left_trim = len(content) - len(content.lstrip())
        right = len(content.rstrip())
        if right > left_trim:
            spans.append(Span(start + left_trim, start + right, content[left_trim:right]))
        start += len(line)
    if text and (not text.splitlines(keepends=True) or start < len(text)):
        tail = text[start:]
        left_trim = len(tail) - len(tail.lstrip())
        right = len(tail.rstrip())
        if right > left_trim:
            spans.append(Span(start + left_trim, start + right, tail[left_trim:right]))
    return spans


def block_spans(text: str) -> list[Span]:
    """Devuelve bloques no vacíos separados por una o más líneas vacías."""
    spans: list[Span] = []
    cursor = 0
    for separator in re.finditer(r"\n[ \t]*\n+", text):
        raw_start, raw_end = cursor, separator.start()
        raw = text[raw_start:raw_end]
        left = len(raw) - len(raw.lstrip())
        right = len(raw.rstrip())
        if right > left:
            spans.append(Span(raw_start + left, raw_start + right, raw[left:right]))
        cursor = separator.end()
    raw = text[cursor:]
    left = len(raw) - len(raw.lstrip())
    right = len(raw.rstrip())
    if right > left:
        spans.append(Span(cursor + left, cursor + right, raw[left:right]))
    return spans


def position_ratio(start: int, text_length: int) -> float:
    return safe_ratio(start, max(text_length, 1))


def is_peripheral(start: int, end: int, text_length: int, ratio: float) -> bool:
    if text_length <= 0:
        return True
    return start / text_length <= ratio or end / text_length >= 1 - ratio


def normalize_for_comparison(text: str) -> str:
    return " ".join(text.casefold().split())


def normalized_block_hash(text: str) -> str:
    return hashlib.sha256(normalize_for_comparison(text).encode("utf-8")).hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def record_stable_key(record: Mapping[str, object], record_number: int) -> str:
    value = {
        "numero_registro": record_number,
        "id": record.get("id"),
        "base_de_datos_origen": record.get("base_de_datos_origen"),
        "ruta_relativa_origen": record.get("ruta_relativa_origen"),
        "archivo_origen": record.get("archivo_origen"),
    }
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def build_candidate_id(
    *,
    input_sha256: str,
    stable_record_key: str,
    reason_codes: Sequence[str],
    start: int,
    end: int,
    fragment_hash: str,
) -> str:
    payload = {
        "audit_version": AUDIT_VERSION,
        "input_sha256": input_sha256,
        "record": stable_record_key,
        "reason_codes": sorted(reason_codes),
        "start": start,
        "end": end,
        "fragment_hash": fragment_hash,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def text_metrics(text: str) -> dict[str, int | float]:
    non_whitespace = [character for character in text if not character.isspace()]
    words = unicode_words(text)
    alphabetic_count = sum(character.isalpha() for character in non_whitespace)
    digit_count = sum(character.isdigit() for character in non_whitespace)
    alphanumeric_count = sum(character.isalnum() for character in non_whitespace)
    uppercase_count = sum(character.isupper() for character in non_whitespace)
    urls = url_matches(text)
    url_characters = sum(len(match.group(0)) for match in urls)
    lines = [line for line in text.splitlines() if line.strip()]
    return {
        "visible_character_count": len(non_whitespace),
        "non_whitespace_character_count": len(non_whitespace),
        "word_count": len(words),
        "alphabetic_character_count": alphabetic_count,
        "alphabetic_ratio": safe_ratio(alphabetic_count, len(non_whitespace)),
        "digit_ratio": safe_ratio(digit_count, len(non_whitespace)),
        "non_alphanumeric_ratio": 1.0 - safe_ratio(alphanumeric_count, len(non_whitespace))
        if non_whitespace else 0.0,
        "uppercase_ratio": safe_ratio(uppercase_count, alphabetic_count),
        "line_count": len(lines),
        "url_count": len(urls),
        "url_character_count": url_characters,
        "url_density": safe_ratio(url_characters, len(non_whitespace)),
        "url_per_word": safe_ratio(len(urls), len(words)),
        "url_per_line": safe_ratio(len(urls), len(lines)),
    }


def line_length_metrics(text: str, short_line_max_chars: int) -> dict[str, object]:
    lengths = [len(line.strip()) for line in text.splitlines() if line.strip()]
    if not lengths:
        return {
            "line_lengths": [], "line_length_mean": 0.0,
            "line_length_median": 0.0, "line_length_stddev": 0.0,
            "short_line_ratio": 0.0, "average_line_length": 0.0,
        }
    mean = statistics.fmean(lengths)
    return {
        "line_lengths": lengths,
        "line_length_mean": mean,
        "line_length_median": statistics.median(lengths),
        "line_length_stddev": statistics.pstdev(lengths),
        "short_line_ratio": safe_ratio(
            sum(length <= short_line_max_chars for length in lengths), len(lengths)
        ),
        "average_line_length": mean,
    }


def repetition_metrics(text: str) -> dict[str, int | float]:
    normalized_blocks = [normalize_for_comparison(span.text) for span in block_spans(text)]
    normalized_blocks = [block for block in normalized_blocks if block]
    counts = Counter(normalized_blocks)
    repeated_occurrences = sum(count - 1 for count in counts.values() if count > 1)
    return {
        "block_occurrence_count": len(normalized_blocks),
        "unique_block_count": len(counts),
        "repeated_block_occurrence_count": repeated_occurrences,
        "repeated_block_ratio": safe_ratio(repeated_occurrences, len(normalized_blocks)),
    }


def lexical_diversity(text: str) -> float:
    words = [word.casefold() for word in unicode_words(text)]
    return safe_ratio(len(set(words)), len(words))


def is_scientific_reference_block(text: str) -> bool:
    return bool((DOI_RE.search(text) or PUBMED_RE.search(text)) and REFERENCE_RE.search(text))


def is_substantive_biomedical(text: str) -> bool:
    words = unicode_words(text)
    return len(words) >= 25 and bool(BIOMEDICAL_RE.search(text))


class Detector(ABC):
    reason_code: str
    name: str
    level: str

    @abstractmethod
    def detect(self, text: str, context: DetectionContext) -> list[DetectorResult]:
        raise NotImplementedError

    def result(
        self,
        *,
        start: int,
        end: int,
        confidence: float,
        metrics: Mapping[str, object],
        action: str = DELETE_SPAN,
        level: str | None = None,
    ) -> DetectorResult:
        return DetectorResult(
            reason_code=self.reason_code,
            detector_name=self.name,
            level=level or self.level,
            start=start,
            end=end,
            confidence=max(0.0, min(1.0, confidence)),
            metrics=metrics,
            proposed_action=action,
        )


NAVIGATION_PHRASE_RE = re.compile(
    r"(?i)\b(?:inicio|buscar|contacto|men[uú]|siguiente|anterior|mapa del sitio|"
    r"iniciar sesi[oó]n|cerrar sesi[oó]n|volver arriba|ir al contenido|home|search|"
    r"next|previous)\b"
)
SEPARATOR_LINE_RE = re.compile(r"(?m)^\s*[-_=|•·]{3,}\s*$")


class NavigationResidualDetector(Detector):
    reason_code = "NAVIGATION_RESIDUAL"
    name = "Menus y barras de navegacion residuales"
    level = "bloque"

    def detect(self, text: str, context: DetectionContext) -> list[DetectorResult]:
        results: list[DetectorResult] = []
        thresholds = context.thresholds
        for block in block_spans(text):
            base = text_metrics(block.text)
            line_stats = line_length_metrics(block.text, thresholds.short_line_max_chars)
            phrases = [match.group(0).casefold() for match in NAVIGATION_PHRASE_RE.finditer(block.text)]
            global_stats = context.global_block_lookup(normalized_block_hash(block.text))
            global_frequency = int(global_stats.get("document_count", 0))
            peripheral = is_peripheral(
                block.start, block.end, len(text), thresholds.peripheral_ratio
            )
            separator_count = len(SEPARATOR_LINE_RE.findall(block.text))
            signals = [
                base["line_count"] >= thresholds.navigation_min_lines
                and line_stats["short_line_ratio"] >= 0.66,
                len(phrases) >= thresholds.navigation_min_phrases,
                base["url_count"] >= 2 or base["url_density"] >= 0.20,
                peripheral,
                global_frequency >= thresholds.repeated_header_min_docs,
                separator_count >= 2,
            ]
            enough_navigation = len(phrases) >= thresholds.navigation_min_phrases
            linked_navigation = len(phrases) >= 1 and base["url_count"] >= 2
            if (
                sum(signals) >= 3
                and (enough_navigation or linked_navigation)
                and not is_substantive_biomedical(block.text)
            ):
                metrics = {
                    **base, **line_stats,
                    "navigation_phrases": sorted(set(phrases)),
                    "navigation_phrase_count": len(phrases),
                    "separator_count": separator_count,
                    "position_ratio": position_ratio(block.start, len(text)),
                    "peripheral": peripheral,
                    "global_document_frequency": global_frequency,
                    "signal_count": sum(signals),
                }
                results.append(
                    self.result(
                        start=block.start, end=block.end,
                        confidence=min(0.97, 0.50 + 0.08 * sum(signals)),
                        metrics=metrics,
                    )
                )
        return results


COOKIE_PHRASES = (
    "uso de cookies", "utilizamos cookies", "aceptar cookies",
    "configurar cookies", "política de cookies", "politica de cookies",
    "preferencias de privacidad", "administrar preferencias",
    "gestionar preferencias", "aceptar todas", "rechazar todas",
    "consentimiento de cookies",
)
COOKIE_ACTION_RE = re.compile(
    r"(?i)\b(?:aceptar(?:\s+todas)?|rechazar(?:\s+todas)?|configurar|administrar|"
    r"gestionar)\b"
)


class CookieNoticeDetector(Detector):
    reason_code = "COOKIE_NOTICE"
    name = "Aviso de cookies"
    level = "bloque"

    def detect(self, text: str, context: DetectionContext) -> list[DetectorResult]:
        results: list[DetectorResult] = []
        for block in block_spans(text):
            folded = block.text.casefold()
            found = sorted({phrase for phrase in COOKIE_PHRASES if phrase in folded})
            action_count = len(COOKIE_ACTION_RE.findall(block.text))
            peripheral = is_peripheral(
                block.start, block.end, len(text), context.thresholds.peripheral_ratio
            )
            unequivocal = any(
                phrase in found
                for phrase in ("utilizamos cookies", "consentimiento de cookies")
            )
            signal_count = len(found) + int(action_count > 0) + int(peripheral)
            if (
                (len(found) >= context.thresholds.cookie_min_signals)
                or (unequivocal and peripheral and len(block.text) <= 700)
            ) and not is_substantive_biomedical(block.text):
                ratio = safe_ratio(len(block.text), len(text))
                metrics = {
                    "phrases_found": found,
                    "match_count": len(found),
                    "action_phrase_count": action_count,
                    "document_ratio": ratio,
                    "position_ratio": position_ratio(block.start, len(text)),
                    "block_length": len(block.text),
                    "peripheral": peripheral,
                    "signal_count": signal_count,
                }
                action = DELETE_RECORD if ratio >= 0.90 else DELETE_SPAN
                level = "registro" if action == DELETE_RECORD else "bloque"
                results.append(
                    self.result(
                        start=0 if action == DELETE_RECORD else block.start,
                        end=len(text) if action == DELETE_RECORD else block.end,
                        confidence=min(0.98, 0.62 + 0.08 * signal_count),
                        metrics=metrics, action=action, level=level,
                    )
                )
        return results


BOILERPLATE_RE = re.compile(
    r"(?i)\b(?:todos los derechos reservados|copyright|aviso legal|pol[ií]tica de "
    r"privacidad|contacto|inicio|men[uú]|mapa del sitio|sitio web oficial|"
    r"powered by|volver arriba)\b"
)


class RepeatedHeaderFooterDetector(Detector):
    reason_code = "REPEATED_HEADER_FOOTER"
    name = "Encabezado o pie repetido"
    level = "bloque"

    def detect(self, text: str, context: DetectionContext) -> list[DetectorResult]:
        results: list[DetectorResult] = []
        thresholds = context.thresholds
        for block in block_spans(text):
            stats = context.global_block_lookup(normalized_block_hash(block.text))
            document_count = int(stats.get("document_count", 0))
            frequency_ratio = safe_ratio(document_count, context.total_documents)
            edge_count = int(stats.get("start_count", 0)) + int(stats.get("end_count", 0))
            edge_ratio = safe_ratio(edge_count, document_count)
            words = unicode_words(block.text)
            boilerplate_count = len(BOILERPLATE_RE.findall(block.text))
            if (
                document_count >= thresholds.repeated_header_min_docs
                and frequency_ratio >= thresholds.repeated_header_min_ratio
                and edge_ratio >= 0.70
                and 1 < len(words) <= 50
                and len(block.text) <= thresholds.repeated_header_max_chars
                and (
                    boilerplate_count >= 1
                    or (
                        len(words) <= 6
                        and not re.search(r"[.!?]", block.text)
                    )
                    or lexical_diversity(block.text) <= 0.70
                )
                and not is_substantive_biomedical(block.text)
            ):
                metrics = {
                    "normalized_block_hash": normalized_block_hash(block.text),
                    "global_document_frequency": document_count,
                    "global_document_ratio": frequency_ratio,
                    "global_start_count": int(stats.get("start_count", 0)),
                    "global_end_count": int(stats.get("end_count", 0)),
                    "global_middle_count": int(stats.get("middle_count", 0)),
                    "global_edge_ratio": edge_ratio,
                    "origin_document_counts": stats.get("origin_counts", {}),
                    "block_length": len(block.text),
                    "word_count": len(words),
                    "lexical_diversity": lexical_diversity(block.text),
                    "boilerplate_signal_count": boilerplate_count,
                    "position_ratio": position_ratio(block.start, len(text)),
                }
                confidence = min(0.98, 0.60 + min(0.20, frequency_ratio * 2) + 0.15 * edge_ratio)
                results.append(
                    self.result(
                        start=block.start, end=block.end,
                        confidence=confidence, metrics=metrics,
                    )
                )
        return results


LINK_LABEL_RE = re.compile(
    r"(?i)\b(?:leer m[aá]s|ver m[aá]s|clic aqu[ií]|enlace|link|siguiente|anterior|"
    r"inicio|home|descargar)\b"
)


class LinkListWithoutContentDetector(Detector):
    reason_code = "LINK_LIST_WITHOUT_CONTENT"
    name = "Lista de enlaces sin contenido"
    level = "bloque"

    def detect(self, text: str, context: DetectionContext) -> list[DetectorResult]:
        results: list[DetectorResult] = []
        for block in block_spans(text):
            base = text_metrics(block.text)
            lines = line_spans(block.text)
            link_lines = sum(bool(URL_RE.search(line.text)) for line in lines)
            line_stats = line_length_metrics(block.text, context.thresholds.short_line_max_chars)
            label_count = len(LINK_LABEL_RE.findall(block.text))
            scientific = is_scientific_reference_block(block.text)
            structural = (
                base["url_count"] >= context.thresholds.link_list_min_urls
                and safe_ratio(link_lines, len(lines)) >= 0.60
                and line_stats["short_line_ratio"] >= 0.50
                and (base["url_per_word"] >= 0.08 or label_count >= 2)
            )
            if scientific:
                structural = structural and base["url_count"] >= 5 and label_count >= 2
            if structural and not is_substantive_biomedical(block.text):
                metrics = {
                    **base, **line_stats,
                    "link_line_count": link_lines,
                    "link_line_ratio": safe_ratio(link_lines, len(lines)),
                    "link_label_count": label_count,
                    "scientific_reference_signals": scientific,
                    "position_ratio": position_ratio(block.start, len(text)),
                }
                results.append(
                    self.result(
                        start=block.start, end=block.end,
                        confidence=min(0.96, 0.65 + 0.04 * int(base["url_count"])),
                        metrics=metrics,
                    )
                )
        return results


AD_EXPLICIT_RE = re.compile(
    r"(?i)\b(?:contenido patrocinado|publicidad|anuncio publicitario|sponsored|advertisement)\b"
)
AD_CTA_RE = re.compile(
    r"(?i)\b(?:comprar ahora|suscribite|suscr[ií]bete|aprovech[aá]|obtener oferta|"
    r"ver oferta|reservar ahora)\b"
)
AD_PROMO_RE = re.compile(r"(?i)\b(?:oferta|descuento|promoci[oó]n|cup[oó]n)\b")
PRICE_RE = re.compile(r"(?i)(?:[$€£]\s*\d|\d\s*(?:%|usd|eur|ars)\b)")
ECONOMICS_RE = re.compile(
    r"(?i)\b(?:costo[- ]efectividad|an[aá]lisis econ[oó]mico|costos? sanitarios?|"
    r"intervalo de confianza|porcentaje de pacientes)\b"
)


class AdvertisingDetector(Detector):
    reason_code = "ADVERTISING"
    name = "Publicidad"
    level = "bloque"

    def detect(self, text: str, context: DetectionContext) -> list[DetectorResult]:
        results: list[DetectorResult] = []
        for block in block_spans(text):
            explicit_count = len(AD_EXPLICIT_RE.findall(block.text))
            cta_count = len(AD_CTA_RE.findall(block.text))
            promo_count = len(AD_PROMO_RE.findall(block.text))
            price_count = len(PRICE_RE.findall(block.text))
            url_count = len(url_matches(block.text))
            peripheral = is_peripheral(
                block.start, block.end, len(text), context.thresholds.peripheral_ratio
            )
            short = len(block.text) <= 700
            global_stats = context.global_block_lookup(normalized_block_hash(block.text))
            global_frequency = int(global_stats.get("document_count", 0))
            signals = [
                explicit_count > 0, cta_count > 0, promo_count > 0,
                price_count > 0, url_count > 0, peripheral, short,
                global_frequency >= context.thresholds.repeated_header_min_docs,
            ]
            combination = (
                explicit_count > 0 and sum(signals[1:]) >= 1
            ) or (
                cta_count > 0 and (price_count > 0 or url_count > 0)
                and peripheral and short
            )
            if (
                combination
                and sum(signals) >= context.thresholds.advertising_min_signals
                and not ECONOMICS_RE.search(block.text)
                and not is_substantive_biomedical(block.text)
            ):
                metrics = {
                    "explicit_marker_count": explicit_count,
                    "call_to_action_count": cta_count,
                    "promotion_phrase_count": promo_count,
                    "price_or_percentage_count": price_count,
                    "url_count": url_count,
                    "block_length": len(block.text),
                    "position_ratio": position_ratio(block.start, len(text)),
                    "peripheral": peripheral,
                    "global_document_frequency": global_frequency,
                    "signal_count": sum(signals),
                }
                results.append(
                    self.result(
                        start=block.start, end=block.end,
                        confidence=min(0.97, 0.56 + 0.06 * sum(signals)),
                        metrics=metrics,
                    )
                )
        return results


HTTP_CODE_RE = re.compile(r"(?i)\b(?:error\s*)?(?:400|401|403|404|500|502|503)\b")
HTTP_PHRASE_RE = re.compile(
    r"(?i)(?:that(?:'|’)s an error|was not found on this server|document has moved "
    r"here|(?:do not|don(?:'|’)t) have permission to access|requested file could not "
    r"be found|p[aá]gina no encontrada|recurso no encontrado|acceso denegado|no (?:tiene|"
    r"tienes) permiso para acceder|error interno del servidor|servicio no disponible|"
    r"robots\.txt)"
)


class HttpErrorPageDetector(Detector):
    reason_code = "HTTP_ERROR_PAGE"
    name = "Pagina de error HTTP"
    level = "registro"

    def detect(self, text: str, context: DetectionContext) -> list[DetectorResult]:
        code_matches = HTTP_CODE_RE.findall(text)
        phrase_matches = HTTP_PHRASE_RE.findall(text)
        metrics = text_metrics(text)
        first_phrase = HTTP_PHRASE_RE.search(text)
        position = position_ratio(first_phrase.start(), len(text)) if first_phrase else 1.0
        short = len(text) <= context.thresholds.http_error_max_chars
        low_documentary_text = int(metrics["word_count"]) <= 100
        error_ratio = safe_ratio(
            sum(len(value) for value in code_matches + phrase_matches), len(text)
        )
        if (
            code_matches and phrase_matches and short and low_documentary_text
            and position <= 0.40 and not is_substantive_biomedical(text)
        ):
            explanation = {
                **metrics,
                "http_code_matches": sorted(set(match.casefold() for match in code_matches)),
                "characteristic_phrase_count": len(phrase_matches),
                "first_phrase_position_ratio": position,
                "error_signature_ratio": error_ratio,
                "document_length": len(text),
            }
            return [
                self.result(
                    start=0, end=len(text), confidence=min(0.99, 0.82 + error_ratio),
                    metrics=explanation, action=DELETE_RECORD,
                )
            ]
        return []


class RepeatedCharactersDetector(Detector):
    reason_code = "REPEATED_CHARACTERS"
    name = "Caracteres repetidos"
    level = "fragmento"

    @staticmethod
    def _threshold(character: str, thresholds: Thresholds) -> int:
        if character.isalpha():
            return thresholds.repeated_letter_min
        if character.isdigit():
            return thresholds.repeated_digit_min
        if character.isspace():
            return thresholds.repeated_whitespace_min
        if character in "-_=":
            return max(30, thresholds.repeated_punctuation_min)
        return thresholds.repeated_punctuation_min

    def detect(self, text: str, context: DetectionContext) -> list[DetectorResult]:
        results: list[DetectorResult] = []
        for match in re.finditer(r"(.)\1{7,}", text, flags=re.DOTALL):
            character = match.group(1)
            count = match.end() - match.start()
            threshold = self._threshold(character, context.thresholds)
            if count < threshold:
                continue
            category = unicodedata.category(character)
            metrics = {
                "character": character,
                "repeat_count": count,
                "threshold": threshold,
                "document_ratio": safe_ratio(count, len(text)),
                "unicode_category": category,
                "unicode_name": unicodedata.name(character, "SIN_NOMBRE"),
            }
            results.append(
                self.result(
                    start=match.start(), end=match.end(),
                    confidence=min(0.98, 0.70 + safe_ratio(count - threshold, threshold)),
                    metrics=metrics,
                )
            )
        # Patrones largos de puntuación (por ejemplo "*|*|"), sin incluir
        # letras o dígitos que podrían ser secuencias biomédicas.
        for match in re.finditer(r"(?P<pattern>[^\w\s]{2,4})(?P=pattern){7,}", text):
            pattern = match.group("pattern")
            repeat_count = len(match.group(0)) // len(pattern)
            metrics = {
                "pattern": pattern,
                "repeat_count": repeat_count,
                "document_ratio": safe_ratio(len(match.group(0)), len(text)),
                "unicode_categories": sorted({unicodedata.category(char) for char in pattern}),
            }
            results.append(
                self.result(
                    start=match.start(), end=match.end(), confidence=0.86,
                    metrics=metrics,
                )
            )
        return results


CODE_KEYWORD_RE = re.compile(
    r"(?i)\b(?:var|let|const|function|return|document|window|getElementById|"
    r"querySelector|addEventListener|console\.log)\b"
)
ASSIGNMENT_RE = re.compile(r"(?m)\b(?:var|let|const)?\s*[A-Za-z_$][\w$]*\s*=(?!=)")
JS_CALL_RE = re.compile(r"\b(?:document|window|console|[A-Za-z_$][\w$]*)\.\w+\s*\([^\n)]*\)\s*;?")
FUNCTION_RE = re.compile(r"\bfunction\s+[A-Za-z_$][\w$]*\s*\(")


class ScrapedCodeDetector(Detector):
    reason_code = "SCRAPED_CODE"
    name = "Codigo o JavaScript raspado"
    level = "bloque"

    def detect(self, text: str, context: DetectionContext) -> list[DetectorResult]:
        results: list[DetectorResult] = []
        for block in block_spans(text):
            words = unicode_words(block.text)
            keyword_count = len(CODE_KEYWORD_RE.findall(block.text))
            symbol_count = sum(character in ";=&[](){}/\\" for character in block.text)
            assignment_count = len(ASSIGNMENT_RE.findall(block.text))
            brace_count = block.text.count("{") + block.text.count("}")
            semicolon_count = block.text.count(";")
            artifact_count = len(re.findall(r"(?i)</?script\b|javascript:", block.text))
            js_call_count = len(JS_CALL_RE.findall(block.text))
            function_count = len(FUNCTION_RE.findall(block.text))
            line_stats = line_length_metrics(block.text, context.thresholds.short_line_max_chars)
            code_keyword_density = safe_ratio(keyword_count, len(words))
            code_symbol_density = safe_ratio(symbol_count, len(block.text))
            signals = [
                keyword_count >= 2 and code_keyword_density >= 0.04,
                code_symbol_density >= 0.08,
                assignment_count >= 2,
                brace_count >= 2 and semicolon_count >= 2,
                artifact_count >= 1,
                js_call_count >= 2,
                function_count >= 1 and brace_count >= 2,
            ]
            if sum(signals) >= context.thresholds.code_min_signals:
                metrics = {
                    "code_keyword_count": keyword_count,
                    "code_keyword_density": code_keyword_density,
                    "code_symbol_count": symbol_count,
                    "code_symbol_density": code_symbol_density,
                    "assignment_count": assignment_count,
                    "brace_count": brace_count,
                    "semicolon_count": semicolon_count,
                    "html_script_artifact_count": artifact_count,
                    "javascript_call_count": js_call_count,
                    "function_declaration_count": function_count,
                    "line_count": len([line for line in block.text.splitlines() if line.strip()]),
                    "average_line_length": line_stats["average_line_length"],
                    "combined_code_score": code_keyword_density + code_symbol_density,
                    "signal_count": sum(signals),
                }
                results.append(
                    self.result(
                        start=block.start, end=block.end,
                        confidence=min(0.98, 0.55 + 0.09 * sum(signals)),
                        metrics=metrics,
                    )
                )
        return results


class NearEmptyDocumentDetector(Detector):
    reason_code = "NEAR_EMPTY_DOCUMENT"
    name = "Pagina practicamente vacia"
    level = "registro"

    def detect(self, text: str, context: DetectionContext) -> list[DetectorResult]:
        metrics = text_metrics(text)
        visible = int(metrics["visible_character_count"])
        words = int(metrics["word_count"])
        alphabetic = int(metrics["alphabetic_character_count"])
        conditions = [
            visible < context.thresholds.near_empty_max_visible,
            words < context.thresholds.near_empty_max_words,
            alphabetic < 5 or float(metrics["alphabetic_ratio"]) < 0.25,
            float(metrics["non_alphanumeric_ratio"]) > 0.60 or visible == 0,
        ]
        # Ser breve no alcanza: se exige además ausencia de texto alfabético o
        # predominio claro de símbolos. Así se conservan títulos clínicos breves.
        if sum(conditions) >= 3 and conditions[2]:
            explanation = {**metrics, "condition_count": sum(conditions)}
            return [
                self.result(
                    start=0, end=len(text), confidence=min(0.95, 0.55 + 0.1 * sum(conditions)),
                    metrics=explanation, action=DELETE_RECORD,
                )
            ]
        return []


INDEX_TITLE_RE = re.compile(r"(?i)\b(?:[ií]ndice|tabla de contenidos|contenido)\b")
END_NUMBER_RE = re.compile(r"\d+\s*$")
PAGE_NUMBER_RE = re.compile(r"^\s*(?:p[aá]g(?:ina)?\.?\s*)?\d+\s*$", re.IGNORECASE)
DOT_LEADER_RE = re.compile(r"\.{3,}\s*\d*\s*$")


class AutomaticIndexDetector(Detector):
    reason_code = "AUTOMATIC_INDEX"
    name = "Indice automatico sin contenido"
    level = "bloque"

    def detect(self, text: str, context: DetectionContext) -> list[DetectorResult]:
        results: list[DetectorResult] = []
        for block in block_spans(text):
            lines = [line.strip() for line in block.text.splitlines() if line.strip()]
            if len(lines) < context.thresholds.automatic_index_min_lines:
                continue
            line_stats = line_length_metrics(block.text, context.thresholds.short_line_max_chars)
            ending_number_ratio = safe_ratio(sum(bool(END_NUMBER_RE.search(line)) for line in lines), len(lines))
            page_number_ratio = safe_ratio(sum(bool(PAGE_NUMBER_RE.match(line)) for line in lines), len(lines))
            dot_leader_count = sum(bool(DOT_LEADER_RE.search(line)) for line in lines)
            dot_leader_density = safe_ratio(dot_leader_count, len(lines))
            link_density = safe_ratio(sum(bool(URL_RE.search(line)) for line in lines), len(lines))
            structures = [
                bool(re.match(r"^(?:\d+(?:\.\d+)*|[IVXLCDM]+)[.)\s]", line, re.I))
                for line in lines
            ]
            structure_ratio = safe_ratio(sum(structures), len(lines))
            title_present = bool(INDEX_TITLE_RE.search("\n".join(lines[:2])))
            low_variance = float(line_stats["line_length_stddev"]) < 1.0
            scientific = is_scientific_reference_block(block.text)
            structural_signals = [
                title_present, ending_number_ratio >= 0.60, page_number_ratio >= 0.40,
                dot_leader_density >= 0.40, link_density >= 0.50,
                structure_ratio >= 0.60, low_variance,
                float(line_stats["short_line_ratio"]) >= 0.75,
            ]
            narrative_lines = sum(len(unicode_words(line)) >= 12 and bool(re.search(r"[.!?]", line)) for line in lines)
            narrative_ratio = safe_ratio(narrative_lines, len(lines))
            qualifies = (
                (title_present and sum(structural_signals[1:]) >= 2)
                or sum(structural_signals) >= 4
            )
            if qualifies and narrative_ratio <= 0.20 and not scientific:
                metrics = {
                    **line_stats,
                    "line_count": len(lines),
                    "ending_number_ratio": ending_number_ratio,
                    "page_number_ratio": page_number_ratio,
                    "dot_leader_density": dot_leader_density,
                    "link_density": link_density,
                    "structure_repetition_ratio": structure_ratio,
                    "index_title_present": title_present,
                    "low_line_length_variance": low_variance,
                    "narrative_line_ratio": narrative_ratio,
                    "structural_signal_count": sum(structural_signals),
                }
                results.append(
                    self.result(
                        start=block.start, end=block.end,
                        confidence=min(0.96, 0.50 + 0.07 * sum(structural_signals)),
                        metrics=metrics,
                    )
                )
        return results


class ExcessiveInternalRepetitionDetector(Detector):
    reason_code = "EXCESSIVE_INTERNAL_REPETITION"
    name = "Repeticiones internas excesivas"
    level = "bloque"

    def detect(self, text: str, context: DetectionContext) -> list[DetectorResult]:
        spans = block_spans(text)
        # Documentos sin párrafos explícitos pueden contener listas repetidas.
        if len(spans) <= 1:
            spans = line_spans(text)
        normalized = [normalize_for_comparison(span.text) for span in spans]
        counts = Counter(value for value in normalized if value)
        indexes_by_value: dict[str, list[int]] = defaultdict(list)
        for index, value in enumerate(normalized):
            indexes_by_value[value].append(index)
        results: list[DetectorResult] = []
        for value, indexes in indexes_by_value.items():
            occurrence_count = counts[value]
            first_span = spans[indexes[0]]
            if (
                occurrence_count < context.thresholds.internal_repetition_min_occurrences
                or len(first_span.text) < context.thresholds.internal_repetition_min_chars
                or len(unicode_words(first_span.text)) < 3
            ):
                continue
            repeated_chars = sum(len(spans[index].text) for index in indexes[1:])
            repeated_ratio = safe_ratio(repeated_chars, len(text))
            previous_index = indexes[0]
            for occurrence_number, index in enumerate(indexes[1:], start=2):
                span = spans[index]
                metrics = {
                    "normalized_block_hash": hashlib.sha256(value.encode("utf-8")).hexdigest(),
                    "occurrence_count": occurrence_count,
                    "occurrence_number": occurrence_number,
                    "first_occurrence_start": first_span.start,
                    "distance_from_previous": span.start - spans[previous_index].end,
                    "repeated_character_count": repeated_chars,
                    "repeated_document_ratio": repeated_ratio,
                    "consecutive": index == previous_index + 1,
                }
                results.append(
                    self.result(
                        start=span.start, end=span.end,
                        confidence=min(0.97, 0.58 + 0.07 * occurrence_count + 0.20 * repeated_ratio),
                        metrics=metrics,
                        level="bloque" if len(block_spans(text)) > 1 else "linea",
                    )
                )
                previous_index = index
        return results


DEFAULT_DETECTOR_CLASSES: tuple[type[Detector], ...] = (
    NavigationResidualDetector,
    CookieNoticeDetector,
    RepeatedHeaderFooterDetector,
    LinkListWithoutContentDetector,
    AdvertisingDetector,
    HttpErrorPageDetector,
    RepeatedCharactersDetector,
    ScrapedCodeDetector,
    NearEmptyDocumentDetector,
    AutomaticIndexDetector,
    ExcessiveInternalRepetitionDetector,
)


def build_detectors(disabled_reason_codes: Iterable[str] = ()) -> list[Detector]:
    disabled = {code.upper() for code in disabled_reason_codes}
    return [detector_class() for detector_class in DEFAULT_DETECTOR_CLASSES if detector_class.reason_code not in disabled]


def consolidate_results(results: Iterable[DetectorResult]) -> list[ConsolidatedResult]:
    """Fusiona spans superpuestos cuando proponen la misma acción.

    La cobertura resultante es el intervalo mínimo que contiene las propuestas.
    Acciones distintas (eliminar registro frente a fragmento) permanecen como
    candidatos separados. Se preservan todos los motivos y las métricas de cada
    detector, incluso cuando un detector produjo más de una evidencia.
    """
    ordered = sorted(
        results,
        key=lambda item: (item.proposed_action, item.start, item.end, item.reason_code),
    )
    groups: list[list[DetectorResult]] = []
    for result in ordered:
        if not groups:
            groups.append([result])
            continue
        current = groups[-1]
        current_start = min(item.start for item in current)
        current_end = max(item.end for item in current)
        compatible = result.proposed_action == current[0].proposed_action
        overlaps = result.start < current_end and result.end > current_start
        exact_empty = result.start == result.end == current_start == current_end
        if compatible and (overlaps or exact_empty):
            current.append(result)
        else:
            groups.append([result])

    consolidated: list[ConsolidatedResult] = []
    for group in groups:
        codes = tuple(sorted({item.reason_code for item in group}))
        names = tuple(sorted({item.detector_name for item in group}))
        levels = {item.level for item in group}
        metrics_by_detector: dict[str, list[Mapping[str, object]]] = defaultdict(list)
        for item in group:
            metrics_by_detector[item.reason_code].append(dict(item.metrics))
        deterministic_metrics = {
            code: metrics_by_detector[code] for code in sorted(metrics_by_detector)
        }
        consolidated.append(
            ConsolidatedResult(
                reason_codes=codes,
                detector_names=names,
                level=next(iter(levels)) if len(levels) == 1 else "fragmento",
                start=min(item.start for item in group),
                end=max(item.end for item in group),
                confidence=max(item.confidence for item in group),
                metrics=deterministic_metrics,
                proposed_action=group[0].proposed_action,
            )
        )
    return consolidated


def detector_catalog() -> dict[str, str]:
    return {detector_class.reason_code: detector_class.name for detector_class in DEFAULT_DETECTOR_CLASSES}


def thresholds_as_dict(thresholds: Thresholds) -> dict[str, object]:
    return asdict(thresholds)
