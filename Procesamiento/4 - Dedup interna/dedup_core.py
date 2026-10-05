"""Detección conservadora de documentos y pasajes repetidos.

La normalización se usa solamente para comparar. Los offsets siempre se refieren
al campo ``texto`` original del JSONL.
"""

from __future__ import annotations

from collections import deque
from bisect import bisect_left
from dataclasses import dataclass
import hashlib
import re
import unicodedata


TOKEN_RE = re.compile(r"[^\W_]+", re.UNICODE)
SENTENCE_BREAK_RE = re.compile(r"(?<=[.!?])\s+|\n+", re.UNICODE)
SOURCE_ORDER = {
    "CoWeSe": 0,
    "SciELO biomedico en espanol": 1,
    "SPACCC": 2,
    "MMedC": 3,
}
VERSION = "1.0"
MIN_PASSAGE_TOKENS = 80
SHINGLE_WORDS = 5
ANCHOR_WORDS = 16
ANCHOR_WINDOW = 48
MINHASH_BUCKETS = 64
MINHASH_BANDS = 16
MAX_ANCHOR_OCCURRENCES = 12
PROTECTED_WORDS = {
    "no", "sin", "nunca", "ningun", "ninguna", "contraindicado", "contraindicada",
    "positivo", "positiva", "negativo", "negativa", "aumenta", "disminuye",
    "mayor", "menor", "mg", "mcg", "ml", "g", "kg", "porcentaje",
}


@dataclass(frozen=True)
class Token:
    value: str
    start: int
    end: int


@dataclass(frozen=True)
class Match:
    a_start: int
    a_end: int
    b_start: int
    b_end: int

    @property
    def words(self) -> int:
        return self.a_end - self.a_start


def tokenize(text: str) -> list[Token]:
    return [
        Token(unicodedata.normalize("NFC", match.group()).casefold(), match.start(), match.end())
        for match in TOKEN_RE.finditer(text)
    ]


def stable_hash(data: bytes, *, size: int = 8) -> bytes:
    return hashlib.blake2b(data, digest_size=size, person=b"dedup-p4").digest()


def shingle_hashes(tokens: list[Token], words: int) -> list[int]:
    values = [token.value for token in tokens]
    return [
        int.from_bytes(stable_hash("\x1f".join(values[pos:pos + words]).encode("utf-8")), "big")
        for pos in range(max(0, len(values) - words + 1))
    ]


def verification_shingles(tokens: list[Token], words: int) -> list[tuple[str, ...]]:
    """Comparación exacta de n-gramas; no cambia las huellas persistidas del índice."""
    values = [token.value for token in tokens]
    return [tuple(values[pos:pos + words]) for pos in range(max(0, len(values) - words + 1))]


def exact_text_key(text: str) -> str:
    """Hash de texto con espacio y caja unificados; conserva cifras y signos."""
    normalized = re.sub(r"\s+", " ", unicodedata.normalize("NFC", text).casefold()).strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def numeric_expressions(text: str) -> list[str]:
    return re.findall(
        r"\d+(?:[.,]\d+)?\s*(?:%|‰|mg|mcg|ml|kg|g|mmhg|µg|μg)?",
        unicodedata.normalize("NFC", text).casefold(), flags=re.UNICODE,
    )


def numeric_subsequence(candidate: str, container: str) -> bool:
    other = iter(numeric_expressions(container))
    return all(any(value == current for value in other) for current in numeric_expressions(candidate))


def minhash_bands(tokens: list[Token]) -> list[str]:
    hashes = shingle_hashes(tokens, SHINGLE_WORDS)
    if len(hashes) < MIN_PASSAGE_TOKENS - SHINGLE_WORDS + 1:
        return []
    minimum = [(1 << 64) - 1] * MINHASH_BUCKETS
    for value in hashes:
        bucket = value & (MINHASH_BUCKETS - 1)
        candidate = value >> 6
        if candidate < minimum[bucket]:
            minimum[bucket] = candidate
    result = []
    for band in range(MINHASH_BANDS):
        chunk = minimum[band * 4:band * 4 + 4]
        payload = bytes([band]) + b"".join(value.to_bytes(8, "big") for value in chunk)
        result.append(stable_hash(payload, size=16).hex())
    return result


def passage_anchors(tokens: list[Token]) -> list[str]:
    """Winnowing: every shared stretch of 16+48-1 words has an anchor."""
    hashes = shingle_hashes(tokens, ANCHOR_WORDS)
    if len(hashes) < ANCHOR_WINDOW:
        return []
    window: deque[int] = deque()
    result: list[str] = []
    last_position = -1
    for position, value in enumerate(hashes):
        while window and hashes[window[-1]] >= value:
            window.pop()
        window.append(position)
        while window[0] <= position - ANCHOR_WINDOW:
            window.popleft()
        if position >= ANCHOR_WINDOW - 1 and window[0] != last_position:
            last_position = window[0]
            result.append(f"{hashes[last_position]:016x}")
    return list(dict.fromkeys(result))


def exact_matches(a: list[Token], b: list[Token], minimum: int = MIN_PASSAGE_TOKENS) -> list[Match]:
    """Extiende anclas de 16 palabras sin alterar el texto original."""
    if len(a) < minimum or len(b) < minimum:
        return []
    a_values = [token.value for token in a]
    b_values = [token.value for token in b]
    a_shingles = verification_shingles(a, ANCHOR_WORDS)
    b_shingles = verification_shingles(b, ANCHOR_WORDS)
    locations: dict[tuple[str, ...], list[int]] = {}
    for index, value in enumerate(b_shingles):
        positions = locations.setdefault(value, [])
        if len(positions) <= MAX_ANCHOR_OCCURRENCES:
            positions.append(index)
    matches: set[Match] = set()
    covered_until: dict[int, int] = {}
    for ai, value in enumerate(a_shingles):
        positions = locations.get(value, ())
        if len(positions) > MAX_ANCHOR_OCCURRENCES:
            continue
        for bi in positions:
            diagonal = bi - ai
            if ai < covered_until.get(diagonal, 0):
                continue
            left_a, left_b = ai, bi
            right_a, right_b = ai + ANCHOR_WORDS, bi + ANCHOR_WORDS
            while left_a and left_b and a_values[left_a - 1] == b_values[left_b - 1]:
                left_a -= 1
                left_b -= 1
            while right_a < len(a) and right_b < len(b) and a_values[right_a] == b_values[right_b]:
                right_a += 1
                right_b += 1
            covered_until[diagonal] = right_a
            if right_a - left_a >= minimum:
                matches.add(Match(left_a, right_a, left_b, right_b))
    return sorted(matches, key=lambda match: (match.a_start, match.b_start, -match.words))


def covered_words(matches: list[Match], side: str) -> int:
    intervals = sorted((getattr(m, side + "_start"), getattr(m, side + "_end")) for m in matches)
    total = 0
    end = 0
    for start, stop in intervals:
        if stop > end:
            total += stop - max(start, end)
            end = stop
    return total


def similarity(a: list[Token], b: list[Token]) -> tuple[float, float, float, list[Match]]:
    a_shingles = set(verification_shingles(a, SHINGLE_WORDS))
    b_shingles = set(verification_shingles(b, SHINGLE_WORDS))
    common = len(a_shingles & b_shingles)
    union = len(a_shingles | b_shingles)
    jaccard = common / union if union else 0.0
    matches = exact_matches(a, b)
    a_coverage = covered_words(matches, "a") / len(a) if a else 0.0
    b_coverage = covered_words(matches, "b") / len(b) if b else 0.0
    return jaccard, a_coverage, b_coverage, matches


def relation(a: list[Token], b: list[Token], jaccard: float, ca: float, cb: float, matches: list[Match]) -> str | None:
    if min(len(a), len(b)) >= MIN_PASSAGE_TOKENS and (ca >= 0.95 or cb >= 0.95):
        return "contenido"
    if min(len(a), len(b)) >= MIN_PASSAGE_TOKENS and jaccard >= 0.85:
        return "casi_total"
    if any(match.words >= MIN_PASSAGE_TOKENS for match in matches):
        return "parcial"
    return None


def substantial_extra(longer_words: int, shorter_words: int) -> bool:
    return longer_words - shorter_words >= max(80, int(shorter_words * 0.1))


def safe_to_drop_variant(tokens: list[Token], matches: list[Match], side: str) -> bool:
    """No propone eliminar una variante con un cambio clínico detectable."""
    if not tokens:
        return False
    covered = bytearray(len(tokens))
    for match in matches:
        start = getattr(match, side + "_start")
        end = getattr(match, side + "_end")
        covered[start:end] = b"\x01" * (end - start)
    uncovered = [token.value for token, flag in zip(tokens, covered) if not flag]
    if len(uncovered) > max(20, int(len(tokens) * 0.05)):
        return False
    try:
        first_covered = covered.index(1)
        last_covered = len(covered) - 1 - covered[::-1].index(1)
    except ValueError:
        return False
    if 0 in covered[first_covered:last_covered + 1]:
        return False
    if any(any(char.isdigit() for char in word) or word in PROTECTED_WORDS for word in uncovered):
        return False
    return True


def source_rank(origin: str) -> int:
    return SOURCE_ORDER.get(origin, len(SOURCE_ORDER))


def preference(record: dict, max_words: int) -> tuple[int, int, int, int]:
    words = int(record["words"])
    incomplete = int(substantial_extra(max_words, words))
    return incomplete, source_rank(str(record["origin"])), -words, int(record["record_no"])


def sentence_spans(text: str) -> list[tuple[int, int]]:
    start = 0
    spans = []
    for boundary in SENTENCE_BREAK_RE.finditer(text):
        if boundary.start() > start and text[start:boundary.start()].strip():
            spans.append((start, boundary.start()))
        start = boundary.end()
    if text[start:].strip():
        spans.append((start, len(text)))
    return spans


def safe_trim_spans(text: str, tokens: list[Token], matches: list[Match], side: str) -> list[tuple[int, int]]:
    """Recorta únicamente oraciones completas cubiertas por un pasaje idéntico."""
    intervals = sorted(
        (tokens[getattr(m, side + "_start")].start, tokens[getattr(m, side + "_end") - 1].end)
        for m in matches
    )
    starts = [token.start for token in tokens]
    selected = []
    for start, end in sentence_spans(text):
        content = text[start:end].strip()
        if not content:
            continue
        first_index = bisect_left(starts, start)
        last_index = bisect_left(starts, end) - 1
        if first_index > last_index or tokens[last_index].end > end:
            continue
        first, last = tokens[first_index].start, tokens[last_index].end
        if any(left <= first and last <= right for left, right in intervals):
            selected.append((start, end))
    grouped: list[tuple[int, int]] = []
    for start, end in selected:
        if grouped and not text[grouped[-1][1]:start].strip():
            grouped[-1] = (grouped[-1][0], end)
        else:
            grouped.append((start, end))
    return [
        (start, end) for start, end in grouped
        if len(tokenize(text[start:end])) >= MIN_PASSAGE_TOKENS
    ]


def remove_spans(text: str, spans: list[tuple[int, int]]) -> str:
    for start, end in sorted(spans, reverse=True):
        text = text[:start] + text[end:]
    return re.sub(r"\n{3,}", "\n\n", text).strip()
