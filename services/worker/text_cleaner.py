"""
Text preprocessing for Sentinel Worker.

Removes stopwords, filler words, and extra whitespace from input text before
it is sent to Gemini.  The original input_text is ALWAYS preserved unchanged
in the database; only the *prompt payload* uses the cleaned version.

If spaCy or the language model cannot be loaded, falls back to a regex-based
approach so the worker never crashes on import.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Final

logger = logging.getLogger(__name__)

SPACY_CLEAN_ENABLED: Final[bool] = os.getenv("SPACY_CLEAN_ENABLED", "1") != "0"
SPACY_MODEL: Final[str] = os.getenv("SPACY_MODEL", "en_core_web_sm")
SPACY_MIN_RETENTION_RATIO: Final[float] = float(os.getenv("SPACY_MIN_RETENTION_RATIO", "0.1"))

FILLER_WORDS: Final[set[str]] = {
    "um", "uh", "hmm", "hm", "er", "ah", "oh", "like",
    "basically", "actually", "literally", "honestly",
    "anyway", "anyways", "well", "so", "yeah", "yep",
    "okay", "ok", "right", "just", "really", "very",
    "stuff", "things", "thing", "kinda", "sorta",
    "gonna", "wanna", "gotta", "y'know", "you know",
    "i mean", "you see",
}

_nlp: object | None = None
_spacy_available: bool = False


def _load_spacy() -> None:
    """Try to load spaCy and the language model exactly once."""
    global _nlp, _spacy_available
    if _nlp is not None:
        return

    try:
        import spacy
        _nlp = spacy.load(SPACY_MODEL)
        _spacy_available = True
        logger.info("spaCy model loaded model=%s", SPACY_MODEL)
    except Exception as exc:
        _spacy_available = False
        _nlp = False
        logger.warning("spaCy unavailable — falling back to regex cleaner: %s", exc)


def clean_text(text: str) -> str:
    """
    Return *text* with stopwords and filler words removed.

    Preserves sentence structure and punctuation.
    Never raises — returns original text on any internal error.
    Skips cleaning entirely if SPACY_CLEAN_ENABLED=0.

    Args:
        text: Raw input text.

    Returns:
        Cleaned text (or original if cleaning fails or is disabled).
    """
    if not SPACY_CLEAN_ENABLED:
        return text

    if not text or not text.strip():
        return text

    try:
        cleaned = _clean_with_spacy(text) if _spacy_available else _clean_with_regex(text)
    except Exception as exc:
        logger.warning("Text cleaning failed, using original: %s", exc)
        return text

    # Safety net: if cleaning removed >90% of tokens, return original
    original_count = len(text.split())
    cleaned_count = len(cleaned.split())
    if cleaned_count < max(1, int(original_count * SPACY_MIN_RETENTION_RATIO)):
        logger.warning(
            "Cleaning removed >90%% of tokens — returning original text"
        )
        return text

    return cleaned


def _clean_with_spacy(text: str) -> str:
    """Use spaCy tokenizer + stopword list + custom fillers."""
    _load_spacy()

    if not _spacy_available:
        return _clean_with_regex(text)

    doc = _nlp(text)
    tokens: list[str] = []

    for token in doc:
        if token.is_punct or token.text in ("\n", "\r"):
            tokens.append(token.text)
            continue

        lower = token.text.lower().strip()

        if token.is_stop:
            continue

        if lower in FILLER_WORDS:
            continue

        if not lower:
            continue

        tokens.append(token.text)

    return _reassemble(tokens)


_filler_pattern: re.Pattern | None = None


def _build_filler_pattern() -> re.Pattern:
    """Build regex pattern for filler words (lazy, cached)."""
    global _filler_pattern
    if _filler_pattern is not None:
        return _filler_pattern

    sorted_fillers = sorted(FILLER_WORDS, key=len, reverse=True)
    escaped = [re.escape(f) for f in sorted_fillers]
    _filler_pattern = re.compile(
        r"\b(?:" + "|".join(escaped) + r")\b",
        re.IGNORECASE,
    )
    return _filler_pattern


def _clean_with_regex(text: str) -> str:
    """Lightweight fallback: remove filler words via regex."""
    pattern = _build_filler_pattern()
    cleaned = pattern.sub("", text)
    cleaned = re.sub(r"  +", " ", cleaned)
    cleaned = re.sub(r"\s+([.,;:!?])", r"\1", cleaned)
    return cleaned.strip()


_NO_SPACE_BEFORE: Final[set[str]] = set(".,;:!?)]}\"'")
_NO_SPACE_AFTER: Final[set[str]] = set("([{\"'")


def _reassemble(tokens: list[str]) -> str:
    """Join tokens with smart spacing around punctuation."""
    if not tokens:
        return ""

    parts = [tokens[0]]

    for prev, curr in zip(tokens, tokens[1:]):
        if curr in _NO_SPACE_BEFORE:
            parts.append(curr)
        elif prev in _NO_SPACE_AFTER:
            parts.append(curr)
        elif curr in ("\n", "\r"):
            parts.append(curr)
        else:
            parts.append(" " + curr)

    return "".join(parts).strip()