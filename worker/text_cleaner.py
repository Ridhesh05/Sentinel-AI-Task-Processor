"""
text_cleaner — spaCy-based text preprocessing for Sentinel Worker.

Removes stopwords, filler words, and extra whitespace from input text before
it is sent to Gemini.  The original input_text is always preserved in the
database; only the *prompt payload* uses the cleaned version.

If spaCy or the language model cannot be loaded (e.g. model not downloaded),
the cleaner falls back to a lightweight regex-based approach so the worker
never crashes on import.
"""

import logging
import os
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Toggle — set SPACY_CLEAN_ENABLED=0 to skip cleaning entirely
# ---------------------------------------------------------------------------
SPACY_CLEAN_ENABLED = os.getenv("SPACY_CLEAN_ENABLED", "1") != "0"

# ---------------------------------------------------------------------------
# Custom filler words (not always in spaCy's stopword list)
# ---------------------------------------------------------------------------
FILLER_WORDS: set[str] = {
    "um", "uh", "hmm", "hm", "er", "ah", "oh", "like",
    "basically", "actually", "literally", "honestly",
    "anyway", "anyways", "well", "so", "yeah", "yep",
    "okay", "ok", "right", "just", "really", "very",
    "stuff", "things", "thing", "kinda", "sorta",
    "gonna", "wanna", "gotta", "y'know", "you know",
    "i mean", "you see",
}

# ---------------------------------------------------------------------------
# spaCy bootstrap (lazy, one-time)
# ---------------------------------------------------------------------------
_nlp = None
_spacy_available = False
_SPACY_MODEL = os.getenv("SPACY_MODEL", "en_core_web_sm")


def _load_spacy():
    """Try to load spaCy and the language model exactly once."""
    global _nlp, _spacy_available
    if _nlp is not None:
        return

    try:
        import spacy  # noqa: E402

        _nlp = spacy.load(_SPACY_MODEL)
        _spacy_available = True
        logger.info("spaCy model loaded model=%s", _SPACY_MODEL)
    except Exception as exc:
        _spacy_available = False
        _nlp = False  # sentinel: we tried and failed
        logger.warning(
            "spaCy unavailable — falling back to regex cleaner: %s", exc
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def clean_text(text: str) -> str:
    """
    Return *text* with stopwords and filler words removed.

    • Preserves sentence structure and punctuation.
    • Never raises — returns original text on any internal error.
    • Logs a one-line summary showing original vs cleaned char count.
    """
    if not SPACY_CLEAN_ENABLED:
        return text

    if not text or not text.strip():
        return text

    try:
        cleaned = _clean_with_spacy(text)
    except Exception as exc:
        logger.warning("Text cleaning failed, using original: %s", exc)
        cleaned = text

    # Safety net: if cleaning removed too much (>90%), return original
    if len(cleaned.split()) < max(1, len(text.split()) * 0.1):
        logger.warning(
            "Cleaning removed >90%% of tokens — returning original text"
        )
        return text

    if cleaned != text:
        logger.debug(
            "Text cleaned original_chars=%d cleaned_chars=%d",
            len(text),
            len(cleaned),
        )

    return cleaned


# ---------------------------------------------------------------------------
# Internal: spaCy path
# ---------------------------------------------------------------------------

def _clean_with_spacy(text: str) -> str:
    """Use spaCy tokenizer + stopword list + custom fillers."""
    _load_spacy()

    if not _spacy_available:
        return _clean_with_regex(text)

    doc = _nlp(text)
    tokens: list[str] = []

    for token in doc:
        # Keep punctuation and newlines for readability
        if token.is_punct or token.text in ("\n", "\r"):
            tokens.append(token.text)
            continue

        lower = token.text.lower().strip()

        # Skip spaCy stopwords
        if token.is_stop:
            continue

        # Skip custom filler words
        if lower in FILLER_WORDS:
            continue

        # Skip pure whitespace tokens
        if not lower:
            continue

        tokens.append(token.text)

    cleaned = _reassemble(tokens)
    return cleaned


# ---------------------------------------------------------------------------
# Internal: regex fallback (no spaCy)
# ---------------------------------------------------------------------------

# Pre-compile the fallback pattern from FILLER_WORDS
_FILLER_PATTERN: re.Pattern | None = None


def _build_filler_pattern() -> re.Pattern:
    global _FILLER_PATTERN
    if _FILLER_PATTERN is None:
        # Sort by length descending so multi-word fillers match first
        sorted_fillers = sorted(FILLER_WORDS, key=len, reverse=True)
        escaped = [re.escape(f) for f in sorted_fillers]
        _FILLER_PATTERN = re.compile(
            r"\b(?:" + "|".join(escaped) + r")\b", re.IGNORECASE
        )
    return _FILLER_PATTERN


def _clean_with_regex(text: str) -> str:
    """Lightweight fallback: remove filler words via regex."""
    pattern = _build_filler_pattern()
    cleaned = pattern.sub("", text)
    # Collapse multiple spaces
    cleaned = re.sub(r"  +", " ", cleaned)
    # Remove spaces before punctuation
    cleaned = re.sub(r"\s+([.,;:!?])", r"\1", cleaned)
    return cleaned.strip()


# ---------------------------------------------------------------------------
# Reassemble tokens with proper spacing
# ---------------------------------------------------------------------------

_NO_SPACE_BEFORE = set(".,;:!?)]}\"'")
_NO_SPACE_AFTER = set("([{\"'")


def _reassemble(tokens: list[str]) -> str:
    """Join tokens with smart spacing around punctuation."""
    if not tokens:
        return ""

    parts: list[str] = [tokens[0]]

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
