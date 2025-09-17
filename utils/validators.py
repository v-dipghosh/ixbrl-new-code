# utils/validators.py

import re

import pandas as pd

from Levenshtein import distance as lev_distance
 
# ---------- Regexes ----------

_URL_RE = re.compile(r'https?://', re.I)
 
DATE_RE = re.compile(r"""(?ix)

    (?:\d{4}[-/]\d{1,2}[-/]\d{1,2} | \d{1,2}[-/]\d{1,2}[-/]\d{2,4})

    | (?:\d{1,2}\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{2,4}

       | (jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{2,4})

""")
 
NUMERIC_ONLY_RE = re.compile(r"""^\s*

    (?!\d{4}[-/]\d{2}[-/]\d{2}\s*$)

    (?!\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\s*$)

    (?!\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\s*$)

    \(?[-+]?(?:\d{1,3}(?:[,\s]\d{3})*|\d+)(?:[.,]\d+)?\)?\s*$""", re.VERBOSE)
 
_STOP = re.compile(r'\b(of|the|and|a|an|to|for|by|in|on|as|is|are|was|were)\b', re.I)

_WORD_SPLIT = re.compile(r"[^\w]+")
 
_SYNONYM_MAP = {

    "borrowing": "loan", "borrowings": "loans", "loans": "loan",

    "payables": "payable", "creditors": "payable",

    "receivables": "receivable", "debtors": "receivable",

    "operations": "operating", "operation": "operating",

    "equivalents": "equivalent",

    "income": "revenue", "turnover": "revenue",

}
 
# ---------- tiny utils to handle None/NaN ----------

def _is_missing(v) -> bool:

    """True for None/NaN (robust to pandas/numpy types)."""

    try:

        return v is None or pd.isna(v)

    except Exception:

        return v is None
 
# ---------- Public helpers used by rules ----------

def has_url(s) -> bool:

    if _is_missing(s):

        return False

    return bool(_URL_RE.search(str(s)))
 
def looks_like_date(s) -> bool:

    if _is_missing(s):

        return False

    return bool(DATE_RE.search(str(s)))
 
def is_numeric_only(value) -> bool:

    # Treat empty as not-numeric; other rules (or LLM) will handle it

    if _is_missing(value):

        return False

    s = str(value).strip()

    return bool(NUMERIC_ONLY_RE.match(s))
 
def _stem(w: str) -> str:

    w = _SYNONYM_MAP.get(w, w)

    if w.endswith("ies") and len(w) > 4:  # policies -> policy

        return w[:-3] + "y"

    if w.endswith("s") and len(w) > 3:

        return w[:-1]

    return w
 
def _normalize_label(s: str) -> str:

    s = str(s or "")

    s = s.replace("’", "'").replace("–", "-").replace("—", "-").replace("·", " ")

    s = s.strip().lower()

    s = re.sub(r'\s*\[\s*(?:true\s*/\s*false|true|false)\s*\]\s*$', '', s, flags=re.I)

    s = _STOP.sub(' ', s)

    s = re.sub(r'[\s/()\-_,]+', ' ', s).strip()

    return s
 
def _keywords(s: str) -> set:

    if not s:

        return set()

    s = _normalize_label(s)

    toks = [t for t in _WORD_SPLIT.split(s) if t]

    stems = {_stem(t) for t in toks if t}

    return {t for t in stems if len(t) > 2}
 
def has_common_word(a: str, b: str) -> bool:

    return len(_keywords(a) & _keywords(b)) > 0
 
def _close_enough(a: str, b: str) -> bool:

    a_n, b_n = _normalize_label(a), _normalize_label(b)

    if not a_n or not b_n:

        return False

    if a_n == b_n:

        return True

    dist = lev_distance(a_n, b_n)

    max_allow = max(1, int(max(len(a_n), len(b_n)) * 0.1))

    return dist <= max_allow
 
def normalize_for_lookup(label: str) -> str:

    return _normalize_label(label or "")

 