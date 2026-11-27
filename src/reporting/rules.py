"""Per-field rules: what a reporting table keeps from an event, and what a de-identified destination
gets instead.

  identified     the table's fields as the contract has them, plus tokens for `tokenize`
  de-identified  the same, except each `generalize` field is cut (a date to its year, a ZIP code to
                 its first three digits, as HIPAA's Safe Harbor method does), and tokens are left out
                 unless reporting.yaml records an expert_determination: a token is derived from the
                 value it replaces, which Safe Harbor doesn't allow as a code (45 CFR 164.514(c));
                 config.py has already refused a person's value here that is neither tokenized nor
                 generalized

Tokens are a keyed HMAC-SHA-256: the same value gets the same token in every table, so reports can
count and join on it, and nobody without the key can turn a token back into the value. In
environments the key is the database's own (postgres-login's tokenize function): versioned, it never
leaves the database, and tokens say which key made them ("v2:..."). Local runs and tests use a key of
their own (REPORTING_TOKEN_KEY).
"""

from __future__ import annotations

import hashlib
import hmac
import re
from collections.abc import Callable
from typing import Any

from .config import Table

Tokenizer = Callable[[str], str]


def token(key: bytes | Tokenizer, value: Any) -> str:
    """A value's token: from the database's tokenize function when `key` is one, else a local HMAC."""
    if callable(key):
        return key(str(value))
    if not key:
        raise ValueError("REPORTING_TOKEN_KEY is required to tokenize fields")
    return hmac.new(key, str(value).encode(), hashlib.sha256).hexdigest()[:32]


def generalize(value: Any, how: str) -> Any:
    if value is None:
        return None
    text = str(value)
    if how == "year":
        m = re.match(r"^(\d{4})", text)
        return int(m.group(1)) if m else None
    if how == "zip3":
        digits = re.sub(r"\D", "", text)
        return digits[:3] if len(digits) >= 3 else None
    raise ValueError(f"unknown generalization {how}")


def row(
    table: Table, data: dict[str, Any], key: bytes | Tokenizer, *, deidentified: bool = False, tokens: bool = False
) -> dict[str, Any]:
    """The table's row from an event's data (already checked against its contract). `tokens`: a
    de-identified row keeps its tokens (an Expert Determination is recorded)."""
    out: dict[str, Any] = {f: data.get(f) for f in table.fields}
    for f in table.tokenize:
        if deidentified and not tokens:
            continue
        out[f] = token(key, data[f]) if data.get(f) is not None else None
    if deidentified:
        for f, how in table.generalize.items():
            out[f] = generalize(out.get(f), how)
    return out
