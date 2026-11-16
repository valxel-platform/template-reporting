"""Per-field rules: what a reporting table keeps from an event, and what a de-identified destination
gets instead.

  identified     the table's fields as the contract has them, plus tokens for `tokenize`
  de-identified  the same, except each `generalize` field is cut (a date to its year, a ZIP code to
                 its first three digits, as HIPAA's Safe Harbor method does); config.py has already
                 refused a person's value here that is neither tokenized nor generalized

Tokens are a keyed HMAC-SHA-256 (the key is a secret of this component, REPORTING_TOKEN_KEY): the same
value gets the same token in every table, so reports can count and join on it, and nobody without
the key can turn a token back into the value.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from typing import Any

from .config import Table


def token(key: bytes, value: Any) -> str:
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


def row(table: Table, data: dict[str, Any], key: bytes, *, deidentified: bool = False) -> dict[str, Any]:
    """The table's row from an event's data (already checked against its contract)."""
    out: dict[str, Any] = {f: data.get(f) for f in table.fields}
    for f in table.tokenize:
        out[f] = token(key, data[f]) if data.get(f) is not None else None
    if deidentified:
        for f, how in table.generalize.items():
            out[f] = generalize(out.get(f), how)
    return out
