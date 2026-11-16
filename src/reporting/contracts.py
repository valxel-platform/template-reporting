"""The approved data contracts of the datasets this repo reads. The SDLC tool writes contracts/<id>.json
(the JSON Schema generated from each ODCS contract, with each property's class as x-classification)
and contracts/lock.yaml (dataset -> version, hash, topic); they ship in the image, so nothing here
reads GitHub at run time.

Every event is counted per dataset and outcome: a Prometheus counter, and one summary line a minute
(`valxel.datasets {...}`) that the SDLC tool reconciles in staging.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator
from prometheus_client import Counter

EVENTS = Counter("reporting_dataset_events", "Events read per dataset and outcome", ["dataset", "outcome"])
SUMMARY = "valxel.datasets"
_counts: dict[str, dict[str, int]] = {}
_lock = threading.Lock()


def count(dataset: str, outcome: str) -> None:
    EVENTS.labels(dataset, outcome).inc()
    with _lock:
        by = _counts.setdefault(dataset, {})
        by[outcome] = by.get(outcome, 0) + 1


def summary_line() -> str | None:
    """The line for what was counted since the last one, or None."""
    with _lock:
        taken = {d: dict(sorted(o.items())) for d, o in sorted(_counts.items()) if o}
        _counts.clear()
    return f"{SUMMARY} {json.dumps(taken, separators=(',', ':'))}" if taken else None


class ContractError(ValueError):
    """An event's data breaks its contract. The message names the field, never its value."""

    def __init__(self, dataset: str, field: str, rule: str) -> None:
        super().__init__(f"{dataset}: {field or '(data)'} breaks the data contract ({rule})")
        self.dataset, self.field, self.rule = dataset, field, rule


@dataclass(frozen=True)
class Contract:
    dataset: str
    topic: str
    version: str
    schema: dict[str, Any]

    def properties(self) -> dict[str, dict[str, Any]]:
        return dict(self.schema.get("properties") or {})

    def check(self, data: dict[str, Any]) -> dict[str, Any]:
        """The data as this copy knows it (unknown properties dropped), or ContractError."""
        known = self.properties()
        kept = {k: v for k, v in data.items() if k in known}
        for e in sorted(Draft202012Validator(self.schema).iter_errors(kept), key=lambda e: list(e.path)):
            field = ".".join(str(p) for p in e.path)
            if not field and e.validator == "required" and "'" in e.message:
                field = e.message.split("'")[1]
            raise ContractError(self.dataset, field, str(e.validator))
        return kept


class Contracts:
    def __init__(self, contracts: list[Contract]) -> None:
        self.by_dataset = {c.dataset: c for c in contracts}

    @classmethod
    def load(cls, directory: str | Path = "contracts") -> Contracts:
        root = Path(directory)
        lock = root / "lock.yaml"
        if not lock.exists():
            return cls([])
        out = []
        for dataset, row in (yaml.safe_load(lock.read_text()) or {}).items():
            schema = json.loads((root / f"{dataset}.json").read_text())
            Draft202012Validator.check_schema(schema)
            out.append(Contract(str(dataset), str(row["topic"]), str(row["version"]), schema))
        return cls(out)

    def for_dataset(self, dataset: str) -> Contract | None:
        return self.by_dataset.get(dataset)
