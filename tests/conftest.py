from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from reporting.config import Config, parse
from reporting.contracts import Contracts

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = Contracts.load(ROOT / "contracts")  # the example contract: synthetic EMR notes


def config(**over: Any) -> Config:
    import yaml

    raw = yaml.safe_load((ROOT / "reporting.yaml").read_text())
    raw.update(over)
    return parse(raw, CONTRACTS, "phi")


class Msg:
    def __init__(self, event: Any, offset: int, key: bytes = b"n-1", err: Any = None, partition: int = 0) -> None:
        self._v = event if isinstance(event, bytes) else json.dumps(event).encode()
        self._o, self._k, self._e, self._p = offset, key, err, partition

    def value(self) -> bytes:
        return self._v

    def key(self) -> bytes:
        return self._k

    def offset(self) -> int:
        return self._o

    def partition(self) -> int:
        return self._p

    def topic(self) -> str:
        return "emr.notes.v1"

    def error(self) -> Any:
        return self._e


class Consumer:
    def __init__(self, conf: dict[str, Any], messages: list[Any]) -> None:
        self.conf, self.messages = conf, list(messages)
        self.committed: list[int] = []
        self.seeks: list[Any] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.topics = topics

    def poll(self, timeout: float) -> Any:
        return self.messages.pop(0) if self.messages else None

    def commit(self, message: Any, asynchronous: bool) -> None:
        self.committed.append(message.offset())

    def seek(self, tp: Any) -> None:
        self.seeks.append(tp)

    def close(self) -> None:
        self.closed = True


class Producer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, bytes | None, dict[str, bytes]]] = []

    def produce(self, topic: str, key: bytes, value: bytes | None, headers: list[tuple[str, bytes]]) -> None:
        self.sent.append((topic, key, value, dict(headers)))

    def flush(self, timeout: float) -> None:
        pass


def note(note_id: str = "n-1", **data: Any) -> dict[str, Any]:
    d = {
        "note_id": note_id,
        "patient_id": "p-77",
        "kind": "progress",
        "body": "Synthetic note.",
        "signed_at": "2026-03-04T10:00:00Z",
        "clinic_id": "north-clinic",
        **data,
    }
    return {
        "specversion": "1.0",
        "id": f"e-{note_id}",
        "type": "valxel.note.saved.v1",
        "subject": f"note/{note_id}",
        "data": d,
    }


@pytest.fixture
def cfg() -> Config:
    return config()
