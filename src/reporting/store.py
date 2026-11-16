"""The reporting store: one current-state table per reporting table, one row per key.

Rows only move forward: each keeps the Kafka offset it came from, and an older event never
overwrites a newer one (a replay after a rebuild is safe). A deletion removes the row. Each table
records when its rows last changed (_updated_at), for retention.

MemoryStore is for tests; PgStore (pgstore.py) is the product's Postgres, with row-level security
by tenant so clinic staff see only their clinic.
"""

from __future__ import annotations

from typing import Any, Protocol


class Store(Protocol):
    def upsert(self, table: str, key: str, row: dict[str, Any], offset: int, partition: int = 0) -> bool: ...

    def delete(self, table: str, key: str) -> None: ...

    def rows(self, table: str, tenant: str | None = None) -> list[dict[str, Any]]: ...

    def generation(self, table: str) -> int: ...

    def rebuild(self, table: str) -> int: ...

    def expire(self, table: str, days: int) -> int: ...


class MemoryStore:
    def __init__(self, tenants: dict[str, str] | None = None, now: float = 0.0) -> None:
        self.tables: dict[str, dict[str, dict[str, Any]]] = {}
        self.offsets: dict[tuple[str, str], tuple[int, int]] = {}
        self.updated: dict[tuple[str, str], float] = {}
        self.gens: dict[str, int] = {}
        self.tenants = tenants or {}  # table -> tenant column
        self.now = now

    def upsert(self, table: str, key: str, row: dict[str, Any], offset: int, partition: int = 0) -> bool:
        seen = self.offsets.get((table, key))
        if seen is not None and seen[0] == partition and seen[1] >= offset:
            return False
        self.tables.setdefault(table, {})[key] = dict(row)
        self.offsets[(table, key)] = (partition, offset)
        self.updated[(table, key)] = self.now
        return True

    def delete(self, table: str, key: str) -> None:
        self.tables.get(table, {}).pop(key, None)

    def rows(self, table: str, tenant: str | None = None) -> list[dict[str, Any]]:
        col = self.tenants.get(table)
        return [r for r in self.tables.get(table, {}).values() if tenant is None or not col or r.get(col) == tenant]

    def generation(self, table: str) -> int:
        return self.gens.get(table, 1)

    def rebuild(self, table: str) -> int:
        self.tables.pop(table, None)
        self.offsets = {k: v for k, v in self.offsets.items() if k[0] != table}
        self.gens[table] = self.generation(table) + 1
        return self.gens[table]

    def expire(self, table: str, days: int) -> int:
        cutoff = self.now - days * 86400
        old = [k for (t, k), at in self.updated.items() if t == table and at < cutoff]
        for k in old:
            self.delete(table, k)
            del self.updated[(table, k)]
        return len(old)
