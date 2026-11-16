"""The Postgres store: typed columns, rows that only move forward, and row-level security by tenant."""

from __future__ import annotations

import os
import uuid

import psycopg
import pytest

from reporting.pgstore import PgStore, column_type, ddl, migrate

from .conftest import CONTRACTS, config

URL = os.environ.get("REPORTING_TEST_DATABASE_URL", "")
pytestmark = pytest.mark.postgres


@pytest.fixture
def store():
    if not URL:
        pytest.skip("set REPORTING_TEST_DATABASE_URL")
    db = f"reporting_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(URL, autocommit=True) as c:
        c.execute(f"create database {db}")
    url = psycopg.conninfo.make_conninfo(URL, dbname=db)
    cfg = config()
    migrate(url, cfg, CONTRACTS)
    migrate(url, cfg, CONTRACTS)  # again: nothing changes
    s = PgStore(url, cfg)
    yield s
    s.close()
    with psycopg.connect(URL, autocommit=True) as c:
        c.execute(f"drop database {db} with (force)")


def test_columns_follow_the_contracts_types() -> None:
    assert [
        column_type({"type": t, "format": f})
        for t, f in [
            ("string", None),
            ("string", "date"),
            ("string", "date-time"),
            ("integer", None),
            ("number", None),
            ("boolean", None),
            ("object", None),
            ("array", None),
            ("x", None),
        ]
    ] == ["text", "date", "timestamptz", "bigint", "double precision", "boolean", "jsonb", "jsonb", "text"]
    assert column_type({"type": "integer"}, tokenized=True) == "text"
    assert any("enable row level security" in str(s) for s in ddl(config(), CONTRACTS))


def test_rows_move_forward_and_reports_see_only_their_tenant(store: PgStore) -> None:
    row = {
        "note_id": "n-1",
        "kind": "progress",
        "signed_at": "2026-03-04T10:00:00Z",
        "clinic_id": "north-clinic",
        "patient_id": "tok",
    }
    assert store.upsert("notes", "n-1", row, 5)
    assert not store.upsert("notes", "n-1", {**row, "kind": "discharge"}, 3)  # an older event never wins
    assert store.upsert("notes", "n-1", {**row, "kind": "discharge"}, 6)
    store.upsert("notes", "n-2", {**row, "note_id": "n-2", "clinic_id": "south-clinic"}, 7)
    assert [r["kind"] for r in store.rows("notes", tenant="north-clinic")] == ["discharge"]
    assert [r["note_id"] for r in store.rows("notes", tenant="south-clinic")] == ["n-2"]
    assert store.rows("notes", tenant="elsewhere") == []
    assert len(store.rows("notes")) == 2  # the owner (the sink) sees every row
    store.delete("notes", "n-2")
    assert store.expire("notes", 1) == 0 and len(store.rows("notes")) == 1
    assert store.generation("notes") == 1 and store.rebuild("notes") == 2 and store.rows("notes") == []
    assert store.generation("missing") == 1
