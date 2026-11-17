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


def report_login(store: PgStore, tenant: str | None) -> str:
    """A report login (a member of reporting_reader), granted one tenant or none."""
    login = f"report_{uuid.uuid4().hex[:8]}"
    with store.conn.cursor() as cur:
        cur.execute(f"create role {login} login password 'synthetic' in role reporting_reader")
    if tenant:
        store.grant_tenant(login, tenant)
    return login


def as_report(store: PgStore, login: str, statements: str) -> list[tuple]:
    url = psycopg.conninfo.make_conninfo(store.url, user=login, password="synthetic")
    with psycopg.connect(url, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(statements)
        while cur.nextset():
            pass
        return cur.fetchall()


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
    assert len(store.rows("notes")) == 2  # the owner (the sink) sees every row
    north, nobody = report_login(store, "north-clinic"), report_login(store, None)
    assert [r[0] for r in as_report(store, north, "select note_id from notes")] == ["n-1"]
    assert as_report(store, nobody, "select note_id from notes") == []  # no grant, no rows
    # What a report's own SQL might try: none of it reaches another tenant's rows.
    assert as_report(
        store, north, "select set_config('reporting.tenant', 'south-clinic', false); select note_id from notes"
    ) == [("n-1",)]
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        as_report(store, north, "select * from _reporting_logins")
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        as_report(store, north, "insert into _reporting_logins values (current_user, 'south-clinic')")
    assert store.revoke_tenant(north, "north-clinic") and not store.revoke_tenant(north, "north-clinic")
    assert as_report(store, north, "select note_id from notes") == []
    store.delete("notes", "n-2")
    assert store.expire("notes", 1) == 0 and len(store.rows("notes")) == 1
    assert store.generation("notes") == 1 and store.rebuild("notes") == 2 and store.rows("notes") == []
    assert store.generation("missing") == 1


def test_tenant_grants_through_the_admin_command(store: PgStore, monkeypatch: pytest.MonkeyPatch) -> None:
    import io

    from reporting import admin
    from reporting.audit import Auditor, MemorySink

    from .conftest import ROOT

    monkeypatch.setenv("REPORTING_CONFIG_PATH", str(ROOT / "reporting.yaml"))
    monkeypatch.setenv("REPORTING_CONTRACTS_DIR", str(ROOT / "contracts"))
    monkeypatch.setenv("REPORTING_DATA_CLASS", "phi")
    monkeypatch.setenv("REPORTING_ACTOR", "analyst-lead")
    sink = MemorySink()
    audit = Auditor(sink, service="reporting", data_class="phi")

    def run(*args: str) -> tuple[int, str]:
        out = io.StringIO()
        return admin.main(list(args), store=store, out=out, audit=audit), out.getvalue()

    assert run("grant-tenant", "report_north", "north-clinic") == (0, "report_north: can see north-clinic\n")
    assert run("tenants") == (0, "report_north: north-clinic\n")
    assert run("revoke-tenant", "report_north", "north-clinic")[0] == 0 and run("tenants") == (0, "")
    assert run("grant-tenant", "report_north")[0] == 2
    actions = [e["data"]["action"] for e in sink.events]
    assert actions == ["reporting.tenant.granted", "reporting.tenant.revoked"]
    assert sink.events[0]["data"]["actor"]["id"] == "analyst-lead"
