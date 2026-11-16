from __future__ import annotations

import io

import pytest

from reporting import admin
from reporting.store import MemoryStore

from .conftest import ROOT


@pytest.fixture(autouse=True)
def paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPORTING_CONFIG_PATH", str(ROOT / "reporting.yaml"))
    monkeypatch.setenv("REPORTING_CONTRACTS_DIR", str(ROOT / "contracts"))
    monkeypatch.setenv("REPORTING_DATA_CLASS", "phi")


def run(*args: str, store: MemoryStore | None = None) -> tuple[int, str]:
    out = io.StringIO()
    return admin.main(list(args), store=store or MemoryStore(), out=out), out.getvalue()


def test_check_status_rebuild_and_expire() -> None:
    assert run("check") == (0, "reporting.yaml is valid: 1 tables, 3 destinations\n")
    store = MemoryStore(now=100 * 86400)
    store.upsert("notes", "n-1", {"note_id": "n-1"}, 1)
    assert run("status", store=store) == (0, "notes: generation 1, 1 rows\n")
    store.now = 4000 * 86400
    assert run("expire", store=store) == (0, "notes: removed 1 rows\n")
    code, text = run("rebuild", "notes", store=store)
    assert code == 0 and "generation 2" in text
    code, text = run("rebuild", "forms")
    assert code == 2 and "reporting-admin" in text


def test_an_invalid_config_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPORTING_DATA_CLASS", "phi")
    monkeypatch.setenv("REPORTING_CONTRACTS_DIR", str(ROOT / "no-contracts"))
    code, text = run("check")
    assert code == 1 and "isn't a data contract" in text


def test_migrate_needs_a_database(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    monkeypatch.setattr(admin, "migrate", lambda url, cfg, contracts: calls.append(url))
    monkeypatch.setenv("REPORTING_DATABASE_URL", "postgresql://example")
    assert run("migrate") == (0, "Migrated.\n") and calls == ["postgresql://example"]
