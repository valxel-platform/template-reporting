"""reporting-admin: check, migrate, status, rebuild <table>, expire.

check     reporting.yaml against the contracts and the product's data class (what G4 approved)
migrate   create or extend the store's tables, their row-level security and the reader role
status    each table's generation and row count
rebuild   empty a table and read its topic from the start again (a new consumer group)
expire    remove rows untouched for longer than each table's retention_days (run daily)
"""

from __future__ import annotations

import sys
from typing import TextIO

from .audit import Actor, Auditor
from .component import COMPONENT
from .config import ConfigError, load
from .contracts import Contracts
from .pgstore import PgStore, migrate
from .settings import Settings
from .store import Store


def main(
    argv: list[str] | None = None,
    *,
    store: Store | None = None,
    out: TextIO = sys.stdout,
    audit: Auditor | None = None,
) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    s = Settings()
    contracts = Contracts.load(s.contracts_dir)
    try:
        cfg = load(s.config_path, contracts, s.data_class)
    except ConfigError as exc:
        print(exc, file=out)
        return 1
    cmd = args[0] if args else "check"
    if cmd == "check":
        print(f"reporting.yaml is valid: {len(cfg.tables)} tables, {len(cfg.destinations)} destinations", file=out)
        return 0
    if cmd == "connectors":
        from .connectors import render

        lake = {k: v for k, v in (("warehouse", s.lake_warehouse), ("database", s.lake_database)) if v}
        print(
            render(cfg, namespace=s.connect_namespace or "<namespace>", cluster=s.connect_cluster, lake=lake),
            file=out,
            end="",
        )
        return 0
    if cmd == "migrate":
        migrate(s.database_url, cfg, contracts)
        print("Migrated.", file=out)
        return 0
    st = store or PgStore(s.database_url, cfg)

    def audited(action: str, table: str, **details: object) -> None:
        """Who changed the store, and what: the person running the command, or the component."""
        from .app import auditor

        who = Actor(id=s.actor, type="user") if s.actor else Actor(id=COMPONENT, type="service")
        (audit or auditor(s)).record(action, actor=who, resource=("reporting_table", [table]), details=dict(details))

    if cmd == "status":
        for name in cfg.tables:
            print(f"{name}: generation {st.generation(name)}, {len(st.rows(name))} rows", file=out)
        return 0
    if cmd == "rebuild" and len(args) > 1 and args[1] in cfg.tables:
        gen = st.rebuild(args[1])
        audited("reporting.table.rebuilt", args[1], generation=gen)
        print(f"{args[1]}: emptied; generation {gen} reads its topic from the start", file=out)
        return 0
    if cmd == "expire":
        for name, t in cfg.tables.items():
            if t.retention_days:
                removed = st.expire(name, t.retention_days)
                audited("reporting.rows.expired", name, removed=removed, retention_days=t.retention_days)
                print(f"{name}: removed {removed} rows", file=out)
        return 0
    print(__doc__, file=out)
    return 2
