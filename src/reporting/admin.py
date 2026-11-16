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

from .config import ConfigError, load
from .contracts import Contracts
from .pgstore import PgStore, migrate
from .settings import Settings
from .store import Store


def main(argv: list[str] | None = None, *, store: Store | None = None, out: TextIO = sys.stdout) -> int:
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
    if cmd == "migrate":
        migrate(s.database_url, cfg, contracts)
        print("Migrated.", file=out)
        return 0
    st = store or PgStore(s.database_url, cfg)
    if cmd == "status":
        for name in cfg.tables:
            print(f"{name}: generation {st.generation(name)}, {len(st.rows(name))} rows", file=out)
        return 0
    if cmd == "rebuild" and len(args) > 1 and args[1] in cfg.tables:
        print(f"{args[1]}: emptied; generation {st.rebuild(args[1])} reads its topic from the start", file=out)
        return 0
    if cmd == "expire":
        for name, t in cfg.tables.items():
            if t.retention_days:
                print(f"{name}: removed {st.expire(name, t.retention_days)} rows", file=out)
        return 0
    print(__doc__, file=out)
    return 2
