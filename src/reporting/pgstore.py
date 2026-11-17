"""The reporting store in the product's Postgres.

`migrate` creates, from reporting.yaml and the contracts:
  - one table per reporting table, its columns typed from the contract (text, bigint, double
    precision, boolean, date, timestamptz, jsonb), keyed by the table's key, with _offset,
    _partition and _updated_at;
  - row-level security on tables with a tenant: a report login (a member of reporting_reader) sees
    a row only when _reporting_logins maps that login to the row's tenant. The database decides,
    from the login the connection was made with (session_user): nothing a report's SQL can set,
    and SET ROLE doesn't change it. A login with no mapping sees no tenant's rows. Mappings are
    managed with `reporting-admin grant-tenant` and `revoke-tenant` (audited); reports across
    tenants need a role made for them;
  - _reporting_state: each table's generation, which `reporting-admin rebuild` raises so the sink
    reads its topics from the start again with a new consumer group.

Every statement is composed with psycopg's sql module: table and column names come from the
approved reporting.yaml and are quoted as identifiers, and every value is a bound parameter.

The sink connects as the owner. Reports connect as a login that is a member of reporting_reader.
Columns are only ever added: a field dropped from reporting.yaml stays until a person drops it.
"""

from __future__ import annotations

import json
from typing import Any

import psycopg
from psycopg import sql

from .config import Config
from .contracts import Contracts

READER = "reporting_reader"


def column_type(prop: dict[str, Any], tokenized: bool = False) -> str:
    if tokenized:
        return "text"
    t, fmt = prop.get("type"), prop.get("format")
    if t == "string":
        return {"date": "date", "date-time": "timestamptz"}.get(str(fmt), "text")
    return {
        "integer": "bigint",
        "number": "double precision",
        "boolean": "boolean",
        "object": "jsonb",
        "array": "jsonb",
    }.get(str(t), "text")


def ddl(cfg: Config, contracts: Contracts) -> list[sql.Composed | sql.SQL]:
    out: list[sql.Composed | sql.SQL] = [
        sql.SQL("create table if not exists _reporting_state (tbl text primary key, generation int not null)"),
        sql.SQL(f"do $$ begin create role {READER} nologin; exception when duplicate_object then null; end $$"),
        # Which report logins see which tenants. Readers can't read or change it; the function the
        # policies call can, for the connection's own login only.
        sql.SQL(
            "create table if not exists _reporting_logins (login text not null, tenant text not null,"
            " primary key (login, tenant))"
        ),
        sql.SQL("revoke all on _reporting_logins from public"),
        sql.SQL(
            "create or replace function reporting_tenants() returns setof text language sql stable"
            " security definer set search_path = pg_catalog, public as"
            " $$ select tenant from public._reporting_logins where login = session_user $$"
        ),
        sql.SQL("revoke all on function reporting_tenants() from public"),
        sql.SQL(f"grant execute on function reporting_tenants() to {READER}"),
    ]
    for t in cfg.tables.values():
        c = contracts.for_dataset(t.dataset)
        props = c.properties() if c else {}
        cols = [
            sql.SQL("{} {}").format(sql.Identifier(f), sql.SQL(column_type(props.get(f, {}), f in t.tokenize)))
            for f in t.columns
        ]
        name = sql.Identifier(t.name)
        out.append(
            sql.SQL(
                "create table if not exists {} ({}, _offset bigint not null, _partition int not null "
                "default 0, _updated_at timestamptz not null default now(), primary key ({}))"
            ).format(name, sql.SQL(", ").join(cols), sql.Identifier(t.key))
        )
        for f in t.columns:
            out.append(
                sql.SQL("alter table {} add column if not exists {} {}").format(
                    name, sql.Identifier(f), sql.SQL(column_type(props.get(f, {}), f in t.tokenize))
                )
            )
        out.append(
            sql.SQL("insert into _reporting_state values ({}, 1) on conflict do nothing").format(sql.Literal(t.name))
        )
        out.append(sql.SQL("grant select on {} to {}").format(name, sql.Identifier(READER)))
        if t.tenant:
            out += [
                sql.SQL("alter table {} enable row level security").format(name),
                sql.SQL("drop policy if exists tenant_rows on {}").format(name),
                sql.SQL(
                    "create policy tenant_rows on {} for select to {} using ({} in (select reporting_tenants()))"
                ).format(name, sql.Identifier(READER), sql.Identifier(t.tenant)),
            ]
    return out


def migrate(url: str, cfg: Config, contracts: Contracts) -> None:
    with psycopg.connect(url, autocommit=True) as conn, conn.cursor() as cur:
        for stmt in ddl(cfg, contracts):
            cur.execute(stmt)


def _q(*parts: str | sql.Composable) -> sql.Composed:
    """A statement from fixed SQL text and composed parts (identifiers, lists), never from values."""
    return sql.Composed([sql.SQL(p) if isinstance(p, str) else p for p in parts])


class PgStore:
    def __init__(self, url: str, cfg: Config) -> None:
        self.url, self.cfg = url, cfg
        self.conn = psycopg.connect(url, autocommit=True)

    def _key(self, table: str) -> sql.Identifier:
        return sql.Identifier(self.cfg.tables[table].key)

    def upsert(self, table: str, key: str, row: dict[str, Any], offset: int, partition: int = 0) -> bool:
        cols = list(row)
        values = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in row.values()]
        t, k = sql.Identifier(table), self._key(table)
        sets = sql.SQL(", ").join(_q(sql.Identifier(c), " = excluded.", sql.Identifier(c)) for c in cols)
        q = _q(
            "insert into ",
            t,
            " (",
            sql.SQL(", ").join(map(sql.Identifier, cols)),
            ", _offset, _partition, _updated_at) values (",
            sql.SQL(", ").join(sql.Placeholder() * len(cols)),
            ", %s, %s, now()) on conflict (",
            k,
            ") do update set ",
            sets,
            ", _offset = excluded._offset, _partition = excluded._partition, _updated_at = now() where ",
            t,
            "._partition <> excluded._partition or ",
            t,
            "._offset < excluded._offset",
        )
        with self.conn.cursor() as cur:
            cur.execute(q, [*values, offset, partition])
            return cur.rowcount == 1

    def delete(self, table: str, key: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(_q("delete from ", sql.Identifier(table), " where ", self._key(table), " = %s"), [key])

    def rows(self, table: str, tenant: str | None = None) -> list[dict[str, Any]]:
        """Every row, as the owner (the sink) sees them. Reports see theirs through their own login."""
        cols = self.cfg.tables[table].columns
        with self.conn.cursor() as cur:
            cur.execute(_q("select ", sql.SQL(", ").join(map(sql.Identifier, cols)), " from ", sql.Identifier(table)))
            out = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
        col = self.cfg.tables[table].tenant
        return [r for r in out if tenant is None or (col and r.get(col) == tenant)]

    def grant_tenant(self, login: str, tenant: str) -> None:
        """Let a report login see a tenant's rows."""
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("insert into _reporting_logins values (%s, %s) on conflict do nothing"), [login, tenant]
            )

    def revoke_tenant(self, login: str, tenant: str) -> bool:
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("delete from _reporting_logins where login = %s and tenant = %s"), [login, tenant])
            return bool(cur.rowcount)

    def tenant_grants(self) -> list[tuple[str, str]]:
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("select login, tenant from _reporting_logins order by login, tenant"))
            return [(str(a), str(b)) for a, b in cur.fetchall()]

    def generation(self, table: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("select generation from _reporting_state where tbl = %s"), [table])
            r = cur.fetchone()
        return int(r[0]) if r else 1

    def rebuild(self, table: str) -> int:
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute(_q("delete from ", sql.Identifier(table)))
            cur.execute(
                sql.SQL("update _reporting_state set generation = generation + 1 where tbl = %s returning generation"),
                [table],
            )
            r = cur.fetchone()
        return int(r[0]) if r else 1

    def expire(self, table: str, days: int) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                _q("delete from ", sql.Identifier(table), " where _updated_at < now() - make_interval(days => %s)"),
                [days],
            )
            return cur.rowcount

    def close(self) -> None:
        self.conn.close()
