"""The reporting store in the product's Postgres.

`migrate` creates, from reporting.yaml and the contracts:
  - one table per reporting table, its columns typed from the contract (text, bigint, double
    precision, boolean, date, timestamptz, jsonb), keyed by the table's key, with _offset,
    _partition and _updated_at;
  - row-level security on tables with a tenant: reporting_reader sees a row only when the
    session's `reporting.tenant` setting names its tenant (set it per login or connection pool in
    the BI tool, never in a query people can edit). Reports across tenants need a role made for it;
  - _reporting_state: each table's generation, which `reporting-admin rebuild` raises so the sink
    reads its topics from the start again with a new consumer group.

Every statement is composed with psycopg's sql module: table and column names come from the
approved reporting.yaml and are quoted as identifiers, and every value is a bound parameter. That is
why the lines Semgrep's raw-SQL rule flags carry a nosemgrep marker.

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
                    "create policy tenant_rows on {} for select to {} using ({} = "
                    "current_setting('reporting.tenant', true))"
                ).format(name, sql.Identifier(READER), sql.Identifier(t.tenant)),
            ]
    return out


def migrate(url: str, cfg: Config, contracts: Contracts) -> None:
    with psycopg.connect(url, autocommit=True) as conn, conn.cursor() as cur:
        for stmt in ddl(cfg, contracts):
            cur.execute(stmt)


class PgStore:
    def __init__(self, url: str, cfg: Config) -> None:
        self.url, self.cfg = url, cfg
        self.conn = psycopg.connect(url, autocommit=True)

    def _key(self, table: str) -> sql.Identifier:
        return sql.Identifier(self.cfg.tables[table].key)

    def upsert(self, table: str, key: str, row: dict[str, Any], offset: int, partition: int = 0) -> bool:
        cols = list(row)
        values = [json.dumps(v) if isinstance(v, (dict, list)) else v for v in row.values()]
        q = sql.SQL(
            "insert into {t} ({cols}, _offset, _partition, _updated_at) values ({vals}, %s, %s, now()) "
            "on conflict ({k}) do update set {sets}, _offset = excluded._offset, _partition = excluded._partition, "
            "_updated_at = now() where {t}._partition <> excluded._partition or {t}._offset < excluded._offset"
        ).format(
            t=sql.Identifier(table),
            cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
            vals=sql.SQL(", ").join(sql.Placeholder() * len(cols)),
            k=self._key(table),
            sets=sql.SQL(", ").join(sql.SQL("{c} = excluded.{c}").format(c=sql.Identifier(c)) for c in cols),
        )
        with self.conn.cursor() as cur:
            cur.execute(
                q, [*values, offset, partition]
            )  # nosemgrep: psycopg sql.Identifier from approved reporting.yaml; values are bound
            return cur.rowcount == 1

    def delete(self, table: str, key: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("delete from {} where {} = %s").format(sql.Identifier(table), self._key(table)), [key]
            )  # nosemgrep: psycopg sql.Identifier from approved reporting.yaml; values are bound

    def rows(self, table: str, tenant: str | None = None) -> list[dict[str, Any]]:
        """As a report would see them: through reporting_reader and its tenant, when one is given."""
        cols = self.cfg.tables[table].columns
        with self.conn.transaction(), self.conn.cursor() as cur:
            if tenant is not None:
                cur.execute(
                    sql.SQL("set local role {}").format(sql.Identifier(READER))
                )  # nosemgrep: psycopg sql.Identifier from approved reporting.yaml; values are bound
                cur.execute(sql.SQL("select set_config('reporting.tenant', %s, true)"), [tenant])
            cur.execute(  # nosemgrep: psycopg sql.Identifier from approved reporting.yaml; values are bound
                sql.SQL("select {} from {}").format(
                    sql.SQL(", ").join(map(sql.Identifier, cols)), sql.Identifier(table)
                )
            )
            return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]

    def generation(self, table: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("select generation from _reporting_state where tbl = %s"), [table])
            r = cur.fetchone()
        return int(r[0]) if r else 1

    def rebuild(self, table: str) -> int:
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute(
                sql.SQL("delete from {}").format(sql.Identifier(table))
            )  # nosemgrep: psycopg sql.Identifier from approved reporting.yaml; values are bound
            cur.execute(
                sql.SQL("update _reporting_state set generation = generation + 1 where tbl = %s returning generation"),
                [table],
            )
            r = cur.fetchone()
        return int(r[0]) if r else 1

    def expire(self, table: str, days: int) -> int:
        with self.conn.cursor() as cur:
            cur.execute(  # nosemgrep: psycopg sql.Identifier from approved reporting.yaml; values are bound
                sql.SQL("delete from {} where _updated_at < now() - make_interval(days => %s)").format(
                    sql.Identifier(table)
                ),
                [days],
            )
            return cur.rowcount

    def close(self) -> None:
        self.conn.close()
