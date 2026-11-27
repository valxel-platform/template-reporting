"""reporting.yaml: which datasets become reporting tables, which fields each keeps and how, and where
they go. Agents write it from the HLD's Reporting section; people approve it at G4 with the data
contracts it reads (the design "Datasets, contracts and reporting").

    name: emr-reporting                 # the component: consumer groups start with it
    topic_prefix: emr.reporting         # filtered topics: <prefix>.<table>.v1 (and .deid.v1)
    tables:
      notes:
        from: emr.notes                 # a dataset in contracts/ (its approved ODCS contract)
        key: note_id                    # the record's id: one row per key
        tenant: clinic_id               # whose row it is: row-level security in the store
        fields: [note_id, kind, signed_at, clinic_id]
        tokenize: [patient_id]          # a keyed HMAC, the same token for the same value
        generalize: {signed_at: year}   # year | zip3: what de-identified destinations get
        retention_days: 2555            # rows untouched for longer are removed (reporting-admin expire)
    destinations:                       # at least one; each reads with its own consumer group
      store: {type: postgres, tables: [notes]}
      lake: {type: iceberg, tables: [notes]}
      customer-warehouse: {type: snowflake, tables: [notes], deidentified: true}

Field rules come from each property's class in the contract (x-classification): `secret` never
leaves, a de-identified destination never gets a `pii` or `phi` value (only its token or its
generalization), and a PHI product keeps a store or a lake in its own account and sends nothing
identified anywhere else.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .contracts import Contracts

NAME = re.compile(r"^[a-z][a-z0-9-]{2,62}$")
TABLE = re.compile(r"^[a-z][a-z0-9_]{0,40}$")
OWN_ACCOUNT = ("postgres", "iceberg")  # the store (written here) and the lake (Kafka Connect, Iceberg sink)
EXPORTS = ("snowflake", "bigquery", "s3", "jdbc")  # Kafka Connect connectors to someone else's system
GENERALIZE = ("year", "zip3")
PERSONAL = ("pii", "phi")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Table:
    name: str
    dataset: str
    key: str
    fields: tuple[str, ...]
    tenant: str = ""
    tokenize: tuple[str, ...] = ()
    generalize: dict[str, str] = field(default_factory=dict)
    retention_days: int = 0

    @property
    def columns(self) -> tuple[str, ...]:
        return self.fields + tuple(f for f in self.tokenize if f not in self.fields)


@dataclass(frozen=True)
class Destination:
    name: str
    type: str
    tables: tuple[str, ...]
    deidentified: bool = False

    @property
    def written_here(self) -> bool:
        """The store: this component writes it. Everything else reads a filtered topic (Kafka Connect)."""
        return self.type == "postgres"


@dataclass(frozen=True)
class Config:
    name: str
    topic_prefix: str
    tables: dict[str, Table]
    destinations: dict[str, Destination]
    # A reference to the Expert Determination (45 CFR 164.514(b)(1)) that lets tokens go to
    # de-identified destinations, recorded by a person. Empty: de-identified copies carry no tokens.
    expert_determination: str = ""

    def topic(self, table: str, deidentified: bool = False) -> str:
        return f"{self.topic_prefix}.{table}.{'deid.' if deidentified else ''}v1"


def parse(raw: dict[str, Any], contracts: Contracts, data_class: str = "general") -> Config:
    problems: list[str] = []
    name = str(raw.get("name") or "")
    if not NAME.match(name):
        problems.append("name must be the component's name (lowercase, digits, hyphens)")
    prefix = str(raw.get("topic_prefix") or "")
    if not re.match(r"^[a-z][a-z0-9.-]*$", prefix):
        problems.append("topic_prefix must be like <product>.reporting")
    tables: dict[str, Table] = {}
    for tname, t in (raw.get("tables") or {}).items():
        tables[tname] = _table(str(tname), t or {}, contracts, problems)
    if not tables:
        problems.append("tables: name at least one")
    # A reference to the Expert Determination (45 CFR 164.514(b)(1)) that lets tokens leave as
    # de-identified, recorded by a person. Empty: tokens of a person's value don't leave.
    expert = str(raw.get("expert_determination") or "").strip()
    dests: dict[str, Destination] = {}
    for dname, d in (raw.get("destinations") or {}).items():
        d = d or {}
        dest = Destination(
            str(dname),
            str(d.get("type") or ""),
            tuple(str(x) for x in d.get("tables") or []),
            bool(d.get("deidentified")),
        )
        dests[dname] = dest
        if dest.type not in OWN_ACCOUNT + EXPORTS:
            problems.append(f"destination {dname}: type must be one of {', '.join(OWN_ACCOUNT + EXPORTS)}")
        for tn in dest.tables:
            if tn not in tables:
                problems.append(f"destination {dname}: no table {tn}")
        if not dest.tables:
            problems.append(f"destination {dname}: list the tables it gets")
        problems += _personal(dest, tables, contracts, data_class)
        if dest.deidentified and not expert:
            for tn in dest.tables:
                t = tables.get(tn)
                if t and t.key in t.tokenize:
                    problems.append(
                        f"destination {dname}: {tn} is keyed by a token, and a de-identified copy carries no tokens"
                        " without a recorded expert_determination (45 CFR 164.514(c)); key it by something else"
                    )
    if not dests:
        problems.append("destinations: name at least one (a store, a lake or an export)")
    if data_class == "phi" and dests and not any(d.type in OWN_ACCOUNT for d in dests.values()):
        problems.append(
            "a PHI product keeps a store or a lake in its own account (postgres or iceberg); "
            "de-identified exports are made from there"
        )
    if problems:
        raise ConfigError("reporting.yaml: " + "; ".join(problems))
    return Config(name, prefix, tables, dests, expert)


def _table(name: str, t: dict[str, Any], contracts: Contracts, problems: list[str]) -> Table:
    if not TABLE.match(name):
        problems.append(f"table {name}: names are lowercase words joined by _")
    dataset = str(t.get("from") or "")
    contract = contracts.for_dataset(dataset)
    if contract is None:
        problems.append(f"table {name}: from {dataset or '(missing)'} isn't a data contract in contracts/")
    known = contract.properties() if contract else {}
    fields = tuple(str(f) for f in t.get("fields") or [])
    tokenize = tuple(str(f) for f in t.get("tokenize") or [])
    generalize = {str(k): str(v) for k, v in (t.get("generalize") or {}).items()}
    key, tenant = str(t.get("key") or ""), str(t.get("tenant") or "")
    if not fields:
        problems.append(f"table {name}: list the fields it keeps")
    for f in (*fields, *tokenize, *generalize):
        if contract and f not in known:
            problems.append(f"table {name}: {dataset} has no field {f}")
        elif contract and known[f].get("x-classification") == "secret":
            problems.append(f"table {name}: {f} is secret and never leaves its service")
    if key not in fields:
        problems.append(f"table {name}: key {key or '(missing)'} must be one of its fields")
    if tenant and tenant not in fields:
        problems.append(f"table {name}: tenant {tenant} must be one of its fields")
    for f, how in generalize.items():
        if how not in GENERALIZE:
            problems.append(f"table {name}: generalize {f} as {' or '.join(GENERALIZE)}")
        if f not in fields:
            problems.append(f"table {name}: generalize {f}, which it doesn't keep")
    for f in tokenize:
        if f in fields:
            problems.append(f"table {name}: {f} is kept and tokenized; keep it or tokenize it")
    days = int(t.get("retention_days") or 0)
    return Table(name, dataset, key, fields, tenant, tokenize, generalize, max(days, 0))


def _personal(dest: Destination, tables: dict[str, Table], contracts: Contracts, data_class: str) -> list[str]:
    """What a destination may receive: an export of a PHI product is de-identified, and a de-identified
    destination never gets a person's value, only its generalization (or, with a recorded Expert
    Determination, its token: rules.row)."""
    out = []
    if data_class == "phi" and dest.type in EXPORTS and not dest.deidentified:
        out.append(
            f"destination {dest.name}: a PHI product's data leaves its account only de-identified (deidentified: true)"
        )
    if not dest.deidentified:
        return out
    for tn in dest.tables:
        t = tables.get(tn)
        contract = contracts.for_dataset(t.dataset) if t else None
        if not t or not contract:
            continue
        props = contract.properties()
        for f in t.fields:
            cls = props.get(f, {}).get("x-classification")
            if cls in PERSONAL and f not in t.generalize and f != t.key:
                out.append(f"destination {dest.name}: {tn}.{f} is {cls}; tokenize or generalize it, or leave it out")
        if props.get(t.key, {}).get("x-classification") in PERSONAL:
            out.append(f"destination {dest.name}: {tn}'s key {t.key} is personal; key the table by a token")

    return out


def load(path: str | Path, contracts: Contracts, data_class: str = "general") -> Config:
    raw = yaml.safe_load(Path(path).read_text()) or {}
    if not isinstance(raw, dict):
        raise ConfigError("reporting.yaml must be a mapping")
    return parse(raw, contracts, data_class)
