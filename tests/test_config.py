"""reporting.yaml against the contracts and the product's data class: what G4 approves."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from reporting.config import ConfigError, load, parse
from reporting.contracts import Contracts

from .conftest import CONTRACTS, ROOT, config


def raw() -> dict:
    return yaml.safe_load((ROOT / "reporting.yaml").read_text())


def test_the_example_is_valid_and_says_where_each_table_goes() -> None:
    cfg = config()
    t = cfg.tables["notes"]
    assert (t.dataset, t.key, t.tenant, t.columns) == (
        "emr.notes",
        "note_id",
        "clinic_id",
        ("note_id", "kind", "signed_at", "clinic_id", "patient_id"),
    )
    assert cfg.topic("notes") == "emr.reporting.notes.v1" and cfg.topic("notes", True) == "emr.reporting.notes.deid.v1"
    assert cfg.destinations["store"].written_here and not cfg.destinations["lake"].written_here
    assert load(ROOT / "reporting.yaml", CONTRACTS, "phi").name == "template-reporting"


@pytest.mark.parametrize(
    ("change", "words"),
    [
        ({"name": "X"}, "name must be the component's name"),
        ({"topic_prefix": ""}, "topic_prefix"),
        ({"tables": {}}, "name at least one"),
        ({"destinations": {}}, "destinations: name at least one"),
        ({"tables": {"notes": {"from": "emr.ghost", "key": "a", "fields": ["a"]}}}, "isn't a data contract"),
        ({"tables": {"Notes": {"from": "emr.notes", "key": "note_id", "fields": ["note_id"]}}}, "lowercase words"),
        ({"tables": {"notes": {"from": "emr.notes", "key": "note_id", "fields": []}}}, "list the fields"),
        (
            {"tables": {"notes": {"from": "emr.notes", "key": "note_id", "fields": ["note_id", "ssn"]}}},
            "has no field ssn",
        ),
        ({"tables": {"notes": {"from": "emr.notes", "key": "id", "fields": ["note_id"]}}}, "key id must be"),
        (
            {
                "tables": {
                    "notes": {"from": "emr.notes", "key": "note_id", "fields": ["note_id"], "tenant": "clinic_id"}
                }
            },
            "tenant clinic_id must be",
        ),
        (
            {
                "tables": {
                    "notes": {
                        "from": "emr.notes",
                        "key": "note_id",
                        "fields": ["note_id", "signed_at"],
                        "generalize": {"signed_at": "month"},
                    }
                }
            },
            "generalize signed_at as year or zip3",
        ),
        (
            {
                "tables": {
                    "notes": {
                        "from": "emr.notes",
                        "key": "note_id",
                        "fields": ["note_id"],
                        "generalize": {"signed_at": "year"},
                    }
                }
            },
            "which it doesn't keep",
        ),
        (
            {
                "tables": {
                    "notes": {
                        "from": "emr.notes",
                        "key": "note_id",
                        "fields": ["note_id", "patient_id"],
                        "tokenize": ["patient_id"],
                    }
                }
            },
            "kept and tokenized",
        ),
    ],
)
def test_tables_are_checked(change: dict, words: str) -> None:
    r = raw()
    r.update(change)
    if change.get("tables"):
        r["destinations"] = {"store": {"type": "postgres", "tables": list(change["tables"])}}
    with pytest.raises(ConfigError, match=words):
        parse(r, CONTRACTS, "phi")


def test_destinations_follow_each_fields_class_and_the_products_data_class() -> None:
    def problems(dests: dict, fields: list[str] | None = None, data_class: str = "phi") -> str:
        r = raw()
        r["destinations"] = dests
        if fields:
            r["tables"]["notes"]["fields"] = fields
        with pytest.raises(ConfigError) as e:
            parse(r, CONTRACTS, data_class)
        return str(e.value)

    assert "type must be one of" in problems({"x": {"type": "excel", "tables": ["notes"]}})
    assert "no table forms" in problems({"store": {"type": "postgres", "tables": ["forms"]}})
    assert "list the tables" in problems({"store": {"type": "postgres", "tables": []}})
    assert "keeps a store or a lake in its own account" in problems(
        {"w": {"type": "snowflake", "tables": ["notes"], "deidentified": True}}
    )
    assert "leaves its account only de-identified" in problems(
        {"store": {"type": "postgres", "tables": ["notes"]}, "w": {"type": "bigquery", "tables": ["notes"]}}
    )
    found = problems(
        {
            "store": {"type": "postgres", "tables": ["notes"]},
            "w": {"type": "s3", "tables": ["notes"], "deidentified": True},
        },
        fields=["note_id", "kind", "signed_at", "clinic_id", "body"],
    )
    assert "notes.body is phi; tokenize or generalize it" in found
    # A general product may export identified data, and needs no store.
    r = raw()
    r["destinations"] = {"w": {"type": "jdbc", "tables": ["notes"]}}
    assert parse(r, CONTRACTS, "general").destinations["w"].type == "jdbc"


def test_secret_fields_never_leave_and_a_personal_key_needs_a_token(tmp_path: Path) -> None:
    import json

    schema = json.loads((ROOT / "contracts/emr.notes.json").read_text())
    schema["properties"]["note_id"]["x-classification"] = "pii"
    schema["properties"]["kind"]["x-classification"] = "secret"
    (tmp_path / "emr.notes.json").write_text(json.dumps(schema))
    (tmp_path / "lock.yaml").write_text((ROOT / "contracts/lock.yaml").read_text())
    with pytest.raises(ConfigError) as e:
        parse(raw(), Contracts.load(tmp_path), "phi")
    assert "kind is secret and never leaves" in str(e.value) and "key note_id is personal" in str(e.value)
    assert Contracts.load(tmp_path / "none").for_dataset("emr.notes") is None
    (tmp_path / "bad.yaml").write_text("- a list")
    with pytest.raises(ConfigError, match="must be a mapping"):
        load(tmp_path / "bad.yaml", CONTRACTS)
