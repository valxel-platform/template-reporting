"""Kafka Connect connectors for the lake and exports, rendered from reporting.yaml."""

from __future__ import annotations

import io

import pytest
import yaml

from reporting import admin
from reporting.connectors import render

from .conftest import ROOT, config


def test_each_destination_reads_only_its_filtered_topic_and_holds_no_credentials() -> None:
    docs = list(yaml.safe_load_all(render(config(), namespace="emr-dev", lake={"warehouse": "s3://lake/w"})))
    by = {d["metadata"]["name"]: d for d in docs}
    assert sorted(by) == ["template-reporting-customer-warehouse-notes", "template-reporting-lake-notes"]
    lake, wh = by["template-reporting-lake-notes"], by["template-reporting-customer-warehouse-notes"]
    assert lake["spec"]["config"]["topics"] == "emr.reporting.notes.v1"
    assert lake["spec"]["config"]["iceberg.catalog.warehouse"] == "s3://lake/w"
    assert lake["spec"]["config"]["iceberg.tables"] == "template_reporting.notes"
    assert wh["spec"]["config"]["topics"] == "emr.reporting.notes.deid.v1"  # de-identified: the deid topic only
    assert (
        wh["spec"]["config"]["snowflake.user.name"] == "${secrets:emr-dev/template-reporting-customer-warehouse:user}"
    )
    assert all(d["metadata"]["labels"] == {"strimzi.io/cluster": "valxel-connect"} for d in docs)


@pytest.mark.parametrize(
    ("kind", "key"), [("bigquery", "topic2TableMap"), ("s3", "file.name.prefix"), ("jdbc", "pk.fields")]
)
def test_other_exports(kind: str, key: str) -> None:
    cfg = config(
        destinations={
            "store": {"type": "postgres", "tables": ["notes"]},
            "x": {"type": kind, "tables": ["notes"], "deidentified": True},
        }
    )
    [doc] = yaml.safe_load_all(render(cfg, namespace="n"))
    assert key in doc["spec"]["config"] and doc["spec"]["config"]["topics"] == "emr.reporting.notes.deid.v1"
    only_store = config(destinations={"store": {"type": "postgres", "tables": ["notes"]}})
    assert render(only_store, namespace="n").startswith("# Rendered") and "KafkaConnector" not in render(
        only_store, namespace="n"
    )


def test_the_admin_command(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPORTING_CONFIG_PATH", str(ROOT / "reporting.yaml"))
    monkeypatch.setenv("REPORTING_CONTRACTS_DIR", str(ROOT / "contracts"))
    monkeypatch.setenv("REPORTING_DATA_CLASS", "phi")
    monkeypatch.setenv("REPORTING_LAKE_DATABASE", "emr_lake")
    out = io.StringIO()
    assert admin.main(["connectors"], out=out) == 0
    assert "emr_lake.notes" in out.getvalue() and "namespace: <namespace>" in out.getvalue()
