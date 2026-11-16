"""Kafka Connect connectors for the lake and exports (D6, D7 of the design "Datasets, contracts and
reporting"), rendered from reporting.yaml: `reporting-admin connectors > connectors.yaml`.

One Strimzi KafkaConnector per destination and table, reading only the table's filtered topic
(`<topic_prefix>.<table>.v1`, or `.deid.v1` for a de-identified destination): never the source,
so a connector can only send what reporting.yaml allows. Their consumer groups are Connect's own
(`connect-<connector>`), under Connect's Kafka identity, which may read only the filtered topics.

  iceberg    the product's lake: Apache Iceberg's sink into Iceberg tables on S3 in the product's
             account, through the Glue catalog; one table per reporting table, keyed by its key
  snowflake  Snowflake's sink (Apache-2.0)
  bigquery   the BigQuery sink (Apache-2.0)
  s3         Aiven's S3 sink (Apache-2.0): JSON files per table
  jdbc       Aiven's JDBC sink (Apache-2.0): upserts by key

Credentials are never here: each connector reads them from a Secret through Strimzi's
KubernetesSecretConfigProvider (`${secrets:<namespace>/<secret>:<key>}`), one Secret per destination
named `<component>-<destination>`.
"""

from __future__ import annotations

from typing import Any

import yaml

from .config import Config

CLASSES = {
    "iceberg": "org.apache.iceberg.connect.IcebergSinkConnector",
    "snowflake": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "bigquery": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
    "s3": "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector",
    "jdbc": "io.aiven.connect.jdbc.JdbcSinkConnector",
}


def _secret(namespace: str, secret: str, key: str) -> str:
    return f"${{secrets:{namespace}/{secret}:{key}}}"


def connector(
    cfg: Config, destination: str, table: str, *, namespace: str, cluster: str, lake: dict[str, str]
) -> dict[str, Any]:
    d, t = cfg.destinations[destination], cfg.tables[table]
    topic = cfg.topic(table, d.deidentified)
    secret = f"{cfg.name}-{destination}"
    conf: dict[str, Any] = {
        "topics": topic,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "errors.tolerance": "none",  # a record that can't be written stops the connector: nothing is lost
    }
    if d.type == "iceberg":
        conf |= {
            "iceberg.tables": f"{lake.get('database', cfg.name.replace('-', '_'))}.{table}",
            "iceberg.tables.default-id-columns": t.key,
            "iceberg.tables.auto-create-enabled": "true",
            "iceberg.tables.evolve-schema-enabled": "true",
            "iceberg.catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
            "iceberg.catalog.warehouse": lake.get("warehouse", "s3://<lake-bucket>/warehouse"),
            "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "iceberg.control.topic": f"{cfg.topic_prefix}.iceberg-control",
        }
    elif d.type == "snowflake":
        conf |= {
            "snowflake.topic2table.map": f"{topic}:{table}",
            "snowflake.url.name": _secret(namespace, secret, "url"),
            "snowflake.user.name": _secret(namespace, secret, "user"),
            "snowflake.private.key": _secret(namespace, secret, "private-key"),
            "snowflake.database.name": _secret(namespace, secret, "database"),
            "snowflake.schema.name": _secret(namespace, secret, "schema"),
        }
    elif d.type == "bigquery":
        conf |= {
            "project": _secret(namespace, secret, "project"),
            "defaultDataset": _secret(namespace, secret, "dataset"),
            "keyfile": _secret(namespace, secret, "keyfile"),
            "keySource": "JSON",
            "topic2TableMap": f"{topic}:{table}",
            "autoCreateTables": "true",
        }
    elif d.type == "s3":
        conf |= {
            "aws.s3.bucket.name": _secret(namespace, secret, "bucket"),
            "aws.s3.region": _secret(namespace, secret, "region"),
            "format.output.type": "jsonl",
            "file.name.prefix": f"{table}/",
        }
    elif d.type == "jdbc":
        conf |= {
            "connection.url": _secret(namespace, secret, "url"),
            "connection.user": _secret(namespace, secret, "user"),
            "connection.password": _secret(namespace, secret, "password"),
            "insert.mode": "upsert",
            "pk.mode": "record_key",
            "pk.fields": t.key,
            "table.name.format": table,
            "auto.create": "true",
            "delete.enabled": "true",
        }
    return {
        "apiVersion": "kafka.strimzi.io/v1",
        "kind": "KafkaConnector",
        "metadata": {
            "name": f"{cfg.name}-{destination}-{table}".replace("_", "-"),
            "namespace": namespace,
            "labels": {"strimzi.io/cluster": cluster},
        },
        "spec": {"class": CLASSES[d.type], "tasksMax": 1, "config": conf},
    }


def render(cfg: Config, *, namespace: str, cluster: str = "valxel-connect", lake: dict[str, str] | None = None) -> str:
    """Every destination that Kafka Connect writes (all but the store), one connector per table."""
    docs = [
        connector(cfg, name, table, namespace=namespace, cluster=cluster, lake=lake or {})
        for name, d in sorted(cfg.destinations.items())
        if not d.written_here
        for table in d.tables
    ]
    head = (
        "# Rendered by reporting-admin connectors from reporting.yaml; don't edit by hand.\n"
        "# Each reads only a filtered topic; credentials come from Secrets, never from here.\n"
    )
    return head + yaml.safe_dump_all(docs, sort_keys=False) if docs else head
