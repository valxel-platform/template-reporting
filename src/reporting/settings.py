"""Settings, from the environment (prefix REPORTING_)."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="REPORTING_")

    config_path: str = "reporting.yaml"
    contracts_dir: str = "contracts"  # written by the SDLC tool, shipped in the image
    data_class: str = "general"  # general | payments | phi: from the product (decides what may leave)
    # The store, as this component's own login (platform-resources postgres-login, access owner): it
    # signs in as its workload identity with a short-lived token (pg_auth gcp-iam or azure-entra; no
    # password anywhere, constitution VX-20), then SETs ROLE pg_role (<database>_owner), so the tables
    # belong to the store's role. workload.yaml binds REPORTING_PG_HOST, _PORT, _DATABASE, _USER,
    # _AUTH and _ROLE. Local runs: database_url (or pg_auth password with pg_password).
    pg_host: str = ""
    pg_port: int = 5432
    pg_database: str = ""
    pg_user: str = ""
    pg_auth: str = "password"  # gcp-iam | azure-entra | password (local and tests only)
    pg_password: str = ""
    pg_sslmode: str = "require"
    pg_role: str = ""
    database_url: str = ""
    # The login's tokenize function (<login>_key.tokenize): tokens from the database's own versioned
    # key, which never reaches this component. Without it (local runs), token_key.
    tokenize_function: str = ""
    kafka_bootstrap: str = ""
    kafka_config: dict[str, str] = {}  # security.protocol, sasl.*: as the platform's kafka-user gives them
    # Local runs and tests only: a key for tokenized fields when there's no tokenize_function.
    token_key: str = ""
    # Kafka Connect (reporting-admin connectors): where the connectors live, and the product's lake.
    connect_namespace: str = ""
    connect_cluster: str = "valxel-connect"
    lake_warehouse: str = ""  # s3://<the product's lake bucket>/warehouse
    lake_database: str = ""  # the Glue database; the component's name by default
    # /metrics (and /healthz) for the platform, on the management port as in every template (8081):
    # the mesh lets the data plane's metrics collector reach that port only. The sink serves no API.
    port: int = 8081
    product: str = ""  # the product's id, e.g. emr: in audit records
    # Audit trail (audit.py): "kafka" in deployed environments, "log" locally. Required (a sink that
    # can't record where data goes doesn't start) for payments and PHI unless set.
    audit_sink: str = "log"
    audit_kafka_bootstrap: str = ""
    audit_topic: str = "audit.v1"
    audit_kafka_config: dict[str, str] = {}  # security.protocol, sasl.*: TLS for payments and PHI
    audit_required: bool | None = None
    environment: str = ""  # dev | staging | production, in audit records
    # Who runs reporting-admin (a person's id, from the platform's exec or the pipeline): the actor
    # of the audit records for rebuild, expire and connectors. Unset: the component itself.
    actor: str = ""
