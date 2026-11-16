"""Settings, from the environment (prefix REPORTING_)."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="REPORTING_")

    config_path: str = "reporting.yaml"
    contracts_dir: str = "contracts"  # written by the SDLC tool, shipped in the image
    data_class: str = "general"  # general | payments | phi: from the product (decides what may leave)
    # The store's owner (migrations and writes). Reports connect as a member of reporting_reader.
    database_url: str = ""
    kafka_bootstrap: str = ""
    kafka_config: dict[str, str] = {}  # security.protocol, sasl.*: as the platform's kafka-user gives them
    # The key for tokenized fields (a secret of this component; the same key gives the same tokens).
    token_key: str = ""
    # Kafka Connect (reporting-admin connectors): where the connectors live, and the product's lake.
    connect_namespace: str = ""
    connect_cluster: str = "valxel-connect"
    lake_warehouse: str = ""  # s3://<the product's lake bucket>/warehouse
    lake_database: str = ""  # the Glue database; the component's name by default
    port: int = 8080  # /metrics (and /healthz) for the platform
