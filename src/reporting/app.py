"""The reporting sink's process: checks reporting.yaml against the contracts, migrates the store,
records where data goes in the audit trail, then runs the sink. JSON logs, traces and error
reports as in every template; Prometheus metrics (and a health check) on REPORTING_PORT."""

from __future__ import annotations

import logging
import signal
import threading

from prometheus_client import start_http_server

from . import errors, logs, telemetry
from .audit import Actor, Auditor, make_sink
from .component import COMPONENT
from .config import Config, load
from .contracts import Contracts
from .pgstore import PgStore, migrate
from .settings import Settings
from .sink import Sink, flow


def auditor(s: Settings) -> Auditor:
    """The audit trail: required for payments and PHI unless REPORTING_AUDIT_REQUIRED says otherwise."""
    required = s.audit_required if s.audit_required is not None else s.data_class in ("payments", "phi")
    sink = make_sink(
        s.audit_sink,
        bootstrap=s.audit_kafka_bootstrap,
        topic=s.audit_topic,
        config=dict(s.audit_kafka_config),
        data_class=s.data_class,
    )
    return Auditor(
        sink,
        service=COMPONENT,
        product=s.product,
        environment=s.environment,
        data_class=s.data_class,
        required=required,
    )


def record_flow(a: Auditor, cfg: Config) -> None:
    """Which fields of which tables go where, each time the sink starts with a reporting.yaml."""
    a.record(
        "reporting.flow.started",
        actor=Actor(id=COMPONENT, type="service"),
        resource=("reporting_table", list(cfg.tables)),
        details=flow(cfg),
    )


def main() -> None:  # pragma: no cover - wiring; the parts are tested
    logs.setup(COMPONENT)
    telemetry.setup(COMPONENT)
    errors.start_errors()
    s = Settings()
    contracts = Contracts.load(s.contracts_dir)
    cfg = load(s.config_path, contracts, s.data_class)  # an invalid reporting.yaml stops it here
    start_http_server(s.port)
    if not s.database_url or not s.kafka_bootstrap:
        logging.warning("REPORTING_DATABASE_URL and REPORTING_KAFKA_BOOTSTRAP aren't set: idle")
        threading.Event().wait()
    migrate(s.database_url, cfg, contracts)
    record_flow(auditor(s), cfg)  # for payments and PHI, a sink that can't record it doesn't start
    kafka = {"bootstrap.servers": s.kafka_bootstrap, **s.kafka_config}
    sink = Sink(cfg, contracts, PgStore(s.database_url, cfg), kafka=kafka, token_key=s.token_key.encode())
    sink.start()
    done = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: done.set())
    done.wait()
    sink.stop.set()
    telemetry.shutdown()
    errors.flush()
