"""The reporting sink's process: checks reporting.yaml against the contracts, migrates the store, then
runs the sink. Prometheus metrics (and a health check) on REPORTING_PORT."""

from __future__ import annotations

import logging
import signal
import threading

from prometheus_client import start_http_server

from .config import load
from .contracts import Contracts
from .pgstore import PgStore, migrate
from .settings import Settings
from .sink import Sink


def main() -> None:  # pragma: no cover - wiring; the parts are tested
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    s = Settings()
    contracts = Contracts.load(s.contracts_dir)
    cfg = load(s.config_path, contracts, s.data_class)  # an invalid reporting.yaml stops it here
    start_http_server(s.port)
    if not s.database_url or not s.kafka_bootstrap:
        logging.warning("REPORTING_DATABASE_URL and REPORTING_KAFKA_BOOTSTRAP aren't set: idle")
        threading.Event().wait()
    migrate(s.database_url, cfg, contracts)
    kafka = {"bootstrap.servers": s.kafka_bootstrap, **s.kafka_config}
    sink = Sink(cfg, contracts, PgStore(s.database_url, cfg), kafka=kafka, token_key=s.token_key.encode())
    sink.start()
    done = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: done.set())
    done.wait()
    sink.stop.set()
