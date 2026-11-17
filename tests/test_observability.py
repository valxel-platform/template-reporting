"""Traces, error reports, JSON logs and the audit trail of the reporting component. Synthetic data."""

from __future__ import annotations

import io
import json
import logging
from typing import Any

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from reporting import admin, errors, logs, telemetry
from reporting.app import auditor, record_flow
from reporting.audit import Auditor, MemorySink
from reporting.settings import Settings
from reporting.sink import Output
from reporting.store import MemoryStore

from .conftest import ROOT, Msg, Producer, config, note
from .test_sink import runner

SPANS = InMemorySpanExporter()
telemetry.setup("template-reporting", exporter=SPANS)
TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"


class Traced(Msg):
    def headers(self) -> list[tuple[str, bytes]]:
        return [("ce_traceparent", f"00-{TRACE}-00f067aa0ba902b7-01".encode()), ("other", b"x")]


@pytest.fixture(autouse=True)
def fresh() -> None:
    SPANS.clear()


def test_each_event_applied_is_a_span_in_its_producers_trace() -> None:
    store = MemoryStore()
    r = runner(config(), Output("store"), [Traced(note("n-1"), 1), Msg(b"not json", 2)], store, Producer())
    r.step()
    r.step()
    applied, skipped = SPANS.get_finished_spans()
    assert applied.name == "reporting.apply" and f"{applied.context.trace_id:032x}" == TRACE
    assert applied.attributes["reporting.outcome"] == "applied" and applied.attributes["reporting.table"] == "notes"
    assert skipped.attributes["reporting.outcome"] == "skipped" and f"{skipped.context.trace_id:032x}" != TRACE


def test_where_data_goes_is_audited_when_the_sink_starts_field_names_only() -> None:
    sink = MemorySink()
    record_flow(Auditor(sink, service="template-reporting", product="emr"), config())
    [event] = sink.events
    data = event["data"]
    assert data["action"] == "reporting.flow.started" and data["actor"]["type"] == "service"
    notes = data["details"]["tables"]["notes"]
    assert "note_id" in notes["fields"] and notes["dataset"] and set(data["details"]["destinations"])
    assert "p-77" not in json.dumps(event)  # never a value


def test_the_audit_trail_is_required_for_payments_and_phi_and_travels_over_tls() -> None:
    assert auditor(Settings(data_class="phi")).required and not auditor(Settings(data_class="general")).required
    assert not auditor(Settings(data_class="phi", audit_required=False)).required
    with pytest.raises(RuntimeError, match="TLS"):
        auditor(Settings(data_class="phi", audit_sink="kafka", audit_kafka_bootstrap="k:9092"))


def test_admin_changes_to_the_store_are_audited_with_who_ran_them(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REPORTING_CONFIG_PATH", str(ROOT / "reporting.yaml"))
    monkeypatch.setenv("REPORTING_CONTRACTS_DIR", str(ROOT / "contracts"))
    monkeypatch.setenv("REPORTING_ACTOR", "ops-lead-3")
    sink, store = MemorySink(), MemoryStore(now=100 * 86400)
    store.upsert("notes", "n-1", {"note_id": "n-1"}, 1)
    store.now = 4000 * 86400
    a = Auditor(sink, service="template-reporting")
    assert admin.main(["expire"], store=store, out=io.StringIO(), audit=a) == 0
    assert admin.main(["rebuild", "notes"], store=store, out=io.StringIO(), audit=a) == 0
    expired, rebuilt = (e["data"] for e in sink.events)
    assert expired["action"] == "reporting.rows.expired" and expired["details"]["removed"] == 1
    assert expired["actor"] == {"id": "ops-lead-3", "type": "user", "client": ""}
    assert rebuilt["action"] == "reporting.table.rebuilt" and rebuilt["resource"] == {
        "type": "reporting_table",
        "ids": ["notes"],
    }
    monkeypatch.delenv("REPORTING_ACTOR")
    assert admin.main(["rebuild", "notes"], store=store, out=io.StringIO(), audit=a) == 0
    assert sink.events[-1]["data"]["actor"]["type"] == "service"


def test_log_lines_are_json_with_the_trace_and_the_exception() -> None:
    out = io.StringIO()
    handler = logging.StreamHandler(out)
    handler.setFormatter(logs.JsonFormatter("template-reporting"))
    log = logging.getLogger("reporting.test")
    log.addHandler(handler)
    try:
        with telemetry.tracer().start_as_current_span("x"):
            try:
                raise ValueError("boom")
            except ValueError:
                log.exception("couldn't apply", extra={"fields": {"table": "notes"}})
    finally:
        log.removeHandler(handler)
    line: dict[str, Any] = json.loads(out.getvalue())
    assert line["trace.id"] and line["table"] == "notes" and "boom" in line["error.stack_trace"]
    logs.setup("template-reporting", "INFO")
    assert isinstance(logging.getLogger().handlers[0].formatter, logs.JsonFormatter)


def test_error_reports_carry_the_component_and_trace_and_nothing_personal() -> None:
    import sentry_sdk

    with telemetry.tracer().start_as_current_span("x"):
        event = errors.scrub(
            {
                "user": {"id": "u"},
                "request": {"url": "/x?a=1"},
                "message": "m",
                "exception": {"values": [{"value": "p-77 broke", "stacktrace": {"frames": [{"vars": {}}]}}]},
                "logentry": {"message": "bad %s", "params": ["p-77"]},
            }
        )
    assert "user" not in event and "request" not in event and "value" not in event["exception"]["values"][0]
    assert event["logentry"] == {"message": "bad %s"} and event["tags"]["trace_id"]
    assert errors.start_errors({}) is False
    assert errors.start_errors({"SENTRY_DSN": "https://k@sentry.example.com/1"})
    assert sentry_sdk.get_isolation_scope()._tags["component"] == "template-reporting"
    errors.flush()
    sentry_sdk.init()
    telemetry.shutdown()
