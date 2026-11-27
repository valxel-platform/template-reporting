"""The sink: checked, transformed events into the store and the filtered topics. Synthetic data."""

from __future__ import annotations

import json
from typing import Any

from reporting.contracts import EVENTS, summary_line
from reporting.rules import generalize, row, token
from reporting.sink import Output, Runner, Sink, outputs
from reporting.store import MemoryStore

from .conftest import CONTRACTS, Consumer, Msg, Producer, config, note

KEY = b"synthetic-key"


def runner(
    cfg: Any,
    output: Output,
    messages: list[Any],
    store: MemoryStore,
    producer: Producer,
    primary: bool = True,
    made: list[Consumer] | None = None,
) -> Runner:
    made = made if made is not None else []
    return Runner(
        cfg,
        CONTRACTS,
        cfg.tables["notes"],
        output,
        store,
        kafka={},
        generation=1,
        token_key=KEY,
        primary=primary,
        consumer_factory=lambda conf: made.append(Consumer(conf, messages)) or made[-1],
        producer_factory=lambda conf: producer,
    )


def test_each_table_feeds_its_store_and_its_filtered_topics_with_their_own_groups() -> None:
    cfg = config()
    assert outputs(cfg, "notes") == [Output("store"), Output("topic"), Output("deid", True)]
    made: list[Consumer] = []
    r = runner(cfg, Output("deid", True), [], MemoryStore(), Producer(), made=made)
    assert made[0].conf["group.id"] == "template-reporting.notes.deid.g1"  # starts with the component's name
    r.stop.set()
    r.run()
    assert made[0].topics == ["emr.notes.v1"] and made[0].closed


def test_the_store_keeps_the_tables_fields_tokens_and_only_moves_forward() -> None:
    store, cfg = MemoryStore(tenants={"notes": "clinic_id"}), config()
    newer, older = note("n-1", kind="discharge"), note("n-1")
    msgs = [
        Msg(newer, 5),
        Msg(older, 3),
        Msg(note("n-2", clinic_id="south-clinic"), 6),
        Msg({**note("n-2"), "type": "valxel.note.deleted.v1", "data": {}}, 7),
    ]
    r = runner(cfg, Output("store"), msgs, store, Producer())
    for _ in range(4):
        r.step()
    [kept] = store.rows("notes")
    assert kept["kind"] == "discharge" and "body" not in kept  # a replayed older event never wins
    assert kept["patient_id"] == token(KEY, "p-77") != "p-77"
    assert kept["signed_at"] == "2026-03-04T10:00:00Z"  # identified: the date as it is
    assert store.rows("notes", tenant="north-clinic") == [kept] and store.rows("notes", tenant="south-clinic") == []


def test_filtered_topics_carry_rows_and_de_identified_ones_generalize() -> None:
    cfg, producer = config(), Producer()
    r = runner(
        cfg,
        Output("deid", True),
        [Msg(note("n-1"), 1), Msg({**note("n-1"), "type": "valxel.note.deleted.v1"}, 2)],
        MemoryStore(),
        producer,
    )
    r.step()
    r.step()
    (topic, key, value, _), (_, _, tombstone, _) = producer.sent
    assert topic == "emr.reporting.notes.deid.v1" and key == b"n-1" and tombstone is None
    assert json.loads(value or b"{}") == {
        "note_id": "n-1",
        "kind": "progress",
        "signed_at": 2026,
        "clinic_id": "north-clinic",
    }  # no token: it's derived from the patient's id, and no Expert Determination is recorded


def test_a_de_identified_copy_carries_tokens_only_with_an_expert_determination() -> None:
    import dataclasses

    cfg, producer = dataclasses.replace(config(), expert_determination="ED-2027-01"), Producer()
    r = runner(cfg, Output("deid", True), [Msg(note("n-1"), 1)], MemoryStore(), producer)
    r.step()
    assert json.loads(producer.sent[0][2] or b"{}")["patient_id"] == token(KEY, "p-77")


def test_an_event_that_breaks_its_contract_is_parked_once_and_counted() -> None:
    cfg, producer, store = config(), Producer(), MemoryStore()
    summary_line()
    bad = note("n-9", kind="memo-with-a-secret")
    made: list[Consumer] = []
    first = runner(
        cfg,
        Output("store"),
        [Msg(bad, 1), Msg(b"not json", 2), None, Msg(None, 3, err="broker")],
        store,
        producer,
        made=made,
    )
    for _ in range(4):
        first.step()
    other = runner(cfg, Output("topic"), [Msg(bad, 1)], store, producer, primary=False)
    other.step()
    [(topic, _, _value, headers)] = producer.sent  # parked by the first runner only
    assert topic == "emr.notes.v1.dlt" and headers["valxel-field"] == b"kind" and headers["valxel-rule"] == b"enum"
    assert b"memo-with-a-secret" not in b"".join(headers.values())
    assert made[0].committed == [1, 2]
    assert summary_line() == 'valxel.datasets {"emr.notes":{"parked":1,"skipped":1}}'
    assert EVENTS.labels("emr.notes", "parked")._value.get() >= 1


def test_a_store_failure_is_retried_and_a_rebuild_starts_new_groups() -> None:
    class Down(MemoryStore):
        def upsert(self, *a: Any, **k: Any) -> bool:
            raise RuntimeError("database down")

    made: list[Consumer] = []
    r = runner(config(), Output("store"), [Msg(note(), 4)], Down(), Producer(), made=made)
    r.stop.set()  # don't wait out the back-off in a test
    r.step()
    assert made[0].committed == [] and made[0].seeks[0].offset == 4
    store = MemoryStore()
    sink = Sink(
        config(),
        CONTRACTS,
        store,
        kafka={},
        token_key=KEY,
        consumer_factory=lambda conf: Consumer(conf, []),
        producer_factory=lambda conf: Producer(),
    )
    sink.refresh(start=False)
    assert [r.group for r in sink.runners["notes"]] == [
        "template-reporting.notes.store.g1",
        "template-reporting.notes.topic.g1",
        "template-reporting.notes.deid.g1",
    ]
    sink.refresh(start=False)  # no change: the same runners
    assert store.rebuild("notes") == 2
    sink.refresh(start=False)
    assert sink.runners["notes"][0].group == "template-reporting.notes.store.g2"


def test_rules() -> None:
    t = config().tables["notes"]
    assert generalize("2026-03-04", "year") == 2026 and generalize("75024-1234", "zip3") == "750"
    assert generalize(None, "year") is None and generalize("x", "year") is None and generalize("12", "zip3") is None
    assert row(t, {"note_id": "n", "patient_id": None}, KEY)["patient_id"] is None
    for bad in ((b"", "x", "token"), (KEY, "x", "nope")):
        try:
            token(bad[0], bad[1]) if bad[2] == "token" else generalize(bad[1], bad[2])
        except ValueError:
            continue
        raise AssertionError(bad)
