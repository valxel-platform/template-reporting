"""The sink: each reporting table's source topic into its outputs.

For every table, one runner per output, each with its own consumer group
(<name>.<table>.<output>.g<generation>), so a slow output never holds up another:

  store   the Postgres store (a postgres destination): upsert by key, delete on a `.deleted` event
  topic   <topic_prefix>.<table>.v1: the table's rows, for Kafka Connect (the lake, identified exports)
  deid    <topic_prefix>.<table>.deid.v1: the same rows de-identified, for de-identified destinations

Kafka Connect reads the filtered topics, never the source: what it can send anywhere is only what
reporting.yaml allows. An event is checked against its data contract first; one that breaks it is
parked on <source topic>.dlt by the table's first runner (the others skip it), naming the field and
never its value. `reporting-admin rebuild <table>` raises the table's generation: its runners start
again from the beginning of the topic with new groups.
"""

from __future__ import annotations

import json
import logging
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .config import Config, Table
from .contracts import ContractError, Contracts, count, summary_line
from .rules import row
from .store import Store

log = logging.getLogger("reporting.sink")

Factory = Callable[[dict[str, Any]], Any]


@dataclass(frozen=True)
class Output:
    kind: str  # store | topic | deid
    deidentified: bool = False


def outputs(cfg: Config, table: str) -> list[Output]:
    """What a table feeds, from the destinations that list it, the store first."""
    dests = [d for d in cfg.destinations.values() if table in d.tables]
    out = []
    if any(d.written_here for d in dests):
        out.append(Output("store"))
    if any(not d.written_here and not d.deidentified for d in dests):
        out.append(Output("topic"))
    if any(d.deidentified for d in dests):
        out.append(Output("deid", deidentified=True))
    return out


class Runner(threading.Thread):
    def __init__(
        self,
        cfg: Config,
        contracts: Contracts,
        table: Table,
        output: Output,
        store: Store,
        *,
        kafka: dict[str, Any],
        generation: int,
        token_key: bytes,
        primary: bool,
        consumer_factory: Factory,
        producer_factory: Factory,
    ) -> None:
        super().__init__(daemon=True, name=f"reporting-{table.name}-{output.kind}")
        self.cfg, self.table, self.output, self.store = cfg, table, output, store
        contract = contracts.for_dataset(table.dataset)
        if contract is None:  # config.parse refuses this; a guard for direct construction
            raise ValueError(f"{table.dataset} has no contract")
        self.contract, self.primary, self.token_key = contract, primary, token_key
        self.group = f"{cfg.name}.{table.name}.{output.kind}.g{generation}"
        self.consumer = consumer_factory(
            {**kafka, "group.id": self.group, "enable.auto.commit": False, "auto.offset.reset": "earliest"}
        )
        self.kafka, self.producer_factory = kafka, producer_factory
        self.producer: Any = None
        self.stop = threading.Event()

    def run(self) -> None:
        self.consumer.subscribe([self.contract.topic])
        try:
            while not self.stop.is_set():
                self.step()
        finally:
            self.consumer.close()

    def _produce(
        self, topic: str, key: str, value: bytes | None, headers: list[tuple[str, bytes]] | None = None
    ) -> None:
        if self.producer is None:
            self.producer = self.producer_factory(self.kafka)
        self.producer.produce(topic, key=key.encode(), value=value, headers=headers or [])
        self.producer.flush(10)

    def step(self) -> None:
        msg = self.consumer.poll(1.0)
        if msg is None:
            return
        if msg.error():
            log.warning("%s: Kafka: %s", self.group, msg.error())
            return
        dataset = self.contract.dataset
        try:
            self.apply(msg)
            if self.primary:
                count(dataset, "applied")
        except ContractError as exc:
            if self.primary:  # park it once, naming the field and never its value
                self._produce(
                    f"{self.contract.topic}.dlt",
                    (msg.key() or b"").decode() or "-",
                    msg.value(),
                    [
                        ("valxel-dataset", exc.dataset.encode()),
                        ("valxel-field", exc.field.encode()),
                        ("valxel-rule", exc.rule.encode()),
                    ],
                )
                count(dataset, "parked")
                log.error(
                    "reporting.event_parked dataset=%s field=%s breaks the data contract (%s) offset=%s",
                    exc.dataset,
                    exc.field,
                    exc.rule,
                    msg.offset(),
                )
        except (ValueError, KeyError, TypeError) as exc:  # unreadable: skipped, never retried forever
            if self.primary:
                count(dataset, "skipped")
            log.error("%s: skipped an unreadable event at offset %s (%s)", self.group, msg.offset(), type(exc).__name__)
        except Exception:  # the store or Kafka: the same record again shortly
            log.exception("%s: couldn't apply the event at offset %s; retrying", self.group, msg.offset())
            self.consumer.seek(_partition(msg))
            self.stop.wait(5)
            return
        self.consumer.commit(message=msg, asynchronous=False)

    def apply(self, msg: Any) -> None:
        event = json.loads(msg.value())
        data = event.get("data") or {}
        if not isinstance(data, dict):
            raise ValueError("an event's data must be an object")
        t = self.table
        if re.search(r"\.deleted(\.v\d+)?$", str(event.get("type") or "")):
            key = str(data.get(t.key) or str(event.get("subject") or "").rsplit("/", 1)[-1])
            if not key:
                raise ValueError("a deletion without the record's key")
            if self.output.kind == "store":
                self.store.delete(t.name, key)
            else:  # a tombstone: compacted topics and Connect's sinks drop the record
                self._produce(self.cfg.topic(t.name, self.output.deidentified), key, None)
            return
        checked = self.contract.check(data)
        r = row(t, checked, self.token_key, deidentified=self.output.deidentified)
        key = str(r[t.key])
        if self.output.kind == "store":
            self.store.upsert(t.name, key, r, msg.offset(), msg.partition())
        else:
            self._produce(self.cfg.topic(t.name, self.output.deidentified), key, json.dumps(r, default=str).encode())


class Sink:
    """Starts every table's runners, and starts them again with new groups after a rebuild."""

    def __init__(
        self,
        cfg: Config,
        contracts: Contracts,
        store: Store,
        *,
        kafka: dict[str, Any],
        token_key: bytes,
        consumer_factory: Factory | None = None,
        producer_factory: Factory | None = None,
    ) -> None:
        self.cfg, self.contracts, self.store, self.kafka, self.token_key = cfg, contracts, store, kafka, token_key
        self.consumer_factory = consumer_factory or _consumer
        self.producer_factory = producer_factory or _producer
        self.runners: dict[str, list[Runner]] = {}
        self.generations: dict[str, int] = {}
        self.stop = threading.Event()

    def refresh(self, start: bool = True) -> None:
        for name, table in self.cfg.tables.items():
            gen = self.store.generation(name)
            if self.generations.get(name) == gen:
                continue
            for r in self.runners.pop(name, []):
                r.stop.set()
            made = [
                Runner(
                    self.cfg,
                    self.contracts,
                    table,
                    o,
                    self.store,
                    kafka=self.kafka,
                    generation=gen,
                    token_key=self.token_key,
                    primary=i == 0,
                    consumer_factory=self.consumer_factory,
                    producer_factory=self.producer_factory,
                )
                for i, o in enumerate(outputs(self.cfg, name))
            ]
            self.runners[name], self.generations[name] = made, gen
            if start:
                for r in made:
                    r.start()

    def start(self) -> threading.Thread:
        def loop() -> None:
            beat = 0
            while not self.stop.is_set():
                try:
                    self.refresh()
                except Exception:
                    log.exception("reporting: couldn't read the tables' generations; trying again")
                beat += 1
                if beat % 2 == 0:  # once a minute: the line the platform reconciles in staging
                    line = summary_line()
                    if line:
                        log.info(line)
                self.stop.wait(30)

        t = threading.Thread(target=loop, daemon=True, name="reporting-supervisor")
        t.start()
        return t


def _partition(msg: Any) -> Any:
    from confluent_kafka import TopicPartition

    return TopicPartition(msg.topic(), msg.partition(), msg.offset())


def _consumer(conf: dict[str, Any]) -> Any:
    from confluent_kafka import Consumer

    return Consumer(conf)


def _producer(conf: dict[str, Any]) -> Any:
    from confluent_kafka import Producer

    return Producer({**conf, "enable.idempotence": True, "acks": "all"})
