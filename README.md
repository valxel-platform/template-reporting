# Reporting template (Python, Kafka, Postgres)

A product's reporting component: approved data contracts into reporting tables and filtered topics,
with each field's class deciding what may go where (the design "Datasets, contracts and reporting").
Valxel builds one per product during the build, from the HLD's Reporting section; people approve
`reporting.yaml` at G4 with the contracts it reads.

| What | How | Why |
| --- | --- | --- |
| Inputs | Each table reads one dataset's topic (its approved ODCS contract, in `contracts/`, written by the SDLC tool and shipped in the image). Unknown properties are dropped, the rest checked; an event that breaks its contract is parked on `<topic>.dlt`, naming the field and never the value | Nothing unapproved reaches a report, and nothing reads GitHub at run time |
| Field rules | `fields` are kept, `tokenize` become a keyed HMAC (`REPORTING_TOKEN_KEY`), `generalize` cuts a value (`year`, `zip3`) for de-identified destinations. `secret` never leaves; a de-identified destination never gets a `pii` or `phi` value | Reports can count and join without holding people's data where they don't need it |
| Store | `type: postgres`: one current-state table per reporting table, typed from the contract, rows that only move forward (Kafka offset), deletions applied. Row-level security by `tenant`: a report login (a member of `reporting_reader`) sees a row only when `reporting-admin grant-tenant <login> <tenant>` gave it that tenant. The database decides from the login itself, so nothing a report's SQL sets can widen it | Clinic staff see only their clinic, in the product's own database |
| Lake and exports | `iceberg`, `snowflake`, `bigquery`, `s3`, `jdbc`: written by Kafka Connect (Iceberg sink, warehouse connectors) from the filtered topics `<topic_prefix>.<table>.v1` and `.deid.v1`, never from the source | Connect can only send what reporting.yaml allows |
| Destinations | At least one. A PHI product keeps a store or a lake in its own account, and its exports are de-identified. Each output reads with its own consumer group (`<name>.<table>.<output>.g<generation>`), so a slow one never holds up another | The design's decisions 8 and 9 |
| Reconciliation | Every event is counted per dataset (`reporting_dataset_events_total`) and summed in a `valxel.datasets {...}` line a minute, which the SDLC tool reconciles in staging | Nothing published goes missing on its way to a report |
| Audit trail | When the sink starts, `reporting.flow.started` records which fields of which tables go to which destinations: kept, tokenized or generalized, field names only. `rebuild` and `expire` record what they changed and who ran them (`REPORTING_ACTOR`). Records go to the product's audit topic as `valxel.audit.v1`. For payments and PHI they're required (the sink doesn't start without one) and Kafka must use TLS | Anyone can see where a product's data went, and who changed the reporting store |
| Observability | JSON log lines with trace ids. Each applied event is a `reporting.apply` span in the producer's trace (from its `traceparent` or `ce_traceparent` header), with its Postgres statements beneath it. Errors go to Sentry or GlitchTip (`SENTRY_DSN`), scrubbed and tagged with the component | A record can be followed from the request that changed it to the report it lands in |
| Rebuild and retention | `reporting-admin rebuild <table>` empties a table and reads its topic from the start with new groups; `reporting-admin expire` (daily) removes rows untouched for `retention_days` | Reports never keep data longer than the source may |

## Run it

```bash
uv sync
uv run reporting-admin check          # reporting.yaml against contracts/ and REPORTING_DATA_CLASS
REPORTING_TEST_DATABASE_URL=postgresql://... uv run pytest
REPORTING_DATABASE_URL=... REPORTING_KAFKA_BOOTSTRAP=... REPORTING_TOKEN_KEY=... uv run reporting-sink
```

Settings (`REPORTING_`): `CONFIG_PATH`, `CONTRACTS_DIR`, `DATA_CLASS`, `DATABASE_URL` (the store's
owner), `KAFKA_BOOTSTRAP`, `KAFKA_CONFIG` (JSON: the platform's kafka-user gives SASL settings),
`TOKEN_KEY`, `PORT` (`/metrics`, `/healthz`), `PRODUCT`, `ENVIRONMENT`, `AUDIT_SINK` (`kafka` in
environments, with `AUDIT_KAFKA_BOOTSTRAP`, `AUDIT_TOPIC` and `AUDIT_KAFKA_CONFIG`), `AUDIT_REQUIRED`,
`ACTOR` (who runs reporting-admin). Also `SENTRY_DSN` and `OTEL_EXPORTER_OTLP_ENDPOINT`. Without a
database and Kafka the process stays idle, serving metrics.

Where each of the platform's capabilities lives (security, observability, audit, business
metrics) is listed in `.valxel/capabilities.yaml`, which the platform's daily check reads
(agentic-sdlc `config/template-contract.yaml`). `supply-chain-pins` keeps base images pinned to
their digests.

## Ports

`/metrics` and `/healthz` are on `REPORTING_PORT`, 8081: the management port, as in every template.
The sink serves no API. The product's mesh lets the platform's metrics collector reach that port
and no other.

## Not here

- **Audit of report queries.** Reports read Postgres directly (Metabase, Superset), so audit them
  in the database: pgaudit on `reporting_reader` logins, shipped to the product's audit topic.
- **The lake's expiry.** Rows in the lake are kept until an Iceberg maintenance job removes those
  past the table's `retention_days`; that job is the platform's (with the lake bucket), not this
  repo's.
- **Kafka Connect itself.** `reporting-admin connectors` renders the connectors (Strimzi
  `KafkaConnector`s, one per destination and table, each reading only its filtered topic, with
  credentials from Secrets). The Connect cluster, its plugins, the lake bucket and Glue database,
  and Connect's Kafka identity come from the platform.
