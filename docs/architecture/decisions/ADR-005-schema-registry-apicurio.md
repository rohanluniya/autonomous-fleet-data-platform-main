# ADR-005: Schema Registry — Apicurio Registry 3.3.5 (Apache-2.0)

- **Status:** Accepted
- **Scope:** Event schema contracts, Avro serialization, and compatibility enforcement

## Context
All fleet events require strict schema contracts with automated backward and forward compatibility validation. Confluent Schema Registry is widely known, but it is distributed under the Confluent Community License (source-available, not OSI-approved open source), violating Non-Negotiable Principle #1 (100% open-source software).

## Options Considered
1. **Apicurio Registry (`apicurio/apicurio-registry:3.3.5`)** — Apache-2.0, official multi-arch (`amd64`, `arm64`), supports Avro/Protobuf/JSON Schema/OpenAPI, backed by PostgreSQL, and implements the Confluent-compatible API at `/apis/ccompat/v7`.
2. **Karapace** — Apache-2.0 Python registry backed by Kafka `_schemas` topic. Lightweight, but narrower artifact governance than Apicurio.

## Decision
Pin **Apicurio Registry `3.3.5`** (`apicurio/apicurio-registry:3.3.5`, Apache-2.0) backed by PostgreSQL for Docker/Kubernetes deployments. Pair it with version-controlled Avro schemas in `schemas/avro/` and an automated Python `fastavro` schema-compatibility test suite (`tests/contracts/test_avro_contracts.py`) so compatibility regressions are caught in CI even without a running registry container.

## Consequences
- Strict Apache-2.0 licensing compliance with zero proprietary restrictions.
- Full wire compatibility with standard Kafka Avro clients via `/apis/ccompat/v7`.
