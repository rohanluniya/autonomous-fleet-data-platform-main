"""Avro schema contract registry, wire framing, and compatibility testing utilities."""

from __future__ import annotations

import io
import json
import struct
from pathlib import Path
from typing import Any

import fastavro
import requests

MAGIC_BYTE = 0x00

TOPIC_TO_SCHEMA: dict[str, tuple[str, int]] = {
    "fleet.telemetry.raw": ("telemetry_event.avsc", 1),
    "fleet.gps.raw": ("gps_event.avsc", 2),
    "fleet.diagnostics.raw": ("diagnostics_event.avsc", 3),
    "fleet.trips.raw": ("trip_event.avsc", 4),
    "fleet.autonomy.raw": ("autonomy_event.avsc", 5),
    "fleet.anomalies.detected": ("anomaly_event.avsc", 6),
    "fleet.vehicle-state": ("vehicle_state_event.avsc", 7),
    "fleet.cdc.vehicle-registry": ("cdc_vehicle_registry.avsc", 8),
    "fleet.telemetry.dlq": ("dlq_event.avsc", 9),
}


class SchemaValidationError(ValueError):
    """Raised when an event violates its registered Avro contract."""


class AvroContractRegistry:
    """Loads Avro schemas from disk, encodes/decodes Confluent/Apicurio wire format,
    and checks backward/forward schema compatibility.
    """

    def __init__(
        self,
        schema_dir: Path | None = None,
        registry_url: str | None = None,
    ) -> None:
        self.schema_dir = (
            schema_dir or Path(__file__).resolve().parent.parent.parent / "schemas" / "avro"
        )
        self.registry_url = registry_url.rstrip("/") if registry_url else None
        self._raw_schemas: dict[int, dict[str, Any]] = {}
        self._parsed_schemas: dict[int, Any] = {}
        self._topic_to_id: dict[str, int] = {}
        self._load_all()

    def _load_all(self) -> None:
        for topic, (filename, schema_id) in TOPIC_TO_SCHEMA.items():
            path = self.schema_dir / filename
            raw = json.loads(path.read_text(encoding="utf-8"))
            parsed = fastavro.parse_schema(raw)
            self._raw_schemas[schema_id] = raw
            self._parsed_schemas[schema_id] = parsed
            self._topic_to_id[topic] = schema_id

    def get_schema_id(self, topic: str) -> int:
        if topic not in self._topic_to_id:
            raise KeyError(f"Unknown topic contract: {topic}")
        return self._topic_to_id[topic]

    def get_parsed_schema(self, topic: str) -> Any:
        return self._parsed_schemas[self.get_schema_id(topic)]

    def get_raw_schema(self, topic: str) -> dict[str, Any]:
        return self._raw_schemas[self.get_schema_id(topic)]

    def serialize(self, topic: str, record: dict[str, Any]) -> bytes:
        schema_id = self.get_schema_id(topic)
        parsed = self._parsed_schemas[schema_id]
        buf = io.BytesIO()
        buf.write(struct.pack(">BI", MAGIC_BYTE, schema_id))
        try:
            fastavro.schemaless_writer(buf, parsed, record)
        except Exception as exc:
            raise SchemaValidationError(
                f"Record failed Avro serialization for topic '{topic}': {exc}"
            ) from exc
        return buf.getvalue()

    def deserialize(
        self,
        payload: bytes,
        *,
        expected_topic: str | None = None,
        reader_schema: Any | None = None,
    ) -> tuple[int, dict[str, Any]]:
        if len(payload) < 5 or payload[0] != MAGIC_BYTE:
            raise SchemaValidationError(
                "Invalid Avro wire framing: missing 5-byte Confluent/Apicurio header"
            )
        schema_id = struct.unpack(">I", payload[1:5])[0]
        if schema_id not in self._parsed_schemas:
            raise SchemaValidationError(f"Unknown schema_id={schema_id} in wire header")
        if expected_topic is not None:
            expected_id = self.get_schema_id(expected_topic)
            if schema_id != expected_id:
                raise SchemaValidationError(
                    f"Schema ID mismatch for '{expected_topic}': got {schema_id}, expected {expected_id}"
                )
        writer_schema = self._parsed_schemas[schema_id]
        buf = io.BytesIO(payload[5:])
        try:
            decoded = fastavro.schemaless_reader(buf, writer_schema, reader_schema)
        except Exception as exc:
            raise SchemaValidationError(f"Avro binary decode failed: {exc}") from exc
        if not isinstance(decoded, dict):
            raise SchemaValidationError("Decoded Avro payload is not a record dict")
        return schema_id, decoded

    def check_compatibility(
        self,
        writer_schema_dict: dict[str, Any],
        reader_schema_dict: dict[str, Any],
        sample_record: dict[str, Any],
    ) -> tuple[bool, str]:
        """Verify reader_schema can deserialize a record written with writer_schema."""
        try:
            writer = fastavro.parse_schema(writer_schema_dict)
            reader = fastavro.parse_schema(reader_schema_dict)
            buf = io.BytesIO()
            fastavro.schemaless_writer(buf, writer, sample_record)
            buf.seek(0)
            decoded = fastavro.schemaless_reader(buf, writer, reader)
            if not isinstance(decoded, dict):
                return False, "Decoded result is not a dict"
            return True, "compatible"
        except Exception as exc:
            return False, str(exc)

    def register_all_remote(self) -> dict[str, int]:
        """Register all Avro schemas against Apicurio Registry Confluent-compat endpoint."""
        if not self.registry_url:
            raise RuntimeError("SCHEMA_REGISTRY_URL is not configured")
        registered: dict[str, int] = {}
        for topic, schema_id in self._topic_to_id.items():
            subject = f"{topic}-value"
            raw = self._raw_schemas[schema_id]
            resp = requests.post(
                f"{self.registry_url}/subjects/{subject}/versions",
                json={"schema": json.dumps(raw), "schemaType": "AVRO"},
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
                timeout=10,
            )
            resp.raise_for_status()
            registered[subject] = int(resp.json()["id"])
        return registered
