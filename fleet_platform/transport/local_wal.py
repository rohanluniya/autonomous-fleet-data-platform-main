"""Disk-backed partitioned append-only Write-Ahead Log (local_wal).

Documented zero-Docker local fallback transport used on hosts without Docker or JVM.
Implements the exact same Murmur2(vehicle_id) partition assignment, binary framing,
consumer-group offset commits, lag calculation, DLQ routing, and bounded replay
semantics as our Kafka 4.3.2 transport adapter.
"""

from __future__ import annotations

import json
import struct
import threading
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from fleet_platform.transport.base import (
    TOPIC_SPECS,
    EventBus,
    TransportRecord,
    murmur2_partition,
)

# Binary record header: offset (Q), timestamp_ms (q), key_len (I), headers_len (I), value_len (I)
_FRAME_STRUCT = struct.Struct(">QqIII")


class BrokerUnavailableError(RuntimeError):
    """Raised when simulated broker outage is active during fault-injection tests."""


class LocalWalEventBus(EventBus):
    """Partitioned file-backed event log with durable offsets and fault-injection hooks."""

    backend_name = "local_wal"

    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir.resolve()
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self._offsets_dir = self.root_dir / "_offsets"
        self._offsets_dir.mkdir(parents=True, exist_ok=True)
        self._meta_dir = self.root_dir / "_meta"
        self._meta_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._broker_online = True
        self._partition_counts: dict[str, int] = {}
        self._next_offsets: dict[tuple[str, int], int] = {}
        self.ensure_topics()

    def set_broker_online(self, online: bool) -> None:
        """Controlled fault-injection hook simulating broker restart or crash."""
        with self._lock:
            self._broker_online = online

    def _assert_online(self) -> None:
        if not self._broker_online:
            raise BrokerUnavailableError("Local WAL broker simulated offline (fault injection)")

    def ensure_topics(self) -> None:
        with self._lock:
            for topic, spec in TOPIC_SPECS.items():
                tdir = self.root_dir / topic
                tdir.mkdir(parents=True, exist_ok=True)
                self._partition_counts[topic] = spec.partitions
                for p in range(spec.partitions):
                    pfile = self._partition_file(topic, p)
                    if not pfile.exists():
                        pfile.touch()
                    self._next_offsets[(topic, p)] = self._count_records_in_file(pfile)

    def _partition_file(self, topic: str, partition: int) -> Path:
        return self.root_dir / topic / f"partition-{partition:03d}.wal"

    def _offset_file(self, consumer_group: str, topic: str) -> Path:
        gdir = self._offsets_dir / consumer_group
        gdir.mkdir(parents=True, exist_ok=True)
        return gdir / f"{topic}.json"

    def _count_records_in_file(self, path: Path) -> int:
        if not path.exists() or path.stat().st_size == 0:
            return 0
        count = 0
        with path.open("rb") as fh:
            while True:
                hdr = fh.read(_FRAME_STRUCT.size)
                if not hdr or len(hdr) < _FRAME_STRUCT.size:
                    break
                _, _, klen, hlen, vlen = _FRAME_STRUCT.unpack(hdr)
                fh.seek(klen + hlen + vlen, 1)
                count += 1
        return count

    def _load_group_offsets(self, consumer_group: str, topic: str) -> dict[int, int]:
        ofile = self._offset_file(consumer_group, topic)
        if not ofile.exists():
            return {}
        raw = json.loads(ofile.read_text(encoding="utf-8"))
        return {int(k): int(v) for k, v in raw.items()}

    def _save_group_offsets(self, consumer_group: str, topic: str, offsets: dict[int, int]) -> None:
        ofile = self._offset_file(consumer_group, topic)
        tmp = ofile.with_suffix(".tmp")
        tmp.write_text(
            json.dumps({str(k): int(v) for k, v in sorted(offsets.items())}, indent=2),
            encoding="utf-8",
        )
        tmp.replace(ofile)

    def produce(
        self,
        topic: str,
        key: str,
        value: bytes,
        *,
        timestamp_ms: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> TransportRecord:
        with self._lock:
            self._assert_online()
            if topic not in self._partition_counts:
                raise KeyError(f"Unknown topic '{topic}'")
            num_parts = self._partition_counts[topic]
            part = murmur2_partition(key, num_parts)
            offset = self._next_offsets[(topic, part)]
            ts_ms = timestamp_ms if timestamp_ms is not None else int(time.time() * 1000)
            hdr_dict = headers or {}
            k_bytes = key.encode("utf-8")
            h_bytes = json.dumps(hdr_dict, separators=(",", ":")).encode("utf-8")
            frame = (
                _FRAME_STRUCT.pack(offset, ts_ms, len(k_bytes), len(h_bytes), len(value))
                + k_bytes
                + h_bytes
                + value
            )
            pfile = self._partition_file(topic, part)
            with pfile.open("ab") as fh:
                fh.write(frame)
            self._next_offsets[(topic, part)] = offset + 1
            return TransportRecord(
                topic=topic,
                partition=part,
                offset=offset,
                key=key,
                value=value,
                timestamp_ms=ts_ms,
                headers=hdr_dict,
            )

    def produce_batch(
        self,
        topic: str,
        items: Sequence[tuple[str, bytes, int | None, dict[str, str] | None]],
    ) -> int:
        if not items:
            return 0
        with self._lock:
            self._assert_online()
            if topic not in self._partition_counts:
                raise KeyError(f"Unknown topic '{topic}'")
            num_parts = self._partition_counts[topic]
            now_ms = int(time.time() * 1000)
            buffers: dict[int, bytearray] = {}
            for key, value, ts_opt, hdr_opt in items:
                part = murmur2_partition(key, num_parts)
                offset = self._next_offsets[(topic, part)]
                ts_ms = ts_opt if ts_opt is not None else now_ms
                k_bytes = key.encode("utf-8")
                h_bytes = (
                    json.dumps(hdr_opt, separators=(",", ":")).encode("utf-8") if hdr_opt else b"{}"
                )
                buf = buffers.setdefault(part, bytearray())
                buf += _FRAME_STRUCT.pack(offset, ts_ms, len(k_bytes), len(h_bytes), len(value))
                buf += k_bytes
                buf += h_bytes
                buf += value
                self._next_offsets[(topic, part)] = offset + 1
            for part, payload in buffers.items():
                pfile = self._partition_file(topic, part)
                with pfile.open("ab") as fh:
                    fh.write(payload)
            return len(items)

    def _read_partition_from(
        self, topic: str, partition: int, start_offset: int, limit: int
    ) -> list[TransportRecord]:
        pfile = self._partition_file(topic, partition)
        if not pfile.exists() or pfile.stat().st_size == 0 or limit <= 0:
            return []
        records: list[TransportRecord] = []
        with pfile.open("rb") as fh:
            while len(records) < limit:
                hdr = fh.read(_FRAME_STRUCT.size)
                if not hdr or len(hdr) < _FRAME_STRUCT.size:
                    break
                offset, ts_ms, klen, hlen, vlen = _FRAME_STRUCT.unpack(hdr)
                if offset < start_offset:
                    fh.seek(klen + hlen + vlen, 1)
                    continue
                k_bytes = fh.read(klen)
                h_bytes = fh.read(hlen)
                v_bytes = fh.read(vlen)
                records.append(
                    TransportRecord(
                        topic=topic,
                        partition=partition,
                        offset=offset,
                        key=k_bytes.decode("utf-8"),
                        value=v_bytes,
                        timestamp_ms=ts_ms,
                        headers=json.loads(h_bytes.decode("utf-8")) if hlen else {},
                    )
                )
        return records

    def consume_batch(
        self,
        topic: str,
        consumer_group: str,
        *,
        max_records: int = 5000,
        auto_commit: bool = False,
    ) -> list[TransportRecord]:
        with self._lock:
            self._assert_online()
            num_parts = self._partition_counts[topic]
            committed = self._load_group_offsets(consumer_group, topic)
            collected: list[TransportRecord] = []
            new_offsets = dict(committed)
            remaining = max_records
            for p in range(num_parts):
                if remaining <= 0:
                    break
                start_off = committed.get(p, 0)
                part_recs = self._read_partition_from(topic, p, start_off, remaining)
                if part_recs:
                    collected.extend(part_recs)
                    new_offsets[p] = part_recs[-1].offset + 1
                    remaining -= len(part_recs)
            if auto_commit and collected:
                self._save_group_offsets(consumer_group, topic, new_offsets)
            return collected

    def commit_offsets(
        self,
        topic: str,
        consumer_group: str,
        partition_offsets: dict[int, int],
    ) -> None:
        with self._lock:
            self._assert_online()
            current = self._load_group_offsets(consumer_group, topic)
            for p, off in partition_offsets.items():
                current[int(p)] = max(current.get(int(p), 0), int(off))
            self._save_group_offsets(consumer_group, topic, current)

    def get_consumer_lag(self, topic: str, consumer_group: str) -> dict[str, Any]:
        with self._lock:
            num_parts = self._partition_counts[topic]
            committed = self._load_group_offsets(consumer_group, topic)
            per_part: dict[int, dict[str, int]] = {}
            total_lag = 0
            for p in range(num_parts):
                hwm = self._next_offsets.get((topic, p), 0)
                cur = committed.get(p, 0)
                lag = max(0, hwm - cur)
                per_part[p] = {"committed": cur, "high_watermark": hwm, "lag": lag}
                total_lag += lag
            return {
                "topic": topic,
                "consumer_group": consumer_group,
                "backend": self.backend_name,
                "total_lag": total_lag,
                "partitions": per_part,
            }

    def read_range(
        self,
        topic: str,
        *,
        start_ts_ms: int | None = None,
        end_ts_ms: int | None = None,
        max_records: int = 100_000,
    ) -> list[TransportRecord]:
        with self._lock:
            self._assert_online()
            num_parts = self._partition_counts[topic]
            matched: list[TransportRecord] = []
            for p in range(num_parts):
                recs = self._read_partition_from(topic, p, 0, max_records - len(matched))
                for r in recs:
                    if start_ts_ms is not None and r.timestamp_ms < start_ts_ms:
                        continue
                    if end_ts_ms is not None and r.timestamp_ms > end_ts_ms:
                        continue
                    matched.append(r)
                    if len(matched) >= max_records:
                        return matched
            return matched

    def topic_stats(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            stats: dict[str, dict[str, Any]] = {}
            for topic, num_parts in self._partition_counts.items():
                total_records = 0
                total_bytes = 0
                for p in range(num_parts):
                    pfile = self._partition_file(topic, p)
                    total_records += self._next_offsets.get((topic, p), 0)
                    if pfile.exists():
                        total_bytes += pfile.stat().st_size
                stats[topic] = {
                    "partitions": num_parts,
                    "records": total_records,
                    "bytes": total_bytes,
                    "backend": self.backend_name,
                }
            return stats
