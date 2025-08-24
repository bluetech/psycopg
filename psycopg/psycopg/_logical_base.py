"""Replication connection management for logical replication.

This module provides primitives to:

- Establish a dedicated replication connection (replication=database).
- Run replication commands using the simple query protocol:
  - IDENTIFY_SYSTEM
  - CREATE_REPLICATION_SLOT (logical)
  - DROP_REPLICATION_SLOT
  - START_REPLICATION LOGICAL
- LogicalStream: manage the CopyBoth streaming loop, parse XLogData frames,
  decode pgoutput payloads into typed ChangeEvent(s), handle primary keepalives,
  periodic standby status updates, and acknowledgements.
"""

# Copyright (C) 2025 The Psycopg Team

from __future__ import annotations

import datetime
from dataclasses import dataclass

# Timestamp base: PostgreSQL epoch (2000-01-01)
_PG_EPOCH = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)


# Dataclasses


@dataclass(frozen=True, slots=True, kw_only=True)
class IdentifySystemResult:
    """Response to IDENTIFY_SYSTEM.

    - systemid: cluster system identifier (text)
    - timeline: current timeline ID (int)
    - xlogpos: current WAL location (LSN as 'XXX/XXXXXXXX')
    - dbname: database name connected to, or None
    """

    systemid: str
    timeline: int
    xlogpos: str
    dbname: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class CreateSlotResult:
    """Response to CREATE_REPLICATION_SLOT (logical).

    - slot_name: the slot name created
    - consistent_point: LSN of consistent point for the slot
    - snapshot_name: exported snapshot name (if EXPORT_SNAPSHOT / USE_SNAPSHOT)
    - output_plugin: plugin name used (e.g., 'pgoutput')
    """

    slot_name: str
    consistent_point: str
    snapshot_name: str | None
    output_plugin: str


# Helpers


def _pg_now_us() -> int:
    # Microseconds since PG epoch
    delta = datetime.datetime.now(datetime.timezone.utc) - _PG_EPOCH
    return int(delta.total_seconds() * 1_000_000)


def _escape_option_value(v: str) -> str:
    # Simple SQL literal escaping for replication options
    return v.replace("'", "''")
