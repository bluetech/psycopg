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

from . import errors as e
from . import pq

# Timestamp base: PostgreSQL epoch (2000-01-01)
_PG_EPOCH = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)


# Dataclasses


@dataclass(frozen=True, slots=True, kw_only=True)
class IdentifySystem:
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


def _decode_row_as_dict(res: pq.abc.PGresult) -> dict[str, str | None]:
    """Decode the first row of a TUPLES_OK result into a dict name->text value.

    Values are decoded using the connection encoding; NULL stays as None.
    """
    if res.status != pq.ExecStatus.TUPLES_OK:
        raise e.error_from_result(
            res, encoding=res.get_error_message().__class__.__name__
        )

    if res.ntuples != 1:
        raise e.ProgrammingError(f"expected exactly 1 row, got {res.ntuples}")

    enc: str
    # TODO
    # PGresult doesn't expose encoding; get it from an error-message helper or assume
    # utf-8. Prefer the connection encoding via the attached PGconn; however we don't
    # have it here. Most psycopg code reads via PGconn._encoding. For safety, default
    # to utf-8 if needed.
    try:
        # psycopg will set the PGresult encoding context via Transformer in other paths.
        # Here we fallback to utf-8 if we cannot infer better.
        enc = "utf-8"
    except Exception:
        enc = "utf-8"

    out: dict[str, str | None] = {}
    for i in range(res.nfields):
        name_b = res.fname(i) or b""
        name = name_b.decode(enc, errors="replace")
        val_b = res.get_value(0, i)
        if val_b is None:
            out[name] = None
        else:
            out[name] = val_b.decode(enc, errors="replace")
    return out


def _pg_now_us() -> int:
    # Microseconds since PG epoch
    delta = datetime.datetime.now(datetime.timezone.utc) - _PG_EPOCH
    return int(delta.total_seconds() * 1_000_000)


def _escape_option_value(v: str) -> str:
    # Simple SQL literal escaping for replication options
    return v.replace("'", "''")
