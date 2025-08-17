Below is a concrete, detailed implementation plan to add logical decoding via pgoutput to psycopg 3, with sync/async support and strongly typed dataclasses. Scope is PostgreSQL ≥ 10, plugin = pgoutput only. Implement phase 1.

Scope and constraints
- Support PostgreSQL ≥ 10.
- Only pgoutput plugin. No wal2json or “raw stream” mode.
- Provide a low-level replication connection and a high-level subscribe() API that returns typed change events.
- Typed dataclasses for all user-facing change events.
- Provide both sync and async variants.
- Use psycopg’s existing waiting loop, libpq get_copy_data/put_copy_data, adapters/Transformer for type decoding, and familiar patterns (context managers, generators, with/async with).

Public API design
1) High-level convenience on psycopg.Connection / psycopg.AsyncConnection
- Connection.subscribe(
  publications: list[str],
  *,
  slot: str | None = None,
  create_slot: bool = True,
  temporary_slot: bool = False,
  drop_slot_on_close: bool = False,
  start_lsn: str | None = None,
  options: dict[str, str] | None = None,  # pgoutput options like proto_version, binary, messages, streaming, two_phase, origin
  status_interval: float = 10.0,          # seconds between periodic status updates
  ack: Literal["write", "flush", "apply"] = "flush",
  ack_every: int = 1,                     # acknowledge every N commits
  stop_after: int | None = None,          # stop after N commits (useful for tests/tools)
) -> Iterator[ChangeEvent]

- AsyncConnection.subscribe(...) -> AsyncIterator[ChangeEvent] with same parameters.

Notes:
- subscribe() will open and own an internal replication connection constructed from the same DSN as the host connection but with replication=database. It won’t reuse the normal SQL connection for streaming.
- When create_slot=True and slot is None, choose a safe default name (e.g., f"psycopg_{dbname}_{pubhash}") or require slot explicitly. Temporary slots are allowed via temporary_slot.
- options defaults:
  - proto_version: "1" (for PG 10–13). If server ≥ 14, we can optionally use "2" or higher later, but keep v1 as initial MVP.
  - binary: "false" to get textual values from pgoutput; we’ll decode them via adapters into Python types.
  - publication_names: computed from the publications list (comma-separated), set by subscribe() automatically.
- ack is how we map the last processed LSN into write_lsn / flush_lsn / apply_lsn fields of the Standby Status Update.
- on error or explicit stop, if drop_slot_on_close is True and the slot is not temporary, issue DROP_REPLICATION_SLOT WAIT.

2) Low-level API to expose replication protocol (internal but public module ok)
- psycopg.replication.ReplicationConnection / AsyncReplicationConnection
  - connect(conninfo: str, …)
  - identify_system() -> dataclass IdentifySystem
  - create_logical_slot(name: str, plugin: str = "pgoutput", *, temporary: bool = False, two_phase: bool = False, snapshot: Literal["export","use","nothing"] = "export", failover: bool | None = None) -> dataclass CreateSlotResult
  - drop_replication_slot(name: str, *, wait: bool = False) -> None
  - start_logical(slot: str, *, start_lsn: str | None = None, options: dict[str, str] | None = None) -> LogicalStream
- LogicalStream (context-managed, iterable/async iterable)
  - __enter__/__exit__ / async variants
  - __iter__/__anext__: yields PgOutputEvent (typed dataclasses) rather than raw bytes.
  - ack(lsn: str, *, write: bool = True, flush: bool = True, apply: bool = True, request_reply: bool = False) -> None
  - set_status_interval(seconds: float) -> None
  - last_received_lsn / last_flushed_lsn / last_applied_lsn: str
  - close()

Event dataclasses (user-facing, strongly typed)
- Base class ChangeEvent (frozen=True):
  - lsn: str
  - xid: int | None
- Begin(lsn: str, xid: int, final_lsn: str | None, commit_time: datetime | None)
- Commit(lsn: str, commit_lsn: str, end_lsn: str, commit_time: datetime)
- Relation(lsn: str, oid: int, schema: str, name: str, replica_identity: Literal["d","n","f","i"], columns: list[ColumnDef])
- Type(lsn: str, oid: int, name: str, namespace: str)
- Insert(lsn: str, xid: int, relation: RelationRef, new: dict[str, Any])
- Update(lsn: str, xid: int, relation: RelationRef, old: dict[str, Any] | None, new: dict[str, Any])
- Delete(lsn: str, xid: int, relation: RelationRef, old: dict[str, Any] | None)
- Truncate(lsn: str, relations: list[RelationRef], cascade: bool, restart_identity: bool)
- Origin(lsn: str, name: str, commit_lsn: str)
- Optional for future versions (hidden until later): StreamStart, StreamStop, StreamCommit (v2+).
- Helper dataclasses:
  - ColumnDef(name: str, flags: set[Literal["key","generated"]], type_oid: int, atttypmod: int | None)
  - RelationRef(oid: int, schema: str, name: str)

Notes:
- All dataclasses frozen=True to align with immutability.
- We keep lsn as “XXX/XXX” string. Internal helpers will convert to/from int64 for comparisons.

Internal architecture and modules
Add new modules under psycopg/psycopg/psycopg:
- _replication_conn.py
  - ReplicationConnection, AsyncReplicationConnection, start/stop commands, slot management, “simple query protocol only”.
  - Build conninfo with replication=database (use conninfo.make_conninfo and override/append).
  - Use waiting.wait_conn for connect and waiting.wait/wait_async for command generators.
- _pgoutput.py
  - Pgoutput decoder:
    - PgOutputDecoder class that maintains Relation and Type caches, and a Transformer for value decoding.
    - parse_xlogdata(payload: bytes) -> list[ChangeEvent] (possibly multiple events per frame).
    - Parsing functions for each message type as per PostgreSQL logical replication protocol v1 (Begin, Commit, Relation, Type, Insert, Update, Delete, Truncate, Origin).
    - Tuple decoding (text mode first): handle NULL/unchanged flags and map to Python via adapters.
- _replication_stream.py
  - LogicalStream (sync) and AsyncLogicalStream (async).
  - Manages CopyBoth receive loop via PGconn.get_copy_data() and sends standby status updates via PGconn.put_copy_data() + flush.
  - Tracks last_received_lsn, last_flushed_lsn, last_applied_lsn; schedules periodic keepalive replies; handles server ‘k’ keepalives.
  - Applies PgOutputDecoder to every XLogData payload and yields events.
- _subscribe.py
  - Convenience plumbing used by Connection.subscribe()/AsyncConnection.subscribe():
    - Creates a ReplicationConnection from the underlying connection’s DSN.
    - Optionally creates slot; executes START_REPLICATION SLOT … LOGICAL with options:
      - publication_names from publications list
      - proto_version (default "1")
      - binary="false"
      - messages="false" unless explicitly asked
      - streaming, two_phase, origin left off in MVP
    - Wraps LogicalStream iteration and applies ack policy (ack_every, ack target dimension).
- __init__.py updates
  - Export user-facing symbols: ChangeEvent and event dataclasses, Connection.subscribe/AsyncConnection.subscribe.

Low-level replication command execution
- Replication mode uses only the simple query protocol (per docs). Use PGconn.send_query with text commands:
  - IDENTIFY_SYSTEM;
  - CREATE_REPLICATION_SLOT name LOGICAL pgoutput [ ( options ) ];
  - DROP_REPLICATION_SLOT name [WAIT];
  - START_REPLICATION SLOT name LOGICAL XXX/XXX (options); if start_lsn is None, use 0/0 per docs and let the server choose max(requested, confirmed_flush_lsn) or start from confirmed_flush_lsn via omission if permitted by PG version (behavior depends on version). For compatibility, we can read the slot’s confirmed_flush_lsn via pg_replication_slots and pass that as start LSN.
- Wait for PGRES_COPY_BOTH after START_REPLICATION; then switch to CopyBoth loop.

LogicalStream generator (sync) and async generator
- State machine:
  - Enter CopyBoth after START_REPLICATION.
  - Loop:
    - Call PGconn.get_copy_data(async_=1). If 0 (would block), WAIT_R via waiting.wait; then consume_input and retry.
    - On data: parse first byte for frame kind:
      - ‘w’ XLogData:
        - Read start_lsn(int64), server_end_lsn(int64), server_time(int64), then plugin payload bytes.
        - Update last_received_lsn to start_lsn.
        - Decode payload via PgOutputDecoder, yielding typed ChangeEvent(s).
        - When encountering Commit, increment a commit count; if commit count % ack_every == 0, ack(commit.commit_lsn) according to ack policy setting which maps commit_lsn into write/flush/apply fields.
      - ‘k’ Primary keepalive:
        - Fields: server_end_lsn(int64), server_time(int64), reply_requested(byte).
        - If reply_requested != 0, send a status update immediately (ack last_flushed_lsn or last_received_lsn per policy) with request_reply flag to force server response.
    - Periodic status update:
      - If now - last_status_sent >= status_interval, send a Standby Status Update with the current ack LSN (write/flush/apply per policy).
    - Sending a status update:
      - Construct payload: ‘r’ + write_lsn(int64) + flush_lsn(int64) + apply_lsn(int64) + client_time(int64) + replyRequested(byte).
      - Use PGconn.put_copy_data(buffer) in a loop while it returns 0 (would block): WAIT_W; then PGconn.flush() similarly: WAIT_W until it returns 0.
- Exit:
  - If server closes CopyBoth, exit yielding no more events; complete START_REPLICATION result set reading per protocol (it’ll send two CommandComplete messages; consume them).
  - close() stops iteration and sends CopyDone; drain server responses.

Pgoutput decoding (v1)
- Implement a byte reader (view + cursor) with helpers to read:
  - int8, int4, int2, int1, uint1; string (length-prefixed or null-terminated per message spec); LSN (int64).
- Messages to support (v1):
  - Begin (‘B’): xid, final_lsn, commit_time (microseconds since 2000-01-01, convert to datetime with session tz if desired; or keep naive UTC).
  - Commit (‘C’): flags, commit_lsn, end_lsn, commit_time.
  - Relation (‘R’): relid (oid), namespace, name, replica_identity byte, ncolumns, per-column: flags (key/other), name, type_oid, atttypmod.
    - Update RelationCache: relid -> RelationMeta(schema, name, replica_identity, columns).
    - Emit Relation event dataclass so the client can observe schema changes.
  - Type (‘Y’): oid, namespace, name. Update TypeCache (oid -> (schema,name)); emit Type event.
  - Insert (‘I’): relid, tuple data (NewTuple). Decode into dict[str, Any] using:
    - Column names from RelationCache; for each field:
      - If field is null: None
      - Else text value: load via Transformer using text format; binary=false for MVP.
  - Update (‘U’): relid, optional OldTuple (key or full), NewTuple. Decode old/new similarly. The “unchanged toast datum” flag should produce a sentinel we can map to None or drop the key. For MVP: missing/unchanged fields omitted in dict; document this behavior.
  - Delete (‘D’): relid, OldTuple (key or full). Decode old.
  - Truncate (‘T’): nrels, options (cascade, restart identity), relids. Map relids to RelationRef using cache (schema/name); if missing, include only oid.
  - Origin (‘O’): origin name, commit_lsn. Emit event.
- Value decoding:
  - Use a Transformer created from the owning connection/adapters (like COPY does) to load text into Python. For builtin types this Just Works; for unknown OIDs, if no loader found, leave as str.
- Caches:
  - RelationCache: relid -> RelationMeta. Invalidate/replace on new Relation msgs.
  - TypeCache: oid -> (schema, name). Used for introspection and future binary decoding upgrades; not strictly necessary for text mode but included for completeness.

Slot management in subscribe()
- On enter:
  - If create_slot=True:
    - If temporary_slot=True: CREATE_REPLICATION_SLOT slot TEMPORARY LOGICAL pgoutput (...).
    - Else: CREATE_REPLICATION_SLOT slot LOGICAL pgoutput (...).
    - Use snapshot='export' by default to match standard tooling unless user requests otherwise. The snapshot_name can be exposed if needed later (not essential to streaming).
  - If create_slot=False and slot is None: error.
- On exit:
  - If drop_slot_on_close: DROP_REPLICATION_SLOT slot [WAIT].
  - Temporary slots are dropped automatically by server.

Ack policy
- ack="write"|"flush"|"apply" controls which of the three LSNs we populate with the commit LSN, and others are set to the last known lower bound (usually same value).
- ack_every=N means we do not send a status update for every commit; we buffer and send after N commits or when status_interval elapses, or when server requests immediate reply.

Errors and edge cases
- Slot already exists and create_slot=True:
  - Either fail with ProgrammingError, or proceed if it’s an identical slot; MVP: raise.
- Missing Relation cache entry on DML:
  - Protocol guarantees a Relation message comes before DML on a relid; but the client might restart mid-stream. If missing, raise a decoding error with context (lsn, relid).
- Keepalive and timeouts:
  - Honor reply_requested. Keep status_interval < wal_sender_timeout/2 by default; document recommended settings.
- Large transactions:
  - v1 doesn’t stream in-progress; events fit within commit boundary. If receiving messages too large for buffer, parsing still ok; Python handles bytes of any size. Document memory implications.

Sync vs Async
- Mirror patterns used in connection and copy:
  - Sync stream uses waiting.wait and PGconn.socket with WAIT_R/W.
  - Async stream uses waiting.wait_async.
  - Expose identical APIs with async variants returning AsyncIterator[ChangeEvent].

Thread-safety and concurrency
- The LogicalStream owns a dedicated PGconn and should not be shared across threads.
- subscribe() is single-stream per call; users can run multiple subscribe() in different threads/tasks if they use different slots or publications.

Integration points in repository
- psycopg/psycopg/psycopg/_replication_conn.py
  - New classes: ReplicationConnection, AsyncReplicationConnection, dataclasses IdentifySystem, CreateSlotResult.
- psycopg/psycopg/psycopg/_replication_stream.py
  - New classes: LogicalStream, AsyncLogicalStream.
- psycopg/psycopg/psycopg/_pgoutput.py
  - New: PgOutputDecoder, caches, event dataclasses, binary readers, v1 parser.
- psycopg/psycopg/psycopg/_subscribe.py
  - New: subscribe plumbing used by Connection/AsyncConnection methods.
- psycopg/psycopg/psycopg/connection.py and connection_async.py
  - Add methods Connection.subscribe()/AsyncConnection.subscribe() delegating to _subscribe module.
- psycopg/psycopg/psycopg/__init__.py
  - Export public symbols: event dataclasses, subscribe() availability.
- docs:
  - docs/advanced/logical_replication.rst (new): overview, server requirements, examples (sync/async), caveats.
  - docs/api/replication.rst (new): API reference.

Dataclass definitions (overview, all frozen=True)
- ChangeEvent (base): lsn: str; xid: int | None
- Begin: lsn, xid, final_lsn: str | None, commit_time: datetime | None
- Commit: lsn, commit_lsn: str, end_lsn: str, commit_time: datetime
- Relation: lsn, oid: int, schema: str, name: str, replica_identity: Literal["d","n","f","i"], columns: list[ColumnDef]
- Type: lsn, oid: int, name: str, namespace: str
- ColumnDef: name: str, flags: frozenset[str], type_oid: int, atttypmod: int | None
- RelationRef: oid: int, schema: str, name: str
- Insert: lsn, xid, relation: RelationRef, new: Mapping[str, Any]
- Update: lsn, xid, relation: RelationRef, old: Mapping[str, Any] | None, new: Mapping[str, Any]
- Delete: lsn, xid, relation: RelationRef, old: Mapping[str, Any] | None
- Truncate: lsn, relations: list[RelationRef], cascade: bool, restart_identity: bool
- Origin: lsn, name: str, commit_lsn: str

Parsing and encoding details (pgoutput v1)
- Implement per upstream docs (protocol-logicalrep-message-formats). Key needs:
  - Reading signed/unsigned ints, strings (null-terminated, length-coded where applicable).
  - TupleData format for Text: number of columns then per column a kind byte (‘n’ null, ‘u’ unchanged toast, ‘t’ text value with length). For MVP: treat ‘u’ as missing value (omit key).
  - All message timestamps are microseconds since 2000-01-01; convert to UTC datetime (timezone-naive or timezone-aware using connection timezone; choose consistent behavior and document).
- Standby Status Update wire format matches physical protocol. Implement a small encoder that builds the correct payload.

Adapters for value decoding
- Create a Transformer from a context bound to the source connection (or a copy thereof) with text format to load textual values into Python.
- For unknown OIDs or types with no registered loader, return the raw str value, as COPY text parsers do.
- For BYTEA and like, pgoutput in text mode should send escaped formats; rely on standard loaders.

Testing plan
- Unit tests for pgoutput decoding:
  - Feed known byte sequences for each message type and verify dataclasses.
  - Relation cache behaviors; Type cache used to enrich Relation if needed.
  - Tuple decoding: nulls, unchanged toast, text values mapping to Python types.
- Integration tests (requires PostgreSQL server):
  - Set wal_level=logical, create publication on test tables, insert/update/delete/truncate, consume via subscribe() and assert sequence of events.
  - Temporary slot lifecycle; permanent slot + drop on close; drop disabled by default.
  - Ack behavior: verify confirmed_flush_lsn moves on server; check pg_replication_slots.
  - Keepalive handling: simulate reply_requested; ensure we respond promptly.
  - Async tests mirroring sync ones.
- Backpressure/WAL retention tests:
  - Intentionally withhold acks and confirm server retains WAL; then resume and ack; confirm retention drops.

Documentation
- New “Logical replication (pgoutput)” page under advanced.
- How-to: server prerequisites, publication creation, slot lifecycle, bookmarks (persisting commit_lsn), failure recovery (resume from start_lsn).
- API reference for subscribe(), event types, ReplicationConnection for advanced control.
- Limitations: pgoutput only, proto v1 only in MVP, no two-phase/streaming features initially.

Performance and memory
- Stream and decode in chunks; avoid copying payload more than necessary.
- Use memoryview slicing for readers.
- Avoid building large intermediate dicts when not needed; only materialize the row dict for Insert/Update/Delete.
- Relation and Type caches keyed by OID with small memory footprint, resettable on close.

Backward compatibility and stability
- New APIs only; no behavior changes to existing code.
- Mark the module experimental in first release; once stable, give semantic versioning guarantees for public classes and dataclasses.

Phased delivery
- Phase 1 (MVP):
  - Pgoutput v1 decoding (Begin/Commit/Relation/Type/Insert/Update/Delete/Truncate/Origin).
  - subscribe() sync + async with ack_every, ack policy, status_interval.
  - Slot create/drop, START_REPLICATION, keepalives, periodic status updates.
  - Text mode value decoding only (binary=false).
  - Docs + tests for PG 10–13.
- Phase 2:
  - Server ≥14 (proto v2) streaming of large transactions: parse and buffer Stream* messages; still deliver events in transactional order; optional parallel hints ignored.
  - Server ≥15 (proto v3) two-phase: BeginPrepare/Prepare/CommitPrepared handling (optional).
  - Server ≥16 (proto v4) parallel metadata: optionally exposed in events.
  - More docs and tests on newer PostgreSQL.

Operational notes for users
- Server:
  - postgresql.conf: wal_level=logical; increase max_replication_slots and max_wal_senders.
  - pg_hba.conf: allow the role to connect to the target database; REPLICATION attribute recommended.
  - Create publication limited to target table(s).
- Client:
  - Use Connection.subscribe() or AsyncConnection.subscribe() with your publication and slot strategy.
  - Persist commit_lsn (e.g., on Commit events) to resume with start_lsn after restarts.
  - Choose ack policy “flush” by default and a reasonable status_interval (e.g., 10s); keep below wal_sender_timeout.

Open decisions to confirm
- Should subscribe() persist bookmarks automatically via a user-provided callback, or leave it entirely to the user? Plan: leave to user; provide examples.
- Datetime timezone: return naive UTC datetimes or timezone-aware using connection timezone. Plan: naive UTC to keep consistent and simple; document.
- Behavior for “unchanged toast datum”: omit keys vs set to special sentinel. Plan: omit keys; document that missing means unchanged.

With this plan, implementation aligns with psycopg’s design ethos: clear separation of low-level protocol plumbing and high-level ergonomic interfaces; generator-driven I/O; context-managed lifetimes; and strong typing via dataclasses for user-facing events.