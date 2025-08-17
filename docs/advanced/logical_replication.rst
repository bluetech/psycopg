.. _logical_replication:

Logical replication
===================

This page explains how to consume PostgreSQL logical replication using
psycopg via the built-in ``pgoutput`` plugin.

Server prerequisites
--------------------

- postgresql.conf::

    wal_level = logical

- pg_hba.conf:
  allow the client role to connect to the database for replication. Typically
  you'll use a normal database role and ``replication=database`` mode.

- Create one or more publications including the tables you want to stream. For example::

    CREATE PUBLICATION pub_app FOR TABLE accounts, orders;

API
---

Synchronous usage
~~~~~~~~~~~~~~~~~

.. code-block:: python

    import psycopg

    DSN = "postgresql://localhost/dbname?user=app&password=secret"

    with psycopg.connect(DSN) as conn:
        # Subscribe to one or more publications.
        for ev in conn.subscribe(
            ["pub_app"],
            slot="psycopg_app_slot",
            create_slot=True,           # create replication slot if missing
            temporary_slot=False,       # permanent slot (server retains WAL)
            drop_slot_on_close=False,   # do not drop automatically
            start_lsn=None,             # let server choose (confirmed flush)
            status_interval=10.0,       # seconds between status updates
            ack_every=1,                # ack every commit
        ):
            if isinstance(ev, psycopg.Begin):
                print("BEGIN xid:", ev.xid, "final_lsn:", ev.final_lsn)
            elif isinstance(ev, psycopg.Insert):
                print("INSERT", ev.relation.schema, ev.relation.name, ev.new)
            elif isinstance(ev, psycopg.Update):
                print("UPDATE", ev.relation.schema, ev.relation.name, ev.old, "->", ev.new)
            elif isinstance(ev, psycopg.Delete):
                print("DELETE", ev.relation.schema, ev.relation.name, ev.old)
            elif isinstance(ev, psycopg.Commit):
                print("COMMIT at", ev.commit_lsn, "time", ev.commit_time)
                # Persist bookmark here to resume later (see "Bookmarks and resume")
            # You may also receive Relation, Type, Message, Truncate, Origin events.

Asynchronous usage
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import asyncio
    import psycopg

    DSN = "postgresql://localhost/dbname?user=app&password=secret"

    async def main():
        async with await psycopg.AsyncConnection.connect(DSN) as conn:
            async for ev in conn.subscribe(
                ["pub_app"],
                slot="psycopg_app_slot",
                create_slot=True,
                temporary_slot=False,
                drop_slot_on_close=False,
                start_lsn=None,
                status_interval=10.0,
                ack_every=1,
            ):
                print(type(ev).__name__, ev)

    asyncio.run(main())

Parameters (subscribe)
~~~~~~~~~~~~~~~~~~~~~~

- ``publications: list[str]``: names of publications to subscribe to.
- ``slot: str | None``: replication slot name. If omitted and ``create_slot=True``,
  psycopg chooses a safe default of the form ``psycopg_{dbname}_{hash}``.
- ``create_slot: bool``: whether to create the slot if it doesn’t exist.
- ``temporary_slot: bool``: request a temporary slot; dropped automatically on disconnect.
- ``drop_slot_on_close: bool``: for permanent slots, drop on clean shutdown.
- ``start_lsn: str | None``: LSN to start from (e.g. ``"0/0"`` or a bookmark).
  If omitted, the server will start at the slot’s confirmed flush position.
- ``messages: bool``: whether to include generic logical messages, such as
  emitted by ``pg_logical_emit_message()``.
- ``status_interval: float``: seconds between periodic status updates.
- ``ack_every: int``: acknowledge every N commits (buffered).
- ``stop_after: int | None``: stop after N commits (useful for tests).

Event types
-----------

All events are immutable dataclasses with a common base :py:class:`psycopg.ChangeEvent`
containing ``lsn: str`` (e.g. ``"16/B8930D28"``).

- :py:class:`psycopg.Begin`:
  - ``xid: int``, ``final_lsn: str | None``, ``commit_time: datetime | None``
- :py:class:`psycopg.Commit`:
  - ``commit_lsn: str``, ``end_lsn: str``, ``commit_time: datetime``
- :py:class:`psycopg.Relation`:
  - ``oid: int``, ``schema: str``, ``name: str``, ``replica_identity: Literal["d", "n", "f", "i"]``,
    ``columns: Sequence[ColumnDef]`` where ``ColumnDef`` has
    ``name: str``, ``flags: AbstractSet[str]``, ``type_oid: int``, ``atttypmod: int | None``
- :py:class:`psycopg.Type`:
  - ``oid: int``, ``name: str``, ``namespace: str``
- :py:class:`psycopg.Insert`:
  - ``xid: int``, ``relation: RelationRef``, ``new: Mapping[str, Any]``
- :py:class:`psycopg.Update`:
  - ``xid: int``, ``relation: RelationRef``,
    ``old: Mapping[str, Any] | None``, ``new: Mapping[str, Any]``
- :py:class:`psycopg.Delete`:
  - ``xid: int``, ``relation: RelationRef``, ``old: Mapping[str, Any] | None``
- :py:class:`psycopg.Truncate`:
  - ``relations: Sequence[RelationRef]``, ``cascade: bool``, ``restart_identity: bool``
- :py:class:`psycopg.Message`:
  - ``flags: AbstractSet[str]``, ``message_lsn: str``,  ``prefix: str``, ``content: bytes``
- :py:class:`psycopg.Origin`:
  - ``name: str``, ``commit_lsn: str``

Notes:

- For Update/Delete, ``old`` may contain only replica identity columns (when available).
- For Update with "unchanged toast datum" fields, missing keys indicate unchanged values.
- Timestamps are returned as naive UTC :py:class:`datetime.datetime`.

Bookmarks and resume
--------------------

To provide exactly-once processing across restarts, store a bookmark (the commit LSN)
when you process a :py:class:`psycopg.Commit` event, then resume from that LSN:

.. code-block:: python

    # On event loop:
    if isinstance(ev, psycopg.Commit):
        persist(ev.commit_lsn)  # write safely to your durable store

    # On startup:
    last = load_bookmark()      # returns a "XXX/XXXXXXXX" LSN or None
    for ev in conn.subscribe(
        ["pub_app"], slot="psycopg_app_slot", start_lsn=last, create_slot=False
    ):
        ...

Acknowledgements (ack) and backpressure
---------------------------------------

To intentionally retain WAL (for testing backpressure), increase ``ack_every``
and/or delay acks; monitor ``pg_replication_slots.confirmed_flush_lsn``.

Operational tips
----------------

- Keep ``status_interval`` well below your server ``wal_sender_timeout`` to avoid disconnects
  (e.g. 10 seconds by default).
- Publication design matters: include only tables you need; add generated columns and keys
  as appropriate for your replica identity strategy.
- Temporary slots are convenient for ETL jobs; permanent slots are typical for services.
- Monitor slot lag and WAL retention (``pg_stat_replication``, ``pg_replication_slots``).
- Don't keep inactive slots around, they will cause WAL to accumulate. Consider
  setting `max_slot_wal_keep_size` as a last-ditch safety measure.

Limitations
-----------

Currently only the built-in pgoutput decoding plugin is supported.

Currently only pgoutput protocol version 1 is supported. Features of later
protocol versions such as streaming of large in-progress transactions and
two-phase commits are not yet supported.

Troubleshooting
---------------

- "slot already exists": either reuse it with ``create_slot=False`` or drop it manually /
  set ``drop_slot_on_close=True`` (for permanent slots).
- "unknown relation" on DML after restart: ensure you resume from a bookmark that precedes
  the first DML after a Relation message; the protocol guarantees a Relation message will arrive
  before DML for a given relid in normal flow.

See also
--------

- PostgreSQL documentation: "Logical Decoding Message Formats (pgoutput)"
- :py:meth:`psycopg.Connection.subscribe`
- :py:meth:`psycopg.AsyncConnection.subscribe`
- Event classes: :py:class:`psycopg.ChangeEvent`, :py:class:`psycopg.Insert`, etc.
