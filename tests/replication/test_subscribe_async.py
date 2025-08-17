from unittest import mock

import pytest

import psycopg

from ..acompat import asleep

if True:  # ASYNC
    import asyncio
else:
    import threading


def _ensure_logical(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("SHOW wal_level")
        wal_level = next(cur)[0]
    if str(wal_level) != "logical":
        pytest.skip(f"wal_level is {wal_level!r} (need 'logical')")


def _setup_objects(conn: psycopg.Connection, table: str, publication: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS public.{table} CASCADE")
        cur.execute(
            f"CREATE TABLE public.{table} (id SERIAL PRIMARY KEY, name TEXT, qty INT)"
        )
        cur.execute(f"DROP PUBLICATION IF EXISTS {publication}")
        cur.execute(f"CREATE PUBLICATION {publication} FOR TABLE public.{table}")
    conn.commit()


def _teardown_objects(conn: psycopg.Connection, table: str, publication: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP PUBLICATION IF EXISTS {publication}")
        cur.execute(f"DROP TABLE IF EXISTS public.{table} CASCADE")
    conn.commit()


@pytest.mark.anyio
async def test_subscribe_end_to_end(dsn: str) -> None:
    table = "lr_tab"
    publication = "lr_pub"

    async def producer() -> None:
        # Small delay to allow the subscriber to enter COPY_BOTH.
        await asleep(0.5)
        async with (
            await psycopg.AsyncConnection.connect(dsn) as conn,
            conn.transaction(),
            conn.cursor() as cur,
        ):
            await cur.execute(
                f"INSERT INTO public.{table} (name, qty) VALUES (%s, %s) RETURNING id",
                ("one", 1),
            )
            row_id = (await anext(cur))[0]

            await cur.execute(
                f"UPDATE public.{table} SET name = %s, qty = %s WHERE id = %s",
                ("two", 2, row_id),
            )

            await cur.execute(
                "SELECT pg_logical_emit_message(true, 'some-prefix', 'updated!')",
            )

            await cur.execute(f"DELETE FROM public.{table} WHERE id = %s", (row_id,))

    events: list[psycopg.ChangeEvent] = []

    async def consumer() -> None:
        async with await psycopg.AsyncConnection.connect(dsn) as conn:
            async for ev in conn.subscribe(
                [publication],
                create_slot=True,
                temporary_slot=True,
                drop_slot_on_close=False,
                start_lsn=None,
                messages=True,
                status_interval=1.0,
                ack_every=1,
                stop_after=1,
            ):
                events.append(ev)

    with psycopg.connect(dsn) as base:
        try:
            _ensure_logical(base)
            _setup_objects(base, table, publication)

            if True:  # ASYNC
                producer_task = asyncio.create_task(producer())
                consumer_task = asyncio.create_task(consumer())
                await consumer_task
                await producer_task
            else:
                producer_task = threading.Thread(target=producer)
                consumer_task = threading.Thread(target=consumer)
                producer_task.start()
                consumer_task.start()
                producer_task.join()
                consumer_task.join()
        finally:
            _teardown_objects(base, table, publication)

    assert events == [
        psycopg.Begin(
            lsn=mock.ANY,
            xid=mock.ANY,
            final_lsn=mock.ANY,
            commit_time=mock.ANY,
        ),
        psycopg.Relation(
            lsn=0,
            oid=mock.ANY,
            schema="public",
            name=table,
            replica_identity="d",
            columns=[
                psycopg.ColumnDef(name="id", flags={"key"}, type_oid=23, atttypmod=-1),
                psycopg.ColumnDef(name="name", flags=set(), type_oid=25, atttypmod=-1),
                psycopg.ColumnDef(name="qty", flags=set(), type_oid=23, atttypmod=-1),
            ],
        ),
        psycopg.Insert(
            lsn=mock.ANY,
            relation=psycopg.RelationRef(oid=mock.ANY, schema="public", name=table),
            new={
                "id": 1,
                "name": "one",
                "qty": 1,
            },
        ),
        psycopg.Update(
            lsn=mock.ANY,
            relation=psycopg.RelationRef(oid=mock.ANY, schema="public", name=table),
            old=None,
            new={
                "id": 1,
                "name": "two",
                "qty": 2,
            },
        ),
        psycopg.Message(
            lsn=mock.ANY,
            flags={"transactional"},
            message_lsn=mock.ANY,
            prefix="some-prefix",
            content=b"updated!",
        ),
        psycopg.Delete(
            lsn=mock.ANY,
            relation=psycopg.RelationRef(oid=mock.ANY, schema="public", name=table),
            old={
                "id": 1,
            },
        ),
        psycopg.Commit(
            lsn=mock.ANY,
            commit_lsn=mock.ANY,
            end_lsn=mock.ANY,
            commit_time=mock.ANY,
        ),
    ]
