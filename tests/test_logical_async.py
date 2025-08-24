from unittest import mock

import pytest

import psycopg

from .acompat import asleep

if True:  # ASYNC
    import asyncio
else:
    import threading


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
            conn.cursor() as cursor,
        ):
            await cursor.execute(
                f"insert into {table} (name, qty) values (%s, %s) returning id",
                ("one", 1),
            )
            row_id = (await anext(cursor))[0]

            await cursor.execute(
                f"update {table} set name = %s, qty = %s where id = %s",
                ("two", 2, row_id),
            )

            await cursor.execute(
                "select pg_logical_emit_message(true, 'some-prefix', 'updated!')",
            )

            await cursor.execute(f"delete from {table} where id = %s", (row_id,))

    events: list[psycopg.ChangeEvent] = []

    async def consumer() -> None:
        async with await psycopg.AsyncConnection.connect(
            dsn, autocommit=True, replication="database"
        ) as conn:
            async with conn.logical() as logical:
                slot = await logical.create_logical_slot("test_slot", temporary=True)
                async with await logical.stream(
                    slot.slot_name,
                    publications=[publication],
                    start_lsn="0/0",
                    messages=True,
                ) as stream:
                    async for ev in stream:
                        events.append(ev)
                        await stream.ack(ev.lsn)
                        if isinstance(ev, psycopg.Commit):
                            break

    with psycopg.connect(dsn) as base:
        try:
            with base.transaction(), base.cursor() as cursor:
                cursor.execute("show wal_level")
                wal_level = next(cursor)[0]
                if str(wal_level) != "logical":
                    pytest.skip(f"wal_level is {wal_level!r} (need 'logical')")

                cursor.execute(f"drop table if exists {table} cascade")
                cursor.execute(
                    f"create table {table} (id serial primary key, name text, qty int)"
                )
                cursor.execute(f"drop publication if exists {publication}")
                cursor.execute(f"create publication {publication} for table {table}")

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
            with base.transaction(), base.cursor() as cursor:
                cursor.execute(f"drop publication if exists {publication}")
                cursor.execute(f"drop table if exists {table} cascade")

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
