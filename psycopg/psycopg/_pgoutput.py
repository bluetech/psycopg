"""pgoutput events and decoder (v1, text mode)."""

# Copyright (C) 2025 The Psycopg Team

from __future__ import annotations

import datetime
from typing import AbstractSet, Any, Final, Literal, Mapping, Sequence, cast
from dataclasses import dataclass

from . import pq
from .abc import Buffer, Transformer

# Helpers: LSN int <-> str conversions


def _lsn_from_int(x: int) -> str:
    """Convert an int64 LSN to the textual 'XXX/XXX' format."""
    hi = (x >> 32) & 0xFFFFFFFF
    lo = x & 0xFFFFFFFF
    return f"{hi:X}/{lo:08X}"


def _lsn_to_int(s: str) -> int:
    """Convert a textual LSN 'XXX/XXX' into an int64 value."""
    s = s.strip()
    if not s:
        return 0
    if "/" not in s:
        raise ValueError(f"invalid LSN: {s!r}")
    hi_s, lo_s = s.split("/", 1)
    return (int(hi_s, 16) << 32) | int(lo_s, 16)


# Event dataclasses


@dataclass(slots=True, frozen=True, kw_only=True)
class ChangeEvent:
    """Base class for all logical decoding events."""

    lsn: int

    __module__ = "psycopg"


@dataclass(slots=True, frozen=True, kw_only=True)
class Begin(ChangeEvent):
    xid: int
    final_lsn: int
    commit_time: datetime.datetime

    __module__ = "psycopg"


@dataclass(slots=True, frozen=True, kw_only=True)
class Message(ChangeEvent):
    flags: AbstractSet[str]
    message_lsn: int
    prefix: str
    content: bytes

    __module__ = "psycopg"


@dataclass(slots=True, frozen=True, kw_only=True)
class Commit(ChangeEvent):
    commit_lsn: int
    end_lsn: int
    commit_time: datetime.datetime

    __module__ = "psycopg"


# d = default (primary key, if any)
# n = nothing
# f = all columns
# i = index
ReplicaIdentity = Literal["d", "n", "f", "i"]


@dataclass(slots=True, frozen=True, kw_only=True)
class ColumnDef:
    name: str
    flags: AbstractSet[str]
    type_oid: int
    atttypmod: int

    __module__ = "psycopg"


@dataclass(slots=True, frozen=True, kw_only=True)
class Relation(ChangeEvent):
    oid: int
    schema: str
    name: str
    replica_identity: ReplicaIdentity
    columns: Sequence[ColumnDef]

    __module__ = "psycopg"


@dataclass(slots=True, frozen=True, kw_only=True)
class Type(ChangeEvent):
    oid: int
    name: str
    namespace: str

    __module__ = "psycopg"


@dataclass(slots=True, frozen=True, kw_only=True)
class RelationRef:
    oid: int
    schema: str
    name: str

    __module__ = "psycopg"


@dataclass(slots=True, frozen=True, kw_only=True)
class Insert(ChangeEvent):
    relation: RelationRef
    new: Mapping[str, Any]

    __module__ = "psycopg"


@dataclass(slots=True, frozen=True, kw_only=True)
class Update(ChangeEvent):
    relation: RelationRef
    old: Mapping[str, Any] | None
    new: Mapping[str, Any]

    __module__ = "psycopg"


@dataclass(slots=True, frozen=True, kw_only=True)
class Delete(ChangeEvent):
    relation: RelationRef
    old: Mapping[str, Any] | None

    __module__ = "psycopg"


@dataclass(slots=True, frozen=True, kw_only=True)
class Truncate(ChangeEvent):
    relations: Sequence[RelationRef]
    cascade: bool
    restart_identity: bool

    __module__ = "psycopg"


@dataclass(slots=True, frozen=True, kw_only=True)
class Origin(ChangeEvent):
    name: str
    commit_lsn: int

    __module__ = "psycopg"


# Internal metadata for caches


@dataclass(slots=True, frozen=True, kw_only=True)
class _RelationMeta:
    oid: int
    schema: str
    name: str
    replica_identity: ReplicaIdentity
    columns: list[ColumnDef]

    def as_ref(self) -> RelationRef:
        return RelationRef(oid=self.oid, schema=self.schema, name=self.name)


@dataclass(slots=True, frozen=True, kw_only=True)
class _TypeMeta:
    oid: int
    namespace: str
    name: str


# Byte reader for pgoutput payload


class _Reader:
    """Simple reader over a memoryview."""

    __slots__ = ("_v", "_i", "len")

    def __init__(self, data: Buffer) -> None:
        v = data if isinstance(data, memoryview) else memoryview(data)
        self._v = v
        self._i = 0
        self.len = len(v)

    def eof(self) -> bool:
        return self._i >= self.len

    def tell(self) -> int:
        return self._i

    def read_byte(self) -> int:
        i = self._i
        if i >= self.len:
            raise ValueError("unexpected EOF while reading byte")
        self._i = i + 1
        return self._v[i]

    def read_bool(self) -> bool:
        return self.read_byte() != 0

    def read_int16(self) -> int:
        i = self._i
        ni = i + 2
        if ni > self.len:
            raise ValueError("unexpected EOF while reading int16")
        self._i = ni
        return int.from_bytes(self._v[i:ni], "big", signed=True)

    def read_int32(self) -> int:
        i = self._i
        ni = i + 4
        if ni > self.len:
            raise ValueError("unexpected EOF while reading int32")
        self._i = ni
        return int.from_bytes(self._v[i:ni], "big", signed=True)

    def read_int64(self) -> int:
        i = self._i
        ni = i + 8
        if ni > self.len:
            raise ValueError("unexpected EOF while reading int64")
        self._i = ni
        return int.from_bytes(self._v[i:ni], "big", signed=True)

    def read_cstring(self, encoding: str = "utf-8") -> str:
        i = self._i
        v = self._v
        end = v.tobytes().find(b"\x00", i)
        if end < 0:
            raise ValueError("unterminated cstring")
        self._i = end + 1
        if end == i:
            return ""
        return v[i:end].tobytes().decode(encoding)

    def read_bytes(self, n: int) -> bytes:
        i = self._i
        ni = i + n
        if ni > self.len:
            raise ValueError("unexpected EOF while reading bytes")
        self._i = ni
        return self._v[i:ni].tobytes()


# Pgoutput decoder (v1, text mode)


class PgOutputDecoder:
    """Decoder for PostgreSQL pgoutput plugin messages (protocol v1), using
    text mode values (binary=false).

    - Maintains Relation and Type caches across messages.
    - Uses a psycopg Transformer to decode textual field values into Python.
    - Parses a plugin payload and emits a list of ChangeEvent objects.
    """

    _EPOCH: Final = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)

    def __init__(self, transformer: Transformer) -> None:
        self._relation_cache: dict[int, _RelationMeta] = {}
        self._type_cache: dict[int, _TypeMeta] = {}
        self._tx = transformer

    # Public API

    def parse_xlogdata(self, payload: Buffer, *, lsn: int) -> list[ChangeEvent]:
        """Parse a pgoutput payload and return a list of ChangeEvent instances.

        :param payload: plugin payload from XLogData
        :param lsn: start LSN from XLogData header

        :raises ValueError: if the payload cannot be parsed
        """
        events: list[ChangeEvent] = []
        r = _Reader(payload)

        while not r.eof():
            tag = chr(r.read_byte())
            match tag:
                case "B":  # Begin
                    events.append(self._parse_begin(r, lsn))
                case "M":  # Message
                    events.append(self._parse_message(r, lsn))
                case "C":  # Commit
                    events.append(self._parse_commit(r, lsn))
                case "R":  # Relation
                    events.append(self._parse_relation(r, lsn))
                case "Y":  # Type
                    events.append(self._parse_type(r, lsn))
                case "I":  # Insert
                    events.append(self._parse_insert(r, lsn))
                case "U":  # Update
                    events.append(self._parse_update(r, lsn))
                case "D":  # Delete
                    events.append(self._parse_delete(r, lsn))
                case "T":  # Truncate
                    events.append(self._parse_truncate(r, lsn))
                case "O":  # Origin
                    events.append(self._parse_origin(r, lsn))
                case _:
                    raise ValueError(f"unsupported pgoutput message tag: {tag!r}")

        return events

    def clear_caches(self) -> None:
        self._relation_cache.clear()
        self._type_cache.clear()

    # Decoding helpers

    @classmethod
    def _ts_to_datetime(cls, micros_since_pg_epoch: int) -> datetime.datetime:
        dt_aware = cls._EPOCH + datetime.timedelta(microseconds=micros_since_pg_epoch)
        return dt_aware.replace(tzinfo=None)

    # Tuple decoding

    def _tuple_map(
        self, r: _Reader, columns: Sequence[ColumnDef]
    ) -> dict[str, str | None]:
        """Decode a TupleData using the provided column metadata.

        For 'unchanged toast' fields, omit the key from the dict.
        """
        ncols = r.read_int16()
        out: dict[str, str | None] = {}
        # Map by position; if counts mismatch, map min range.
        count = min(ncols, len(columns))
        for i in range(count):
            kind = chr(r.read_byte())
            col = columns[i]
            name = col.name
            match kind:
                case "n":  # NULL
                    out[name] = None
                case "u":  # unchanged TOASTed value
                    continue
                case "t":  # text formatted value
                    length = r.read_int32()
                    data = r.read_bytes(length)
                    loader = self._tx.get_loader(col.type_oid, pq.Format.TEXT)
                    out[name] = loader.load(data)
                case "b":  # binary formatted value
                    length = r.read_int32()
                    data = r.read_bytes(length)
                    loader = self._tx.get_loader(col.type_oid, pq.Format.BINARY)
                    out[name] = loader.load(data)
                case _:
                    raise ValueError(f"unknown tuple field kind: {kind!r}")
        # If server sent more fields than we have columns metadata for, skip them.
        for _ in range(ncols - count):
            kind = chr(r.read_byte())
            match kind:
                case "n" | "u":
                    continue
                case "t" | "b":
                    length = r.read_int32()
                    _ = r.read_bytes(length)
                case _:
                    raise ValueError(f"unknown tuple field kind: {kind!r}")
        return out

    # Internal: relation/type cache utilities

    def _get_relation_meta(self, relid: int) -> _RelationMeta:
        try:
            return self._relation_cache[relid]
        except KeyError:
            # Unknown relation (e.g., mid-stream resume): create minimal.
            meta = _RelationMeta(
                oid=relid, schema="", name="", replica_identity="d", columns=[]
            )
            self._relation_cache[relid] = meta
            return meta

    # Parsers

    def _parse_begin(self, r: _Reader, lsn: int) -> Begin:
        # 'B' Begin:
        # int64 final_lsn, int64 commit_time, int32 xid
        final_lsn = r.read_int64()
        commit_time = r.read_int64()
        xid = r.read_int32()
        return Begin(
            lsn=lsn,
            xid=xid,
            final_lsn=final_lsn,
            commit_time=self._ts_to_datetime(commit_time),
        )

    def _parse_message(self, r: _Reader, lsn: int) -> Message:
        # 'M' Message:
        # int8 flags, int64 lsn, cstring prefix, int32 length, content
        cflags = r.read_byte()
        flags = set()
        if cflags & 0x01:
            flags.add("transactional")
        message_lsn = r.read_int64()
        prefix = r.read_cstring()
        length = r.read_int32()
        content = r.read_bytes(length)
        return Message(
            lsn=lsn,
            flags=flags,
            message_lsn=message_lsn,
            prefix=prefix,
            content=content,
        )

    def _parse_commit(self, r: _Reader, lsn: int) -> Commit:
        # 'C' Commit:
        # int8 flags, int64 commit_lsn, int64 end_lsn, int64 commit_time
        _ = r.read_byte()
        commit_lsn = r.read_int64()
        end_lsn = r.read_int64()
        commit_time = r.read_int64()
        return Commit(
            lsn=lsn,
            commit_lsn=commit_lsn,
            end_lsn=end_lsn,
            commit_time=self._ts_to_datetime(commit_time),
        )

    def _parse_relation(self, r: _Reader, lsn: int) -> Relation:
        # 'R' Relation:
        # int32 relid, cstring namespace, cstring name, int8 replica_identity,
        # int16 ncols, then per-column:
        #   int8 flags, cstring name, int32 type_oid, int32 atttypmod
        relid = r.read_int32()
        namespace = r.read_cstring()
        name = r.read_cstring()
        rid = cast(ReplicaIdentity, chr(r.read_byte()))
        ncols = r.read_int16()
        cols: list[ColumnDef] = []
        for _ in range(ncols):
            cflags = r.read_byte()
            flags = set()
            if cflags & 0x01:
                flags.add("key")
            if cflags & 0x02:
                flags.add("generated")
            colname = r.read_cstring()
            type_oid = r.read_int32()
            atttypmod = r.read_int32()
            cols.append(
                ColumnDef(
                    name=colname,
                    flags=flags,
                    type_oid=type_oid,
                    atttypmod=atttypmod,
                )
            )
        meta = _RelationMeta(
            oid=relid,
            schema=namespace,
            name=name,
            replica_identity=rid,
            columns=cols,
        )
        self._relation_cache[relid] = meta
        return Relation(
            lsn=lsn,
            oid=relid,
            schema=namespace,
            name=name,
            replica_identity=rid,
            columns=cols,
        )

    def _parse_type(self, r: _Reader, lsn: int) -> Type:
        # 'Y' Type:
        # int32 oid, cstring namespace, cstring name
        oid = r.read_int32()
        namespace = r.read_cstring()
        name = r.read_cstring()
        self._type_cache[oid] = _TypeMeta(oid=oid, namespace=namespace, name=name)
        return Type(lsn=lsn, oid=oid, name=name, namespace=namespace)

    def _parse_insert(self, r: _Reader, lsn: int) -> Insert:
        # 'I' Insert:
        # int32 relid, 'N', TupleData
        relid = r.read_int32()
        if r.read_byte() != ord("N"):
            raise ValueError("expected 'N' for new tuple in Insert")
        meta = self._get_relation_meta(relid)
        new_row = self._tuple_map(r, meta.columns)
        return Insert(
            lsn=lsn,
            relation=meta.as_ref(),
            new=new_row,
        )

    def _parse_update(self, r: _Reader, lsn: int) -> Update:
        # 'U' Update:
        # int32 relid,
        # optional: 'K' TupleData (key) OR 'O' TupleData (old full)
        # mandatory: 'N' TupleData (new)
        relid = r.read_int32()
        meta = self._get_relation_meta(relid)

        old_row: Mapping[str, Any] | None = None

        tag = r.read_byte()
        if tag == ord("K"):
            key_cols = [c for c in meta.columns if "key" in c.flags]
            old_row = self._tuple_map(r, key_cols)
            tag = r.read_byte()
        elif tag == ord("O"):
            old_row = self._tuple_map(r, meta.columns)
            tag = r.read_byte()

        if tag != ord("N"):
            raise ValueError("expected 'N' with new tuple in Update")
        new_row = self._tuple_map(r, meta.columns)

        return Update(
            lsn=lsn,
            relation=meta.as_ref(),
            old=old_row,
            new=new_row,
        )

    def _parse_delete(self, r: _Reader, lsn: int) -> Delete:
        # 'D' Delete:
        # int32 relid,
        # then either 'K' TupleData (key) or 'O' TupleData (old full)
        relid = r.read_int32()
        meta = self._get_relation_meta(relid)

        tag = r.read_byte()
        if tag == ord("K"):
            key_cols = [c for c in meta.columns if "key" in c.flags]
            old_row = self._tuple_map(r, key_cols)
        elif tag == ord("O"):
            old_row = self._tuple_map(r, meta.columns)
        else:
            raise ValueError("expected 'K' or 'O' in Delete")

        return Delete(
            lsn=lsn,
            relation=meta.as_ref(),
            old=old_row,
        )

    def _parse_truncate(self, r: _Reader, lsn: int) -> Truncate:
        # 'T' Truncate:
        # int32 nrels, int8 options, then nrels * int32 relid
        nrels = r.read_int32()
        opts = r.read_byte()
        cascade = bool(opts & 0x01)
        restart_identity = bool(opts & 0x02)
        rels: list[RelationRef] = []
        for _ in range(nrels):
            relid = r.read_int32()
            meta = self._relation_cache.get(relid)
            if meta:
                rels.append(meta.as_ref())
            else:
                rels.append(RelationRef(oid=relid, schema="", name=""))
        return Truncate(
            lsn=lsn,
            relations=rels,
            cascade=cascade,
            restart_identity=restart_identity,
        )

    def _parse_origin(self, r: _Reader, lsn: int) -> Origin:
        # 'O' Origin:
        # int64 commit_lsn, cstring origin_name
        commit_lsn = r.read_int64()
        name = r.read_cstring()
        return Origin(
            lsn=lsn,
            name=name,
            commit_lsn=commit_lsn,
        )
