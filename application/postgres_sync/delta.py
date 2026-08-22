"""Pure deterministic complete-state delta planning for PostgreSQL Gold sync."""

from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Final

from application.postgres_sync.contracts import (
    GoldDeltaPlan,
    GoldRowDigest,
    GoldRowKey,
    GoldRowPayload,
    payload_from_mapping,
)

_SEPARATOR: Final[bytes] = b"\x1f"


def _utc_datetime_bytes(value: datetime) -> bytes:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime value must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError("datetime value must use UTC")
    normalized = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = normalized - epoch
    epoch_microseconds = ((delta.days * 86_400 + delta.seconds) * 1_000_000) + delta.microseconds
    return f"dt:{epoch_microseconds}".encode("ascii")


def _canonical_value(value: object) -> bytes:
    if value is None:
        return b"null"
    if isinstance(value, bool):
        return b"bool:1" if value else b"bool:0"
    if isinstance(value, int):
        return f"int:{value}".encode("ascii")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite floating-point values cannot be hashed deterministically")
        normalized = 0.0 if value == 0.0 else value
        return f"float:{format(normalized, '.17g')}".encode("ascii")
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("non-finite decimal values cannot be hashed deterministically")
        return f"decimal:{value.normalize()}".encode("ascii")
    if isinstance(value, datetime):
        return _utc_datetime_bytes(value)
    if isinstance(value, date):
        return f"date:{value.isoformat()}".encode("ascii")
    if isinstance(value, str):
        return b"str:" + value.encode("utf-8")
    if isinstance(value, bytes):
        return b"bytes:" + value.hex().encode("ascii")
    if isinstance(value, (tuple, list)):
        encoded = [_canonical_value(item) for item in value]
        return b"list:[" + _SEPARATOR.join(encoded) + b"]"
    if isinstance(value, Mapping):
        items: list[bytes] = []
        for raw_key in sorted(value, key=lambda item: str(item)):
            key = str(raw_key)
            items.append(b"key:" + key.encode("utf-8") + b"=" + _canonical_value(value[raw_key]))
        return b"map:{" + _SEPARATOR.join(items) + b"}"
    raise TypeError(f"unsupported deterministic hash value type: {type(value).__name__}")


def canonical_row_hash(row: Mapping[str, object]) -> str:
    """Hash a row using exact mapping order, type tags, null markers, and UTC microseconds."""

    digest = hashlib.sha256()
    for column_name, value in row.items():
        digest.update(b"column:")
        digest.update(column_name.encode("utf-8"))
        digest.update(b"=")
        digest.update(_canonical_value(value))
        digest.update(_SEPARATOR)
    return digest.hexdigest()


def build_source_digests(
    rows: Sequence[Mapping[str, object]],
) -> tuple[tuple[GoldRowPayload, GoldRowDigest], ...]:
    """Create deterministic payload/digest pairs and reject duplicate logical keys."""

    by_key: dict[GoldRowKey, tuple[GoldRowPayload, GoldRowDigest]] = {}
    for row in rows:
        payload = payload_from_mapping(row)
        if payload.key in by_key:
            raise ValueError(f"duplicate source Gold key: {payload.key}")
        digest = GoldRowDigest(payload.key, canonical_row_hash(row))
        by_key[payload.key] = (payload, digest)
    return tuple(by_key[key] for key in sorted(by_key))


def _target_digest_map(target_digests: Sequence[GoldRowDigest]) -> dict[GoldRowKey, GoldRowDigest]:
    result: dict[GoldRowKey, GoldRowDigest] = {}
    for digest in target_digests:
        if digest.key in result:
            raise ValueError(f"duplicate PostgreSQL digest key: {digest.key}")
        result[digest.key] = digest
    return result


def plan_gold_delta(
    source_rows: Sequence[Mapping[str, object]],
    target_digests: Sequence[GoldRowDigest],
    *,
    state_exists: bool,
) -> GoldDeltaPlan:
    """Compare complete source and digest state without timestamp-watermark assumptions."""

    if not state_exists and target_digests:
        raise ValueError("Gold digest state exists without an authoritative sync checkpoint")

    source_pairs = build_source_digests(source_rows)
    source_by_key = {payload.key: (payload, digest) for payload, digest in source_pairs}
    target_by_key = _target_digest_map(target_digests)

    inserts: list[GoldRowPayload] = []
    updates: list[GoldRowPayload] = []
    deletes: list[GoldRowKey] = []
    unchanged: list[GoldRowKey] = []

    for key in sorted(source_by_key):
        payload, source_digest = source_by_key[key]
        target_digest = target_by_key.get(key)
        if target_digest is None:
            inserts.append(payload)
        elif target_digest.row_sha256 == source_digest.row_sha256:
            unchanged.append(key)
        else:
            updates.append(payload)

    for key in sorted(target_by_key):
        if key not in source_by_key:
            deletes.append(key)

    return GoldDeltaPlan(
        inserts=tuple(inserts),
        updates=tuple(updates),
        deletes=tuple(deletes),
        unchanged=tuple(unchanged),
        source_digests=tuple(digest for _, digest in source_pairs),
    )
