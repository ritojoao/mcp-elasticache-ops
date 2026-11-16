"""Metadata of one key: type, TTL, encoding, memory, length, idle time, and consumer groups of a stream.

Never reads a value. Every command is metadata only: TYPE, PTTL, OBJECT, MEMORY USAGE, the length command of the
type, and XINFO GROUPS for streams (group names, consumer and pending counts, lag).
"""

from __future__ import annotations

from typing import Any

import redis

from ._core import LENGTH_COMMAND, _finding, _num, _try
from .keyspace import DEFAULT_COLLECTION_THRESHOLD

MEMORY_USAGE_SAMPLES = 5  # elements MEMORY USAGE inspects in a nested type; 0 would walk the whole key
BIG_KEY_BYTES = 10 * 1024 * 1024  # a key above this is a finding
MAX_GROUPS = 50  # consumer groups reported per stream
STREAM_LAG_WARN = 100_000  # entries a group is behind the stream tail


def _text(value: Any) -> str:
    return value.decode("utf-8", "replace") if isinstance(value, bytes) else str(value)


def _group(raw: Any) -> dict | None:
    """One XINFO GROUPS row as a dict. redis-py returns dicts; a raw RESP2 reply is a flat list."""
    if isinstance(raw, dict):
        row = {_text(k): v for k, v in raw.items()}
    elif isinstance(raw, list | tuple) and len(raw) % 2 == 0:
        row = {_text(raw[i]): raw[i + 1] for i in range(0, len(raw), 2)}
    else:
        return None
    lag = row.get("lag")
    return {
        "name": _text(row.get("name", "")),
        "consumers": int(_num(row.get("consumers"))),
        "pending": int(_num(row.get("pending"))),
        "last_delivered_id": _text(row.get("last-delivered-id", "")),
        "lag": int(_num(lag)) if lag is not None else None,
    }


def _stream_groups(r: Any, key: str, findings: list[dict]) -> dict:
    raw, err = _try(r.execute_command, "XINFO GROUPS", key)
    if err is not None:
        findings.append(_finding("info", f"XINFO GROUPS unavailable ({err})"))
        return {"error": err}
    rows = [g for g in (_group(x) for x in raw) if g] if isinstance(raw, list | tuple) else []
    for g in rows:
        if g["pending"] > 0 and g["consumers"] == 0:
            findings.append(
                _finding(
                    "warning",
                    f"group {g['name']!r} has {g['pending']} pending entries and no consumers; "
                    "they are never acknowledged until a consumer claims them",
                )
            )
        if g["lag"] is not None and g["lag"] >= STREAM_LAG_WARN:
            findings.append(_finding("warning", f"group {g['name']!r} is {g['lag']} entries behind the stream tail"))
    return {"count": len(rows), "groups": rows[:MAX_GROUPS], "truncated": len(rows) > MAX_GROUPS}


def key_info(r: Any, key: str) -> dict:
    """Type, TTL, encoding, memory, length, idle time or LFU counter, and stream groups of ``key``."""
    findings: list[dict] = []
    out: dict[str, Any] = {"key": key, "findings": findings}
    try:
        typ = _text(r.execute_command("TYPE", key))
    except redis.ResponseError as exc:
        message = str(exc)
        if message.startswith(("MOVED", "ASK")):
            findings.append(
                _finding(
                    "warning",
                    f"the key lives on another shard; run this tool against the node that owns its slot ({message})",
                )
            )
        else:
            findings.append(_finding("warning", f"TYPE failed ({message})"))
        out["exists"] = None
        return out
    out["exists"] = typ != "none"
    if typ == "none":
        findings.append(_finding("info", "key does not exist on this node (never set, expired or evicted)"))
        return out
    out["type"] = typ

    pttl, _ = _try(r.execute_command, "PTTL", key)
    if isinstance(pttl, int) and pttl >= 0:
        out["ttl_seconds"] = round(pttl / 1000, 3)
        out["persistent"] = False
    elif pttl == -1:
        out["ttl_seconds"] = None
        out["persistent"] = True
    encoding, _ = _try(r.execute_command, "OBJECT ENCODING", key)
    if encoding is not None:
        out["encoding"] = _text(encoding)
    size, _ = _try(r.execute_command, "MEMORY USAGE", key, "SAMPLES", MEMORY_USAGE_SAMPLES)
    if isinstance(size, int):
        out["memory_bytes"] = size
        if size >= BIG_KEY_BYTES:
            findings.append(_finding("warning", f"big key: {size} bytes (above {BIG_KEY_BYTES})"))
    length_cmd = LENGTH_COMMAND.get(typ)
    if length_cmd:
        length, _ = _try(r.execute_command, length_cmd, key)
        if isinstance(length, int):
            out["length"] = length
            if typ != "string" and length >= DEFAULT_COLLECTION_THRESHOLD:
                findings.append(_finding("warning", f"large {typ}: {length} elements"))
    # IDLETIME works under LRU policies and FREQ under LFU; the other one answers with an error, so ask both.
    idle, _ = _try(r.execute_command, "OBJECT IDLETIME", key)
    if isinstance(idle, int):
        out["idle_seconds"] = idle
    freq, _ = _try(r.execute_command, "OBJECT FREQ", key)
    if isinstance(freq, int):
        out["lfu_frequency"] = freq
    if typ == "stream":
        out["groups"] = _stream_groups(r, key, findings)
    if out.get("persistent") and out.get("memory_bytes", 0) >= BIG_KEY_BYTES:
        findings.append(_finding("info", "big key without TTL: it stays until deleted, whatever the eviction policy"))
    return out
