"""node_key_info: metadata of one key, stream groups included, and the allowlist around XINFO."""

from __future__ import annotations

import pytest
import redis

from mcp_elasticache_ops import dataplane
from mcp_elasticache_ops.safe_client import ReadOnlyViolation, check
from tests.fakes import FakeRedis


def _commands(key: str, typ: str, **extra) -> dict:
    base = {
        f"TYPE {key}": typ,
        f"PTTL {key}": -1,
        f"OBJECT ENCODING {key}": "raw",
        f"MEMORY USAGE {key} SAMPLES 5": 64,
        f"OBJECT IDLETIME {key}": 12,
        f"OBJECT FREQ {key}": redis.ResponseError("An LFU maxmemory policy is not selected"),
    }
    base.update(extra)
    return base


def test_missing_key_reports_it_and_stops():
    r = FakeRedis()
    r.commands = {"TYPE nope": "none"}
    out = dataplane.key_info(r, "nope")
    assert out["exists"] is False
    assert "type" not in out
    assert out["findings"][0]["severity"] == "info"


def test_string_key_without_ttl():
    r = FakeRedis()
    r.commands = _commands("k", "string", **{"STRLEN k": 3})
    out = dataplane.key_info(r, "k")
    assert out["type"] == "string"
    assert out["persistent"] is True and out["ttl_seconds"] is None
    assert out["encoding"] == "raw"
    assert out["memory_bytes"] == 64
    assert out["length"] == 3
    assert out["idle_seconds"] == 12
    assert "lfu_frequency" not in out  # the node answered OBJECT FREQ with an error under an LRU policy
    assert out["findings"] == []


def test_ttl_is_reported_in_seconds():
    r = FakeRedis()
    r.commands = _commands("k", "hash", **{"PTTL k": 90_500, "HLEN k": 2})
    out = dataplane.key_info(r, "k")
    assert out["ttl_seconds"] == 90.5
    assert out["persistent"] is False


def test_big_key_and_large_collection_are_findings():
    r = FakeRedis()
    r.commands = _commands("k", "list", **{"MEMORY USAGE k SAMPLES 5": 20 * 1024 * 1024, "LLEN k": 50_000})
    out = dataplane.key_info(r, "k")
    messages = [f["message"] for f in out["findings"]]
    assert any(m.startswith("big key") for m in messages)
    assert any(m.startswith("large list") for m in messages)
    assert any("without TTL" in m for m in messages)


def test_lfu_counter_when_the_policy_is_lfu():
    r = FakeRedis()
    r.commands = _commands(
        "k",
        "string",
        **{
            "STRLEN k": 1,
            "OBJECT IDLETIME k": redis.ResponseError("An LFU maxmemory policy is selected, idle time not tracked"),
            "OBJECT FREQ k": 7,
        },
    )
    out = dataplane.key_info(r, "k")
    assert out["lfu_frequency"] == 7
    assert "idle_seconds" not in out


def test_stream_groups_and_orphaned_pending_entries():
    groups = [
        {"name": "workers", "consumers": 0, "pending": 4, "last-delivered-id": "1-1", "entries-read": 9, "lag": 3},
        {"name": "audit", "consumers": 2, "pending": 0, "last-delivered-id": "1-9", "lag": 250_000},
    ]
    r = FakeRedis()
    r.commands = _commands("s", "stream", **{"XLEN s": 10, "XINFO GROUPS s": groups})
    out = dataplane.key_info(r, "s")
    assert out["groups"]["count"] == 2
    assert out["groups"]["groups"][0] == {
        "name": "workers",
        "consumers": 0,
        "pending": 4,
        "last_delivered_id": "1-1",
        "lag": 3,
    }
    messages = [f["message"] for f in out["findings"]]
    assert any("'workers'" in m and "no consumers" in m for m in messages)
    assert any("'audit'" in m and "behind" in m for m in messages)


def test_stream_groups_accept_a_raw_resp2_reply():
    flat = [["name", "g", "consumers", 1, "pending", 0, "last-delivered-id", "0-0", "lag", None]]
    r = FakeRedis()
    r.commands = _commands("s", "stream", **{"XLEN s": 0, "XINFO GROUPS s": flat})
    out = dataplane.key_info(r, "s")
    assert out["groups"]["groups"][0]["lag"] is None
    assert out["findings"] == []


def test_stream_group_cap():
    groups = [{"name": f"g{i}", "consumers": 1, "pending": 0} for i in range(60)]
    r = FakeRedis()
    r.commands = _commands("s", "stream", **{"XLEN s": 0, "XINFO GROUPS s": groups})
    out = dataplane.key_info(r, "s")
    assert out["groups"]["count"] == 60
    assert len(out["groups"]["groups"]) == dataplane.MAX_GROUPS
    assert out["groups"]["truncated"] is True


def test_moved_names_the_owner():
    r = FakeRedis()
    r.commands = {"TYPE k": redis.ResponseError("MOVED 3999 10.0.0.2:6379")}
    out = dataplane.key_info(r, "k")
    assert out["exists"] is None
    assert "another shard" in out["findings"][0]["message"]
    assert "10.0.0.2:6379" in out["findings"][0]["message"]


def test_xinfo_groups_is_allowed_and_the_value_leaking_forms_are_not():
    check(("XINFO GROUPS", "s"))
    check(("XINFO", "GROUPS", "s"))
    for args in (
        ("XINFO STREAM", "s"),
        ("XINFO", "STREAM", "s", "FULL"),
        ("XINFO CONSUMERS", "s", "g"),
        ("XINFO HELP",),
        ("XINFO",),
    ):
        with pytest.raises(ReadOnlyViolation):
            check(args)
    with pytest.raises(ReadOnlyViolation):
        check(("XINFO GROUPS", "a", "b"))
