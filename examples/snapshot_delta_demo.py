#!/usr/bin/env python3
"""快照增量存储与回看方式调用示例（仅依赖标准库）。

用法：
    python3 examples/snapshot_delta_demo.py [base_url]

默认 base_url 为 http://localhost:8000。

背景：调度频繁切换水源、隔离和复供时，每次操作只改变少量阀位 / 水源运行
状态 / 待修复故障。自本轮起，这类**小范围变更**的快照不再把全网状态整体
落盘，而是在同一事务内只写一行“相对上一条快照的净变化”（增量载荷，
表 state_snapshot_deltas）；整网导入、旧库基线、增量链过深或变化面过大
时仍写全量。快照编号、摘要链前驱关系与摘要计算口径（按完整冻结状态的
canonical_json）完全不变，HTTP 接口与错误语义不变。

本脚本依次演示（回看方式与既有版本完全一致）：
  1. 整网导入            -> 快照 N（network_import，强制全量存储）
  2. 隔离提交（关 P4）    -> 快照 N+1（isolation_submit，增量存储）
  3. 复供确认            -> 快照 N+2（restore_confirm，增量存储）
  4. 水源停运/恢复       -> 快照 N+3/N+4（source_switch，增量存储）
  5. 按编号读取          -> 仍返回操作当时的**完整**冻结状态（读取时透明还原）
  6. 相邻对照            -> 六类净变化，字段与口径不变
  7. 审计区间核验        -> 摘要按还原后的完整状态复现，verdict=ok
  8. 整网重导            -> 版本重置为 1，旧快照编号不覆盖、仍可查询

脚本末尾打印运维可直接执行的 SQL，用于查看本轮的存储格式
（state_format / state_snapshot_deltas 增量行）。
"""
import json
import os
import sys
import urllib.error
import urllib.request

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
    "BASE_URL", "http://localhost:8000"
)
SAMPLE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "sample_network.json"
)

# 64 位十六进制占位摘要（仅用于第一次“取边界”调用，必然不匹配）
PLACEHOLDER_DIGEST = "0" * 64


def call(method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def show(title: str, status: int, body: dict) -> None:
    print(f"\n=== {title} -> HTTP {status} ===")
    print(json.dumps(body, ensure_ascii=False, indent=2))


def main() -> None:
    with open(SAMPLE, encoding="utf-8") as f:
        network = json.load(f)

    # 1. 整网导入：快照 N（network_import 强制全量，作为增量链的回溯终点）
    status, body = call("POST", "/api/network/import", network)
    show("1. 整网导入（全量快照）", status, body)
    assert status == 200 and body["version"] == 1, body
    snap_import = body["snapshot_id"]

    # 2. 隔离提交：关 P4 隔离故障 P6 -> 快照 N+1（小范围变更，增量存储）
    status, body = call(
        "POST",
        "/api/isolation/submit",
        {"fault_pipe_id": "P6", "close_pipe_ids": ["P4"], "version": 1},
    )
    show("2. 隔离提交（增量快照，U3 失供）", status, body)
    assert status == 200, body
    event_id = body["event_id"]
    snap_isolation = body["snapshot_id"]
    assert snap_isolation == snap_import + 1, body

    # 3. 复供确认：重开 P4 -> 快照 N+2（增量存储）
    status, body = call(
        "POST", f"/api/isolation/events/{event_id}/restore", {"version": 2}
    )
    show("3. 复供确认（增量快照，U3 恢复）", status, body)
    assert status == 200 and body["version"] == 3, body
    snap_restore = body["snapshot_id"]
    assert snap_restore == snap_isolation + 1, body

    # 4. 水源停运再恢复 -> 快照 N+3 / N+4（增量存储）
    status, body = call(
        "POST",
        "/api/sources/switch",
        {"targets": [{"source_id": "S", "running": False}], "version": 3},
    )
    show("4a. 水源 S 停运（增量快照）", status, body)
    assert status == 200 and body["changed"] is True, body
    snap_stop = body["snapshot_id"]
    assert snap_stop == snap_restore + 1, body
    status, body = call(
        "POST",
        "/api/sources/switch",
        {"targets": [{"source_id": "S", "running": True}], "version": 4},
    )
    show("4b. 水源 S 恢复（增量快照）", status, body)
    assert status == 200 and body["changed"] is True, body
    snap_restart = body["snapshot_id"]
    assert snap_restart == snap_stop + 1, body

    # 5. 按编号读取：增量存储的快照仍返回操作当时的完整冻结状态
    status, body = call("GET", f"/api/snapshots/{snap_isolation}")
    show("5. 按编号读取隔离后快照（完整状态透明还原）", status, body)
    assert status == 200, body
    state = body["state"]
    assert body["op_type"] == "isolation_submit", body
    assert state["open_faults"] == [
        {"event_id": event_id, "fault_pipe_id": "P6"}
    ], body
    assert state["supplied_sink_ids"] == ["U1", "U2"], body
    valves = {p["id"]: p["valve_closed"] for p in state["pipes"]}
    assert valves["P4"] is True, body  # 当时 P4 已关闭

    # 5b. 快照列表：编号升序连续，游标分页口径不变
    status, body = call("GET", f"/api/snapshots?after_id={snap_import - 1}&limit=100")
    show("5b. 快照列表（编号升序）", status, body)
    assert status == 200, body
    assert [r["id"] for r in body] == [
        snap_import, snap_isolation, snap_restore, snap_stop, snap_restart
    ], body

    # 6. 相邻对照：复供快照 vs 隔离快照 -> P4 重开、故障移除、U3 恢复供水
    status, body = call("GET", f"/api/snapshots/{snap_restore}/diff")
    show("6. 相邻对照（复供 vs 隔离）", status, body)
    assert status == 200, body
    assert body["base_snapshot_id"] == snap_isolation, body
    assert body["valve_positions"]["changed"] == [
        {"pipe_id": "P4", "valve_closed_before": True, "valve_closed_after": False}
    ], body
    assert body["open_faults"]["removed"] == [
        {"event_id": event_id, "fault_pipe_id": "P6"}
    ], body
    assert body["supplied_sinks"]["added"] == ["U3"], body

    # 7. 审计区间核验：摘要按还原后的完整状态复现，链不断、口径不变
    status, chain = call("GET", "/api/audit/chain")
    show("7a. 摘要链信息", status, chain)
    assert status == 200 and chain["latest_snapshot_id"] == snap_restart, chain
    genesis = chain["genesis_digest"]
    status, probe = call(
        "POST",
        "/api/audit/verify",
        {
            "start_id": snap_import,
            "end_id": snap_restart,
            "pre_start_digest": genesis,
            "end_digest": PLACEHOLDER_DIGEST,
        },
    )
    assert status == 200 and probe["verdict"] == "boundary_mismatch", probe
    saved_pre = probe["expected_pre_start_digest"]
    saved_end = probe["expected_end_digest"]
    status, body = call(
        "POST",
        "/api/audit/verify",
        {
            "start_id": snap_import,
            "end_id": snap_restart,
            "pre_start_digest": saved_pre,
            "end_digest": saved_end,
        },
    )
    show("7b. 全区间核验（增量快照同样逐条重算，预期 ok）", status, body)
    assert status == 200 and body["ok"] is True, body
    assert [d["snapshot_id"] for d in body["digests"]] == [
        snap_import, snap_isolation, snap_restore, snap_stop, snap_restart
    ], body

    # 8. 整网重导：版本重置为 1，新快照继续递增（强制全量），旧快照仍可查询
    status, body = call("POST", "/api/network/import", network)
    show("8a. 整网重导（版本重置为 1，旧快照不覆盖）", status, body)
    assert status == 200 and body["version"] == 1, body
    assert body["snapshot_id"] == snap_restart + 1, body
    status, body = call("GET", f"/api/snapshots/{snap_isolation}")
    show("8b. 重导后旧快照仍按当时状态还原", status, body)
    assert status == 200, body
    assert body["state"]["open_faults"] == [
        {"event_id": event_id, "fault_pipe_id": "P6"}
    ], body
    assert body["state"]["supplied_sink_ids"] == ["U1", "U2"], body

    print(
        """
=== 9. 存储格式运维查验（psql 直接执行，只读） ===

-- 各快照的存储形式与落盘体量：full 为全量、delta 为增量（state 为 NULL）
SELECT id, op_type, state_format,
       pg_column_size(state) AS state_bytes
FROM   state_snapshots
ORDER  BY id;

-- 增量行：相对上一条现存快照（base_snapshot_id）的净变化与链深
SELECT snapshot_id, base_snapshot_id, depth,
       jsonb_pretty(delta) AS delta
FROM   state_snapshot_deltas
ORDER  BY snapshot_id;

-- 摘要链不因增量存储而改变：仍是一快照一摘要、前驱连续
SELECT snapshot_id, digest, prev_digest
FROM   state_snapshot_chains
ORDER  BY snapshot_id;

回看方式（HTTP 不变）：
  GET  /api/snapshots/{id}        按编号读取当时完整状态（增量透明还原）
  GET  /api/snapshots/{id}/diff   相邻快照六类净变化对照
  POST /api/audit/verify          凭事先保存的边界摘要只读核验任意区间
"""
    )
    print("全部断言通过：增量存储下回看、对照与审计核验行为与既有版本一致。")


if __name__ == "__main__":
    main()
