#!/usr/bin/env python3
"""快照增量存储（delta）与回看方式调用示例（仅依赖标准库）。

用法：
    python3 examples/snapshot_delta_demo.py [base_url]

默认 base_url 为 http://localhost:8000；请在**空库**上运行
（例如 docker compose down -v 后重新 up），脚本按快照编号 1..6 断言。

背景：调度频繁切换水源、隔离和复供时，早期版本每次操作都把全网状态写进
新快照，新增持久化量随管网规模线性增长。本轮改为**全量 + 增量混合存储**：

- 整网导入 / 旧库基线 / 全库首条：写**全量**快照（state 列）；
- 隔离提交、复供确认、水源切换等小范围变更：只写相对上一条快照的
  **净变化**（增量行：base_snapshot_id + delta 列，格式
  water-snapshot-delta/v1），新增持久化量随变化量而非管网规模增长；
- 连续增量过深（默认 32 层）或增量不比全量更小时，自动回写全量。

对调度员与审计员**完全透明**：按编号读取、相邻对照、审计区间核验都还原
操作当时的完整状态与原有字段；摘要链仍按完整状态计算；快照编号、既有
摘要与链的前驱关系不可重写。

本脚本依次演示：
  1. 整网导入            -> 快照 1：storage=full
  2. 隔离提交（关 P4）    -> 快照 2：storage=delta，实存字节远小于完整状态
  3. 复供确认            -> 快照 3：delta；水源停运/恢复 -> 快照 4/5：delta
  4. 回看方式不变         -> 按编号读取仍得完整状态；相邻对照仍得六类净变化
  5. 审计区间核验         -> 跨全量/增量混合区间 verdict=ok
  6. 整网重导            -> 快照 6 回到 full；旧增量快照仍可按编号查询
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


def storage(snapshot_id: int) -> dict:
    status, body = call("GET", f"/api/snapshots/{snapshot_id}/storage")
    assert status == 200, body
    return body


def main() -> None:
    with open(SAMPLE, encoding="utf-8") as f:
        network = json.load(f)

    # 1. 整网导入 -> 快照 1（network_import）：整网重定义，恒为全量存储
    status, body = call("POST", "/api/network/import", network)
    show("1. 整网导入（预期版本 1、快照 1）", status, body)
    assert status == 200 and body["snapshot_id"] == 1, body
    info = storage(1)
    show("1b. 快照 1 存储形态（预期 full，实存 == 完整状态字节数）", 200, info)
    assert info["storage"] == "full" and info["base_snapshot_id"] is None, info
    assert info["stored_bytes"] == info["state_bytes"], info

    # 2. 隔离提交：关 P4 隔离故障 P6 -> 快照 2（小范围变更：增量存储）
    status, body = call(
        "POST",
        "/api/isolation/submit",
        {"fault_pipe_id": "P6", "close_pipe_ids": ["P4"], "version": 1},
    )
    show("2. 隔离提交（预期版本 2、事件 1、快照 2）", status, body)
    assert status == 200 and body["snapshot_id"] == 2, body
    info2 = storage(2)
    show(
        "2b. 快照 2 存储形态（预期 delta：只存 P4 阀位 + 新增故障 + U3 移出）",
        200,
        info2,
    )
    assert info2["storage"] == "delta" and info2["base_snapshot_id"] == 1, info2
    assert info2["stored_bytes"] < info2["state_bytes"] // 2, info2
    print(
        f"    -> 新增持久化量 {info2['stored_bytes']} 字节，"
        f"完整状态 {info2['state_bytes']} 字节（不随管网规模增长）"
    )

    # 3. 复供确认 -> 快照 3；水源停运/恢复 -> 快照 4/5（均为增量）
    status, body = call("POST", "/api/isolation/events/1/restore", {"version": 2})
    show("3a. 复供确认（预期版本 3、快照 3）", status, body)
    assert status == 200 and body["snapshot_id"] == 3, body
    status, body = call(
        "POST",
        "/api/sources/switch",
        {"targets": [{"source_id": "S", "running": False}], "version": 3},
    )
    assert status == 200 and body["snapshot_id"] == 4, body
    status, body = call(
        "POST",
        "/api/sources/switch",
        {"targets": [{"source_id": "S", "running": True}], "version": 4},
    )
    show("3b. 水源停运/恢复（预期快照 4/5）", status, body)
    assert status == 200 and body["snapshot_id"] == 5, body
    for sid, base in ((3, 2), (4, 3), (5, 4)):
        info = storage(sid)
        assert info["storage"] == "delta" and info["base_snapshot_id"] == base, info
    print("    -> 快照 3/4/5 均为增量行，基准依次为上一条快照")

    # 4. 回看方式不变：按编号读取仍还原操作当时完整状态与原有字段
    status, body = call("GET", "/api/snapshots/2")
    show("4a. 读取快照 2（增量行，仍还原当时完整状态）", status, body)
    assert status == 200, body
    assert body["op_type"] == "isolation_submit" and body["network_version"] == 2
    assert body["state"]["open_faults"] == [
        {"event_id": 1, "fault_pipe_id": "P6"}
    ], body
    assert body["state"]["supplied_sink_ids"] == ["U1", "U2"], body
    p4 = next(p for p in body["state"]["pipes"] if p["id"] == "P4")
    assert p4["valve_closed"] is True, body
    assert len(body["state"]["nodes"]) == 7 and len(body["state"]["pipes"]) == 6

    status, body = call("GET", "/api/snapshots/2/diff")
    show("4b. 相邻对照 1 -> 2（六类净变化，与全量存储时一致）", status, body)
    assert status == 200, body
    assert body["valve_positions"]["changed"] == [
        {"pipe_id": "P4", "valve_closed_before": False, "valve_closed_after": True}
    ], body
    assert body["supplied_sinks"]["removed"] == ["U3"], body

    # 5. 审计区间核验：跨全量/增量混合区间逐条重算（摘要按完整状态计算）
    status, chain = call("GET", "/api/audit/chain")
    genesis = chain["genesis_digest"]
    status, probe = call(
        "POST",
        "/api/audit/verify",
        {
            "start_id": 1,
            "end_id": 5,
            "pre_start_digest": genesis,
            "end_digest": PLACEHOLDER_DIGEST,
        },
    )
    assert status == 200 and probe["verdict"] == "boundary_mismatch", probe
    status, body = call(
        "POST",
        "/api/audit/verify",
        {
            "start_id": 1,
            "end_id": 5,
            "pre_start_digest": probe["expected_pre_start_digest"],
            "end_digest": probe["expected_end_digest"],
        },
    )
    show("5. 审计区间核验 1..5（混合存储区间，预期 verdict=ok）", status, body)
    assert status == 200 and body["verdict"] == "ok", body
    assert [d["snapshot_id"] for d in body["digests"]] == [1, 2, 3, 4, 5], body

    # 6. 整网重导：快照 6 回到全量（重导后的新锚点）；旧增量快照仍可查询
    status, body = call("POST", "/api/network/import", network)
    show("6. 整网重导（版本重置 1；快照 6 不覆盖 1-5）", status, body)
    assert status == 200 and body["snapshot_id"] == 6, body
    info6 = storage(6)
    assert info6["storage"] == "full", info6
    status, body = call("GET", "/api/snapshots/2")
    assert status == 200 and body["network_version"] == 2, body
    assert body["state"]["open_faults"] == [
        {"event_id": 1, "fault_pipe_id": "P6"}
    ], body
    print("    -> 快照 6 为全量；快照 2（增量行）重导后仍还原当时完整状态")

    # 汇总：存储形态与持久化量对照
    print("\n=== 存储形态汇总 ===")
    print(f"{'快照':<6}{'操作':<18}{'形态':<8}{'基准':<8}{'实存字节':<12}{'完整状态字节'}")
    for sid in range(1, 7):
        info = storage(sid)
        print(
            f"{sid:<6}{info['op_type']:<18}{info['storage']:<8}"
            f"{str(info['base_snapshot_id']):<8}{info['stored_bytes']:<12}"
            f"{info['state_bytes']}"
        )

    print("\n全部快照增量存储示例执行完成。")


if __name__ == "__main__":
    main()
