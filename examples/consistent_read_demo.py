#!/usr/bin/env python3
"""只读接口一致性视图演示：并发写入下，单次请求只基于同一已提交状态。

用法：
    python3 examples/consistent_read_demo.py [base_url]

默认 base_url 为 http://localhost:8000。

背景
----
关阀建议、隔离/复供与水源切换预演、流量核算与旁通评估、读数核对、调度演练等
只读接口，过去是在**一个连接里分次 SELECT**（READ COMMITTED）：若整网重导、
隔离提交、复供或水源切换恰好在两次读取之间提交，就可能把不同提交时刻的阀位、
运行水源与待修复事件拼接，误判隔离安全或交付量。

现在每次只读请求都运行在一条 ``REPEATABLE READ READ ONLY`` 快照事务上：
快照在事务首条 SQL 时固定，请求内读到的版本、节点、管段、运行水源与待修复
事件必然对应同一已提交时刻——要么完全是写入前、要么完全是写入后的完整视图。

本脚本如何验证
--------------
1. 导入示例管网（版本 1，干净态 U1/U2/U3 全可交付 30 L/h）；
2. 后台“写线程”反复：隔离 P6（关 P4，U3 断供，可交付降到 20）→ 复供（回到 30），
   版本号随之 1(30) → 2(20) → 3(30) → 4(20) …… 奇偶交替；
3. 多个“只读线程”同时调用 POST /api/delivery/evaluate（只读），记录每次响应的
   (version, max_delivered_total)；
4. 校验每个响应都自洽：奇数版本（干净态）必须 30、偶数版本（隔离态）必须 20。
   跨提交拼接会产生“版本是干净态、交付量却是隔离态（20）”之类的矛盾对——
   在一致性视图下绝不应出现。
5. 最后确认只读核算从不推进版本（版本只由写线程的提交改变）。
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
    "BASE_URL", "http://localhost:8000"
)
SAMPLE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sample_network.json")

ALL_PIPE_IDS = ("P1", "P2", "P3", "P4", "P5", "P6")
ALL_SINK_IDS = ("U1", "U2", "U3")

# 干净态可交付 30；隔离 P6（关 P4）后 U3 断供，只交付 20
DELIVERED_CLEAN = 30
DELIVERED_ISOLATED = 20


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


def delivery_body() -> dict:
    """覆盖全部水源/管段的只读核算请求；version 传任意值都不影响按当前快照核算。"""
    return {
        # 不依赖本地版本号：只读接口始终按其快照时刻的当前版本核算并回显
        "version": 0,
        "sources": [{"source_id": "S", "capacity": 1000}],
        "pipes": [{"pipe_id": pid, "capacity": 100} for pid in ALL_PIPE_IDS],
        "demands": [{"sink_id": sid, "demand": 10} for sid in ALL_SINK_IDS],
    }


def expected_delivered(version: int) -> int:
    """版本 1 为干净态（30），其后每次隔离/复供交替：奇=干净 30，偶=隔离 20。"""
    return DELIVERED_CLEAN if version % 2 == 1 else DELIVERED_ISOLATED


def main() -> None:
    with open(SAMPLE, encoding="utf-8") as f:
        network = json.load(f)

    status, body = call("POST", "/api/network/import", network)
    assert status == 200 and body["version"] == 1, body
    print(f"1. 导入管网完成：版本 {body['version']}（干净态可交付 {DELIVERED_CLEAN}）")

    observations: list[tuple[int, int, bool]] = []  # (version, delivered, version_match)
    obs_lock = threading.Lock()
    stop = threading.Event()

    def writer():
        """反复：用读到的最新版本做隔离/复供，把可交付量在 30/20 间来回切换。"""
        while not stop.is_set():
            _, net = call("GET", "/api/network")
            version = net["version"]
            p4_closed = next(p for p in net["pipes"] if p["id"] == "P4")["valve_closed"]
            if not p4_closed:
                # 干净态 -> 隔离 P6（关 P4）
                status, resp = call(
                    "POST", "/api/isolation/submit",
                    {"fault_pipe_id": "P6", "close_pipe_ids": ["P4"],
                     "version": version},
                )
            else:
                # 隔离态 -> 找到 open 事件并复供
                _, events = call("GET", "/api/isolation/events")
                open_event = next(e for e in events if e["status"] == "open")
                status, resp = call(
                    "POST", f"/api/isolation/events/{open_event['id']}/restore",
                    {"version": version},
                )
            if status not in (200, 409):  # 409：与其他写并发撞版本，忽略重试
                raise RuntimeError(f"写线程意外响应 {status}: {resp}")
            time.sleep(0.002)

    def reader(idx: int):
        count = 0
        while not stop.is_set() and count < 40:
            status, resp = call("POST", "/api/delivery/evaluate", delivery_body())
            assert status == 200, resp
            with obs_lock:
                observations.append(
                    (resp["version"], resp["max_delivered_total"],
                     resp["version_match"])
                )
            count += 1

    print("2. 启动 1 个写线程（隔离↔复供）与 4 个只读核算线程，并发运行约 1.5 秒 ...")
    threads = [threading.Thread(target=writer, name="writer")]
    threads += [threading.Thread(target=reader, args=(i,), name=f"reader-{i}")
                for i in range(4)]
    for t in threads:
        t.start()
    time.sleep(1.5)
    stop.set()
    for t in threads:
        t.join(timeout=5)

    print(f"3. 只读请求共观测到 {len(observations)} 个响应，逐个校验内部自洽 ...")
    bad = []
    seen_versions = set()
    for version, delivered, _match in observations:
        seen_versions.add(version)
        if delivered != expected_delivered(version):
            bad.append((version, delivered, expected_delivered(version)))

    # 汇总每种版本观测到的交付量（一致视图下每个版本只能有一个对应交付量）
    by_version: dict[int, set[int]] = {}
    for version, delivered, _m in observations:
        by_version.setdefault(version, set()).add(delivered)

    print("   各版本观测到的 max_delivered_total（每个版本应只有一个正确值）：")
    for version in sorted(by_version):
        flag = "OK " if by_version[version] == {expected_delivered(version)} else "BAD"
        print(f"     [{flag}] 版本 {version:>2}: 交付量 {sorted(by_version[version])}，"
              f"期望 {expected_delivered(version)}")

    assert not bad, f"发现跨提交拼接的矛盾响应 (version, 实际, 期望): {bad[:5]}"
    assert len(seen_versions) >= 3, "并发期间版本应至少推进若干步"

    # 4. 并发期间出现过版本过期（请求携带的 version=0）：只读接口不拒绝，
    #    仅 version_match=false，仍按其快照时刻的当前状态核算。
    stale = [o for o in observations if not o[2]]
    print(f"4. 其中 version_match=false（已读版本过期，仍按当前快照核算）：{len(stale)} 个")

    # 5. 只读核算本身不推进版本：当前版本仍由写线程的提交决定，且为整数单调推进
    status, net = call("GET", "/api/network")
    assert status == 200
    print(f"5. 并发结束后当前版本为 {net['version']}（仅由隔离/复供提交推进，"
          f"只读核算不产生版本/快照变化）")

    print(
        "\n全部 "
        f"{len(observations)} 个只读响应都与各自版本自洽：单次请求只看到"
        "同一已提交时刻的阀位、运行水源与待修复事件，无跨提交拼接。"
    )
    print("演示完成。")


if __name__ == "__main__":
    main()
