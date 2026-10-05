#!/usr/bin/env python3
"""并发复供确认示例：两个调度员基于同一版本确认不同事件时的最终串行判定。

用法：
    python3 examples/restore_concurrency_demo.py [base_url]

默认 base_url 为 http://localhost:8000。仅依赖标准库（urllib + threading）。

背景
----
复供确认是写操作：重开阀门、版本 +1、事件置 restored、同事务冻结快照。
旧实现取锁顺序为“先锁自家事件行 → 再锁 network_meta 版本行 → 最后锁其余
事件行”。两个调度员**基于同一版本并发确认两条不同的待修复事件**时：

    请求 A：锁事件1 → 等版本行
    请求 B：锁事件2 → 等版本行
    二者拿到版本行后又互相等待对方的事件行 → 数据库检测到锁环，
    把其中一个事务中止为死锁（HTTP 500）；与整网重导（先版本行后 TRUNCATE
    事件表）交错时锁序相反，同样可能死锁。

修复后所有写事务共用**同一把锁序**：先锁 network_meta 版本行（单行，天然
串行点），再按事件 id 升序锁事件行。并发确认因此被串行化，并在锁内按
**最终已提交状态**复核：

- 两个原本单独看都安全的同版确认，**至多一个成功（200）**，另一个沿用
  `409 VERSION_CONFLICT`（携带 current_version / expected_version），
  客户端重读后重预演即可；
- 与整网重导交错：重导先提交则旧事件被清空，复供沿用 `404 EVENT_NOT_FOUND`；
  复供先成功则重导随后整体重写（复供结果随整网替换失效，符合既有语义）；
- 事件已复供沿用 `409 EVENT_ALREADY_RESTORED`；最终状态下不安全沿用
  `422 RESTORE_UNSAFE`；
- 失败请求**不部分重开阀门、不推进版本、不留下事件与快照**。

本脚本演示
----------
1. 导入“两条互不影响的支管”管网，提交两条隔离事件（事件 1/2，版本 3），
   各自的复供单独预演都 safe=true；
2. 两个线程**同时**带 version=3 确认复供事件 1 与事件 2，断言恰好一个 200、
   另一个 409 VERSION_CONFLICT，并核对版本恰好 +1、只有成功方阀门重开、
   只产生一条复供快照；
3. 重新制造两条事件，复供线程与整网重导线程并发：无 500，复供结果只能是
   200（赢）或 404 EVENT_NOT_FOUND（重导赢，旧事件消失）；
4. 串行复核 404（不存在）、409（已复供）、422（不安全：叠加故障场景）。

整个过程只使用公开 HTTP API；不需要数据库直连或额外依赖。
"""
import json
import os
import sys
import threading
import urllib.error
import urllib.request

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else os.environ.get(
    "BASE_URL", "http://localhost:8000"
)

# 两条互不影响的独立支管（同一版本下两个复供互不影响、都安全）：
# S --V1[阀]-- J1 --F1-- U1
# S --V2[阀]-- J2 --F2-- U2
TWIN_BRANCH_NETWORK = {
    "nodes": [
        {"id": "S", "type": "source"},
        {"id": "J1", "type": "junction"},
        {"id": "J2", "type": "junction"},
        {"id": "U1", "type": "sink"},
        {"id": "U2", "type": "sink"},
    ],
    "pipes": [
        {"id": "V1", "from": "S", "to": "J1", "has_valve": True},
        {"id": "F1", "from": "J1", "to": "U1"},
        {"id": "V2", "from": "S", "to": "J2", "has_valve": True},
        {"id": "F2", "from": "J2", "to": "U2"},
    ],
}

# 叠加故障片区（用于 422 RESTORE_UNSAFE 演示）：
# S-P1[阀]-A-P2[阀]-B-P4[阀]-D-P6(无阀,外层故障)-U3
#                    |                |
#                  P3[阀]            P7(无阀,区内故障)-E-P8[阀]-U4
NESTED_NETWORK = {
    "nodes": [
        {"id": "S", "type": "source"},
        {"id": "A", "type": "junction"},
        {"id": "B", "type": "junction"},
        {"id": "D", "type": "junction"},
        {"id": "E", "type": "junction"},
        {"id": "U1", "type": "sink"},
        {"id": "U2", "type": "sink"},
        {"id": "U3", "type": "sink"},
        {"id": "U4", "type": "sink"},
    ],
    "pipes": [
        {"id": "P1", "from": "S", "to": "A", "has_valve": True},
        {"id": "P2", "from": "A", "to": "B", "has_valve": True},
        {"id": "P3", "from": "A", "to": "U1", "has_valve": True},
        {"id": "P5", "from": "B", "to": "U2", "has_valve": True},
        {"id": "P4", "from": "B", "to": "D", "has_valve": True},
        {"id": "P6", "from": "D", "to": "U3", "has_valve": False},
        {"id": "P7", "from": "D", "to": "E", "has_valve": False},
        {"id": "P8", "from": "E", "to": "U4", "has_valve": True},
    ],
}


def call(method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def show(title: str, status: int, body: dict) -> None:
    print(f"\n=== {title} -> HTTP {status} ===")
    print(json.dumps(body, ensure_ascii=False, indent=2))


def require(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def submit_isolation(fault_pipe_id: str, close: list[str], version: int) -> int:
    st, body = call(
        "POST",
        "/api/isolation/submit",
        {"fault_pipe_id": fault_pipe_id,
         "close_pipe_ids": close, "version": version},
    )
    require(st == 200, f"隔离提交失败：{status_label(st, body)}")
    return body["event_id"]


def status_label(st: int, body: dict) -> str:
    code = body.get("error", {}).get("code", "OK")
    return f"{st} {code}"


def get_network() -> dict:
    st, body = call("GET", "/api/network")
    require(st == 200, f"读网失败：{st}")
    return body


def latest_snapshot_id() -> int | None:
    """读取现存最大快照编号（列表升序分页，用 after 游标取尾页）。"""
    after: int | None = None
    last = None
    while True:
        suffix = "" if after is None else f"&after_id={after}"
        st, body = call("GET", f"/api/snapshots?limit=500{suffix}")
        require(st == 200, f"快照列表失败：{st}")
        if not body:
            return last
        last = body[-1]["id"]
        after = last


def demo_concurrent_safe_confirms() -> None:
    print("\n########## 1. 两个同版安全确认并发：恰好一个成功，另一个 409 ##########")
    st, body = call("POST", "/api/network/import", TWIN_BRANCH_NETWORK)
    require(st == 200, body)
    e1 = submit_isolation("F1", ["V1"], 1)
    e2 = submit_isolation("F2", ["V2"], 2)
    version = 3
    print(f"已提交事件 {e1}(故障 F1/关 V1)、{e2}(故障 F2/关 V2)，当前版本 {version}")

    # 两个复供单独预演都安全
    for eid in (e1, e2):
        st, body = call(
            "POST", f"/api/isolation/events/{eid}/restore/preview",
            {"version": version},
        )
        require(st == 200 and body["safe"], f"事件 {eid} 预演应安全：{body}")
    print("两个事件各自预演 safe=true（单独复供都安全）")

    snap_before = latest_snapshot_id()
    results: dict[str, tuple[int, dict]] = {}

    def confirm(tag: str, event_id: int) -> None:
        results[tag] = call(
            "POST", f"/api/isolation/events/{event_id}/restore",
            {"version": version},
        )

    t1 = threading.Thread(target=confirm, args=("事件1", e1))
    t2 = threading.Thread(target=confirm, args=("事件2", e2))
    t1.start(); t2.start(); t1.join(); t2.join()

    for tag, (st, body) in results.items():
        print(f"{tag} -> {status_label(st, body)}")
        if st == 200:
            show(f"{tag} 复供成功响应", st, body)
        else:
            err = body["error"]
            require(err["code"] == "VERSION_CONFLICT", body)
            print(f"   冲突明细：current_version={err['current_version']}, "
                  f"expected_version={err['expected_version']}")

    labels = sorted(status_label(st, body) for st, body in results.values())
    require(labels == ["200 OK", "409 VERSION_CONFLICT"],
            f"并发结果不符合串行判定：{labels}")

    net = get_network()
    require(net["version"] == version + 1,
            f"版本应恰好 +1 为 {version + 1}，实际 {net['version']}")
    closed = sorted(p["id"] for p in net["pipes"] if p.get("valve_closed"))
    require(len(closed) == 1, f"应恰好一个阀门仍关闭，实际 {closed}")
    snap_after = latest_snapshot_id()
    winner_body = next(b for st, b in results.values() if st == 200)
    # 失败方不得留下快照：并发期间只产生成功方响应里那一条复供快照
    require(snap_after == winner_body["snapshot_id"] == (snap_before + 1),
            f"快照异常：before={snap_before} after={snap_after} "
            f"winner={winner_body.get('snapshot_id')}")
    print(f"核对通过：版本 {version}→{version + 1}；仍关闭阀门 {closed}；"
          f"并发期间仅新增快照 {snap_after}（失败方无部分写入、无快照）")

    # 输家按 409 提示重读版本后重预演：另一条故障仍在时同样安全，可成功复供
    loser = e1 if results["事件1"][0] == 409 else e2
    latest = net["version"]
    st, body = call(
        "POST", f"/api/isolation/events/{loser}/restore/preview",
        {"version": latest},
    )
    require(st == 200 and body["safe"], body)
    st, body = call(
        "POST", f"/api/isolation/events/{loser}/restore", {"version": latest}
    )
    show(f"输家重读版本({latest})后重新确认事件 {loser}", st, body)
    require(st == 200, body)
    require(get_network()["version"] == latest + 1, "输家重试后版本应再 +1")


def demo_restore_vs_reimport() -> None:
    print("\n########## 2. 复供与整网重导并发：无 500；输者按语义 404/200 ##########")
    st, _ = call("POST", "/api/network/import", TWIN_BRANCH_NETWORK)
    require(st == 200, st)
    e1 = submit_isolation("F1", ["V1"], 1)
    e2 = submit_isolation("F2", ["V2"], 2)
    version = 3
    print(f"已提交事件 {e1}/{e2}，版本 {version}；复供事件 {e1} 与整网重导并发")

    outcomes: dict[str, tuple[int, dict]] = {}

    def do_restore() -> None:
        outcomes["复供"] = call(
            "POST", f"/api/isolation/events/{e1}/restore", {"version": version}
        )

    def do_reimport() -> None:
        outcomes["重导"] = call("POST", "/api/network/import", TWIN_BRANCH_NETWORK)

    t1 = threading.Thread(target=do_restore)
    t2 = threading.Thread(target=do_reimport)
    t1.start(); t2.start(); t1.join(); t2.join()

    for tag, (st, body) in outcomes.items():
        print(f"{tag} -> {status_label(st, body)}")
    require(all(st < 500 for st, _ in outcomes.values()),
            f"并发出现 5xx（死锁回归）：{outcomes}")
    require(outcomes["重导"][0] == 200, outcomes)
    restore_st, restore_body = outcomes["复供"]
    code = restore_body.get("error", {}).get("code", "OK")
    require(code in ("OK", "EVENT_NOT_FOUND", "VERSION_CONFLICT"), restore_body)
    # 重导最终落地：版本重置为 1、事件清空、阀门全开
    net = get_network()
    require(net["version"] == 1 and not any(
        p.get("valve_closed") for p in net["pipes"]
    ), "重导后状态应为版本 1、阀门全开")
    st, events = call("GET", "/api/isolation/events")
    require(st == 200 and events == [], events)
    if code == "EVENT_NOT_FOUND":
        print("重导先提交：旧事件随整网替换清空，复供沿用 404 EVENT_NOT_FOUND")
    elif code == "OK":
        print("复供先提交成功（200），随后整网重导把版本重置为 1、清空事件")
    else:
        print("复供排在重导之后且版本已重置：409 VERSION_CONFLICT")


def demo_serial_error_codes() -> None:
    print("\n########## 3. 串行复核：404 / 409 已复供 / 422 不安全 ##########")
    call("POST", "/api/network/import", TWIN_BRANCH_NETWORK)
    e1 = submit_isolation("F1", ["V1"], 1)

    st, body = call("POST", "/api/isolation/events/99999/restore", {"version": 3})
    show("复供不存在的事件", st, body)
    require((st, body["error"]["code"]) == (404, "EVENT_NOT_FOUND"), body)

    # 404 之后重置为叠加故障管网（404 不改变任何状态，但演示需要该管网）
    call("POST", "/api/network/import", NESTED_NETWORK)
    e_outer = submit_isolation("P6", ["P4"], 1)
    e_inner = submit_isolation("P7", [], 2)
    version = 3
    st, preview = call(
        "POST", f"/api/isolation/events/{e_outer}/restore/preview",
        {"version": version},
    )
    require(st == 200 and not preview["safe"], preview)
    print(f"外层事件 {e_outer} 预演 safe=false：重开 P4 会让区内故障 "
          f"{preview['block_reasons']}")
    st, body = call(
        "POST", f"/api/isolation/events/{e_outer}/restore", {"version": version}
    )
    show(f"确认复供外层事件 {e_outer}（区内故障 P7 未修复）", st, body)
    require((st, body["error"]["code"]) == (422, "RESTORE_UNSAFE"), body)
    net = get_network()
    require(net["version"] == version, "422 后版本不得推进")

    # 先复供区内事件（安全），再重复复供它 -> 409 EVENT_ALREADY_RESTORED
    st, body = call(
        "POST", f"/api/isolation/events/{e_inner}/restore", {"version": version}
    )
    require(st == 200, body)
    st, body = call(
        "POST", f"/api/isolation/events/{e_inner}/restore", {"version": version + 1}
    )
    show(f"重复复供已 restored 的事件 {e_inner}", st, body)
    require((st, body["error"]["code"]) == (409, "EVENT_ALREADY_RESTORED"), body)

    # 旧版本请求在外层事件变安全后仍按版本判定 -> 409，不重开阀门
    st, body = call(
        "POST", f"/api/isolation/events/{e_outer}/restore", {"version": version}
    )
    require((st, body["error"]["code"]) == (409, "VERSION_CONFLICT"), body)
    print("用旧版本 3 复供外层事件 -> 409 VERSION_CONFLICT（当前版本已为 4）")
    st, body = call(
        "POST", f"/api/isolation/events/{e_outer}/restore", {"version": version + 1}
    )
    show(f"用最新版本 4 复供外层事件 {e_outer}", st, body)
    require(st == 200, body)


def main() -> None:
    print(f"目标服务：{BASE_URL}")
    demo_concurrent_safe_confirms()
    demo_restore_vs_reimport()
    demo_serial_error_codes()
    print("\n全部并发复供示例执行完成：无死锁/500，失败请求均无部分写入。")


if __name__ == "__main__":
    main()
