"""快照增量存储纯算法测试（不依赖数据库与第三方库）。

覆盖：
- compute_delta / apply_delta 往返一致：还原结果与 freeze_state 的完整
  冻结状态逐字节一致（canonical_json 相同），摘要可按还原状态复现；
- 小范围变更的增量载荷不随未变化的管网规模增长；
- should_store_full 存储决策（强制全量 / 无基线 / 链深上限 / 体积比例）。

运行：python3 -m unittest tests.test_snapshot_delta -v
"""
import os
import sys
import unittest
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import audit  # noqa: E402
from app import snapshot_delta  # noqa: E402
from app.planning import Node, Pipe, freeze_state  # noqa: E402


def build_state(*, valve_closed=(), sources_stopped=(), faults=(), extra=""):
    """构造一个小型双水源管网的完整冻结状态。

    S1 -Q1- C -Q2- U1 ；S2 -Q3- C ；C -Q4- U2（Q* 均带阀）
    """
    nodes = {
        "S1": Node(id="S1", type="source", running="S1" not in sources_stopped),
        "S2": Node(id="S2", type="source", running="S2" not in sources_stopped),
        "C": Node(id="C", type="junction"),
        "U1": Node(id="U1", type="sink"),
        "U2": Node(id="U2", type="sink"),
    }
    if extra:
        nodes[extra] = Node(id=extra, type="junction")
    pipes = {
        "Q1": Pipe(id="Q1", from_node="S1", to_node="C", has_valve=True,
                   valve_closed="Q1" in valve_closed),
        "Q2": Pipe(id="Q2", from_node="C", to_node="U1", has_valve=True,
                   valve_closed="Q2" in valve_closed),
        "Q3": Pipe(id="Q3", from_node="S2", to_node="C", has_valve=True,
                   valve_closed="Q3" in valve_closed),
        "Q4": Pipe(id="Q4", from_node="C", to_node="U2", has_valve=True,
                   valve_closed="Q4" in valve_closed),
    }
    return freeze_state(nodes, pipes, list(faults))


def big_state(n: int, *, close_pipe: str | None = None):
    """构造 n 个用水点的星型大管网：S -Pi- Ji -Ti- Ui（i 从 0 到 n-1）。

    星型拓扑下关闭一条 Pi 只影响 Ui 一个用水点（小范围变更），
    未变化的 n-1 条支路不应产生任何新增持久化量。
    """
    nodes = {"S": Node(id="S", type="source")}
    pipes = {}
    for i in range(n):
        j, u = f"J{i}", f"U{i}"
        nodes[j] = Node(id=j, type="junction")
        nodes[u] = Node(id=u, type="sink")
        pid = f"P{i}"
        pipes[pid] = Pipe(
            id=pid, from_node="S", to_node=j, has_valve=True,
            valve_closed=(pid == close_pipe),
        )
        pipes[f"T{i}"] = Pipe(id=f"T{i}", from_node=j, to_node=u)
    return freeze_state(nodes, pipes, [])


def roundtrip(base, target):
    delta = snapshot_delta.compute_delta(base, target)
    return snapshot_delta.apply_delta(base, delta), delta


class RoundTripTests(unittest.TestCase):
    def assert_roundtrip_identical(self, base, target):
        restored, _delta = roundtrip(base, target)
        self.assertEqual(restored, target)
        # canonical_json 逐字节一致：摘要链可按还原后的完整状态复现计算
        self.assertEqual(
            audit.canonical_json(restored), audit.canonical_json(target)
        )

    def test_valve_toggle(self):
        base = build_state()
        target = build_state(valve_closed=("Q2",))
        self.assert_roundtrip_identical(base, target)

    def test_source_switch(self):
        base = build_state()
        target = build_state(sources_stopped=("S1",))
        self.assert_roundtrip_identical(base, target)

    def test_isolation_submit_adds_fault_and_drops_sink(self):
        base = build_state()
        target = build_state(valve_closed=("Q2",), faults=[(1, "Q2")])
        self.assert_roundtrip_identical(base, target)

    def test_restore_removes_fault(self):
        base = build_state(valve_closed=("Q2",), faults=[(1, "Q2")])
        target = build_state()
        self.assert_roundtrip_identical(base, target)

    def test_network_import_wholesale_replace(self):
        base = build_state()
        other = build_state(extra="J9")
        other["nodes"] = [n for n in other["nodes"] if n["id"] != "U2"]
        other["pipes"] = [p for p in other["pipes"] if p["id"] != "Q4"]
        other["supplied_sink_ids"] = ["U1"]
        self.assert_roundtrip_identical(base, other)

    def test_no_change_yields_empty_delta(self):
        base = build_state()
        restored, delta = roundtrip(base, build_state())
        self.assertEqual(restored, base)
        for section in ("nodes", "pipes", "open_faults"):
            self.assertEqual(delta[section], {"upsert": [], "remove": []})
        self.assertEqual(delta["supplied_sink_ids"], {"add": [], "remove": []})

    def test_chained_deltas_reconstruct(self):
        """连续小变更逐跳套用：与每次直接冻结的完整状态一致。"""
        states = [
            build_state(),
            build_state(valve_closed=("Q2",), faults=[(1, "Q2")]),
            build_state(valve_closed=("Q2",), faults=[(1, "Q2")],
                        sources_stopped=("S1",)),
            build_state(sources_stopped=("S1",)),
        ]
        rebuilt = states[0]
        for target in states[1:]:
            rebuilt = snapshot_delta.apply_delta(
                rebuilt, snapshot_delta.compute_delta(rebuilt, target)
            )
            self.assertEqual(
                audit.canonical_json(rebuilt), audit.canonical_json(target)
            )

    def test_digest_reproducible_from_reconstructed_state(self):
        """新摘要继续以可复现的完整状态计算：还原态与全量态摘要相同。"""
        base = build_state()
        target = build_state(valve_closed=("Q2",), faults=[(1, "Q2")])
        restored, _ = roundtrip(base, target)
        when = datetime(2026, 10, 5, 8, 0, 0, tzinfo=timezone.utc)
        kwargs = dict(
            snapshot_id=7,
            op_type="isolation_submit",
            network_version=2,
            event_id=1,
            created_at=when,
            prev_digest=audit.GENESIS_DIGEST,
        )
        self.assertEqual(
            audit.snapshot_digest(state=restored, **kwargs),
            audit.snapshot_digest(state=target, **kwargs),
        )


class IncrementalSizeTests(unittest.TestCase):
    def test_small_change_delta_size_independent_of_network_size(self):
        """小范围状态变更的新增持久化量不随未变化的管网规模线性增长。"""
        sizes = {}
        for n in (50, 200, 800):
            base = big_state(n)
            target = big_state(n, close_pipe="P3")
            delta = snapshot_delta.compute_delta(base, target)
            sizes[n] = (
                snapshot_delta.delta_payload_size(delta),
                snapshot_delta.state_payload_size(target),
            )
        # 全量状态随管网规模线性增长
        self.assertGreater(sizes[800][1], 4 * sizes[50][1])
        # 增量载荷基本恒定（只含 1 条管段对象 + 少量可达用水点净变化），
        # 且不随规模增长（800 与 50 的增量大小相当，远小于各自全量）
        self.assertLess(sizes[800][0], 2 * sizes[50][0])
        for n, (delta_size, full_size) in sizes.items():
            self.assertLess(delta_size, full_size // 10, f"n={n}")

    def test_delta_payload_contains_only_changed_entities(self):
        base = big_state(100)
        target = big_state(100, close_pipe="P7")
        delta = snapshot_delta.compute_delta(base, target)
        self.assertEqual([p["id"] for p in delta["pipes"]["upsert"]], ["P7"])
        self.assertEqual(delta["pipes"]["remove"], [])
        self.assertEqual(delta["nodes"], {"upsert": [], "remove": []})
        # 星型拓扑下 P7 关闭仅 U7 失供：可达集合净变化只有一项
        self.assertEqual(delta["supplied_sink_ids"]["add"], [])
        self.assertEqual(delta["supplied_sink_ids"]["remove"], ["U7"])


class StorageDecisionTests(unittest.TestCase):
    def test_forced_full_cases(self):
        self.assertTrue(snapshot_delta.should_store_full(
            force_full=True, base_available=True, next_depth=1,
            max_depth=32, delta_size=1, full_size=1000, full_ratio=0.6,
        ))
        self.assertTrue(snapshot_delta.should_store_full(
            force_full=False, base_available=False, next_depth=1,
            max_depth=32, delta_size=1, full_size=1000, full_ratio=0.6,
        ))

    def test_depth_limit(self):
        self.assertTrue(snapshot_delta.should_store_full(
            force_full=False, base_available=True, next_depth=33,
            max_depth=32, delta_size=1, full_size=1000, full_ratio=0.6,
        ))
        self.assertFalse(snapshot_delta.should_store_full(
            force_full=False, base_available=True, next_depth=32,
            max_depth=32, delta_size=1, full_size=1000, full_ratio=0.6,
        ))

    def test_ratio_limit(self):
        self.assertTrue(snapshot_delta.should_store_full(
            force_full=False, base_available=True, next_depth=1,
            max_depth=32, delta_size=600, full_size=1000, full_ratio=0.6,
        ))
        self.assertFalse(snapshot_delta.should_store_full(
            force_full=False, base_available=True, next_depth=1,
            max_depth=32, delta_size=599, full_size=1000, full_ratio=0.6,
        ))

    def test_zero_max_depth_disables_delta(self):
        self.assertTrue(snapshot_delta.should_store_full(
            force_full=False, base_available=True, next_depth=1,
            max_depth=0, delta_size=1, full_size=1000, full_ratio=0.6,
        ))


if __name__ == "__main__":
    unittest.main()
