"""快照增量存储（delta）纯算法测试（不依赖数据库与第三方库）。

覆盖：
- snapshot_state_delta / apply_snapshot_state_delta 的往返还原：
  隔离、复供、水源切换等操作序列中，每一步 增量套用(上一条状态) == 当步完整状态；
- 还原结果与 freeze_state 直接冻结的载荷逐字节等价（canonical_json 一致），
  因此摘要链按还原后完整状态计算的结果与存储形态无关；
- 增量载荷只含净变化（阀位/故障/供水点/水源运行状态），
  其大小随变化量增长、不随未变化的管网规模增长；
- 节点/管段增删、空增量、未知格式拒绝等边界。

运行：python3 -m unittest tests.test_snapshot_delta -v
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import audit  # noqa: E402
from app.planning import (  # noqa: E402
    Node,
    Pipe,
    SNAPSHOT_DELTA_FORMAT,
    apply_snapshot_state_delta,
    freeze_state,
    snapshot_state_delta,
)


def _network():
    """与 tests.test_api 相同的演示管网：S--P1--A--P2--B--P4--D--P6--U3 等。"""
    nodes = {
        "S": Node(id="S", type="source", name="一水厂"),
        "A": Node(id="A", type="junction"),
        "B": Node(id="B", type="junction"),
        "D": Node(id="D", type="junction"),
        "U1": Node(id="U1", type="sink"),
        "U2": Node(id="U2", type="sink"),
        "U3": Node(id="U3", type="sink"),
    }
    pipes = {
        "P1": Pipe(id="P1", from_node="S", to_node="A", has_valve=True),
        "P2": Pipe(id="P2", from_node="A", to_node="B", has_valve=True),
        "P3": Pipe(id="P3", from_node="A", to_node="U1", has_valve=True),
        "P4": Pipe(id="P4", from_node="B", to_node="D", has_valve=True),
        "P5": Pipe(id="P5", from_node="B", to_node="U2", has_valve=True),
        "P6": Pipe(id="P6", from_node="D", to_node="U3", has_valve=False),
    }
    return nodes, pipes


def _close(pipes, pipe_id):
    p = pipes[pipe_id]
    pipes[pipe_id] = Pipe(p.id, p.from_node, p.to_node, p.has_valve, True, p.name)


def _open(pipes, pipe_id):
    p = pipes[pipe_id]
    pipes[pipe_id] = Pipe(p.id, p.from_node, p.to_node, p.has_valve, False, p.name)


def _switch(nodes, source_id, running):
    n = nodes[source_id]
    nodes[source_id] = Node(n.id, n.type, n.name, running)


class DeltaRoundTripTests(unittest.TestCase):
    def test_operation_sequence_round_trips_exactly(self):
        """导入 → 隔离 → 复供 → 停运 → 恢复：每步增量套用后 == 当步完整状态。"""
        nodes, pipes = _network()
        states = [freeze_state(nodes, pipes, [])]  # 1. 整网导入（全量）

        _close(pipes, "P4")  # 2. 隔离提交：关 P4，故障 P6 待修复
        states.append(freeze_state(nodes, pipes, [(1, "P6")]))

        _open(pipes, "P4")  # 3. 复供确认：重开 P4，事件 1 复供
        states.append(freeze_state(nodes, pipes, []))

        _switch(nodes, "S", False)  # 4. 停运唯一水源
        states.append(freeze_state(nodes, pipes, []))

        _switch(nodes, "S", True)  # 5. 恢复水源
        states.append(freeze_state(nodes, pipes, []))

        for prev, new in zip(states, states[1:]):
            delta = snapshot_state_delta(prev, new)
            self.assertEqual(delta["format"], SNAPSHOT_DELTA_FORMAT)
            restored = apply_snapshot_state_delta(prev, delta)
            self.assertEqual(restored, new)
            # 逐字节等价：摘要链按还原后完整状态计算与存储形态无关
            self.assertEqual(
                audit.canonical_json(restored), audit.canonical_json(new)
            )

    def test_delta_chains_compose(self):
        """连续多条增量依次套用（全量 + delta + delta + ...）仍还原每步状态。"""
        nodes, pipes = _network()
        states = [freeze_state(nodes, pipes, [])]
        _close(pipes, "P4")
        states.append(freeze_state(nodes, pipes, [(1, "P6")]))
        _close(pipes, "P5")  # 第二个隔离事件：关 P5，故障 P2 待修复
        states.append(freeze_state(nodes, pipes, [(1, "P6"), (2, "P2")]))

        rebuilt = states[0]
        for new in states[1:]:
            rebuilt = apply_snapshot_state_delta(
                rebuilt, snapshot_state_delta(rebuilt, new)
            )
            self.assertEqual(rebuilt, new)

    def test_isolation_delta_contains_only_net_changes(self):
        """隔离提交的增量只含：P4 阀位、新增待修复故障、U3 移出供水点。"""
        nodes, pipes = _network()
        before = freeze_state(nodes, pipes, [])
        _close(pipes, "P4")
        after = freeze_state(nodes, pipes, [(1, "P6")])

        delta = snapshot_state_delta(before, after)
        self.assertEqual(delta["nodes"], {"upserted": [], "removed_ids": []})
        self.assertEqual(delta["pipes"]["removed_ids"], [])
        self.assertEqual(
            delta["pipes"]["upserted"],
            [
                {
                    "id": "P4",
                    "from": "B",
                    "to": "D",
                    "has_valve": True,
                    "valve_closed": True,
                    "name": None,
                }
            ],
        )
        self.assertEqual(
            delta["open_faults"],
            {"added": [{"event_id": 1, "fault_pipe_id": "P6"}], "removed_event_ids": []},
        )
        self.assertEqual(
            delta["supplied_sink_ids"], {"added": [], "removed": ["U3"]}
        )

    def test_restore_delta_removes_fault_and_restores_sink(self):
        nodes, pipes = _network()
        _close(pipes, "P4")
        before = freeze_state(nodes, pipes, [(1, "P6")])
        _open(pipes, "P4")
        after = freeze_state(nodes, pipes, [])

        delta = snapshot_state_delta(before, after)
        self.assertEqual(delta["open_faults"]["added"], [])
        self.assertEqual(delta["open_faults"]["removed_event_ids"], [1])
        self.assertEqual(delta["supplied_sink_ids"]["added"], ["U3"])
        self.assertEqual(delta["supplied_sink_ids"]["removed"], [])
        self.assertEqual(apply_snapshot_state_delta(before, delta), after)

    def test_source_switch_delta_contains_running_flag(self):
        nodes, pipes = _network()
        before = freeze_state(nodes, pipes, [])
        _switch(nodes, "S", False)
        after = freeze_state(nodes, pipes, [])

        delta = snapshot_state_delta(before, after)
        self.assertEqual(delta["pipes"], {"upserted": [], "removed_ids": []})
        self.assertEqual(
            delta["nodes"]["upserted"],
            [{"id": "S", "type": "source", "name": "一水厂", "running": False}],
        )
        self.assertEqual(
            delta["supplied_sink_ids"],
            {"added": [], "removed": ["U1", "U2", "U3"]},
        )
        self.assertEqual(apply_snapshot_state_delta(before, delta), after)

    def test_node_and_pipe_add_remove_round_trip(self):
        """拓扑变化（整网重定义级别的增删）也能用增量正确表达与还原。"""
        nodes, pipes = _network()
        before = freeze_state(nodes, pipes, [])
        del nodes["D"], nodes["U3"]
        del pipes["P4"], pipes["P6"]
        nodes["E"] = Node(id="E", type="junction")
        nodes["U9"] = Node(id="U9", type="sink")
        pipes["P9"] = Pipe(id="P9", from_node="B", to_node="E", has_valve=True)
        pipes["P10"] = Pipe(id="P10", from_node="E", to_node="U9", has_valve=False)
        after = freeze_state(nodes, pipes, [])

        delta = snapshot_state_delta(before, after)
        self.assertEqual(delta["nodes"]["removed_ids"], ["D", "U3"])
        self.assertEqual(delta["pipes"]["removed_ids"], ["P4", "P6"])
        self.assertEqual(
            [n["id"] for n in delta["nodes"]["upserted"]], ["E", "U9"]
        )
        self.assertEqual(
            [p["id"] for p in delta["pipes"]["upserted"]], ["P10", "P9"]
        )
        self.assertEqual(apply_snapshot_state_delta(before, delta), after)

    def test_empty_delta_round_trip(self):
        nodes, pipes = _network()
        state = freeze_state(nodes, pipes, [])
        delta = snapshot_state_delta(state, state)
        self.assertEqual(delta["nodes"], {"upserted": [], "removed_ids": []})
        self.assertEqual(delta["pipes"], {"upserted": [], "removed_ids": []})
        self.assertEqual(
            delta["open_faults"], {"added": [], "removed_event_ids": []}
        )
        self.assertEqual(
            delta["supplied_sink_ids"], {"added": [], "removed": []}
        )
        self.assertEqual(apply_snapshot_state_delta(state, delta), state)

    def test_unknown_delta_format_rejected(self):
        nodes, pipes = _network()
        state = freeze_state(nodes, pipes, [])
        with self.assertRaises(ValueError):
            apply_snapshot_state_delta(state, {"format": "water-snapshot-delta/v0"})
        with self.assertRaises(ValueError):
            apply_snapshot_state_delta(state, {"nodes": {}})


class DeltaSizeTests(unittest.TestCase):
    """新增持久化量随变化量增长，不随未变化的管网规模线性增长。"""

    def _large_network(self, n_sinks: int):
        """星形管网：1 水源 + 1 连接点 + n 用水点（各一条带阀支管）。"""
        nodes = {"S": Node(id="S", type="source"), "J": Node(id="J", type="junction")}
        pipes = {
            "P0": Pipe(id="P0", from_node="S", to_node="J", has_valve=True)
        }
        for i in range(n_sinks):
            nodes[f"U{i:04d}"] = Node(id=f"U{i:04d}", type="sink")
            pipes[f"P{i + 1:04d}"] = Pipe(
                id=f"P{i + 1:04d}", from_node="J", to_node=f"U{i:04d}",
                has_valve=True,
            )
        return nodes, pipes

    def test_small_change_delta_size_independent_of_network_size(self):
        sizes = {}
        for n_sinks in (50, 500):
            nodes, pipes = self._large_network(n_sinks)
            before = freeze_state(nodes, pipes, [])
            p = pipes["P0001"]
            pipes["P0001"] = Pipe(
                p.id, p.from_node, p.to_node, p.has_valve, True, p.name
            )
            after = freeze_state(nodes, pipes, [(1, "PX")])
            delta = snapshot_state_delta(before, after)
            sizes[n_sinks] = (
                len(audit.canonical_json(delta).encode("utf-8")),
                len(audit.canonical_json(after).encode("utf-8")),
            )
            self.assertEqual(apply_snapshot_state_delta(before, delta), after)

        delta_50, full_50 = sizes[50]
        delta_500, full_500 = sizes[500]
        # 全量快照随管网规模约 10 倍增长；同一处变更的增量几乎不变
        self.assertGreater(full_500, full_50 * 5)
        self.assertLess(delta_500, delta_50 * 2)
        # 增量远小于当步全量
        self.assertLess(delta_500, full_500 // 10)

    def test_digest_over_restored_state_matches_direct_state(self):
        """摘要链按还原后完整状态计算：与直接冻结状态的摘要完全一致。"""
        from datetime import datetime, timezone

        nodes, pipes = self._large_network(30)
        before = freeze_state(nodes, pipes, [])
        p = pipes["P0001"]
        pipes["P0001"] = Pipe(p.id, p.from_node, p.to_node, p.has_valve, True)
        after = freeze_state(nodes, pipes, [(1, "PX")])
        restored = apply_snapshot_state_delta(
            before, snapshot_state_delta(before, after)
        )
        created = datetime(2026, 10, 5, 8, 0, 0, tzinfo=timezone.utc)
        common = dict(
            snapshot_id=2,
            op_type="isolation_submit",
            network_version=2,
            event_id=1,
            created_at=created,
            prev_digest=audit.GENESIS_DIGEST,
        )
        self.assertEqual(
            audit.snapshot_digest(state=restored, **common),
            audit.snapshot_digest(state=after, **common),
        )


if __name__ == "__main__":
    unittest.main()
