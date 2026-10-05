"""真实 PostgreSQL 上的增量（delta）快照存储回归测试（默认跳过）。

仅在 RUN_DB_TESTS=1 且 DATABASE_URL 指向可连的 PostgreSQL 时运行；
通过 tests._real_pg.load_real_db() 取未被 tests.test_api 内存替身
monkeypatch 的真实 app.db 副本。

覆盖：
1. 隔离/复供/水源切换只写增量行（state 为空、base_snapshot_id 指上一条），
   按编号读取/列表/相邻对照仍还原操作当时完整状态与原有字段；
2. 摘要链按还原后的完整状态计算：链上摘要与用还原状态重算的结果一致，
   区间核验（含起点为增量行、基准在区间之外）通过；
3. 整网重导写全量、旧增量快照仍可查询；历史全量与新增量共存；
4. 业务变更/快照/链记录同事务回滚，编号空洞不影响后续接链与增量基准；
5. 连续增量深度达到上限自动回写全量；增量不更省时也回退全量。

运行：
    DATABASE_URL=postgresql://water:water@localhost:5432/water \
    RUN_DB_TESTS=1 python3 -m unittest tests.test_snapshot_delta_pg -v
"""
import os
import unittest

import psycopg2

from app import audit
from app.config import get_settings
from app.planning import Node, Pipe, freeze_state

from tests._real_pg import load_real_db

db = load_real_db()

# S --V1[阀]-- J1 --F1-- U1
# S --V2[阀]-- J2 --F2-- U2
NODES = [
    Node(id="S", type="source"),
    Node(id="J1", type="junction"),
    Node(id="J2", type="junction"),
    Node(id="U1", type="sink"),
    Node(id="U2", type="sink"),
]
PIPES = [
    Pipe(id="V1", from_node="S", to_node="J1", has_valve=True),
    Pipe(id="F1", from_node="J1", to_node="U1"),
    Pipe(id="V2", from_node="S", to_node="J2", has_valve=True),
    Pipe(id="F2", from_node="J2", to_node="U2"),
]


class _PlanResultStub:
    def __init__(self, sinks):
        self.affected_sink_ids = sinks


def _raw_row(snapshot_id):
    """直读存储行（不还原）：state / base_snapshot_id / delta 原始形态。"""
    conn = psycopg2.connect(get_settings().database_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT state, base_snapshot_id, delta "
                "FROM state_snapshots WHERE id = %s",
                (snapshot_id,),
            )
            row = cur.fetchone()
            cur.execute(
                "SELECT digest, prev_digest FROM state_snapshot_chains "
                "WHERE snapshot_id = %s",
                (snapshot_id,),
            )
            chain = cur.fetchone()
        return row, chain
    finally:
        conn.close()


def _current_frozen_state():
    """当前管网 + 待修复事件按 freeze_state 冻结的完整状态（期望值）。"""
    with db.read_transaction() as conn:
        _v, _ts, nodes, pipes = db.load_snapshot(conn)
        open_faults = [(e["id"], e["fault_pipe_id"]) for e in db.list_open_events(conn)]
    return freeze_state(nodes, pipes, open_faults)


def _prev_chain_digest(snapshot_id):
    """编号严格小于 snapshot_id 的最近一条链摘要；没有则为创世摘要。"""
    conn = psycopg2.connect(get_settings().database_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT digest FROM state_snapshot_chains "
                "WHERE snapshot_id < %s ORDER BY snapshot_id DESC LIMIT 1",
                (snapshot_id,),
            )
            row = cur.fetchone()
        return row[0] if row else audit.GENESIS_DIGEST
    finally:
        conn.close()


@unittest.skipUnless(
    os.environ.get("RUN_DB_TESTS") == "1",
    "需要真实 PostgreSQL：设置 RUN_DB_TESTS=1 且 DATABASE_URL 可连时运行",
)
class SnapshotDeltaPostgresTests(unittest.TestCase):
    def setUp(self):
        db.init_db()  # 幂等：DDL 可重复执行（既有数据升级路径）

    def _import(self):
        with db.transaction() as conn:
            version, snapshot_id = db.replace_network(conn, NODES, PIPES)
        return version, snapshot_id

    def _submit(self, version, fault="F1", close=("V1",), sinks=("U1",)):
        with db.transaction() as conn:
            outcome = db.submit_isolation(
                conn, version, fault, list(close), _PlanResultStub(list(sinks))
            )
        return outcome

    def _switch(self, version, running):
        with db.transaction() as conn:
            outcome = db.switch_sources(conn, version, [("S", running)])
        return outcome

    # ---------------------------------------------------------- 存储形态与还原

    def test_small_ops_persist_delta_and_reads_resolve_full_state(self):
        version, snap_import = self._import()
        expected = {snap_import: _current_frozen_state()}

        out = self._submit(version)  # 隔离：关 V1，故障 F1 待修复
        snap_submit = out["snapshot_id"]
        expected[snap_submit] = _current_frozen_state()

        with db.transaction() as conn:
            out = db.confirm_restore(
                conn, out["event_id"], out["version"], ["V1"], ["U1"]
            )
        snap_restore = out["snapshot_id"]
        expected[snap_restore] = _current_frozen_state()

        out = self._switch(out["version"], False)  # 停运水源
        snap_off = out["snapshot_id"]
        expected[snap_off] = _current_frozen_state()

        out = self._switch(out["version"], True)  # 恢复水源
        snap_on = out["snapshot_id"]
        expected[snap_on] = _current_frozen_state()

        # 存储形态：导入为全量；四个小范围操作均为增量，基准为上一条快照
        (row, _chain) = _raw_row(snap_import)
        self.assertIsNotNone(row[0])  # state 非空：全量
        ids = [snap_submit, snap_restore, snap_off, snap_on]
        for sid, base in zip(ids, [snap_import] + ids[:-1]):
            row, _chain = _raw_row(sid)
            self.assertIsNone(row[0], f"快照 {sid} 应为增量行（state 为空）")
            self.assertEqual(row[1], base, f"快照 {sid} 的基准应为 {base}")
            self.assertIsNotNone(row[2], f"快照 {sid} 应有增量载荷")

        # 按编号读取：还原操作当时完整状态，与当时冻结的完整状态逐项一致
        with db.read_transaction() as conn:
            for sid, want in expected.items():
                got = db.get_state_snapshot(conn, sid)
                self.assertIsNotNone(got, sid)
                self.assertEqual(got["state"], want, f"快照 {sid} 还原状态不符")
            # 列表接口同样还原完整状态
            listed = {
                r["id"]: r["state"]
                for r in db.list_state_snapshots(conn, limit=10, after_id=snap_import - 1)
            }
            for sid, want in expected.items():
                self.assertEqual(listed[sid], want, f"列表中快照 {sid} 状态不符")
            # 相邻对照的基线读取（get_previous_state_snapshot）同样还原
            prev = db.get_previous_state_snapshot(conn, snap_submit)
            self.assertEqual(prev["state"], expected[snap_import])

        # 存储量：增量行实存字节数远小于还原后完整状态
        with db.read_transaction() as conn:
            for sid in ids:
                info = db.get_snapshot_storage(conn, sid)
                self.assertEqual(info["storage"], "delta")
                self.assertEqual(info["base_snapshot_id"], sid - 1)
                self.assertLess(info["stored_bytes"], info["state_bytes"] // 2)
            info = db.get_snapshot_storage(conn, snap_import)
            self.assertEqual(info["storage"], "full")
            self.assertEqual(info["stored_bytes"], info["state_bytes"])

    def test_chain_digest_computed_over_full_state_and_verifies(self):
        version, snap_import = self._import()
        out = self._submit(version)
        with db.transaction() as conn:
            out = db.confirm_restore(
                conn, out["event_id"], out["version"], ["V1"], ["U1"]
            )
        out = self._switch(out["version"], False)
        snap_last = out["snapshot_id"]

        # 链上每条摘要 == 用还原后完整状态重算的摘要（存储形态无关）
        with db.read_transaction() as conn:
            rows = db.list_snapshot_chain_rows(conn, snap_import, snap_last)
        self.assertEqual(len(rows), 4)
        prev_digest = _prev_chain_digest(snap_import)
        for row in rows:
            self.assertEqual(row["stored_prev_digest"], prev_digest)
            recomputed = audit.compute_snapshot_digest(row, prev_digest)
            self.assertEqual(
                recomputed,
                row["stored_digest"],
                f"快照 {row['id']} 的链摘要不等于按完整状态重算值",
            )
            prev_digest = row["stored_digest"]

        # 区间核验通过（含区间首条为增量行、其基准在区间之外的子区间）
        first_digest = rows[0]["stored_digest"]
        result = audit.verify_interval(
            rows[1:],  # 从增量行开始的子区间
            pre_start_digest=first_digest,
            end_digest=rows[-1]["stored_digest"],
        )
        self.assertEqual(result["verdict"], audit.VERDICT_OK, result.get("failure"))
        result = audit.verify_interval(
            rows,
            pre_start_digest=_prev_chain_digest(snap_import),
            end_digest=rows[-1]["stored_digest"],
        )
        self.assertEqual(result["verdict"], audit.VERDICT_OK, result.get("failure"))

    def test_diff_across_delta_snapshots(self):
        version, snap_import = self._import()
        out = self._submit(version)
        snap_submit = out["snapshot_id"]
        with db.read_transaction() as conn:
            base = db.get_state_snapshot(conn, snap_import)
            target = db.get_state_snapshot(conn, snap_submit)
        from app.planning import diff_states

        diff = diff_states(base["state"], target["state"])
        self.assertEqual(
            diff["valve_positions"]["changed"],
            [{"pipe_id": "V1", "valve_closed_before": False,
              "valve_closed_after": True}],
        )
        self.assertEqual(
            diff["open_faults"]["added"],
            [{"event_id": out["event_id"], "fault_pipe_id": "F1"}],
        )
        self.assertEqual(diff["supplied_sinks"]["removed"], ["U1"])

    # ---------------------------------------------------------- 共存 / 重导 / 回滚

    def test_reimport_writes_full_and_old_deltas_remain_readable(self):
        version, snap_import = self._import()
        out = self._submit(version)
        snap_submit = out["snapshot_id"]
        with db.read_transaction() as conn:
            old_state = db.get_state_snapshot(conn, snap_submit)["state"]

        # 整网重导：版本重置为 1，新快照为全量；旧增量快照原样可读
        version2, snap_reimport = self._import()
        self.assertEqual(version2, 1)
        row, _chain = _raw_row(snap_reimport)
        self.assertIsNotNone(row[0], "整网重导必须写全量快照")
        self.assertIsNone(row[1])
        with db.read_transaction() as conn:
            self.assertEqual(
                db.get_state_snapshot(conn, snap_submit)["state"], old_state
            )
            self.assertEqual(
                db.get_state_snapshot(conn, snap_import)["network_version"], 1
            )

        # 重导后的第一个小范围操作：以重导全量为基准写增量
        out = self._submit(version2)
        row, _chain = _raw_row(out["snapshot_id"])
        self.assertIsNone(row[0])
        self.assertEqual(row[1], snap_reimport)

    def test_rollback_leaves_no_partial_snapshot_or_chain(self):
        _version, snap_import = self._import()
        conn = psycopg2.connect(get_settings().database_url)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT (SELECT count(*) FROM state_snapshots), "
                    "(SELECT count(*) FROM state_snapshot_chains), "
                    "(SELECT version FROM network_meta WHERE id = 1)"
                )
                before = cur.fetchone()
        finally:
            conn.close()

        # 业务变更 + 快照 + 链记录在同一事务内：中途失败整体回滚
        with self.assertRaises(RuntimeError):
            with db.transaction() as tx:
                db.submit_isolation(tx, 1, "F1", ["V1"], _PlanResultStub(["U1"]))
                raise RuntimeError("模拟提交前失败")

        conn = psycopg2.connect(get_settings().database_url)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT (SELECT count(*) FROM state_snapshots), "
                    "(SELECT count(*) FROM state_snapshot_chains), "
                    "(SELECT version FROM network_meta WHERE id = 1)"
                )
                after = cur.fetchone()
        finally:
            conn.close()
        self.assertEqual(after, before, "回滚不得留下快照/链/版本的部分写入")

        # 回滚造成的编号空洞：下一条快照仍接“最近一条现存快照”，
        # 增量基准同样跳过空洞
        out = self._submit(1)
        sid = out["snapshot_id"]
        self.assertGreater(sid, snap_import + 1)  # 回滚消耗了一个编号
        row, chain = _raw_row(sid)
        self.assertEqual(row[1], snap_import)  # 增量基准为最近现存快照
        _row, import_chain = _raw_row(snap_import)
        self.assertEqual(chain[1], import_chain[0])  # prev_digest 接 import

    # ---------------------------------------------------------- 回退全量的两种情形

    def test_delta_depth_cap_forces_periodic_full_snapshot(self):
        self._import()
        total = db.SNAPSHOT_DELTA_MAX_DEPTH + 2
        ids = []
        version = 1
        for i in range(total):
            out = self._switch(version, running=(i % 2 == 1))
            version = out["version"]
            ids.append(out["snapshot_id"])

        # state 列非空即全量行
        forms = {sid: _raw_row(sid)[0][0] is not None for sid in ids}
        full_positions = [i for i, sid in enumerate(ids, start=1) if forms[sid]]
        # 第 1..32 条为增量（深度 1..32），第 33 条达到上限回写全量，
        # 第 34 条重新以增量链接
        self.assertEqual(full_positions, [db.SNAPSHOT_DELTA_MAX_DEPTH + 1])

        # 最深的增量行（第 32 条）与回写全量后的增量行都能正确还原
        expected = _current_frozen_state()  # 最后一次切换（i=33，奇数）后 S 运行
        with db.read_transaction() as conn:
            self.assertEqual(
                db.get_state_snapshot(conn, ids[-1])["state"], expected
            )
            deepest = db.get_state_snapshot(conn, ids[db.SNAPSHOT_DELTA_MAX_DEPTH - 1])
        self.assertIsNotNone(deepest)
        # 第 32 次切换（i=31，奇数）后水源为运行
        s_node = next(n for n in deepest["state"]["nodes"] if n["id"] == "S")
        self.assertTrue(s_node["running"])

    def test_oversized_delta_falls_back_to_full(self):
        self._import()
        big_a = {
            "nodes": [
                {"id": f"N{i:03d}", "type": "junction", "name": None,
                 "running": True}
                for i in range(40)
            ],
            "pipes": [],
            "open_faults": [],
            "supplied_sink_ids": [],
        }
        big_b = {
            "nodes": [
                {"id": f"M{i:03d}", "type": "sink", "name": f"点{i}",
                 "running": True}
                for i in range(40)
            ],
            "pipes": [],
            "open_faults": [],
            "supplied_sink_ids": [f"M{i:03d}" for i in range(40)],
        }
        with db.transaction() as conn:
            sid_a = db.insert_state_snapshot(
                conn, op_type="source_switch", network_version=2, state=big_a
            )
        with db.transaction() as conn:
            sid_b = db.insert_state_snapshot(
                conn, op_type="source_switch", network_version=3, state=big_b
            )
        # 相对上一条几乎全变：增量不比全量小，两条都回退为全量存储
        for sid in (sid_a, sid_b):
            row, _chain = _raw_row(sid)
            self.assertIsNotNone(row[0], f"快照 {sid} 应回退为全量")
            self.assertIsNone(row[1])
        with db.read_transaction() as conn:
            self.assertEqual(db.get_state_snapshot(conn, sid_b)["state"], big_b)

    # ---------------------------------------------------------- 启动升级幂等

    def test_init_db_is_idempotent_and_preserves_chain(self):
        _version, snap_import = self._import()
        out = self._submit(1)
        _row, chain_before = _raw_row(out["snapshot_id"])
        db.init_db()  # 重复执行 DDL/基线/建链：既有数据升级路径幂等
        db.init_db()
        _row, chain_after = _raw_row(out["snapshot_id"])
        self.assertEqual(chain_before, chain_after, "重复初始化不得改写既有摘要")
        _row, import_chain = _raw_row(snap_import)
        self.assertIsNotNone(import_chain)


if __name__ == "__main__":
    unittest.main()
