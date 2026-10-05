"""快照增量存储的数据访问层测试（用 psycopg2 桩替身，无需真实 PostgreSQL）。

以独立模块名加载 app/db.py 源码（与 tests/test_db_readonly.py 同一手法），
psycopg2 替换为内存桩：在内存表中模拟 state_snapshots /
state_snapshot_deltas / state_snapshot_chains / state_snapshot_chain_meta
四张表，覆盖：

- insert_state_snapshot 的存储决策：首条全量直连创世摘要、小范围变更写
  增量（state 为 NULL + 一行 delta）、整网导入强制全量并重置链深、
  链深/体积比例超限回退全量、上一条缺链记录 fail-closed；
- 摘要语义不变：无论全量还是增量存储，链上摘要都等于按**完整冻结状态**
  计算的 SHA-256，前驱关系与编号顺序不变；
- 读取透明还原：get/list/previous/list_snapshot_chain_rows 对增量快照
  返回重建后的完整状态，audit.verify_interval 可直接核验；
- 首次启用补建链（anchor_snapshot_chain）可处理增量存储的现存快照。

运行：python3 -m unittest tests.test_snapshot_delta_db -v
"""
import importlib
import importlib.util
import json
import os
import sys
import types
import unittest
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_ = importlib.import_module("app")

from app import audit  # noqa: E402
from app import snapshot_delta  # noqa: E402
from app.planning import Node, Pipe, freeze_state  # noqa: E402

_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app", "db.py"
)
_MODULE_NAME = "app._db_delta_isolated"


def _psycopg2_stubs() -> dict:
    pg = types.ModuleType("psycopg2")
    extras = types.ModuleType("psycopg2.extras")
    errors = types.ModuleType("psycopg2.errors")
    extensions = types.ModuleType("psycopg2.extensions")

    class RealDictCursor:  # 哨兵：db 层以此标识字典行游标
        pass

    extras.RealDictCursor = RealDictCursor

    class UndefinedTable(Exception):
        pass

    errors.UndefinedTable = UndefinedTable
    extensions.ISOLATION_LEVEL_REPEATABLE_READ = 2

    class OperationalError(Exception):
        pass

    pg.OperationalError = OperationalError
    pg.extras = extras
    pg.errors = errors
    pg.extensions = extensions

    def _connect(*args, **kwargs):
        raise RuntimeError("桩环境不允许真实连接")

    pg.connect = _connect
    return {
        "psycopg2": pg,
        "psycopg2.extras": extras,
        "psycopg2.errors": errors,
        "psycopg2.extensions": extensions,
    }


def _load_isolated_db():
    """以独立模块名加载 app/db.py，psycopg2 临时替换为内存桩。

    app.config 依赖 pydantic_settings（纯算法测试环境未必安装），这里同样
    以提供默认值的桩模块替代；测试可再 monkeypatch 模块级 get_settings。
    """
    stubs = _psycopg2_stubs()
    config_stub = types.ModuleType("app.config")
    config_stub.get_settings = lambda: types.SimpleNamespace(
        database_url="postgresql://stub",
        db_connect_retries=1,
        db_connect_retry_interval=0.0,
        history_limit=100,
        snapshot_delta_max_depth=32,
        snapshot_delta_full_ratio=0.6,
    )
    stubs["app.config"] = config_stub
    saved = {name: sys.modules.get(name) for name in stubs}
    sys.modules.update(stubs)
    try:
        spec = importlib.util.spec_from_file_location(_MODULE_NAME, _DB_PATH)
        module = importlib.util.module_from_spec(spec)
        module.__package__ = "app"
        sys.modules[_MODULE_NAME] = module
        spec.loader.exec_module(module)
    finally:
        for name, mod in saved.items():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod
    return module


db = _load_isolated_db()


# ---------------------------------------------------------------- 内存桩


class StubTables:
    def __init__(self):
        self.snapshots = {}   # id -> dict(op_type, network_version, event_id,
                              #           state, state_format, created_at)
        self.deltas = {}      # snapshot_id -> dict(base_snapshot_id, depth, delta)
        self.chains = {}      # snapshot_id -> dict(digest, prev_digest)
        self.chain_meta = {}  # 单行：{anchor_snapshot_id, anchor_kind}
        self.next_snapshot_id = 1


def _norm(sql: str) -> str:
    return " ".join(sql.split()).upper()


class StubCursor:
    def __init__(self, tables: StubTables, dict_rows: bool):
        self.tables = tables
        self.dict_rows = dict_rows
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    # -- 查询分发 ----------------------------------------------------------

    def execute(self, sql, params=None):
        params = list(params or [])
        norm = _norm(sql)
        t = self.tables
        self._rows = []

        if norm.startswith("SELECT PG_ADVISORY_XACT_LOCK"):
            return
        if "D.DEPTH" in norm:  # insert_state_snapshot 的上一条现存快照查询
            if t.snapshots:
                sid = max(t.snapshots)
                chain = t.chains.get(sid)
                delta = t.deltas.get(sid)
                self._rows = [(
                    sid,
                    chain["digest"] if chain else None,
                    delta["depth"] if delta else None,
                )]
            return
        if norm.startswith("INSERT INTO STATE_SNAPSHOT_DELTAS"):
            sid, base, depth, delta_json = params
            t.deltas[sid] = {
                "base_snapshot_id": base,
                "depth": depth,
                "delta": json.loads(delta_json),
            }
            return
        if norm.startswith("INSERT INTO STATE_SNAPSHOT_CHAINS"):
            sid, digest, prev = params
            if "ON CONFLICT" in norm:  # 补建链：不覆盖既有链行
                t.chains.setdefault(sid, {"digest": digest, "prev_digest": prev})
            else:
                t.chains[sid] = {"digest": digest, "prev_digest": prev}
            return
        if norm.startswith("INSERT INTO STATE_SNAPSHOT_CHAIN_META"):
            if "FROM STATE_SNAPSHOT_CHAINS" in norm:  # 已有链：指向最早入链
                if not t.chain_meta and t.chains:
                    t.chain_meta = {
                        "anchor_snapshot_id": min(t.chains),
                        "anchor_kind": params[0],
                    }
            elif "WHERE NOT EXISTS" in norm or "ON CONFLICT" in norm:
                if not t.chain_meta:
                    t.chain_meta = {
                        "anchor_snapshot_id": params[0],
                        "anchor_kind": params[1],
                    }
            return
        if norm.startswith("INSERT INTO STATE_SNAPSHOTS"):
            sid = t.next_snapshot_id
            t.next_snapshot_id += 1
            created_at = datetime(2026, 10, 5, 8, 0, sid, tzinfo=timezone.utc)
            if "'DELTA'" in norm:
                op_type, version, event_id = params
                state, fmt = None, "delta"
            else:
                op_type, version, event_id, state_json = params
                state, fmt = json.loads(state_json), "full"
            t.snapshots[sid] = {
                "op_type": op_type,
                "network_version": version,
                "event_id": event_id,
                "state": state,
                "state_format": fmt,
                "created_at": created_at,
            }
            self._rows = [(sid, created_at)]
            return
        if "FROM STATE_SNAPSHOT_DELTAS" in norm:  # ANY(%s) 批量取增量行
            ids = set(params[0])
            self._rows = [
                {
                    "snapshot_id": sid,
                    "base_snapshot_id": d["base_snapshot_id"],
                    "delta": d["delta"],
                }
                for sid, d in t.deltas.items() if sid in ids
            ]
            return
        if norm.startswith("SELECT ID, STATE FROM STATE_SNAPSHOTS"):
            ids = set(params[0])
            self._rows = [
                {"id": sid, "state": t.snapshots[sid]["state"]}
                for sid in sorted(ids) if sid in t.snapshots
            ]
            return
        if "STORED_DIGEST" in norm:  # list_snapshot_chain_rows
            start, end = params
            self._rows = [
                self._chain_row(sid) for sid in sorted(t.snapshots)
                if start <= sid <= end
            ]
            return
        if norm.startswith("SELECT COUNT(*) AS N FROM STATE_SNAPSHOT_CHAINS"):
            self._rows = [{"n": len(t.chains)}]
            return
        if norm.startswith("SELECT COUNT(*) AS N FROM STATE_SNAPSHOTS"):
            self._rows = [{"n": len(t.snapshots)}]
            return
        if norm.startswith("SELECT DIGEST FROM STATE_SNAPSHOT_CHAINS"):
            chain = t.chains.get(params[0])
            self._rows = [{"digest": chain["digest"]}] if chain else []
            return
        if norm.startswith("SELECT ANCHOR_SNAPSHOT_ID"):
            if t.chain_meta:
                self._rows = [{
                    "anchor_snapshot_id": t.chain_meta["anchor_snapshot_id"],
                    "anchor_kind": t.chain_meta["anchor_kind"],
                    "anchored_at": None,
                }]
            return
        if norm.startswith("SELECT COUNT(*), MIN(ID)"):
            ids = sorted(t.snapshots)
            self._rows = [(
                len(ids),
                ids[0] if ids else None,
                ids[-1] if ids else None,
                len(t.chains),
            )]
            return
        if norm.startswith("SELECT 1 FROM STATE_SNAPSHOTS"):
            self._rows = [(1,)] if params[0] in t.snapshots else []
            return
        if "FROM STATE_SNAPSHOTS" in norm and "ORDER BY ID ASC" in norm:
            # list_state_snapshots（含 LIMIT）与补建链全量扫描（无 LIMIT）
            ids = sorted(t.snapshots)
            if "WHERE ID >" in norm:
                ids = [i for i in ids if i > params[0]]
                limit = params[1]
            else:
                limit = params[0] if "LIMIT" in norm else None
            if "LIMIT" in norm:
                ids = ids[:limit]
            self._rows = [self._snapshot_row(sid) for sid in ids]
            return
        if "WHERE ID <" in norm and "ORDER BY ID DESC LIMIT 1" in norm:
            ids = [i for i in sorted(t.snapshots) if i < params[0]]
            self._rows = [self._snapshot_row(ids[-1])] if ids else []
            return
        if "FROM STATE_SNAPSHOTS WHERE ID =" in norm:
            sid = params[0]
            self._rows = [self._snapshot_row(sid)] if sid in t.snapshots else []
            return
        raise AssertionError(f"桩未覆盖的 SQL：{norm}")

    def _snapshot_row(self, sid):
        s = self.tables.snapshots[sid]
        return {
            "id": sid,
            "op_type": s["op_type"],
            "network_version": s["network_version"],
            "event_id": s["event_id"],
            "state": s["state"],
            "created_at": s["created_at"],
        }

    def _chain_row(self, sid):
        row = self._snapshot_row(sid)
        chain = self.tables.chains.get(sid)
        row["stored_digest"] = chain["digest"] if chain else None
        row["stored_prev_digest"] = chain["prev_digest"] if chain else None
        return row

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class StubConn:
    def __init__(self, tables: StubTables):
        self.tables = tables

    def cursor(self, cursor_factory=None):
        return StubCursor(self.tables, dict_rows=cursor_factory is not None)


# ---------------------------------------------------------------- 测试数据


def make_state(*, closed=(), stopped=(), faults=()):
    """双水源小管网的完整冻结状态（freeze_state 真实计算可达用水点）。

    S1 -P1- J -P3- U1 ；S2 -P2- J ；J -P4- U2（P* 均带阀）。
    规模足够让单阀/单源变更的增量载荷明显小于全量（触发增量存储）。
    """
    nodes = {
        "S1": Node(id="S1", type="source", running="S1" not in stopped),
        "S2": Node(id="S2", type="source", running="S2" not in stopped),
        "J": Node(id="J", type="junction"),
        "U1": Node(id="U1", type="sink"),
        "U2": Node(id="U2", type="sink"),
    }
    pipes = {
        "P1": Pipe(id="P1", from_node="S1", to_node="J", has_valve=True,
                   valve_closed="P1" in closed),
        "P2": Pipe(id="P2", from_node="S2", to_node="J", has_valve=True,
                   valve_closed="P2" in closed),
        "P3": Pipe(id="P3", from_node="J", to_node="U1", has_valve=True,
                   valve_closed="P3" in closed),
        "P4": Pipe(id="P4", from_node="J", to_node="U2", has_valve=True,
                   valve_closed="P4" in closed),
    }
    return freeze_state(nodes, pipes, list(faults))


def add_snapshot_row(tables, sid, state, fmt, *, op_type="network_import"):
    """绕过 insert_state_snapshot 直接摆一条快照行（构造历史/损坏场景）。"""
    tables.snapshots[sid] = {
        "op_type": op_type,
        "network_version": 1,
        "event_id": None,
        "state": state,
        "state_format": fmt,
        "created_at": datetime(2026, 10, 5, 9, 0, sid, tzinfo=timezone.utc),
    }
    tables.next_snapshot_id = max(tables.next_snapshot_id, sid + 1)


class DeltaStoreTestCase(unittest.TestCase):
    def setUp(self):
        self.tables = StubTables()
        self.conn = StubConn(self.tables)

    def insert(self, state, *, op_type="source_switch", version=1, event_id=None):
        return db.insert_state_snapshot(
            self.conn,
            op_type=op_type,
            network_version=version,
            state=state,
            event_id=event_id,
        )

    def chain_digest_of(self, sid, state, prev_digest):
        snap = self.tables.snapshots[sid]
        return audit.snapshot_digest(
            snapshot_id=sid,
            op_type=snap["op_type"],
            network_version=snap["network_version"],
            event_id=snap["event_id"],
            created_at=snap["created_at"],
            state=state,
            prev_digest=prev_digest,
        )


class WritePathTests(DeltaStoreTestCase):
    def test_first_snapshot_full_with_genesis_anchor(self):
        state = make_state()
        sid = self.insert(state, op_type="network_import")
        self.assertEqual(sid, 1)
        row = self.tables.snapshots[1]
        self.assertEqual(row["state_format"], "full")
        self.assertEqual(row["state"], state)
        self.assertNotIn(1, self.tables.deltas)
        chain = self.tables.chains[1]
        self.assertEqual(chain["prev_digest"], audit.GENESIS_DIGEST)
        self.assertEqual(
            chain["digest"], self.chain_digest_of(1, state, audit.GENESIS_DIGEST)
        )
        self.assertEqual(
            self.tables.chain_meta,
            {"anchor_snapshot_id": 1, "anchor_kind": audit.ANCHOR_LIVE_ORIGIN},
        )

    def test_small_change_stored_as_delta(self):
        s1 = make_state()
        self.insert(s1, op_type="network_import")
        s2 = make_state(closed=("P3",))
        sid2 = self.insert(s2, op_type="isolation_submit", version=2, event_id=1)
        # 第二条只写增量：state 为 NULL，另有一行 delta 指向上一条
        self.assertEqual(self.tables.snapshots[sid2]["state_format"], "delta")
        self.assertIsNone(self.tables.snapshots[sid2]["state"])
        delta_row = self.tables.deltas[sid2]
        self.assertEqual(delta_row["base_snapshot_id"], 1)
        self.assertEqual(delta_row["depth"], 1)
        self.assertEqual(
            delta_row["delta"],
            __import__("app.snapshot_delta", fromlist=["compute_delta"])
            .compute_delta(s1, s2),
        )
        # 摘要仍按完整冻结状态计算，前驱为第一条摘要
        chain = self.tables.chains[sid2]
        self.assertEqual(chain["prev_digest"], self.tables.chains[1]["digest"])
        self.assertEqual(
            chain["digest"],
            self.chain_digest_of(sid2, s2, self.tables.chains[1]["digest"]),
        )

    def test_delta_chain_depth_grows_and_reconstructs(self):
        states = [
            make_state(),
            make_state(closed=("P3",)),
            make_state(closed=("P3",), stopped=("S1",)),
            make_state(stopped=("S1",)),
        ]
        sids = [
            self.insert(s, op_type=op, version=i + 1)
            for i, (s, op) in enumerate(zip(states, [
                "network_import", "isolation_submit", "source_switch",
                "restore_confirm",
            ]))
        ]
        self.assertEqual([self.tables.deltas[s]["depth"] for s in sids[1:]],
                         [1, 2, 3])
        # 逐条读取均还原为操作当时的完整状态
        for sid, expected in zip(sids, states):
            self.assertEqual(
                db.get_state_snapshot(self.conn, sid)["state"], expected
            )
        # 摘要链：每条都等于按完整状态计算的摘要
        prev = audit.GENESIS_DIGEST
        for sid, expected in zip(sids, states):
            self.assertEqual(
                self.tables.chains[sid]["digest"],
                self.chain_digest_of(sid, expected, prev),
            )
            prev = self.tables.chains[sid]["digest"]

    def test_import_forces_full_and_resets_depth(self):
        self.insert(make_state(), op_type="network_import")
        self.insert(make_state(closed=("P3",)), op_type="isolation_submit",
                    version=2, event_id=1)
        sid3 = self.insert(make_state(), op_type="network_import", version=1)
        self.assertEqual(self.tables.snapshots[sid3]["state_format"], "full")
        self.assertNotIn(sid3, self.tables.deltas)
        sid4 = self.insert(make_state(closed=("P3",)),
                           op_type="isolation_submit", version=2, event_id=2)
        # 重导后的第一条增量重新从深度 1 计
        self.assertEqual(self.tables.deltas[sid4]["base_snapshot_id"], sid3)
        self.assertEqual(self.tables.deltas[sid4]["depth"], 1)

    def test_depth_limit_falls_back_to_full(self):
        original = db.get_settings
        db.get_settings = lambda: types.SimpleNamespace(
            snapshot_delta_max_depth=1, snapshot_delta_full_ratio=0.6
        )
        self.addCleanup(setattr, db, "get_settings", original)
        self.insert(make_state(), op_type="network_import")
        sid2 = self.insert(make_state(closed=("P3",)),
                           op_type="isolation_submit", version=2, event_id=1)
        sid3 = self.insert(make_state(closed=("P3",), stopped=("S1",)),
                           op_type="source_switch", version=3)
        self.assertEqual(self.tables.snapshots[sid2]["state_format"], "delta")
        # 第二条增量已达深度上限：第三条回退全量，链深重新归零
        self.assertEqual(self.tables.snapshots[sid3]["state_format"], "full")
        self.assertNotIn(sid3, self.tables.deltas)

    def test_ratio_limit_falls_back_to_full(self):
        original = db.get_settings
        db.get_settings = lambda: types.SimpleNamespace(
            snapshot_delta_max_depth=32, snapshot_delta_full_ratio=0.0
        )
        self.addCleanup(setattr, db, "get_settings", original)
        self.insert(make_state(), op_type="network_import")
        sid2 = self.insert(make_state(closed=("P3",)),
                           op_type="isolation_submit", version=2, event_id=1)
        self.assertEqual(self.tables.snapshots[sid2]["state_format"], "full")

    def test_missing_prev_chain_fails_closed(self):
        add_snapshot_row(self.tables, 5, make_state(), "full")
        # 现存快照 5 无链记录：拒绝接链，且不产生任何新行
        with self.assertRaises(RuntimeError):
            self.insert(make_state(closed=("P3",)), op_type="isolation_submit")
        self.assertEqual(set(self.tables.snapshots), {5})
        self.assertEqual(self.tables.deltas, {})
        self.assertEqual(self.tables.chains, {})


class ReadPathTests(DeltaStoreTestCase):
    def _build_three(self):
        states = [
            make_state(),
            make_state(closed=("P3",)),
            make_state(closed=("P3",), stopped=("S1",)),
        ]
        ops = ["network_import", "isolation_submit", "source_switch"]
        sids = [
            self.insert(s, op_type=op, version=i + 1)
            for i, (s, op) in enumerate(zip(states, ops))
        ]
        return sids, states

    def test_get_and_previous_and_list_reconstruct(self):
        sids, states = self._build_three()
        self.assertEqual(
            db.get_previous_state_snapshot(self.conn, sids[2])["state"], states[1]
        )
        rows = db.list_state_snapshots(self.conn, limit=10)
        self.assertEqual([r["state"] for r in rows], states)
        rows = db.list_state_snapshots(self.conn, limit=10, after_id=sids[0])
        self.assertEqual([r["state"] for r in rows], states[1:])

    def test_chain_rows_reconstruct_and_verify_ok(self):
        sids, states = self._build_three()
        rows = db.list_snapshot_chain_rows(self.conn, sids[0], sids[-1])
        self.assertEqual([r["state"] for r in rows], states)
        result = audit.verify_interval(
            rows,
            pre_start_digest=audit.GENESIS_DIGEST,
            end_digest=self.tables.chains[sids[-1]]["digest"],
        )
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["verdict"], "ok")

    def test_chain_info_and_exists(self):
        sids, _states = self._build_three()
        self.assertTrue(db.state_snapshot_exists(self.conn, sids[1]))
        self.assertFalse(db.state_snapshot_exists(self.conn, 999))
        info = db.get_chain_info(self.conn)
        self.assertEqual(info["snapshot_count"], 3)
        self.assertEqual(info["chained_count"], 3)
        self.assertEqual(info["earliest_snapshot_id"], sids[0])
        self.assertEqual(info["latest_snapshot_id"], sids[-1])
        self.assertEqual(info["anchor_kind"], audit.ANCHOR_LIVE_ORIGIN)


class BackfillTests(DeltaStoreTestCase):
    def test_anchor_backfill_over_delta_rows(self):
        """首次启用建链：现存快照含增量行时，按还原后的完整状态补链。"""
        s1 = make_state()
        s2 = make_state(closed=("P3",))
        s3 = make_state(closed=("P3",), stopped=("S1",))
        add_snapshot_row(self.tables, 1, s1, "full")
        add_snapshot_row(self.tables, 2, None, "delta", op_type="isolation_submit")
        add_snapshot_row(self.tables, 3, None, "delta", op_type="source_switch")
        from app import snapshot_delta

        self.tables.deltas[2] = {
            "base_snapshot_id": 1, "depth": 1,
            "delta": snapshot_delta.compute_delta(s1, s2),
        }
        self.tables.deltas[3] = {
            "base_snapshot_id": 2, "depth": 2,
            "delta": snapshot_delta.compute_delta(s2, s3),
        }

        result = db.anchor_snapshot_chain(self.conn)
        self.assertEqual(result["anchor_snapshot_id"], 1)
        self.assertEqual(result["snapshot_count"], 3)
        self.assertEqual(
            self.tables.chain_meta,
            {"anchor_snapshot_id": 1,
             "anchor_kind": audit.ANCHOR_INITIAL_BACKFILL},
        )
        # 链上摘要与按完整状态计算的一致，且区间核验通过
        prev = audit.GENESIS_DIGEST
        for sid, state in ((1, s1), (2, s2), (3, s3)):
            snap = self.tables.snapshots[sid]
            expected = audit.snapshot_digest(
                snapshot_id=sid,
                op_type=snap["op_type"],
                network_version=snap["network_version"],
                event_id=snap["event_id"],
                created_at=snap["created_at"],
                state=state,
                prev_digest=prev,
            )
            self.assertEqual(self.tables.chains[sid]["digest"], expected)
            prev = expected
        rows = db.list_snapshot_chain_rows(self.conn, 1, 3)
        verified = audit.verify_interval(
            rows,
            pre_start_digest=audit.GENESIS_DIGEST,
            end_digest=self.tables.chains[3]["digest"],
        )
        self.assertTrue(verified["ok"], verified)

    def test_backfill_idempotent_and_skips_empty(self):
        self.assertIsNone(db.anchor_snapshot_chain(self.conn))  # 空库不建链
        add_snapshot_row(self.tables, 4, make_state(), "full")
        first = db.anchor_snapshot_chain(self.conn)
        self.assertEqual(first["anchor_snapshot_id"], 4)
        digest = self.tables.chains[4]["digest"]
        self.assertIsNone(db.anchor_snapshot_chain(self.conn))  # 幂等不重算
        self.assertEqual(self.tables.chains[4]["digest"], digest)


if __name__ == "__main__":
    unittest.main()
