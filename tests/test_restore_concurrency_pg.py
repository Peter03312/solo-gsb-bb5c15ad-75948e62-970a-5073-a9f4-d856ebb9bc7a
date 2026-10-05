"""真实 PostgreSQL 上的复供确认并发/锁序回归测试（默认跳过）。

仅在 RUN_DB_TESTS=1 且 DATABASE_URL 指向可连的 PostgreSQL 时运行；
通过 tests._real_pg.load_real_db() 取未被 tests.test_api 内存替身 monkeypatch
的真实 app.db 副本，因此可与其它测试模块以任意顺序同进程运行。

覆盖：
1. 两个调度员基于同一版本并发确认两条原本均安全的复供：统一锁序（先版本行、
   后按 id 升序锁事件行）使事务串行，恰好一个 200、另一个 409 VERSION_CONFLICT，
   绝不出现数据库死锁（40P01 / 500），版本恰好 +1、仅成功方阀门重开；
2. 复供与整网重导最紧密交错：同样无死锁，复供赢则 200、重导赢则 404
   EVENT_NOT_FOUND（旧事件随 TRUNCATE 消失）或 409 VERSION_CONFLICT。

运行：
    DATABASE_URL=postgresql://water:water@localhost:5432/water \
    RUN_DB_TESTS=1 python3 -m unittest tests.test_restore_concurrency_pg -v
"""
import os
import threading
import time
import unittest
from urllib.parse import urlparse

import psycopg2
from psycopg2 import sql as pg_sql

from app.config import get_settings
from app.planning import Node, Pipe, evaluate_restore

from tests._real_pg import load_real_db

db = load_real_db()

# 两条互不影响的独立支管：S-V1-J1-F1-U1 与 S-V2-J2-F2-U2
TWIN_NODES = [
    Node(id="S", type="source"),
    Node(id="J1", type="junction"),
    Node(id="J2", type="junction"),
    Node(id="U1", type="sink"),
    Node(id="U2", type="sink"),
]
TWIN_PIPES = [
    Pipe(id="V1", from_node="S", to_node="J1", has_valve=True),
    Pipe(id="F1", from_node="J1", to_node="U1"),
    Pipe(id="V2", from_node="S", to_node="J2", has_valve=True),
    Pipe(id="F2", from_node="J2", to_node="U2"),
]


class _PlanResultStub:
    def __init__(self, sinks):
        self.affected_sink_ids = sinks


def restore_confirm_route(event_id: int, expected_version: int, sink: dict) -> None:
    """按修复后路由层的顺序执行一次复供确认，把结果写入 sink。"""
    try:
        with db.transaction() as conn:
            # 锁序第一步：network_meta 版本行
            version, _ts, nodes, pipes = db.load_snapshot(conn, for_update=True)
            # 锁序第二步：按 id 升序锁全部事件行（与整网重导/其他复供一致）
            all_events = db.list_events_for_update(conn)
            event = next((e for e in all_events if e["id"] == event_id), None)
            if event is None:
                sink[event_id] = (404, "EVENT_NOT_FOUND")
                return
            if event["status"] != db.EVENT_OPEN:
                sink[event_id] = (409, "EVENT_ALREADY_RESTORED")
                return
            other = db.list_open_events(conn, exclude_event_id=event_id)
            result = evaluate_restore(
                nodes,
                pipes,
                reopen_pipe_ids=frozenset(event["closed_pipe_ids"]),
                other_fault_pipe_ids=frozenset(e["fault_pipe_id"] for e in other),
            )
            if version != expected_version:
                raise db.VersionConflict(version, expected_version)
            if not result.safe:
                sink[event_id] = (422, "RESTORE_UNSAFE")
                return
            db.confirm_restore(
                conn,
                event_id=event_id,
                current_version=version,
                reopened_pipe_ids=list(event["closed_pipe_ids"]),
                restored_sink_ids=result.restored_sink_ids,
            )
        sink[event_id] = (200, "OK")
    except db.VersionConflict:
        sink[event_id] = (409, "VERSION_CONFLICT")
    except Exception as exc:  # 死锁 40P01 等：测试必须失败的信号
        sink[event_id] = (500, type(exc).__name__)


@unittest.skipUnless(
    os.environ.get("RUN_DB_TESTS") == "1",
    "需要真实 PostgreSQL：设置 RUN_DB_TESTS=1 且 DATABASE_URL 可连时运行",
)
class RestoreConcurrencyPostgresTests(unittest.TestCase):
    def setUp(self):
        # 建表/迁移（幂等）
        db.init_db()
        self._shorten_deadlock_timeout()
        self.e1, self.e2 = self._reset_two_events()

    def _shorten_deadlock_timeout(self):
        # 新连接生效：锁环一旦回归，各连接在 ~1s 内即被中止而非干等
        db_name = urlparse(get_settings().database_url).path.lstrip("/") or "postgres"
        conn = psycopg2.connect(get_settings().database_url)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    pg_sql.SQL(
                        "ALTER DATABASE {} SET deadlock_timeout = '1s'"
                    ).format(pg_sql.Identifier(db_name))
                )
        finally:
            conn.close()

    def _reset_two_events(self):
        with db.transaction() as conn:
            db.replace_network(conn, TWIN_NODES, TWIN_PIPES)
        with db.transaction() as conn:
            e1 = db.submit_isolation(
                conn, 1, "F1", ["V1"], _PlanResultStub(["U1"])
            )["event_id"]
        with db.transaction() as conn:
            e2 = db.submit_isolation(
                conn, 2, "F2", ["V2"], _PlanResultStub(["U2"])
            )["event_id"]
        return e1, e2

    def test_concurrent_same_version_restores_serialize_without_deadlock(self):
        e1, e2 = self.e1, self.e2
        for round_idx in range(10):
            if round_idx:
                e1, e2 = self._reset_two_events()
            sink: dict = {}
            threads = [
                threading.Thread(
                    target=restore_confirm_route, args=(eid, 3, sink)
                )
                for eid in (e1, e2)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(20)
                self.assertFalse(t.is_alive(), "复供事务挂起（疑似死锁未被检测）")

            outcomes = sorted(sink.values())
            self.assertFalse(
                any(st == 500 for st, _ in outcomes), sink
            )
            self.assertEqual(
                outcomes, [(200, "OK"), (409, "VERSION_CONFLICT")], sink
            )
            with db.read_transaction() as conn:
                version, _ts, _nodes, pipes = db.load_snapshot(conn)
                open_events = db.list_open_events(conn)
            self.assertEqual(version, 4)
            self.assertEqual(len(open_events), 1)
            # 仅失败方那侧阀门仍关闭（恰好一个）
            closed = sorted(p.id for p in pipes.values() if p.valve_closed)
            self.assertEqual(len(closed), 1)
            self.assertIn(closed[0], ("V1", "V2"))

    def test_restore_interleaved_with_reimport_never_deadlocks(self):
        sink: dict = {}

        def do_restore():
            restore_confirm_route(self.e1, 3, sink)

        def do_import():
            try:
                with db.transaction() as conn:
                    db.replace_network(conn, TWIN_NODES, TWIN_PIPES)
                sink["import"] = (200, "OK")
            except Exception as exc:
                sink["import"] = (500, type(exc).__name__)

        # 屏障连接先持版本行：重导排在等待队列第一位，复供第二位
        barrier = psycopg2.connect(get_settings().database_url)
        barrier.autocommit = False
        with barrier.cursor() as cur:
            cur.execute("SELECT version FROM network_meta WHERE id=1 FOR UPDATE")

        t_import = threading.Thread(target=do_import)
        t_import.start()
        time.sleep(0.8)  # 重导已排队在版本行锁上
        t_restore = threading.Thread(target=do_restore)
        t_restore.start()
        time.sleep(1.0)  # 复供已锁住自家事件行并排队在版本行锁上
        barrier.commit()  # 放行：旧代码此刻必成锁环；新代码统一锁序串行通过
        barrier.close()
        t_import.join(20)
        t_restore.join(20)
        self.assertFalse(
            t_import.is_alive() or t_restore.is_alive(),
            "存在事务挂起（疑似死锁）",
        )

        self.assertEqual(sink.get("import"), (200, "OK"), sink)
        self.assertNotIn(500, [st for st, _ in sink.values()], sink)
        self.assertIn(
            sink.get(self.e1),
            [(200, "OK"), (404, "EVENT_NOT_FOUND"),
             (409, "VERSION_CONFLICT")],
            sink,
        )


if __name__ == "__main__":
    unittest.main()
