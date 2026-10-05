"""db.read_transaction 的连接级语义测试（无需真实 PostgreSQL）。

用 unittest.mock 替换 psycopg2.connect，验证只读快照事务：
- 连接在首条 SQL 之前被设置为 REPEATABLE READ + READ ONLY（autocommit=False）；
- 正常结束与异常结束都回滚并关闭（不提交任何变更）；
- 每个只读请求开新连接（各取当时快照），同一请求内分次读取复用同一连接。

注意：tests.test_api 在 discover 同进程内会把全局 app.db.read_transaction 换成
内存替身。为不受其影响，这里以独立模块名重新加载一份 app.db 源码（归属 app
包，相对导入正常），只对这份副本打桩。

运行：python3 -m unittest tests.test_db_readonly -v
"""
import os
import sys
import unittest
import importlib.util
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 触发 app 包初始化（后续以独立模块名重载 app.db 时，其相对导入 from . import ... 可用）
import importlib

_ = importlib.import_module("app")

_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app", "db.py"
)


def _fresh_db():
    """以独立模块名重新加载一份未被替身污染的 app.db（相对导入仍可用）。"""
    spec = importlib.util.spec_from_file_location(
        "app._db_readonly_isolated", _DB_PATH
    )
    module = importlib.util.module_from_spec(spec)
    module.__package__ = "app"
    sys.modules["app._db_readonly_isolated"] = module
    spec.loader.exec_module(module)
    return module


class FakeConn:
    def __init__(self):
        self.set_session_calls = []
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def set_session(self, *, isolation_level, readonly, autocommit):
        self.set_session_calls.append(
            {"isolation_level": isolation_level, "readonly": readonly,
             "autocommit": autocommit}
        )

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1

    def close(self):
        self.closed = True


class ReadTransactionTests(unittest.TestCase):
    def setUp(self):
        # 隔离副本 + 对其 psycopg2.connect 打桩；不触碰全局 app.db
        self.isolated = _fresh_db()
        self.fake = FakeConn()
        self.patch = mock.patch.object(
            self.isolated.psycopg2, "connect", return_value=self.fake
        )
        self.patch.start()

    def tearDown(self):
        self.patch.stop()
        sys.modules.pop("app._db_readonly_isolated", None)

    def test_sets_repeatable_read_readonly_before_first_sql(self):
        with self.isolated.read_transaction() as conn:
            # 进入事务体前 set_session 已调用（对随后开启的事务生效）
            self.assertEqual(len(self.fake.set_session_calls), 1)
            params = self.fake.set_session_calls[0]
            self.assertEqual(
                params["isolation_level"],
                self.isolated.extensions.ISOLATION_LEVEL_REPEATABLE_READ,
            )
            self.assertTrue(params["readonly"])
            self.assertFalse(params["autocommit"])
            self.assertIs(conn, self.fake)
            # 事务体内不提交、不回滚
            self.assertEqual(self.fake.commit_count, 0)
            self.assertEqual(self.fake.rollback_count, 0)

        # 正常结束：回滚释放快照（只读无变更），绝不提交，并关闭连接
        self.assertEqual(self.fake.commit_count, 0)
        self.assertEqual(self.fake.rollback_count, 1)
        self.assertTrue(self.fake.closed)

    def test_rolls_back_and_closes_on_exception(self):
        with self.assertRaises(ValueError):
            with self.isolated.read_transaction():
                raise ValueError("boom")
        # 异常路径同样回滚、关闭，不吞异常
        self.assertEqual(self.fake.commit_count, 0)
        self.assertEqual(self.fake.rollback_count, 1)
        self.assertTrue(self.fake.closed)

    def test_each_request_opens_a_fresh_snapshot_connection(self):
        # 每次只读请求开启一条新连接（各取当时的数据库快照）；
        # 同一请求内的分次读取复用该连接，因此共享同一张快照。
        conns = [self.fake, FakeConn()]
        iterator = iter(conns)
        self.patch.stop()
        self.patch = mock.patch.object(
            self.isolated.psycopg2,
            "connect",
            side_effect=lambda *a, **k: next(iterator),
        )
        self.patch.start()

        with self.isolated.read_transaction() as first:
            self.assertIs(first, conns[0])
        with self.isolated.read_transaction() as second:
            self.assertIs(second, conns[1])

        # 两条连接各自配置了 REPEATABLE READ READ ONLY 并在结束后回滚关闭
        for fake in conns:
            self.assertEqual(len(fake.set_session_calls), 1)
            self.assertTrue(fake.set_session_calls[0]["readonly"])
            self.assertEqual(fake.commit_count, 0)
            self.assertEqual(fake.rollback_count, 1)
            self.assertTrue(fake.closed)


if __name__ == "__main__":
    unittest.main()
