"""PostgreSQL 访问层：建表、当前快照读取、整网导入、隔离提交、复供、水源切换、状态回看快照。

状态快照采用**全量 + 增量（delta）混合存储**：整网导入与旧库基线写全量
快照；隔离提交、复供确认、水源切换等小范围变更只持久化相对上一条现存
快照的净变化（增量行引用其基准快照编号）。读取时沿基准链回溯还原完整
冻结状态，对外字段与全量快照完全一致；摘要链始终按还原后的完整状态计算，
与存储形态无关。
"""
from __future__ import annotations

import json
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Iterator

import psycopg2
import psycopg2.extras
from psycopg2 import extensions
from psycopg2.errors import UndefinedTable

from . import audit
from .config import get_settings
from .planning import (
    Node,
    Pipe,
    PlanResult,
    OP_BASELINE,
    OP_ISOLATION_SUBMIT,
    OP_NETWORK_IMPORT,
    OP_RESTORE_CONFIRM,
    OP_SOURCE_SWITCH,
    apply_snapshot_state_delta,
    freeze_state,
    snapshot_state_delta,
)

DDL = """
CREATE TABLE IF NOT EXISTS network_meta (
    id          INTEGER PRIMARY KEY DEFAULT 1,
    version     INTEGER NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT network_meta_singleton CHECK (id = 1)
);

INSERT INTO network_meta (id, version)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS nodes (
    id      TEXT PRIMARY KEY,
    type    TEXT NOT NULL CHECK (type IN ('source', 'sink', 'junction')),
    name    TEXT,
    running BOOLEAN NOT NULL DEFAULT TRUE
);

-- 兼容旧库：建表后补齐水源运行状态列（旧库水源视为运行中）
ALTER TABLE nodes
    ADD COLUMN IF NOT EXISTS running BOOLEAN NOT NULL DEFAULT TRUE;

CREATE TABLE IF NOT EXISTS pipes (
    id           TEXT PRIMARY KEY,
    from_node    TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    to_node      TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    has_valve    BOOLEAN NOT NULL DEFAULT FALSE,
    valve_closed BOOLEAN NOT NULL DEFAULT FALSE,
    name         TEXT,
    CONSTRAINT pipes_no_self_loop CHECK (from_node <> to_node)
);

CREATE INDEX IF NOT EXISTS idx_pipes_from_node ON pipes (from_node);
CREATE INDEX IF NOT EXISTS idx_pipes_to_node ON pipes (to_node);

CREATE TABLE IF NOT EXISTS isolation_events (
    id                BIGSERIAL PRIMARY KEY,
    version_before    INTEGER NOT NULL,
    version_after     INTEGER NOT NULL,
    fault_pipe_id     TEXT NOT NULL,
    closed_pipe_ids   JSONB NOT NULL,
    affected_sink_ids JSONB NOT NULL,
    status            TEXT NOT NULL DEFAULT 'open'
                        CHECK (status IN ('open', 'restored')),
    restored_at       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 兼容旧库：建表后补齐状态列（整网替换会清空事件，旧事件随替换失效）
ALTER TABLE isolation_events
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'open';
ALTER TABLE isolation_events
    ADD COLUMN IF NOT EXISTS restored_at TIMESTAMPTZ;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'isolation_events_status_check'
    ) THEN
        ALTER TABLE isolation_events
            ADD CONSTRAINT isolation_events_status_check
            CHECK (status IN ('open', 'restored'));
    END IF;
END$$;

-- 状态回看快照：只增不改、编号全局单调、永不复用。
-- 整网导入不截断本表，因此即使版本重置为 1，此前快照依然保留可读。
-- event_id 不加外键：整网导入会清空隔离事件，但历史快照仍须自包含可读。
CREATE TABLE IF NOT EXISTS state_snapshots (
    id           BIGSERIAL PRIMARY KEY,
    op_type      TEXT NOT NULL CHECK (op_type IN
                    ('network_import', 'isolation_submit',
                     'restore_confirm', 'source_switch', 'baseline')),
    network_version INTEGER NOT NULL,
    event_id     BIGINT,
    state        JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 增量（delta）快照存储：小范围状态变更只持久化相对上一条现存快照的净变化，
-- 新增持久化量随变化量增长，不随未变化的管网规模线性增长。
-- 全量行：state 非空、delta/base_snapshot_id 为空（历史快照即此形态，原样保留）；
-- 增量行：state 为空，base_snapshot_id 指向其增量所对照的上一条现存快照，
--         delta 为净变化载荷（格式 water-snapshot-delta/v1）。
-- 两种形态可任意共存；读取时沿 base_snapshot_id 回溯还原完整冻结状态。
ALTER TABLE state_snapshots
    ALTER COLUMN state DROP NOT NULL;
ALTER TABLE state_snapshots
    ADD COLUMN IF NOT EXISTS base_snapshot_id BIGINT
        REFERENCES state_snapshots (id);
ALTER TABLE state_snapshots
    ADD COLUMN IF NOT EXISTS delta JSONB;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'state_snapshots_storage_check'
    ) THEN
        ALTER TABLE state_snapshots
            ADD CONSTRAINT state_snapshots_storage_check
            CHECK (
                (state IS NOT NULL AND delta IS NULL AND base_snapshot_id IS NULL)
                OR (state IS NULL AND delta IS NOT NULL AND base_snapshot_id IS NOT NULL)
            );
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_state_snapshots_version
    ON state_snapshots (network_version);

CREATE INDEX IF NOT EXISTS idx_state_snapshots_base
    ON state_snapshots (base_snapshot_id);

-- 快照摘要链：与 state_snapshots 一一对应，只增不改。
-- digest/prev_digest 为 64 位十六进制 SHA-256；每条快照在原状态变更事务内接链。
-- state_snapshots 永不删除/截断，故外键不加 ON DELETE 行为；整网导入不清空本表。
-- 回滚造成的 BIGSERIAL 编号空洞不算断链：链接只指向“实际存在”的上一条快照。
CREATE TABLE IF NOT EXISTS state_snapshot_chains (
    snapshot_id  BIGINT PRIMARY KEY
                     REFERENCES state_snapshots (id),
    digest       TEXT NOT NULL CHECK (char_length(digest) = 64),
    prev_digest  TEXT NOT NULL CHECK (char_length(prev_digest) = 64)
);

-- 建链起点（anchor）单行表：标明首次建链从哪条现存快照开始。
-- anchor_kind = initial_backfill：首次启用时为现存快照建链（其前是否曾被改写不作断言）；
-- anchor_kind = live_origin：空库启用，首条状态变更快照即为起点，直连创世摘要。
CREATE TABLE IF NOT EXISTS state_snapshot_chain_meta (
    id           INTEGER PRIMARY KEY DEFAULT 1,
    anchor_snapshot_id BIGINT NOT NULL REFERENCES state_snapshots (id),
    anchor_kind  TEXT NOT NULL CHECK (anchor_kind IN
                     ('initial_backfill', 'live_origin')),
    anchored_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chain_meta_singleton CHECK (id = 1)
);
"""

# 事务级咨询锁：所有接链写入先取此锁，配合各写事务已持有的 network_meta
# 行锁，保证并发写入下“上一条现存快照”的读取—接链串行化，绝不形成分叉。
CHAIN_LOCK_KEY = 741289001

# 增量快照：连续增量链的长度上限。超过即重写一条全量快照，
# 把单次读取需要回溯的行数限制在上限之内（增量行本身只占 O(变化量) 空间）。
SNAPSHOT_DELTA_MAX_DEPTH = 32

# 这些操作永远写全量快照：整网导入重新定义全部管网状态（也是重导后的新锚点），
# 旧库基线没有可对照的前身。
_ALWAYS_FULL_OPS = frozenset({OP_NETWORK_IMPORT, OP_BASELINE})

# 事件状态：待修复 / 已复供
EVENT_OPEN = "open"
EVENT_RESTORED = "restored"


@contextmanager
def get_conn() -> Iterator:
    """获取一个数据库连接，调用方负责 commit/rollback。"""
    conn = psycopg2.connect(get_settings().database_url)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction() -> Iterator:
    """事务上下文：正常结束提交，异常回滚。"""
    with get_conn() as conn:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise


@contextmanager
def read_transaction() -> Iterator:
    """只读快照事务上下文：整个请求在同一张**一致性快照**上分次取数。

    连接开启后立即把事务设为 ``REPEATABLE READ READ ONLY``：快照在事务首条
    SQL 时固定，事务内读到的版本、节点、管段、运行水源与待修复事件必然
    对应同一已提交时刻。整网重导 / 隔离提交 / 复供 / 水源切换若在请求
    两次读取之间提交，本请求要么完全看到写入前、要么完全看到写入后，绝不
    拼接不同提交时刻的阀位、水源与事件。

    READ ONLY 是数据库对“只读接口绝不写入”的最后一道防线（只读请求误写
    会直接报错而不是产生数据）。上下文正常结束时回滚释放快照（没有任何
    变更需要提交），异常时同样回滚并原样抛出。
    """
    conn = psycopg2.connect(get_settings().database_url)
    try:
        # 必须在事务首条 SQL 之前设置：set_session 对随后开启的事务生效。
        conn.set_session(
            isolation_level=extensions.ISOLATION_LEVEL_REPEATABLE_READ,
            readonly=True,
            autocommit=False,
        )
        try:
            yield conn
        finally:
            # 只读事务无变更可提交：回滚仅用于结束事务、释放一致性快照
            conn.rollback()
    finally:
        conn.close()


def init_db() -> None:
    """启动时建表；等待 PostgreSQL 就绪。

    旧库上线（已存在管网数据但从无快照）时，仅以**当下状态**建立一条
    baseline 基线快照；绝不按既有事件伪造早期快照。空库与已有快照的库
    不产生基线。
    """
    settings = get_settings()
    last_error: Exception | None = None
    for attempt in range(1, settings.db_connect_retries + 1):
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(DDL)
                _ensure_baseline_if_legacy(conn)
                # 首次启用：为现存快照建立连续摘要链并标明建链起点（幂等）
                anchor_snapshot_chain(conn)
                conn.commit()
            return
        except (psycopg2.OperationalError, UndefinedTable) as exc:  # pragma: no cover
            last_error = exc
            time.sleep(settings.db_connect_retry_interval)
    raise RuntimeError(f"数据库初始化失败：{last_error}")


def _ensure_baseline_if_legacy(conn) -> None:
    """旧库上线基线：快照表为空且当前存在管网时，冻结当下状态为 baseline。

    必须在建表 DDL 之后、同一事务内执行；不修改管网版本与任何业务状态。
    """
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM state_snapshots")
        if cur.fetchone()[0] > 0:
            return
        cur.execute("SELECT count(*) FROM nodes")
        if cur.fetchone()[0] == 0:
            return
    version, _updated_at, nodes, pipes = load_snapshot(conn)
    open_faults = [(e["id"], e["fault_pipe_id"]) for e in list_open_events(conn)]
    insert_state_snapshot(
        conn,
        op_type=OP_BASELINE,
        network_version=version,
        state=freeze_state(nodes, pipes, open_faults),
        anchor_kind=audit.ANCHOR_INITIAL_BACKFILL,
    )


# ---------------------------------------------------------------- 快照


def load_snapshot(conn, *, for_update: bool = False) -> tuple[int, datetime | None, dict[str, Node], dict[str, Pipe]]:
    """读取当前版本与全部节点/管段。for_update=True 时锁定元数据行。"""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"SELECT version, updated_at FROM network_meta WHERE id = 1"
            f"{' FOR UPDATE' if for_update else ''}"
        )
        row = cur.fetchone()
        version = row["version"] if row else 0
        updated_at = row["updated_at"] if row else None

        cur.execute("SELECT id, type, name, running FROM nodes")
        nodes = {
            r["id"]: Node(
                id=r["id"], type=r["type"], name=r["name"], running=r["running"]
            )
            for r in cur.fetchall()
        }

        cur.execute(
            "SELECT id, from_node, to_node, has_valve, valve_closed, name "
            "FROM pipes ORDER BY id"
        )
        pipes = {
            r["id"]: Pipe(
                id=r["id"],
                from_node=r["from_node"],
                to_node=r["to_node"],
                has_valve=r["has_valve"],
                valve_closed=r["valve_closed"],
                name=r["name"],
            )
            for r in cur.fetchall()
        }
    return version, updated_at, nodes, pipes


# ---------------------------------------------------------------- 整网导入


def replace_network(conn, nodes: list[Node], pipes: list[Pipe]) -> tuple[int, int]:
    """在已开启的事务中整网替换：清空旧数据并重写，版本重置为 1。

    返回 (新版本号, 快照编号)。导入是对管网的整体重新定义，因此版本从头
    计数；历史事件也随整网替换一并清空，避免引用旧编号。

    状态快照表（state_snapshots）**不**清空：编号不可复用，导入后版本
    虽重置为 1，此前快照依旧保留；本次导入在同一事务内另冻结一条新快照。
    """
    with conn.cursor() as cur:
        cur.execute("SELECT version FROM network_meta WHERE id = 1 FOR UPDATE")
        if cur.fetchone() is None:
            cur.execute("INSERT INTO network_meta (id, version) VALUES (1, 0)")

        # 外键 pipes -> nodes：必须在同一条 TRUNCATE 中清空（CASCADE 仅作用于
        # 业务表）；state_snapshots 刻意不在清空列表内，历史快照永不被导入覆盖。
        cur.execute("TRUNCATE isolation_events, pipes, nodes")

        cur.executemany(
            "INSERT INTO nodes (id, type, name, running) "
            "VALUES (%s, %s, %s, %s)",
            [(n.id, n.type, n.name, n.running) for n in nodes],
        )
        cur.executemany(
            "INSERT INTO pipes (id, from_node, to_node, has_valve, valve_closed, name) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            [
                (p.id, p.from_node, p.to_node, p.has_valve, p.valve_closed, p.name)
                for p in pipes
            ],
        )
        cur.execute(
            "UPDATE network_meta SET version = 1, updated_at = now() WHERE id = 1"
        )

    # 同一事务内冻结“操作后”快照：版本 1、无历史事件、无待修复故障
    node_map = {n.id: n for n in nodes}
    pipe_map = {p.id: p for p in pipes}
    snapshot_id = insert_state_snapshot(
        conn,
        op_type=OP_NETWORK_IMPORT,
        network_version=1,
        state=freeze_state(node_map, pipe_map, []),
    )
    return 1, snapshot_id


# ---------------------------------------------------------------- 隔离提交


def submit_isolation(
    conn,
    expected_version: int,
    fault_pipe_id: str,
    close_pipe_ids: list[str],
    result: PlanResult,
) -> dict:
    """在已开启的事务中完成提交。

    调用方须先用当前快照重新评估并确认 result.isolatable 且无阻断问题。
    本函数只负责：锁行 → 版本比对 → 原子更新阀位、版本 → 写事件。
    返回新版本信息；版本过期抛出 VersionConflict。
    """
    with conn.cursor() as cur:
        cur.execute("SELECT version FROM network_meta WHERE id = 1 FOR UPDATE")
        row = cur.fetchone()
        current_version = row[0] if row else 0
        if current_version != expected_version:
            raise VersionConflict(current_version, expected_version)

        if close_pipe_ids:
            cur.execute(
                "UPDATE pipes SET valve_closed = TRUE "
                "WHERE id = ANY(%s) AND has_valve = TRUE",
                (close_pipe_ids,),
            )
        new_version = current_version + 1
        cur.execute(
            "UPDATE network_meta SET version = %s, updated_at = now() WHERE id = 1",
            (new_version,),
        )
        cur.execute(
            "INSERT INTO isolation_events "
            "(version_before, version_after, fault_pipe_id, "
            " closed_pipe_ids, affected_sink_ids, status) "
            "VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s) RETURNING id, created_at",
            (
                current_version,
                new_version,
                fault_pipe_id,
                json.dumps(close_pipe_ids),
                json.dumps(result.affected_sink_ids),
                EVENT_OPEN,
            ),
        )
        event_id, created_at = cur.fetchone()

    # 同一事务内冻结“操作后”快照：新阀位 + 全部待修复故障（含本事件）
    _post_version, _updated_at, post_nodes, post_pipes = load_snapshot(conn)
    open_faults = [(e["id"], e["fault_pipe_id"]) for e in list_open_events(conn)]
    snapshot_id = insert_state_snapshot(
        conn,
        op_type=OP_ISOLATION_SUBMIT,
        network_version=new_version,
        event_id=event_id,
        state=freeze_state(post_nodes, post_pipes, open_faults),
    )

    return {
        "version": new_version,
        "event_id": event_id,
        "created_at": created_at,
        "snapshot_id": snapshot_id,
    }


class VersionConflict(Exception):
    def __init__(self, current: int, expected: int):
        self.current = current
        self.expected = expected
        super().__init__(f"版本过期：当前版本 {current}，提交基于版本 {expected}")


# ---------------------------------------------------------------- 水源状态切换


def switch_sources(
    conn,
    expected_version: int,
    targets: list[tuple[str, bool]],
) -> dict:
    """在已开启的事务中原子完成水源运行状态切换。

    调用方须先用当前快照完成结构与安全性复核（仅水源可切换、无重复、
    切换后待修复故障无重新进水风险）。
    本函数只负责：锁版本行 → 版本比对 → 更新水源状态 → 版本 +1。
    版本过期抛出 VersionConflict；无实际变化不应调用本函数。
    """
    with conn.cursor() as cur:
        cur.execute("SELECT version FROM network_meta WHERE id = 1 FOR UPDATE")
        row = cur.fetchone()
        current_version = row[0] if row else 0
        if current_version != expected_version:
            raise VersionConflict(current_version, expected_version)

        for source_id, running in targets:
            # 仅水源节点可切换（调用方已复核，SQL 再约束一次防止部分写入）
            cur.execute(
                "UPDATE nodes SET running = %s WHERE id = %s AND type = 'source'",
                (running, source_id),
            )
            if cur.rowcount != 1:
                raise RuntimeError(f"水源节点不存在或不可切换：{source_id}")

        new_version = current_version + 1
        cur.execute(
            "UPDATE network_meta SET version = %s, updated_at = now() WHERE id = 1",
            (new_version,),
        )

    # 同一事务内冻结“操作后”快照：新水源运行状态、阀位/故障不变
    _post_version, _updated_at, post_nodes, post_pipes = load_snapshot(conn)
    open_faults = [(e["id"], e["fault_pipe_id"]) for e in list_open_events(conn)]
    snapshot_id = insert_state_snapshot(
        conn,
        op_type=OP_SOURCE_SWITCH,
        network_version=new_version,
        state=freeze_state(post_nodes, post_pipes, open_faults),
    )

    return {"version": new_version, "snapshot_id": snapshot_id}


# ---------------------------------------------------------------- 复供


def _decode_jsonb(value):
    return json.loads(value) if isinstance(value, str) else value


def _serialize_event_row(r: dict) -> dict:
    return {
        "id": r["id"],
        "version_before": r["version_before"],
        "version_after": r["version_after"],
        "fault_pipe_id": r["fault_pipe_id"],
        "closed_pipe_ids": _decode_jsonb(r["closed_pipe_ids"]),
        "affected_sink_ids": _decode_jsonb(r["affected_sink_ids"]),
        "status": r["status"],
        "restored_at": r.get("restored_at"),
        "created_at": r["created_at"],
    }


_EVENT_COLUMNS = (
    "id, version_before, version_after, fault_pipe_id, "
    "closed_pipe_ids, affected_sink_ids, status, restored_at, created_at"
)


def get_event(conn, event_id: int, *, for_update: bool = False) -> dict | None:
    """按编号读取隔离事件；for_update=True 时锁定事件行。"""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"SELECT {_EVENT_COLUMNS} FROM isolation_events WHERE id = %s"
            f"{' FOR UPDATE' if for_update else ''}",
            (event_id,),
        )
        row = cur.fetchone()
    return _serialize_event_row(row) if row else None


def list_open_events(conn, *, exclude_event_id: int | None = None,
                     for_update: bool = False) -> list[dict]:
    """读取全部待修复(status=open)事件；可排除指定事件（即正在复供的事件）。"""
    sql = f"SELECT {_EVENT_COLUMNS} FROM isolation_events WHERE status = %s"
    params: list = [EVENT_OPEN]
    if exclude_event_id is not None:
        sql += " AND id <> %s"
        params.append(exclude_event_id)
    sql += " ORDER BY id"
    if for_update:
        sql += " FOR UPDATE"
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [_serialize_event_row(r) for r in rows]


def list_events_for_update(conn) -> list[dict]:
    """按编号升序锁定**全部**事件行并返回（含待修复与已复供）。

    复供确认在持有 network_meta 版本行锁后调用本函数：所有写事务共用
    “先版本行、后事件行（事件行一律按 id 升序）”这一全局唯一锁序，
    两个并发复供不会再各先锁自家事件而形成锁环，与整网重导（先锁版本行、
    后 TRUNCATE 事件表）也锁序一致，绝不产生数据库死锁。
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"SELECT {_EVENT_COLUMNS} FROM isolation_events ORDER BY id FOR UPDATE"
        )
        rows = cur.fetchall()
    return [_serialize_event_row(r) for r in rows]


def confirm_restore(
    conn,
    event_id: int,
    current_version: int,
    reopened_pipe_ids: list[str],
    restored_sink_ids: list[str],
) -> dict:
    """在已开启的事务内完成复供确认。

    调用方（路由层）须已按全局锁序先锁 network_meta 版本行、再按 id 升序
    锁定全部事件行，并基于锁定后的当前快照完成事件状态、版本与安全性复核。
    本函数只负责实际写入：重开阀门 → 版本 +1 → 事件置为已复供（条件更新
    兜底并发），全部步骤与随后的状态快照在同一事务内原子生效；任何一步
    失败整体回滚，绝不部分重开阀门、推进版本或留下事件与快照。
    """
    with conn.cursor() as cur:
        if reopened_pipe_ids:
            cur.execute(
                "UPDATE pipes SET valve_closed = FALSE "
                "WHERE id = ANY(%s) AND has_valve = TRUE",
                (reopened_pipe_ids,),
            )
        new_version = current_version + 1
        cur.execute(
            "UPDATE network_meta SET version = %s, updated_at = now() WHERE id = 1",
            (new_version,),
        )
        cur.execute(
            "UPDATE isolation_events "
            "SET status = %s, restored_at = now() "
            "WHERE id = %s AND status = %s",
            (EVENT_RESTORED, event_id, EVENT_OPEN),
        )
        if cur.rowcount != 1:  # 并发下事件已被复供/删除：拒绝且不产生部分变更
            raise RuntimeError(f"隔离事件 {event_id} 已复供或不存在")
        cur.execute("SELECT restored_at FROM isolation_events WHERE id = %s", (event_id,))
        restored_at = cur.fetchone()[0]

    # 同一事务内冻结“操作后”快照：阀门已重开、本事件不再计入待修复故障
    _post_version, _updated_at, post_nodes, post_pipes = load_snapshot(conn)
    open_faults = [(e["id"], e["fault_pipe_id"]) for e in list_open_events(conn)]
    snapshot_id = insert_state_snapshot(
        conn,
        op_type=OP_RESTORE_CONFIRM,
        network_version=new_version,
        event_id=event_id,
        state=freeze_state(post_nodes, post_pipes, open_faults),
    )

    return {
        "version": new_version,
        "restored_at": restored_at,
        "snapshot_id": snapshot_id,
    }


# ---------------------------------------------------------------- 历史


def list_events(conn, limit: int) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"SELECT {_EVENT_COLUMNS} "
            "FROM isolation_events ORDER BY id DESC LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
    return [_serialize_event_row(r) for r in rows]


# ---------------------------------------------------------------- 状态快照


_SNAPSHOT_COLUMNS = (
    "id, op_type, network_version, event_id, state, "
    "base_snapshot_id, delta, created_at"
)


def _serialize_snapshot_row(row: dict) -> dict:
    return {
        "id": row["id"],
        "op_type": row["op_type"],
        "network_version": row["network_version"],
        "event_id": row["event_id"],
        "state": _decode_jsonb(row["state"]),
        "created_at": row["created_at"],
    }


def _canonical_size(value) -> int:
    """固定编码下的字节数：用于全量/增量载荷的持久化量对比。"""
    return len(audit.canonical_json(value).encode("utf-8"))


def _fetch_storage_row(conn, snapshot_id: int) -> dict | None:
    """读取快照行的存储形态列（state / base_snapshot_id / delta）。"""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT id, state, base_snapshot_id, delta "
            "FROM state_snapshots WHERE id = %s",
            (snapshot_id,),
        )
        return cur.fetchone()


def _resolve_state_and_depth(conn, row: dict) -> tuple[dict, int]:
    """还原一行的完整冻结状态，并返回其连续增量深度（全量行为 0）。

    沿 base_snapshot_id 逐行回溯到最近的全量快照，再正向套用增量载荷；
    结果与 freeze_state 直接冻结的载荷逐字节等价。写入侧保证连续增量
    深度不超过 SNAPSHOT_DELTA_MAX_DEPTH，回溯超出该上限即数据被外部
    破坏（成环/悬空），fail-closed 报错。
    """
    chain: list[dict] = []
    current = row
    while current["state"] is None:
        chain.append(current)
        if len(chain) > SNAPSHOT_DELTA_MAX_DEPTH:
            raise RuntimeError(
                f"增量快照链深度异常：从快照 {row['id']} 回溯超过 "
                f"{SNAPSHOT_DELTA_MAX_DEPTH} 层仍未到达全量快照"
            )
        base_id = current["base_snapshot_id"]
        current = _fetch_storage_row(conn, base_id)
        if current is None:  # 基准快照被外部破坏：fail-closed，绝不静默拼接
            raise RuntimeError(
                f"增量快照 {chain[-1]['id']} 的基准快照 {base_id} 不存在"
            )
    state = _decode_jsonb(current["state"])
    for delta_row in reversed(chain):
        state = apply_snapshot_state_delta(state, _decode_jsonb(delta_row["delta"]))
    return state, len(chain)


def _resolve_row_state(conn, row: dict, cache: dict[int, dict],
                       by_id: dict[int, dict] | None = None) -> dict:
    """还原一行的完整冻结状态（带跨行缓存；by_id 可复用同批已取出的行）。"""
    if row["state"] is not None:
        return _decode_jsonb(row["state"])
    cached = cache.get(row["id"])
    if cached is not None:
        return cached
    chain: list[dict] = []
    current = row
    while current["state"] is None:
        if current["id"] in cache:
            state = cache[current["id"]]
            break
        chain.append(current)
        if len(chain) > SNAPSHOT_DELTA_MAX_DEPTH:
            raise RuntimeError(
                f"增量快照链深度异常：从快照 {row['id']} 回溯超过 "
                f"{SNAPSHOT_DELTA_MAX_DEPTH} 层仍未到达全量快照"
            )
        base_id = current["base_snapshot_id"]
        current = (by_id or {}).get(base_id) or _fetch_storage_row(conn, base_id)
        if current is None:
            raise RuntimeError(
                f"增量快照 {chain[-1]['id']} 的基准快照 {base_id} 不存在"
            )
    else:
        state = _decode_jsonb(current["state"])
    for delta_row in reversed(chain):
        state = apply_snapshot_state_delta(state, _decode_jsonb(delta_row["delta"]))
        cache[delta_row["id"]] = state
    return state


def _resolve_rows(conn, rows: list[dict]) -> list[dict]:
    """就地把 rows 中增量行的 state 还原为完整冻结状态（全量行原样）。"""
    by_id = {row["id"]: row for row in rows}
    cache: dict[int, dict] = {}
    for row in rows:
        if row["state"] is None:
            row["state"] = _resolve_row_state(conn, row, cache, by_id)
    return rows


def insert_state_snapshot(
    conn,
    *,
    op_type: str,
    network_version: int,
    state: dict,
    event_id: int | None = None,
    anchor_kind: str | None = None,
) -> int:
    """在调用方事务内冻结一条状态快照并接入摘要链，返回不可复用的快照编号。

    仅插入、不更新、不删除；快照编号由 BIGSERIAL 全局单调分配，整网导入
    也不重置该序列。必须在状态实际变更后、**同一事务**内调用：快照行与
    摘要链行在同一事务内写入，事务回滚则二者同时回滚，链上不可能出现
    “有链无快照”或“有快照无链”的半完成记录。

    存储形态（对外透明，读取时统一还原为完整冻结状态）：
      - 整网导入 / 旧库基线 / 全库首条：写**全量**快照（state 列）；
      - 其余小范围变更：若相对上一条现存快照的**增量载荷**比全量更小、
        且连续增量深度未达 SNAPSHOT_DELTA_MAX_DEPTH，则只写**增量**快照
        （base_snapshot_id + delta 列），新增持久化量随变化量而非管网
        规模增长；否则回退全量（同时重置增量链深度）。

    摘要链规则与存储形态无关：摘要始终按**还原后的完整冻结状态**计算。
    链接编号严格小于自身的最近一条**现存**快照（回滚造成的 BIGSERIAL
    空洞自动跳过）；该现存快照无链记录时 fail-closed 抛错，绝不静默接
    创世摘要形成分叉。全库首条快照使用固定创世摘要，并记录建链起点：
      - live_origin：空库启用后的正常首条快照；
      - initial_backfill：旧库上线基线（与首次建链同批的现存快照）。
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # 接链临界区串行化：正常写路径已持有 network_meta FOR UPDATE，
        # 咨询锁再兜底保证任何路径下并发写入都不会各自接上一条而分叉。
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (CHAIN_LOCK_KEY,))

        # 上一条现存快照（含存储形态与链摘要）。临界区内不存在并发插入，
        # 因此编号最大者即“编号严格小于本条”的最近一条。
        cur.execute(
            "SELECT s.id, s.state, s.base_snapshot_id, s.delta, c.digest "
            "FROM state_snapshots s "
            "LEFT JOIN state_snapshot_chains c ON c.snapshot_id = s.id "
            "ORDER BY s.id DESC LIMIT 1"
        )
        prev_row = cur.fetchone()

        # 存储形态决策：默认全量；小范围变更且增量更省、链不深时写增量。
        base_snapshot_id: int | None = None
        delta_payload: dict | None = None
        if prev_row is not None and op_type not in _ALWAYS_FULL_OPS:
            prev_state, prev_depth = _resolve_state_and_depth(conn, prev_row)
            if prev_depth < SNAPSHOT_DELTA_MAX_DEPTH:
                candidate = snapshot_state_delta(prev_state, state)
                if _canonical_size(candidate) < _canonical_size(state):
                    base_snapshot_id = prev_row["id"]
                    delta_payload = candidate

        if delta_payload is not None:
            cur.execute(
                "INSERT INTO state_snapshots "
                "(op_type, network_version, event_id, base_snapshot_id, delta) "
                "VALUES (%s, %s, %s, %s, %s::jsonb) "
                "RETURNING id, created_at",
                (
                    op_type,
                    network_version,
                    event_id,
                    base_snapshot_id,
                    json.dumps(delta_payload),
                ),
            )
        else:
            cur.execute(
                "INSERT INTO state_snapshots (op_type, network_version, event_id, state) "
                "VALUES (%s, %s, %s, %s::jsonb) "
                "RETURNING id, created_at",
                (op_type, network_version, event_id, json.dumps(state)),
            )
        inserted = cur.fetchone()
        snapshot_id, created_at = inserted["id"], inserted["created_at"]

        if prev_row is None:
            prev_digest = audit.GENESIS_DIGEST
            kind = anchor_kind or audit.ANCHOR_LIVE_ORIGIN
        elif prev_row["digest"] is None:
            # 上一条现存快照未入链：启动建链缺失或链被外部破坏，拒绝写出分叉链
            raise RuntimeError(
                f"快照 {prev_row['id']} 缺少摘要链记录，无法为快照 {snapshot_id} 接链"
            )
        else:
            prev_digest = prev_row["digest"]
            kind = None  # 已有起点，非 anchor

        # 摘要按完整冻结状态计算（state 为调用方冻结的完整状态，
        # 与上面选择全量或增量存储无关）。
        digest = audit.snapshot_digest(
            snapshot_id=snapshot_id,
            op_type=op_type,
            network_version=network_version,
            event_id=event_id,
            created_at=created_at,
            state=state,
            prev_digest=prev_digest,
        )
        cur.execute(
            "INSERT INTO state_snapshot_chains (snapshot_id, digest, prev_digest) "
            "VALUES (%s, %s, %s)",
            (snapshot_id, digest, prev_digest),
        )
        if kind is not None:
            cur.execute(
                "INSERT INTO state_snapshot_chain_meta "
                "(id, anchor_snapshot_id, anchor_kind) "
                "VALUES (1, %s, %s) ON CONFLICT (id) DO NOTHING",
                (snapshot_id, kind),
            )
        return snapshot_id


def get_state_snapshot(conn, snapshot_id: int) -> dict | None:
    """按编号读取冻结快照（增量行已还原完整状态）；不存在返回 None。"""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"SELECT {_SNAPSHOT_COLUMNS} FROM state_snapshots WHERE id = %s",
            (snapshot_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    _resolve_rows(conn, [row])
    return _serialize_snapshot_row(row)


def list_state_snapshots(
    conn, *, limit: int, after_id: int | None = None
) -> list[dict]:
    """按编号升序列出快照元数据（含还原后的完整冻结状态）。

    after_id 为游标：只返回编号严格大于它的快照；不传时从头返回。
    """
    sql = f"SELECT {_SNAPSHOT_COLUMNS} FROM state_snapshots"
    params: list = []
    if after_id is not None:
        sql += " WHERE id > %s"
        params.append(after_id)
    sql += " ORDER BY id ASC LIMIT %s"
    params.append(limit)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    _resolve_rows(conn, rows)
    return [_serialize_snapshot_row(r) for r in rows]


def get_previous_state_snapshot(conn, snapshot_id: int) -> dict | None:
    """读取编号严格小于 snapshot_id 的最近一条快照；为首条快照时返回 None。"""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"SELECT {_SNAPSHOT_COLUMNS} FROM state_snapshots "
            "WHERE id < %s ORDER BY id DESC LIMIT 1",
            (snapshot_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    _resolve_rows(conn, [row])
    return _serialize_snapshot_row(row)


def get_snapshot_storage(conn, snapshot_id: int) -> dict | None:
    """读取快照的存储形态与持久化量（只读）；不存在返回 None。

    storage=full：state 列即完整冻结状态，stored_bytes == state_bytes；
    storage=delta：只持久化相对 base_snapshot_id 的净变化，
    stored_bytes 为增量载荷字节数，state_bytes 为还原后完整状态字节数。
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"SELECT {_SNAPSHOT_COLUMNS} FROM state_snapshots WHERE id = %s",
            (snapshot_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    if row["state"] is not None:
        state = _decode_jsonb(row["state"])
        return {
            "id": row["id"],
            "op_type": row["op_type"],
            "storage": "full",
            "base_snapshot_id": None,
            "state_bytes": _canonical_size(state),
            "stored_bytes": _canonical_size(state),
            "created_at": row["created_at"],
        }
    state = _resolve_row_state(conn, row, {})
    return {
        "id": row["id"],
        "op_type": row["op_type"],
        "storage": "delta",
        "base_snapshot_id": row["base_snapshot_id"],
        "state_bytes": _canonical_size(state),
        "stored_bytes": _canonical_size(_decode_jsonb(row["delta"])),
        "created_at": row["created_at"],
    }


# ---------------------------------------------------------------- 快照摘要链


def anchor_snapshot_chain(conn) -> dict | None:
    """首次启用时为现存快照建立连续摘要链，并标明建链起点（幂等）。

    - 链已存在：只补齐缺失的起点元数据（不改动任何既有摘要），返回 None；
    - 快照表为空：不建链（首条状态变更快照将以 live_origin 直连创世摘要）；
    - 链为空且存在快照：按编号升序为**所有现存快照**接链，首条以固定创世
      摘要为前驱并记 anchor_kind=initial_backfill。这是首次启用建链：
      此前快照是否曾被改写不作断言，链只保证建链之后可核验。

    必须在 DDL（与旧库基线）之后、同一事务内执行。
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT count(*) AS n FROM state_snapshot_chains")
        if cur.fetchone()["n"] > 0:
            # 已有链：确保元数据指向最早入链快照，幂等不重算
            cur.execute(
                "INSERT INTO state_snapshot_chain_meta "
                "(id, anchor_snapshot_id, anchor_kind) "
                "SELECT 1, snapshot_id, %s FROM state_snapshot_chains "
                "ORDER BY snapshot_id ASC LIMIT 1 "
                "ON CONFLICT (id) DO NOTHING",
                (audit.ANCHOR_INITIAL_BACKFILL,),
            )
            return None

        cur.execute("SELECT count(*) AS n FROM state_snapshots")
        if cur.fetchone()["n"] == 0:
            return None

        cur.execute("SELECT pg_advisory_xact_lock(%s)", (CHAIN_LOCK_KEY,))
        cur.execute(
            f"SELECT {_SNAPSHOT_COLUMNS} FROM state_snapshots ORDER BY id ASC"
        )
        rows = cur.fetchall()
        # 增量行先还原为完整冻结状态：摘要始终按完整状态计算，与存储形态无关
        _resolve_rows(conn, rows)

        prev_digest = audit.GENESIS_DIGEST
        anchor_id = None
        for idx, row in enumerate(rows):
            sid = row["id"]
            # 已存在链行（并发/部分建链）：沿用其摘要作为后继前驱，绝不重算覆盖
            cur.execute(
                "SELECT digest FROM state_snapshot_chains WHERE snapshot_id = %s",
                (sid,),
            )
            existing = cur.fetchone()
            if existing is not None:
                # 沿用已存在链行的摘要作为后继前驱，绝不重算覆盖
                prev_digest = existing["digest"]
                if anchor_id is None:
                    anchor_id = sid
                continue
            digest = audit.snapshot_digest(
                snapshot_id=sid,
                op_type=row["op_type"],
                network_version=row["network_version"],
                event_id=row["event_id"],
                created_at=row["created_at"],
                state=row["state"],
                prev_digest=prev_digest,
            )
            cur.execute(
                "INSERT INTO state_snapshot_chains (snapshot_id, digest, prev_digest) "
                "VALUES (%s, %s, %s) ON CONFLICT (snapshot_id) DO NOTHING",
                (sid, digest, prev_digest),
            )
            if anchor_id is None:
                anchor_id = sid
            prev_digest = digest

        # 起点元数据缺失时才补写；若已存在（例如 live_origin 起点），保留原标记
        cur.execute(
            "INSERT INTO state_snapshot_chain_meta "
            "(id, anchor_snapshot_id, anchor_kind) "
            "SELECT 1, %s, %s WHERE NOT EXISTS (SELECT 1 FROM state_snapshot_chain_meta WHERE id = 1)",
            (anchor_id, audit.ANCHOR_INITIAL_BACKFILL),
        )
    return {
        "anchor_snapshot_id": anchor_id,
        "anchor_kind": audit.ANCHOR_INITIAL_BACKFILL,
        "snapshot_count": len(rows),
    }


def get_chain_meta(conn) -> dict | None:
    """读取建链起点元数据；尚未启用（空库未写过快照）时返回 None。"""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT anchor_snapshot_id, anchor_kind, anchored_at "
            "FROM state_snapshot_chain_meta WHERE id = 1"
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "anchor_snapshot_id": row["anchor_snapshot_id"],
        "anchor_kind": row["anchor_kind"],
        "anchored_at": row["anchored_at"],
    }


def get_chain_info(conn) -> dict:
    """链总览：建链起点 + 现存/已入链快照数与最早/最晚编号（审计员选边界用）。"""
    meta = get_chain_meta(conn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), min(id), max(id), "
            "(SELECT count(*) FROM state_snapshot_chains) "
            "FROM state_snapshots"
        )
        total, min_id, max_id, chained = cur.fetchone()
    return {
        "anchor_snapshot_id": meta["anchor_snapshot_id"] if meta else None,
        "anchor_kind": meta["anchor_kind"] if meta else None,
        "anchored_at": meta["anchored_at"] if meta else None,
        "snapshot_count": total,
        "chained_count": chained,
        "earliest_snapshot_id": min_id,
        "latest_snapshot_id": max_id,
    }


def state_snapshot_exists(conn, snapshot_id: int) -> bool:
    """快照编号是否实际存在（回滚空洞不算存在）。"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM state_snapshots WHERE id = %s", (snapshot_id,)
        )
        return cur.fetchone() is not None


def list_snapshot_chain_rows(conn, start_id: int, end_id: int) -> list[dict]:
    """只读取出闭区间 [start_id, end_id] 内**实际存在**的快照及链摘要。

    按编号升序；回滚造成的编号空洞自然不产生行（核验时不算断链）。
    LEFT JOIN 使缺少链记录的快照（被外部破坏）也能返回，由核验逻辑定位
    为链路断裂，而不是静默忽略。返回项含 stored_digest / stored_prev_digest。
    增量行的 state 已沿基准链（可能延伸到区间起点之前）还原为完整冻结状态。
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"SELECT s.id, s.op_type, s.network_version, s.event_id, "
            f"s.state, s.base_snapshot_id, s.delta, s.created_at, "
            f"c.digest AS stored_digest, "
            f"c.prev_digest AS stored_prev_digest "
            f"FROM state_snapshots s "
            f"LEFT JOIN state_snapshot_chains c ON c.snapshot_id = s.id "
            f"WHERE s.id BETWEEN %s AND %s ORDER BY s.id ASC",
            (start_id, end_id),
        )
        rows = cur.fetchall()
    _resolve_rows(conn, rows)
    return [dict(r) for r in rows]
