"""状态快照的增量（delta）存储：小范围变更只持久化相对前一条快照的净变化。

背景：隔离提交、复供确认、水源切换等高频调度操作每次只改变少量阀位 /
水源运行状态 / 待修复故障，但历史上每条快照都把**全网**完整状态写入
``state_snapshots.state``，新增持久化量随未变化的管网规模线性增长。

本模块提供纯函数（不依赖数据库与第三方库）：

- :func:`compute_delta`：由“上一条完整状态 → 本次完整状态”计算增量载荷；
- :func:`apply_delta`：由“基线完整状态 + 增量载荷”还原完整状态，
  结果与 :func:`app.planning.freeze_state` 的输出逐字节一致（同样的排序
  口径：节点/管段按编号、待修复故障按事件编号、可达用水点按编号排序），
  因此摘要链仍可对**还原后的完整状态**复现计算；
- :func:`should_store_full`：存储决策——整网重导/旧库基线等天然全量，
  或增量链过深、增量载荷接近全量大小时回退全量，限制读取时的回溯深度。

增量载荷格式（确定性 JSON，键与列表顺序固定，便于比对与调试）::

    {
      "format": "water-snapshot-delta/v1",
      "nodes":  {"upsert": [<完整节点对象>, ...], "remove": [<节点编号>, ...]},
      "pipes":  {"upsert": [<完整管段对象>, ...], "remove": [<管段编号>, ...]},
      "open_faults": {"upsert": [<完整故障对象>, ...], "remove": [<事件编号>, ...]},
      "supplied_sink_ids": {"add": [<用水点编号>, ...], "remove": [...]}
    }

- 节点/管段以 ``id`` 为标识：新增或任一字段变化的存**完整新对象**
  （upsert），消失的存编号（remove）；未变化的实体不产生任何字节；
- 待修复故障以 ``event_id`` 为标识，口径同上；
- 可达用水点为集合净变化（add/remove）。

快照编号、摘要与链前驱关系与全量存储时代完全一致：增量只是持久化形式，
读取时先还原完整状态再返回/重算摘要。
"""
from __future__ import annotations

from typing import Any

from .audit import canonical_json

# 增量载荷格式标识：写入 delta["format"]，改动即构成新格式
DELTA_FORMAT = "water-snapshot-delta/v1"


# ---------------------------------------------------------------- 增量计算


def _index_by(items: list[dict], key: str) -> dict[Any, dict]:
    return {item[key]: item for item in items}


def _diff_entities(
    base_items: list[dict], target_items: list[dict], key: str
) -> dict:
    """按标识字段对照两列实体，返回 {"upsert": [...], "remove": [...]}。

    upsert 为新增或内容变化的**目标侧完整对象**（按标识升序）；
    remove 为基线有而目标没有的标识（升序）。未变化实体不占任何空间。
    """
    base = _index_by(base_items, key)
    target = _index_by(target_items, key)
    upsert = [
        target[k] for k in sorted(target) if k not in base or base[k] != target[k]
    ]
    remove = sorted(k for k in base if k not in target)
    return {"upsert": upsert, "remove": remove}


def compute_delta(base_state: dict, target_state: dict) -> dict:
    """计算 target_state 相对 base_state 的增量载荷（两者均为完整冻结状态）。"""
    base_sinks = set(base_state.get("supplied_sink_ids", []))
    target_sinks = set(target_state.get("supplied_sink_ids", []))
    return {
        "format": DELTA_FORMAT,
        "nodes": _diff_entities(
            base_state.get("nodes", []), target_state.get("nodes", []), "id"
        ),
        "pipes": _diff_entities(
            base_state.get("pipes", []), target_state.get("pipes", []), "id"
        ),
        "open_faults": _diff_entities(
            base_state.get("open_faults", []),
            target_state.get("open_faults", []),
            "event_id",
        ),
        "supplied_sink_ids": {
            "add": sorted(target_sinks - base_sinks),
            "remove": sorted(base_sinks - target_sinks),
        },
    }


# ---------------------------------------------------------------- 增量套用


def _apply_entities(
    base_items: list[dict], section: dict, key: str
) -> list[dict]:
    """基线实体列 + {"upsert","remove"} -> 完整实体列（按标识升序）。"""
    merged = _index_by(base_items, key)
    for k in section.get("remove", []):
        merged.pop(k, None)
    for item in section.get("upsert", []):
        merged[item[key]] = item
    return [merged[k] for k in sorted(merged)]


def apply_delta(base_state: dict, delta: dict) -> dict:
    """由基线完整状态与增量载荷还原完整冻结状态。

    输出排序口径与 freeze_state 一致：节点/管段按编号升序、待修复故障按
    事件编号升序、可达用水点按编号升序；因此“全量存储的状态”与
    “基线 + 增量还原的状态”canonical_json 逐字节一致，摘要可复现。
    """
    base_sinks = set(base_state.get("supplied_sink_ids", []))
    sink_section = delta.get("supplied_sink_ids", {})
    sinks = (base_sinks - set(sink_section.get("remove", []))) | set(
        sink_section.get("add", [])
    )
    return {
        "nodes": _apply_entities(
            base_state.get("nodes", []), delta.get("nodes", {}), "id"
        ),
        "pipes": _apply_entities(
            base_state.get("pipes", []), delta.get("pipes", {}), "id"
        ),
        "open_faults": _apply_entities(
            base_state.get("open_faults", []),
            delta.get("open_faults", {}),
            "event_id",
        ),
        "supplied_sink_ids": sorted(sinks),
    }


# ---------------------------------------------------------------- 存储决策


def state_payload_size(state: dict) -> int:
    """完整冻结状态的可复现编码字节数（与摘要链同一 canonical_json 口径）。"""
    return len(canonical_json(state).encode("utf-8"))


def delta_payload_size(delta: dict) -> int:
    """增量载荷的可复现编码字节数。"""
    return len(canonical_json(delta).encode("utf-8"))


def should_store_full(
    *,
    force_full: bool,
    base_available: bool,
    next_depth: int,
    max_depth: int,
    delta_size: int,
    full_size: int,
    full_ratio: float,
) -> bool:
    """存储决策：返回 True 表示本条快照应存全量，False 存增量。

    - force_full：整网重导 / 旧库基线等天然全量检查点（变化本身是整网级，
      增量不会更小；同时充当增量链的固定回溯终点）；
    - base_available=False：全库首条快照，无基线可差；
    - next_depth > max_depth：限制增量链深度，读取回溯跳数有界；
    - delta_size >= full_ratio * full_size：变化面过大时增量不省空间，
      直接存全量（同时把链深重新归零）。
    """
    if force_full or not base_available:
        return True
    if next_depth > max_depth:
        return True
    return delta_size >= full_ratio * full_size
