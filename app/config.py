"""应用配置。

通过环境变量覆盖默认值，Compose 中默认指向 PostgreSQL 服务。
"""
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", case_sensitive=False)

    # 形如 postgresql://user:pass@host:port/dbname
    database_url: str = "postgresql://water:water@localhost:5432/water"
    db_connect_retries: int = 30
    db_connect_retry_interval: float = 1.0
    # 历史记录接口最多返回条数
    history_limit: int = 100
    # 快照增量存储：距最近全量快照的最大增量跳数（超过则本条改存全量，
    # 读取回溯深度由此有界；0 表示关闭增量存储、全部存全量）
    snapshot_delta_max_depth: int = 32
    # 增量载荷达到全量状态的该比例时直接存全量（变化面过大增量不省空间）
    snapshot_delta_full_ratio: float = 0.6


@lru_cache
def get_settings() -> Settings:
    return Settings()
