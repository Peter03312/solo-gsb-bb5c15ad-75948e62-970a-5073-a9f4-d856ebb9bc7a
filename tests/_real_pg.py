"""供真实 PostgreSQL 集成测试使用的“未被内存替身 patch”的 app.db 副本。

tests/test_api.py 在导入时会把 app.db 的数据访问函数整体替换为内存替身；
RUN_DB_TESTS=1 下的真实库测试（无论位于 test_api.py 还是独立测试模块，
且不依赖导入先后顺序）需要一份干净的真实访问层。

这里不做 importlib.reload（那会改写共享的 app.db 模块、污染后续替身测试），
而是在独立模块命名空间 app._real_db 中执行 app/db.py 源码：相对导入
（from . import audit 等）仍解析到真实 app 包，得到的模块函数全部为真实
PostgreSQL 实现，且与 app.db 单例互不影响。
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

_DB_SOURCE = Path(__file__).resolve().parent.parent / "app" / "db.py"


def load_real_db():
    """返回（必要时惰性创建并缓存）未被 monkeypatch 的真实 app.db 模块副本。"""
    name = "app._real_db"
    existing = sys.modules.get(name)
    if existing is not None:
        return existing

    module = types.ModuleType(name)
    module.__package__ = "app"
    module.__file__ = str(_DB_SOURCE)
    sys.modules[name] = module
    try:
        with _DB_SOURCE.open(encoding="utf-8") as f:
            code = compile(f.read(), str(_DB_SOURCE), "exec")
        exec(code, module.__dict__)
    except Exception:
        del sys.modules[name]
        raise
    return module
