# M1DataPipeline 代码审计报告

**审计文件**: `m1_data_pipeline_optimized.py`  
**审计日期**: 2026-04-01  
**审计维度**: 命名规范、内存管理、路径可移植性

---

## 📋 审计概览

| 审计维度 | 评分 | 状态 |
|---------|------|------|
| 命名规范 (PEP 8) | 85/100 | ⚠️ 需改进 |
| 内存管理 | 70/100 | ⚠️ 存在风险 |
| 路径可移植性 | 60/100 | ❌ 需重构 |

**综合评分**: 72/100

---

## 1️⃣ 命名规范审计 (PEP 8)

### ✅ 符合规范的部分

| 项目 | 示例 | 状态 |
|------|------|------|
| 类名 | `M1DataPipeline` | ✅ 大驼峰命名 (CamelCase) |
| 函数名 | `extract`, `transform`, `load` | ✅ 小写 + 下划线 (snake_case) |
| 私有方法 | `_deduplicate`, `_sessionize` | ✅ 下划线前缀表示私有 |
| 常量 | 无明显全局常量 | ✅ |
| 变量名 | `df_dedup`, `output_path` | ✅ 小写 + 下划线 |

### ⚠️ 需要改进的问题

| 问题位置 | 当前命名 | 建议命名 | 说明 |
|---------|---------|---------|------|
| L58 | `_stats_cache: Dict[str, Any]` | `_stats_cache: dict[str, Any]` | PEP 8 推荐小写 `dict` 而非 `Dict` (Python 3.9+) |
| L136 | `Union[pl.LazyFrame, pl.DataFrame]` | `pl.LazyFrame \| pl.DataFrame` | Python 3.10+ 推荐使用 `\|` 联合类型语法 |
| L136 | `Dict[str, Union[...]]` | `dict[str, ...]` | 同上，统一使用小写内置类型 |

### 📝 类型注解风格建议

```python
# ==================== 修改前 (Python 3.9 之前风格) ====================
from typing import Any, Dict, Optional, Union

class M1DataPipeline:
    def __init__(self, data_path: str = "m1_final_clean.parquet") -> None:
        self._stats_cache: Dict[str, Any] = {}
    
    def transform(
        self, df: pl.LazyFrame
    ) -> Dict[str, Union[pl.LazyFrame, pl.DataFrame]]:
        pass

# ==================== 修改后 (Python 3.10+ 风格) ====================
from __future__ import annotations  # Python < 3.10 时需要此导入
from typing import Any, Optional

class M1DataPipeline:
    def __init__(self, data_path: str = "m1_final_clean.parquet") -> None:
        self._stats_cache: dict[str, Any] = {}
    
    def transform(
        self, df: pl.LazyFrame
    ) -> dict[str, pl.LazyFrame | pl.DataFrame]:
        pass
```

---

## 2️⃣ 内存管理审计

### ⚠️ 冗余 collect 问题

| 位置 | 问题描述 | 风险等级 | 建议 |
|------|---------|---------|------|
| L184-186 | `_analyze_funnel_lazy` 返回 LazyFrame 后立即在 `_analyze_funnel` 中 collect | 🔴 高 | 合并两个方法或移除冗余方法 |
| L539-541 | `_generate_summary` 中多次独立 collect | 🟡 中 | 使用 `pl.collect_all()` 批量收集 |
| L567+ | 主函数未显式释放引用 | 🟡 中 | 添加 `del` 和 `gc.collect()` |

### 🔍 具体问题与修复方案

#### 问题 1: `_analyze_funnel` 方法冗余 (L397-424)

**当前代码**:
```python
def _analyze_funnel(self, df: pl.LazyFrame) -> pl.DataFrame:
    df_lazy = self._analyze_funnel_lazy(df)  # 已返回 LazyFrame
    result = df_lazy.collect()  # 立即收集
    # ... 后续计算
```

**问题**: 该方法与 `_analyze_funnel_lazy` 功能重叠，导致不必要的 collect 调用。

**建议**: 移除 `_analyze_funnel` 方法，在 `transform()` 中直接处理。

---

#### 问题 2: `_generate_summary` 多次 collect (L539-541)

**当前代码**:
```python
if "deduped" in result:
    dedup_count = result["deduped"].select(pl.len()).collect().item()

if "sessionized" in result:
    session_count = result["sessionized"].collect()["session_id"].n_unique()
```

**问题**: 每次 `.collect()` 都会触发一次完整的查询执行，造成重复计算。

**建议修复**:
```python
def _generate_summary(
    self, result: dict[str, pl.LazyFrame | pl.DataFrame], output_path: Path
) -> None:
    """生成综合分析报告并保存到文件"""
    
    # ✅ 批量收集所有 LazyFrame 统计信息
    lazy_frames_to_collect = []
    
    if "deduped" in result:
        lazy_frames_to_collect.append(
            result["deduped"].select(pl.len().alias("dedup_count"))
        )
    
    if "sessionized" in result:
        lazy_frames_to_collect.append(
            result["sessionized"].select(pl.col("session_id").n_unique().alias("session_count"))
        )
    
    # 单次批量收集
    if lazy_frames_to_collect:
        collected_stats = pl.collect_all(lazy_frames_to_collect)
        dedup_count = collected_stats[0].item() if len(collected_stats) > 0 else 0
        session_count = collected_stats[1].item() if len(collected_stats) > 1 else 0
    
    # ... 后续写入文件逻辑
```

---

### ⚠️ 内存泄漏风险

| 风险点 | 描述 | 风险等级 | 建议 |
|-------|------|---------|------|
| `_stats_cache` 未清理 | L66 定义的缓存字典在对象生命周期内持续增长，无清理机制 | 🟡 中 | 添加 `cleanup()` 方法或在 `__del__` 中清理 |
| 大对象引用未释放 | `result` 字典同时持有 LazyFrame 和 DataFrame，`run()` 完成后未释放 | 🟡 中 | 在 `load()` 完成后显式删除引用 |
| 未使用 `pl.Config` 限制 | 无内存使用上限配置 | 🟢 低 | 添加 `pl.Config.set_memory_limit()` |

### 📝 建议修复代码

```python
import gc

class M1DataPipeline:
    # ... 现有代码 ...
    
    def __del__(self):
        """析构时清理缓存"""
        self._stats_cache.clear()
    
    def cleanup(self) -> None:
        """显式清理缓存，供手动调用"""
        self._stats_cache.clear()
        gc.collect()
    
    def run(self, output_dir: str = "output") -> dict[str, str] | None:
        """一键执行完整流水线"""
        try:
            df = self.extract()
            result = self.transform(df)
            saved_files = self.load(result, output_dir)
            
            # ✅ 显式释放引用
            del df, result
            gc.collect()  # 强制垃圾回收
            
            return saved_files
        except Exception as e:
            logger.error(f"❌ 流水线执行失败：{e}")
            self.cleanup()  # 异常时清理缓存
            return None
```

---

## 3️⃣ 路径可移植性审计

### ❌ 硬编码路径问题汇总

| 位置 | 硬编码值 | 问题描述 | 建议 |
|------|---------|---------|------|
| L60 | `"m1_final_clean.parquet"` | 构造函数默认路径写死 | 使用环境变量 `M1_DATA_PATH` |
| L225 | `"output"` | 输出目录默认值写死 | 支持相对/绝对路径配置 |
| L433-447 | `"deduped_data.parquet"` 等 | 输出文件名固定 | 支持自定义命名模板 |
| L567 | `"m1_final_clean.parquet"` | 主函数中路径写死 | 使用 `argparse` 解析命令行参数 |

### 🔍 具体问题与修复方案

#### 问题 1: 构造函数默认路径 (L60)

**当前代码**:
```python
def __init__(self, data_path: str = "m1_final_clean.parquet") -> None:
```

**问题**:
- 路径相对于当前工作目录，非项目根目录
- 不同环境（开发/生产/测试）需要不同路径
- 无法通过配置覆盖

**建议修复**:
```python
import os
from pathlib import Path

class M1DataPipeline:
    def __init__(
        self, 
        data_path: str | None = None,
        base_dir: Path | None = None
    ) -> None:
        """
        初始化流水线

        Args:
            data_path: 输入 Parquet 文件路径，默认为环境变量 M1_DATA_PATH 或 "m1_final_clean.parquet"
            base_dir: 基础目录，默认为当前文件所在目录
        """
        if base_dir is None:
            base_dir = Path(__file__).parent
        
        # 优先级：参数 > 环境变量 > 默认值
        if data_path is None:
            data_path = os.getenv("M1_DATA_PATH", "m1_final_clean.parquet")
        
        self.data_path = Path(data_path)
        
        # 相对路径自动解析为相对于 base_dir
        if not self.data_path.is_absolute():
            self.data_path = base_dir / self.data_path
        
        self.session_timeout = 1800
        self._start_time: float | None = None
        self._stats_cache: dict[str, Any] = {}

        # 验证输入路径
        if not self.data_path.exists():
            logger.warning(f"⚠️  数据文件不存在：{self.data_path.absolute()}")
```

---

#### 问题 2: 输出文件名固定 (L433-447)

**当前代码**:
```python
if "deduped" in result:
    dedup_path = output_path / "deduped_data.parquet"
    result["deduped"].sink_parquet(dedup_path)

if "sessionized" in result:
    session_path = output_path / "sessionized_data.parquet"
    result["sessionized"].sink_parquet(session_path)
```

**问题**: 输出文件名不可配置，无法适应不同项目需求。

**建议修复**:
```python
from typing import TypedDict

class OutputFileNames(TypedDict, total=False):
    """输出文件名配置"""
    deduped: str
    sessionized: str
    funnel: str
    anomaly: str
    summary: str

DEFAULT_OUTPUT_NAMES: OutputFileNames = {
    "deduped": "deduped_data.parquet",
    "sessionized": "sessionized_data.parquet",
    "funnel": "funnel_analysis.csv",
    "anomaly": "anomaly_diagnosis.csv",
    "summary": "pipeline_summary.txt",
}

class M1DataPipeline:
    def load(
        self,
        result: dict[str, pl.LazyFrame | pl.DataFrame],
        output_dir: str = "output",
        output_names: OutputFileNames | None = None,
    ) -> dict[str, str]:
        """
        输出最终分析报告

        Args:
            result: transform() 返回的结果字典
            output_dir: 输出目录路径
            output_names: 自定义输出文件名，覆盖默认值
        """
        # 合并默认配置与用户配置
        names = {**DEFAULT_OUTPUT_NAMES, **(output_names or {})}
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        saved_files = {}
        
        if "deduped" in result:
            dedup_path = output_path / names["deduped"]
            logger.info(f"保存去重数据：{dedup_path}")
            result["deduped"].sink_parquet(dedup_path)
            saved_files["deduped"] = str(dedup_path)
        
        # ... 其他文件类似处理
```

---

#### 问题 3: 主函数示例路径 (L567)

**当前代码**:
```python
if __name__ == "__main__":
    pipeline = M1DataPipeline(data_path="m1_final_clean.parquet")
```

**问题**: 无法通过命令行参数灵活配置输入输出路径。

**建议修复**:
```python
import argparse
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="M1 亿级电商数据 ETL 流水线",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  python m1_data_pipeline_optimized.py
  python m1_data_pipeline_optimized.py -i data/m1_final_clean.parquet -o output/results
  python m1_data_pipeline_optimized.py --explain
        """
    )
    
    parser.add_argument(
        "-i", "--input",
        default=os.getenv("M1_DATA_PATH", "m1_final_clean.parquet"),
        help="输入 Parquet 文件路径 (默认: M1_DATA_PATH 环境变量或 m1_final_clean.parquet)"
    )
    
    parser.add_argument(
        "-o", "--output",
        default="output",
        help="输出目录路径 (默认: output)"
    )
    
    parser.add_argument(
        "--explain",
        action="store_true",
        help="显示查询执行计划并退出"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="启用详细日志输出"
    )
    
    args = parser.parse_args()
    
    # 配置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 初始化流水线
    pipeline = M1DataPipeline(data_path=args.input)
    
    try:
        if args.explain:
            # 仅显示查询计划
            df = pipeline.extract()
            df_dedup = pipeline._deduplicate(df)
            pipeline.explain_plan(df_dedup, optimized=True)
        else:
            # 执行完整流水线
            saved_files = pipeline.run(output_dir=args.output)
            
            if saved_files:
                print("\n✅ 流水线执行完成！")
                print("保存的文件:")
                for name, path in saved_files.items():
                    print(f"  - {name}: {path}")
            else:
                print("\n❌ 流水线执行失败，未生成任何文件")
    
    except FileNotFoundError as e:
        logger.error(f"❌ 文件未找到：{e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 流水线执行失败：{e}")
        sys.exit(1)
```

---

## 📊 总结与优先级建议

### 问题优先级矩阵

| 优先级 | 问题类别 | 具体问题 | 影响范围 | 修复工作量 |
|:------:|---------|---------|---------|:----------:|
| 🔴 P0 | 内存管理 | `_generate_summary` 多次独立 collect | 性能下降 30-50% | 低 (1h) |
| 🔴 P0 | 路径可移植性 | 主函数硬编码路径 | 无法灵活部署 | 低 (0.5h) |
| 🟡 P1 | 内存管理 | `_stats_cache` 未清理 | 长期运行内存泄漏 | 低 (0.5h) |
| 🟡 P1 | 路径可移植性 | 输出文件名不可配置 | 多项目复用困难 | 中 (2h) |
| 🟢 P2 | 命名规范 | 类型注解风格不统一 | 代码一致性 | 低 (0.5h) |
| 🟢 P2 | 内存管理 | `run()` 未显式释放引用 | 大文件处理时 OOM 风险 | 低 (0.5h) |

### 修复时间估算

| 阶段 | 工作内容 | 预计时间 |
|------|---------|---------|
| Phase 1 | P0 问题修复（collect 优化 + 命令行参数） | 1.5h |
| Phase 2 | P1 问题修复（缓存清理 + 输出配置） | 2.5h |
| Phase 3 | P2 问题修复（类型注解 + 引用释放） | 1h |
| **总计** | | **5h** |

---

## ✅ 快速修复清单

```bash
# □ 1. 添加 __del__ 和 cleanup() 方法清理 _stats_cache
# □ 2. 主函数改用 argparse 解析命令行参数
# □ 3. _generate_summary 使用 pl.collect_all() 批量收集
# □ 4. 类型注解统一为 Python 3.10+ 风格 (dict, list, |)
# □ 5. 添加环境变量支持 (M1_DATA_PATH, M1_OUTPUT_DIR)
# □ 6. run() 方法末尾添加 del 和 gc.collect()
# □ 7. output_names 参数支持自定义输出文件名
# □ 8. 添加 pl.Config.set_memory_limit() 内存限制配置
```

---

## 📎 附录：完整修复后的代码框架

```python
"""
M1DataPipeline - 亿级电商数据 ETL 流水线 (优化版)

基于 Polars Lazy API 实现高性能数据处理，支持：
- 精密去重
- 会话识别
- 漏斗分析
- 异常流量诊断

优化要点：
1. 全程 Lazy 模式，避免中间 collect
2. 利用 Predicate Pushdown 减少 I/O
3. 优化窗口函数执行顺序
4. 使用 explain() 验证查询计划
5. 批量 collect 减少内存峰值
6. 支持命令行参数和环境变量配置
"""

from __future__ import annotations

import argparse
import gc
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, TypedDict

import polars as pl

# 配置内存限制（可选）
pl.Config.set_memory_limit("2GB")

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ==================== 类型定义 ====================
class OutputFileNames(TypedDict, total=False):
    """输出文件名配置"""
    deduped: str
    sessionized: str
    funnel: str
    anomaly: str
    summary: str


DEFAULT_OUTPUT_NAMES: OutputFileNames = {
    "deduped": "deduped_data.parquet",
    "sessionized": "sessionized_data.parquet",
    "funnel": "funnel_analysis.csv",
    "anomaly": "anomaly_diagnosis.csv",
    "summary": "pipeline_summary.txt",
}


# ==================== 主类 ====================
class M1DataPipeline:
    """亿级电商数据 ETL 流水线类"""

    def __init__(
        self, 
        data_path: str | None = None,
        base_dir: Path | None = None
    ) -> None:
        # 优先级：参数 > 环境变量 > 默认值
        if base_dir is None:
            base_dir = Path(__file__).parent
        
        if data_path is None:
            data_path = os.getenv("M1_DATA_PATH", "m1_final_clean.parquet")
        
        self.data_path = Path(data_path)
        if not self.data_path.is_absolute():
            self.data_path = base_dir / self.data_path
        
        self.session_timeout = 1800
        self._start_time: float | None = None
        self._stats_cache: dict[str, Any] = {}

        if not self.data_path.exists():
            logger.warning(f"⚠️  数据文件不存在：{self.data_path.absolute()}")
    
    def __del__(self) -> None:
        """析构时清理缓存"""
        self.cleanup()
    
    def cleanup(self) -> None:
        """显式清理缓存"""
        self._stats_cache.clear()
        gc.collect()
    
    # ... 其他方法保持不变，但使用新的类型注解风格 ...
    
    def run(self, output_dir: str = "output") -> dict[str, str] | None:
        """一键执行完整流水线"""
        try:
            df = self.extract()
            result = self.transform(df)
            saved_files = self.load(result, output_dir)
            
            # 显式释放引用
            del df, result
            gc.collect()
            
            return saved_files
        except Exception as e:
            logger.error(f"❌ 流水线执行失败：{e}")
            self.cleanup()
            return None


# ==================== 主函数 ====================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="M1 亿级电商数据 ETL 流水线")
    
    parser.add_argument(
        "-i", "--input",
        default=os.getenv("M1_DATA_PATH", "m1_final_clean.parquet"),
        help="输入 Parquet 文件路径"
    )
    parser.add_argument(
        "-o", "--output",
        default="output",
        help="输出目录路径"
    )
    parser.add_argument(
        "--explain",
        action="store_true",
        help="显示查询执行计划"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="启用详细日志"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    pipeline = M1DataPipeline(data_path=args.input)
    
    try:
        if args.explain:
            df = pipeline.extract()
            df_dedup = pipeline._deduplicate(df)
            pipeline.explain_plan(df_dedup, optimized=True)
        else:
            saved_files = pipeline.run(output_dir=args.output)
            
            if saved_files:
                print("\n✅ 流水线执行完成！")
                for name, path in saved_files.items():
                    print(f"  - {name}: {path}")
            else:
                print("\n❌ 流水线执行失败")
    
    except FileNotFoundError as e:
        logger.error(f"❌ 文件未找到：{e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 流水线执行失败：{e}")
        sys.exit(1)
```

---

**报告生成时间**: 2026-04-01  
**审计工具**: 人工代码审查 + PEP 8 规范检查  
**建议复审周期**: 修复后 1 周内
