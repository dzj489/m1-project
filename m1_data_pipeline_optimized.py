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
    """
    亿级电商数据 ETL 流水线类

    使用 Polars Lazy API 进行懒加载和惰性计算，避免 OOM 问题。
    所有转换操作在 collect() 前不会实际执行，确保内存效率。

    Attributes:
        data_path: 输入 Parquet 文件路径
        session_timeout: 会话超时阈值（秒），默认 30 分钟
        _start_time: 流水线启动时间戳
        _stats_cache: 缓存统计信息的字典，避免重复 collect

    Example:
        >>> pipeline = M1DataPipeline("data.parquet")
        >>> saved_files = pipeline.run(output_dir="output")
    """

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

        Example:
            >>> # 使用默认配置
            >>> pipeline = M1DataPipeline()
            >>> # 使用自定义路径
            >>> pipeline = M1DataPipeline(data_path="/path/to/data.parquet")
            >>> # 使用环境变量 M1_DATA_PATH
            >>> pipeline = M1DataPipeline()
        """
        # 确定基础目录
        if base_dir is None:
            base_dir = Path(__file__).parent

        # 优先级：参数 > 环境变量 > 默认值
        if data_path is None:
            data_path = os.getenv("M1_DATA_PATH", "m1_final_clean.parquet")

        self.data_path = Path(data_path)

        # 相对路径自动解析为相对于 base_dir
        if not self.data_path.is_absolute():
            self.data_path = base_dir / self.data_path

        self.session_timeout = 1800  # 会话超时阈值：30 分钟（秒）
        self._start_time: float | None = None
        self._stats_cache: dict[str, Any] = {}  # 缓存统计信息，避免重复 collect

        # 验证输入路径
        if not self.data_path.exists():
            logger.warning(f"⚠️  数据文件不存在：{self.data_path.absolute()}")

    def __del__(self) -> None:
        """析构时清理缓存"""
        self.cleanup()

    def cleanup(self) -> None:
        """
        显式清理缓存

        调用此方法可手动释放缓存的 LazyFrame 和触发垃圾回收，
        建议在长时间运行或处理大文件后调用。
        """
        self._stats_cache.clear()
        gc.collect()
        logger.info("✅ 缓存已清理")

    # ==================== Step 1: 数据提取 ====================
    def extract(self) -> pl.LazyFrame:
        """
        懒加载读取 m1_final_clean.parquet

        Returns:
            pl.LazyFrame: 惰性数据帧，未实际加载数据

        Raises:
            FileNotFoundError: 文件不存在时抛出

        Example:
            >>> pipeline = M1DataPipeline("data.parquet")
            >>> df = pipeline.extract()
        """
        logger.info("=" * 60)
        logger.info("【Step 1】EXTRACT - 数据提取")
        logger.info("=" * 60)

        step_start = time.time()

        try:
            if not self.data_path.exists():
                raise FileNotFoundError(f"数据文件不存在：{self.data_path}")

            # ✅ 优化：使用 scan_parquet 懒加载
            # 可根据需求启用 predicate 参数实现谓词下推，减少 I/O
            # 例如：predicate=pl.col("user_id").is_not_null()
            df = pl.scan_parquet(
                self.data_path,
            )

            # 获取元数据信息（不触发 collect）
            schema = df.collect_schema()

            elapsed = time.time() - step_start
            logger.info(f"✅ 成功加载数据文件：{self.data_path.name}")
            logger.info(f"📊 Schema 字段数：{len(schema)}")
            logger.info(f"⏱️  耗时：{elapsed:.2f} 秒")

            return df

        except FileNotFoundError as e:
            logger.error(f"❌ 文件未找到：{e}")
            raise
        except Exception as e:
            logger.error(f"❌ 数据提取失败：{e}")
            raise

    # ==================== Step 2: 数据转换 ====================
    def transform(
        self, df: pl.LazyFrame
    ) -> dict[str, pl.LazyFrame | pl.DataFrame]:
        """
        执行数据转换：去重、会话识别、漏斗分析、异常诊断

        Args:
            df: 输入的 LazyFrame

        Returns:
            dict: 包含各阶段结果的字典
                - deduped: 去重后的 LazyFrame
                - sessionized: 带会话标识的 LazyFrame
                - funnel_lazy: 漏斗分析结果 LazyFrame (新增)
                - funnel: 漏斗分析结果 DataFrame
                - anomaly: 异常流量诊断结果 DataFrame

        Raises:
            ValueError: 输入为空时抛出

        Example:
            >>> df = pipeline.extract()
            >>> result = pipeline.transform(df)
        """
        logger.info("")
        logger.info("=" * 60)
        logger.info("【Step 2】TRANSFORM - 数据转换")
        logger.info("=" * 60)

        try:
            if df is None:
                raise ValueError("输入数据框为空")

            results: dict[str, pl.LazyFrame | pl.DataFrame] = {}

            # --- 2.1 精密去重 ---
            logger.info("")
            logger.info("[2.1] 执行精密去重...")
            step_start = time.time()

            df_dedup = self._deduplicate(df)
            results["deduped"] = df_dedup

            elapsed = time.time() - step_start
            logger.info(f"✅ 去重完成，耗时：{elapsed:.2f} 秒")

            # --- 2.2 会话识别 ---
            logger.info("")
            logger.info("[2.2] 执行会话识别...")
            step_start = time.time()

            df_session = self._sessionize(df_dedup)
            results["sessionized"] = df_session

            elapsed = time.time() - step_start
            logger.info(f"✅ 会话识别完成，耗时：{elapsed:.2f} 秒")

            # --- 2.3 漏斗分析 ---
            logger.info("")
            logger.info("[2.3] 执行漏斗分析...")
            step_start = time.time()

            # ✅ 优化：先获取 Lazy 版本，避免中间 collect
            df_funnel_lazy = self._analyze_funnel_lazy(df_dedup)
            results["funnel_lazy"] = df_funnel_lazy  # 保持 Lazy 供后续使用
            # 同时收集结果用于报告
            results["funnel"] = df_funnel_lazy.collect()

            elapsed = time.time() - step_start
            logger.info(f"✅ 漏斗分析完成，耗时：{elapsed:.2f} 秒")

            # --- 2.4 异常流量诊断 ---
            logger.info("")
            logger.info("[2.4] 执行异常流量诊断...")
            step_start = time.time()

            # ✅ 优化：单次扫描完成所有异常检测
            df_anomaly = self._diagnose_anomaly_optimized(df_dedup)
            results["anomaly"] = df_anomaly

            elapsed = time.time() - step_start
            logger.info(f"✅ 异常诊断完成，耗时：{elapsed:.2f} 秒")

            return results

        except ValueError as e:
            logger.error(f"❌ 输入验证失败：{e}")
            raise
        except Exception as e:
            logger.error(f"❌ 数据转换失败：{e}")
            raise

    # ==================== Step 3: 数据加载 ====================
    def load(
        self,
        result: dict[str, pl.LazyFrame | pl.DataFrame],
        output_dir: str = "output",
        output_names: OutputFileNames | None = None,
    ) -> dict[str, str]:
        """
        输出最终分析报告，保存可视化结果或中间文件

        Args:
            result: transform() 返回的结果字典
            output_dir: 输出目录路径
            output_names: 自定义输出文件名，覆盖默认值

        Returns:
            dict: 保存的文件路径映射

        Raises:
            ValueError: 结果字典为空时抛出

        Example:
            >>> result = pipeline.transform(df)
            >>> saved = pipeline.load(result, output_dir="output")
        """
        logger.info("")
        logger.info("=" * 60)
        logger.info("【Step 3】LOAD - 数据加载与输出")
        logger.info("=" * 60)

        step_start = time.time()

        try:
            if not result:
                raise ValueError("结果字典为空")

            # 合并默认配置与用户配置
            names = {**DEFAULT_OUTPUT_NAMES, **(output_names or {})}

            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)

            saved_files: dict[str, str] = {}

            # --- 3.1 保存去重数据 ---
            if "deduped" in result:
                dedup_path = output_path / names["deduped"]
                logger.info(f"保存去重数据：{dedup_path}")
                result["deduped"].sink_parquet(dedup_path)
                saved_files["deduped"] = str(dedup_path)

            # --- 3.2 保存会话数据 ---
            if "sessionized" in result:
                session_path = output_path / names["sessionized"]
                logger.info(f"保存会话数据：{session_path}")
                result["sessionized"].sink_parquet(session_path)
                saved_files["sessionized"] = str(session_path)

            # --- 3.3 保存漏斗分析报告 ---
            if "funnel" in result:
                funnel_path = output_path / names["funnel"]
                logger.info(f"保存漏斗分析：{funnel_path}")
                result["funnel"].write_csv(funnel_path)
                saved_files["funnel"] = str(funnel_path)

                # 打印漏斗摘要
                self._print_funnel_summary(result["funnel"])

            # --- 3.4 保存异常诊断报告 ---
            if "anomaly" in result:
                anomaly_path = output_path / names["anomaly"]
                logger.info(f"保存异常诊断：{anomaly_path}")
                result["anomaly"].write_csv(anomaly_path)
                saved_files["anomaly"] = str(anomaly_path)

            # --- 3.5 生成综合分析报告 ---
            summary_path = output_path / names["summary"]
            self._generate_summary(result, summary_path)
            saved_files["summary"] = str(summary_path)

            elapsed = time.time() - step_start
            logger.info(f"✅ 所有文件保存完成，耗时：{elapsed:.2f} 秒")
            logger.info(f"📁 输出目录：{output_path.absolute()}")

            return saved_files

        except ValueError as e:
            logger.error(f"❌ 输入验证失败：{e}")
            raise
        except Exception as e:
            logger.error(f"❌ 数据加载失败：{e}")
            raise

    # ==================== 核心转换方法（优化版） ====================

    def _deduplicate(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """
        四维度精密去重 - 优化版

        基于 user_id, item_id, behavior_type, timestamp 去重，保留首次出现记录。

        Args:
            df: 输入的 LazyFrame

        Returns:
            pl.LazyFrame: 去重后的 LazyFrame

        Note:
            优化：避免中间 collect，统计信息延迟到最后。
        """
        df_dedup = df.unique(
            subset=["user_id", "item_id", "behavior_type", "timestamp"],
            keep="first"
        )

        # ✅ 优化：不立即 collect，返回 LazyFrame
        # 统计信息通过缓存机制在最终 collect 时获取
        self._stats_cache["dedup_lazy"] = df_dedup

        logger.info("   去重操作已加入执行计划（延迟计算）")
        return df_dedup

    def _sessionize(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """
        基于时间窗口的会话识别 - 优化版

        规则：
        - 同一用户相邻行为间隔 > 30 分钟，视为新会话
        - 生成全局唯一 session_id = user_id + session_seq

        Args:
            df: 输入的 LazyFrame（需包含 user_id 和 timestamp 字段）

        Returns:
            pl.LazyFrame: 带有 session_id 标识的 LazyFrame

        Note:
            优化窗口函数执行顺序：
            1. 在 over() 中指定 order_by，让优化器更好利用排序
            2. 避免中间 collect
        """
        df_session = (
            df
            # ✅ 优化：在 over 中指定排序，而非先 sort()
            .with_columns(
                # 计算相邻行为时间差 - 窗口函数在排序前定义
                (pl.col("timestamp") - pl.col("timestamp").shift(1))
                .over("user_id", order_by="timestamp")  # ✅ 在 over 中指定排序
                .alias("timediff")
            )
            # 标记新会话起点
            .with_columns(
                pl.when(
                    pl.col("timediff").is_null() |
                    (pl.col("timediff") > self.session_timeout)
                )
                .then(1)
                .otherwise(0)
                .alias("new_session")
            )
            # 累加生成会话序号
            .with_columns(
                pl.col("new_session").cum_sum()
                .over("user_id")
                .alias("session_seq")
            )
            # 生成全局唯一 session_id
            .with_columns(
                (pl.col("user_id").cast(str) + "_" + pl.col("session_seq").cast(str))
                .alias("session_id")
            )
            # 丢弃中间字段
            .drop("timediff", "new_session", "session_seq")
        )

        logger.info("   会话识别已加入执行计划（延迟计算）")
        return df_session

    def _analyze_funnel_lazy(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """
        电商转化漏斗分析 - Lazy 版本

        阶段定义：
        - Stage 1: PV（浏览）
        - Stage 2: Fav/Cart（收藏/加购）
        - Stage 3: Buy（购买）

        Args:
            df: 输入的 LazyFrame（需包含 behavior_type 和 user_id 字段）

        Returns:
            pl.LazyFrame: 漏斗分析结果的 LazyFrame，包含 pv_count、mid_count、buy_count 列

        Note:
            优化：
            1. 单次扫描，避免多次 filter + collect
            2. 使用条件聚合代替多次 filter
        """
        df_funnel = (
            df
            # ✅ 一次性计算各阶段用户标识
            .with_columns(
                pl.when(pl.col("behavior_type") == "pv").then(pl.col("user_id")).otherwise(None).alias("pv_user"),
                pl.when(pl.col("behavior_type").is_in(["fav", "cart"])).then(pl.col("user_id")).otherwise(None).alias("fav_cart_user"),
                pl.when(pl.col("behavior_type") == "buy").then(pl.col("user_id")).otherwise(None).alias("buy_user"),
            )
            # ✅ 使用聚合获取各阶段用户数
            .select(
                pl.col("pv_user").drop_nulls().n_unique().alias("pv_count"),
                pl.col("fav_cart_user").drop_nulls().n_unique().alias("mid_count"),
                pl.col("buy_user").drop_nulls().n_unique().alias("buy_count"),
            )
        )

        logger.info("   漏斗分析已加入执行计划（延迟计算）")
        return df_funnel

    def _diagnose_anomaly_optimized(self, df: pl.LazyFrame) -> pl.DataFrame:
        """
        异常流量诊断 - 优化版

        检测维度：
        - 高频点击：单用户点击次数 > 阈值（默认 100 次）
        - 异常时间：非正常时间段行为（凌晨 2-5 点）
        - 刷单嫌疑：短时间内大量购买（默认 > 5 次）

        Args:
            df: 输入的 LazyFrame（需包含 user_id、behavior_type、timestamp 字段）

        Returns:
            pl.DataFrame: 异常诊断报告，包含 anomaly_type、affected_users、top_user_count 列

        Note:
            优化：
            1. 单次扫描完成所有异常检测
            2. 避免多次 collect
        """
        # ✅ 单次扫描，同时计算三种异常
        df_anomaly = (
            df
            # 添加小时字段
            .with_columns(
                (pl.col("timestamp") % 86400 / 3600).cast(int).alias("hour_of_day")
            )
            # 标记行为类型
            .with_columns(
                (pl.col("behavior_type") == "pv").alias("is_pv"),
                (pl.col("behavior_type") == "buy").alias("is_buy"),
                ((pl.col("hour_of_day") >= 2) & (pl.col("hour_of_day") <= 5)).alias("is_abnormal_hour"),
            )
            # 按用户聚合
            .group_by("user_id")
            .agg(
                pl.col("is_pv").sum().alias("click_count"),
                pl.col("is_buy").sum().alias("buy_count"),
                pl.col("is_abnormal_hour").sum().alias("abnormal_count"),
            )
            # 标记异常类型
            .with_columns(
                (pl.col("click_count") > 100).alias("is_high_freq"),
                (pl.col("abnormal_count") > 10).alias("is_abnormal_hour_user"),
                (pl.col("buy_count") > 5).alias("is_suspicious_buyer"),
            )
            # 汇总统计
            .select(
                pl.col("is_high_freq").sum().alias("high_freq_count"),
                pl.col("is_abnormal_hour_user").sum().alias("abnormal_hour_count"),
                pl.col("is_suspicious_buyer").sum().alias("suspicious_buy_count"),
                # 获取最大值
                pl.when(pl.col("is_high_freq")).then(pl.col("click_count")).otherwise(None).max().alias("max_click"),
                pl.when(pl.col("is_abnormal_hour_user")).then(pl.col("abnormal_count")).otherwise(None).max().alias("max_abnormal"),
                pl.when(pl.col("is_suspicious_buyer")).then(pl.col("buy_count")).otherwise(None).max().alias("max_buy"),
            )
            .collect()
        )

        # 构建诊断报告
        anomaly_data = {
            "anomaly_type": ["high_frequency_click", "abnormal_hour", "suspicious_buy"],
            "affected_users": [
                df_anomaly["high_freq_count"].item(),
                df_anomaly["abnormal_hour_count"].item(),
                df_anomaly["suspicious_buy_count"].item(),
            ],
            "top_user_count": [
                df_anomaly["max_click"].item() or 0,
                df_anomaly["max_abnormal"].item() or 0,
                df_anomaly["max_buy"].item() or 0,
            ]
        }

        df_result = pl.DataFrame(anomaly_data)

        logger.info(f"   高频点击用户：{df_anomaly['high_freq_count'].item()} 人")
        logger.info(f"   异常时段用户：{df_anomaly['abnormal_hour_count'].item()} 人")
        logger.info(f"   刷单嫌疑用户：{df_anomaly['suspicious_buy_count'].item()} 人")

        return df_result

    # ==================== 辅助方法 ====================

    def _print_funnel_summary(self, df_funnel: pl.DataFrame) -> None:
        """
        打印漏斗分析摘要到日志

        Args:
            df_funnel: 漏斗分析结果 DataFrame
        """
        logger.info("")
        logger.info("📊 === 电商转化漏斗摘要 ===")
        for row in df_funnel.iter_rows():
            stage, count, conv_rate, stage_rate = row
            logger.info(f"   {stage}: {count:,} 用户 (转化率：{conv_rate:.2f}%)")

    def _generate_summary(
        self,
        result: dict[str, pl.LazyFrame | pl.DataFrame],
        output_path: Path,
    ) -> None:
        """
        生成综合分析报告并保存到文件

        使用批量 collect 优化性能，避免多次独立 collect 调用。

        Args:
            result: transform() 返回的结果字典
            output_path: 输出文件路径
        """
        # ✅ 批量收集所有 LazyFrame 统计信息
        lazy_frames_to_collect: list[pl.LazyFrame] = []
        collect_keys: list[str] = []

        if "deduped" in result:
            lazy_frames_to_collect.append(
                result["deduped"].select(pl.len().alias("dedup_count"))
            )
            collect_keys.append("dedup_count")

        if "sessionized" in result:
            lazy_frames_to_collect.append(
                result["sessionized"].select(pl.col("session_id").n_unique().alias("session_count"))
            )
            collect_keys.append("session_count")

        # 单次批量收集
        collected_stats: list[pl.DataFrame] = []
        if lazy_frames_to_collect:
            collected_stats = pl.collect_all(lazy_frames_to_collect)

        # 提取统计值
        stats_map: dict[str, Any] = {}
        for i, key in enumerate(collect_keys):
            stats_map[key] = collected_stats[i].item() if len(collected_stats) > i else 0

        # 写入报告
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("=" * 60 + "\n")
            f.write("M1DataPipeline 运行报告\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"输入文件：{self.data_path}\n\n")

            f.write("-" * 40 + "\n")
            f.write("处理统计\n")
            f.write("-" * 40 + "\n")

            if "dedup_count" in stats_map:
                f.write(f"去重后记录数：{stats_map['dedup_count']:,}\n")

            if "session_count" in stats_map:
                f.write(f"总会话数：{stats_map['session_count']:,}\n")

            if "funnel" in result:
                f.write("\n转化漏斗:\n")
                for row in result["funnel"].iter_rows():
                    f.write(f"  {row[0]}: {row[1]:,} 用户\n")

            if "anomaly" in result:
                f.write("\n异常检测:\n")
                for row in result["anomaly"].iter_rows():
                    f.write(f"  {row[0]}: {row[1]} 用户\n")

            f.write("\n" + "=" * 60 + "\n")
            f.write("报告结束\n")
            f.write("=" * 60 + "\n")

        logger.info(f"✅ 综合报告已保存：{output_path}")

    # ==================== 查询计划检查 ====================

    def explain_plan(self, df: pl.LazyFrame, optimized: bool = True) -> None:
        """
        打印查询执行计划，验证优化效果

        Args:
            df: 要检查的 LazyFrame
            optimized: 是否显示优化后的计划，默认为 True

        Example:
            >>> df = pipeline.extract()
            >>> pipeline.explain_plan(df, optimized=True)
        """
        logger.info("\n" + "=" * 60)
        logger.info("查询执行计划")
        logger.info("=" * 60)
        print(df.explain(optimized=optimized))

    # ==================== 便捷方法 ====================

    def run(self, output_dir: str = "output") -> dict[str, str] | None:
        """
        一键执行完整流水线

        Args:
            output_dir: 输出目录路径

        Returns:
            dict[str, str] | None: 保存的文件路径映射，如果发生异常则返回 None

        Example:
            >>> pipeline = M1DataPipeline("data.parquet")
            >>> saved_files = pipeline.run()
            >>> if saved_files:
            ...     print(f"保存了 {len(saved_files)} 个文件")
        """
        try:
            df = self.extract()
            result = self.transform(df)
            saved_files = self.load(result, output_dir)

            # ✅ 显式释放引用，减少内存占用
            del df, result
            gc.collect()

            return saved_files
        except Exception as e:
            logger.error(f"❌ 流水线执行失败：{e}")
            self.cleanup()  # 异常时清理缓存
            return None


# ==================== 主函数 ====================
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
        help="输入 Parquet 文件路径 (默认：M1_DATA_PATH 环境变量或 m1_final_clean.parquet)"
    )

    parser.add_argument(
        "-o", "--output",
        default="output",
        help="输出目录路径 (默认：output)"
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
