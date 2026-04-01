"""
M1DataPipeline - 亿级电商数据 ETL 流水线

基于 Polars Lazy API 实现高性能数据处理，支持：
- 精密去重
- 会话识别
- 漏斗分析
- 异常流量诊断
"""

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import polars as pl

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class M1DataPipeline:
    """
    亿级电商数据 ETL 流水线类

    使用 Polars Lazy API 进行懒加载和惰性计算，避免 OOM 问题。
    所有转换操作在 collect() 前不会实际执行，确保内存效率。

    Attributes:
        data_path: 输入 Parquet 文件路径
        session_timeout: 会话超时阈值（秒），默认 30 分钟
        _start_time: 流水线启动时间戳

    Example:
        >>> pipeline = M1DataPipeline("data.parquet")
        >>> saved_files = pipeline.run(output_dir="output")
    """

    def __init__(self, data_path: str = "m1_final_clean.parquet") -> None:
        """
        初始化流水线
        
        Args:
            data_path: 输入 Parquet 文件路径
        """
        self.data_path = Path(data_path)
        self.session_timeout = 1800  # 会话超时阈值：30 分钟（秒）
        self._start_time: Optional[float] = None
        
        # 验证输入路径
        if not self.data_path.exists():
            logger.warning(f"⚠️  数据文件不存在：{self.data_path.absolute()}")
    
    # ==================== Step 1: 数据提取 ====================
    def extract(self) -> pl.LazyFrame:
        """
        懒加载读取 m1_final_clean.parquet
        
        Returns:
            pl.LazyFrame: 惰性数据帧，未实际加载数据
            
        Raises:
            FileNotFoundError: 文件不存在时抛出
        """
        logger.info("=" * 60)
        logger.info("【Step 1】EXTRACT - 数据提取")
        logger.info("=" * 60)
        
        step_start = time.time()
        
        try:
            if not self.data_path.exists():
                raise FileNotFoundError(f"数据文件不存在：{self.data_path}")
            
            # 使用 scan_parquet 懒加载，不占用内存
            df = pl.scan_parquet(self.data_path)
            
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
    ) -> Dict[str, Union[pl.LazyFrame, pl.DataFrame]]:
        """
        执行数据转换：去重、会话识别、漏斗分析、异常诊断
        
        Args:
            df: 输入的 LazyFrame
            
        Returns:
            dict: 包含各阶段结果的字典
                - deduped: 去重后的 LazyFrame
                - sessionized: 带会话标识的 LazyFrame
                - funnel: 漏斗分析结果 DataFrame
                - anomaly: 异常流量诊断结果 DataFrame
                
        Raises:
            ValueError: 输入为空时抛出
        """
        logger.info("")
        logger.info("=" * 60)
        logger.info("【Step 2】TRANSFORM - 数据转换")
        logger.info("=" * 60)
        
        try:
            if df is None:
                raise ValueError("输入数据框为空")
            
            results = {}
            
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
            
            df_funnel = self._analyze_funnel(df_dedup)
            results["funnel"] = df_funnel
            
            elapsed = time.time() - step_start
            logger.info(f"✅ 漏斗分析完成，耗时：{elapsed:.2f} 秒")
            
            # --- 2.4 异常流量诊断 ---
            logger.info("")
            logger.info("[2.4] 执行异常流量诊断...")
            step_start = time.time()
            
            df_anomaly = self._diagnose_anomaly(df_dedup)
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
        result: Dict[str, Union[pl.LazyFrame, pl.DataFrame]],
        output_dir: str = "output",
    ) -> Dict[str, str]:
        """
        输出最终分析报告，保存可视化结果或中间文件
        
        Args:
            result: transform() 返回的结果字典
            output_dir: 输出目录路径
            
        Returns:
            dict: 保存的文件路径映射
            
        Raises:
            ValueError: 结果字典为空时抛出
        """
        logger.info("")
        logger.info("=" * 60)
        logger.info("【Step 3】LOAD - 数据加载与输出")
        logger.info("=" * 60)
        
        step_start = time.time()
        
        try:
            if not result:
                raise ValueError("结果字典为空")
            
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            
            saved_files = {}
            
            # --- 3.1 保存去重数据 ---
            if "deduped" in result:
                dedup_path = output_path / "deduped_data.parquet"
                logger.info(f"保存去重数据：{dedup_path}")
                result["deduped"].sink_parquet(dedup_path)
                saved_files["deduped"] = str(dedup_path)
            
            # --- 3.2 保存会话数据 ---
            if "sessionized" in result:
                session_path = output_path / "sessionized_data.parquet"
                logger.info(f"保存会话数据：{session_path}")
                result["sessionized"].sink_parquet(session_path)
                saved_files["sessionized"] = str(session_path)
            
            # --- 3.3 保存漏斗分析报告 ---
            if "funnel" in result:
                funnel_path = output_path / "funnel_analysis.csv"
                logger.info(f"保存漏斗分析：{funnel_path}")
                result["funnel"].write_csv(funnel_path)
                saved_files["funnel"] = str(funnel_path)
                
                # 打印漏斗摘要
                self._print_funnel_summary(result["funnel"])
            
            # --- 3.4 保存异常诊断报告 ---
            if "anomaly" in result:
                anomaly_path = output_path / "anomaly_diagnosis.csv"
                logger.info(f"保存异常诊断：{anomaly_path}")
                result["anomaly"].write_csv(anomaly_path)
                saved_files["anomaly"] = str(anomaly_path)
            
            # --- 3.5 生成综合分析报告 ---
            summary_path = output_path / "pipeline_summary.txt"
            self._generate_summary(result, summary_path)
            saved_files["summary"] = str(summary_path)
            
            elapsed = time.time() - step_start
            logger.info(f"✅ 所有文件保存完成，总耗时：{elapsed:.2f} 秒")
            logger.info(f"📁 输出目录：{output_path.absolute()}")
            
            return saved_files
            
        except ValueError as e:
            logger.error(f"❌ 输入验证失败：{e}")
            raise
        except Exception as e:
            logger.error(f"❌ 数据加载失败：{e}")
            raise
    
    # ==================== 核心转换方法（私有） ====================

    def _deduplicate(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """
        四维度精密去重

        基于 user_id, item_id, behavior_type, timestamp 去重，保留首次出现记录。

        Args:
            df: 输入的 LazyFrame

        Returns:
            pl.LazyFrame: 去重后的 LazyFrame

        Note:
            重复率计算会触发一次 collect() 操作。
        """
        original_count = df.select(pl.len()).collect().item()
        
        df_dedup = df.unique(
            subset=["user_id", "item_id", "behavior_type", "timestamp"],
            keep="first"
        )
        
        deduped_count = df_dedup.select(pl.len()).collect().item()
        dup_rate = (original_count - deduped_count) / original_count * 100 if original_count > 0 else 0
        
        logger.info(f"   去重前：{original_count:,} 条")
        logger.info(f"   去重后：{deduped_count:,} 条")
        logger.info(f"   重复率：{dup_rate:.2f}%")
        
        return df_dedup
    
    def _sessionize(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """
        基于时间窗口的会话识别

        规则：
        - 同一用户相邻行为间隔 > 30 分钟，视为新会话
        - 生成全局唯一 session_id = user_id + session_seq

        Args:
            df: 输入的 LazyFrame（需包含 user_id 和 timestamp 字段）

        Returns:
            pl.LazyFrame: 带有 session_id 标识的 LazyFrame

        Note:
            此方法会触发一次 collect() 用于统计信息，然后重新返回 LazyFrame。
        """
        df_session = (
            df
            # 按时间排序（组内有序）
            .sort("timestamp")
            .set_sorted("timestamp")
            # 计算相邻行为时间差
            .with_columns(
                (pl.col("timestamp") - pl.col("timestamp").shift(1))
                .over("user_id")
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
        
        # 收集统计信息（触发一次计算）
        result = df_session.collect()
        total_sessions = result["session_id"].n_unique()
        total_records = len(result)
        
        logger.info(f"   总记录数：{total_records:,}")
        logger.info(f"   总会话数：{total_sessions:,}")
        logger.info(f"   平均每会话：{total_records/total_sessions:.2f} 条行为")
        
        # 重新返回 LazyFrame 供后续使用
        return df_session
    
    def _analyze_funnel(self, df: pl.LazyFrame) -> pl.DataFrame:
        """
        电商转化漏斗分析

        阶段定义：
        - Stage 1: PV（浏览）
        - Stage 2: Fav/Cart（收藏/加购）
        - Stage 3: Buy（购买）

        Args:
            df: 输入的 LazyFrame（需包含 behavior_type 字段）

        Returns:
            pl.DataFrame: 漏斗分析结果，包含 stage、user_count、conversion_rate、stage_rate 列
        """
        # 提取各阶段用户集合（保持 Lazy）
        pv_users = df.filter(pl.col("behavior_type") == "pv").select("user_id").unique()
        fav_cart_users = df.filter(
            pl.col("behavior_type").is_in(["fav", "cart"])
        ).select("user_id").unique()
        buy_users = df.filter(pl.col("behavior_type") == "buy").select("user_id").unique()
        
        # 漏斗连接（Lazy Join）
        pv_to_mid = pv_users.join(fav_cart_users, on="user_id", how="inner")
        mid_to_buy = pv_to_mid.join(buy_users, on="user_id", how="inner")
        
        # 执行计算
        pv_count = pv_users.select(pl.len()).collect().item()
        mid_count = pv_to_mid.select(pl.len()).collect().item()
        buy_count = mid_to_buy.select(pl.len()).collect().item()
        
        # 计算转化率
        rate_pv_to_mid = (mid_count / pv_count * 100) if pv_count > 0 else 0
        rate_mid_to_buy = (buy_count / mid_count * 100) if mid_count > 0 else 0
        rate_pv_to_buy = (buy_count / pv_count * 100) if pv_count > 0 else 0
        
        # 构建结果 DataFrame
        funnel_data = {
            "stage": ["PV", "Fav/Cart", "Buy"],
            "user_count": [pv_count, mid_count, buy_count],
            "conversion_rate": [100.0, rate_pv_to_mid, rate_pv_to_buy],
            "stage_rate": [100.0, rate_pv_to_mid, rate_mid_to_buy]
        }
        
        df_funnel = pl.DataFrame(funnel_data)
        
        logger.info(f"   PV 用户数：{pv_count:,}")
        logger.info(f"   收藏/加购用户数：{mid_count:,}")
        logger.info(f"   购买用户数：{buy_count:,}")
        
        return df_funnel
    
    def _diagnose_anomaly(self, df: pl.LazyFrame) -> pl.DataFrame:
        """
        异常流量诊断

        检测维度：
        - 高频点击：单用户点击次数 > 阈值（默认 100 次）
        - 异常时间：非正常时间段行为（凌晨 2-5 点）
        - 刷单嫌疑：短时间内大量购买（默认 > 5 次）

        Args:
            df: 输入的 LazyFrame（需包含 user_id、behavior_type、timestamp 字段）

        Returns:
            pl.DataFrame: 异常诊断报告，包含 anomaly_type、affected_users、top_user_count 列
        """
        # --- 高频点击检测 ---
        click_threshold = 100  # 单用户点击超过 100 次视为高频
        high_freq_users = (
            df
            .filter(pl.col("behavior_type") == "pv")
            .group_by("user_id")
            .agg(pl.len().alias("click_count"))
            .filter(pl.col("click_count") > click_threshold)
            .sort("click_count", descending=True)
            .collect()
        )
        
        # --- 异常时间段检测 ---
        # 假设 timestamp 为 Unix 时间戳（秒），转换为小时
        df_with_hour = df.with_columns(
            (pl.col("timestamp") % 86400 / 3600).cast(int).alias("hour_of_day")
        )
        
        abnormal_hour_users = (
            df_with_hour
            .filter((pl.col("hour_of_day") >= 2) & (pl.col("hour_of_day") <= 5))
            .group_by("user_id")
            .agg(pl.len().alias("abnormal_count"))
            .filter(pl.col("abnormal_count") > 10)
            .sort("abnormal_count", descending=True)
            .collect()
        )
        
        # --- 刷单嫌疑检测 ---
        buy_threshold = 5  # 单用户购买超过 5 次视为可疑
        suspicious_buyers = (
            df
            .filter(pl.col("behavior_type") == "buy")
            .group_by("user_id")
            .agg(pl.len().alias("buy_count"))
            .filter(pl.col("buy_count") > buy_threshold)
            .sort("buy_count", descending=True)
            .collect()
        )
        
        # 构建诊断报告
        anomaly_data = {
            "anomaly_type": ["high_frequency_click", "abnormal_hour", "suspicious_buy"],
            "affected_users": [
                len(high_freq_users),
                len(abnormal_hour_users),
                len(suspicious_buyers)
            ],
            "top_user_count": [
                high_freq_users["click_count"].max() if len(high_freq_users) > 0 else 0,
                abnormal_hour_users["abnormal_count"].max() if len(abnormal_hour_users) > 0 else 0,
                suspicious_buyers["buy_count"].max() if len(suspicious_buyers) > 0 else 0
            ]
        }
        
        df_anomaly = pl.DataFrame(anomaly_data)
        
        logger.info(f"   高频点击用户：{len(high_freq_users)} 人")
        logger.info(f"   异常时段用户：{len(abnormal_hour_users)} 人")
        logger.info(f"   刷单嫌疑用户：{len(suspicious_buyers)} 人")
        
        return df_anomaly
    
    # ==================== 辅助方法 ====================
    
    def _print_funnel_summary(self, df_funnel: pl.DataFrame) -> None:
        """打印漏斗分析摘要"""
        logger.info("")
        logger.info("📊 === 电商转化漏斗摘要 ===")
        for row in df_funnel.iter_rows():
            stage, count, conv_rate, stage_rate = row
            logger.info(f"   {stage}: {count:,} 用户 (转化率：{conv_rate:.2f}%)")
    
    def _generate_summary(self, result: dict, output_path: Path) -> None:
        """生成综合分析报告"""
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("=" * 60 + "\n")
            f.write("M1DataPipeline 运行报告\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"输入文件：{self.data_path}\n\n")
            
            f.write("-" * 40 + "\n")
            f.write("处理统计\n")
            f.write("-" * 40 + "\n")
            
            if "deduped" in result:
                dedup_count = result["deduped"].select(pl.len()).collect().item()
                f.write(f"去重后记录数：{dedup_count:,}\n")
            
            if "sessionized" in result:
                session_count = result["sessionized"].collect()["session_id"].n_unique()
                f.write(f"总会话数：{session_count:,}\n")
            
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
    
    # ==================== 便捷方法 ====================
    
    def run(self, output_dir: str = "output") -> dict[str, str]:
        """
        一键执行完整流水线
        
        Args:
            output_dir: 输出目录
            
        Returns:
            dict: 保存的文件路径映射
        """
        df = self.extract()
        result = self.transform(df)
        return self.load(result, output_dir)


# ==================== 主函数 ====================
if __name__ == "__main__":
    # 示例用法
    pipeline = M1DataPipeline(data_path="m1_final_clean.parquet")
    
    try:
        # 方式一：分步执行
        # df = pipeline.extract()
        # result = pipeline.transform(df)
        # saved = pipeline.load(result, output_dir="output")
        
        # 方式二：一键执行
        saved_files = pipeline.run(output_dir="output")
        
        print("\n✅ 流水线执行完成！")
        print("保存的文件:")
        for name, path in saved_files.items():
            print(f"  - {name}: {path}")
            
    except Exception as e:
        logger.error(f"❌ 流水线执行失败：{e}")
