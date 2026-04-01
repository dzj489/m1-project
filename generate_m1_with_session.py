"""
生成包含 session_id 的 m1_final_clean.parquet 文件

用途：为测试器准备包含完整字段的数据文件
"""

import polars as pl
import logging
import time
from pathlib import Path

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def generate_session_id(df: pl.LazyFrame, session_timeout: int = 1800) -> pl.LazyFrame:
    """
    为数据添加 session_id 字段
    
    Args:
        df: 输入 LazyFrame（必须包含 user_id, timestamp）
        session_timeout: 会话超时阈值（秒），默认 30 分钟
        
    Returns:
        带 session_id 的 LazyFrame
    """
    df_session = (
        df
        .sort("timestamp")
        .set_sorted("timestamp")
        .with_columns(
            # 计算相邻行为时间差
            timediff=(pl.col("timestamp") - pl.col("timestamp").shift(1))
            .over("user_id")
        )
        .with_columns(
            # 标记新会话起点
            new_session=pl.when(
                pl.col("timediff").is_null() | 
                (pl.col("timediff") > session_timeout)
            ).then(1).otherwise(0)
        )
        .with_columns(
            # 累加生成会话序号
            session_seq=pl.col("new_session").cum_sum().over("user_id")
        )
        .with_columns(
            # 生成全局唯一 session_id
            session_id=(pl.col("user_id").cast(str) + "_" + pl.col("session_seq").cast(str))
        )
        .drop("timediff", "new_session", "session_seq")
    )
    
    return df_session


def main():
    """主函数：生成带 session_id 的 m1_final_clean.parquet"""
    
    start_time = time.time()
    
    print("=" * 60)
    print("生成包含 session_id 的 m1_final_clean.parquet")
    print("=" * 60)
    print()
    
    # ==================== 配置（使用绝对路径） ====================
    base_dir = Path(r"C:\Users\86155\.npm-global\data_practice\04exp")
    
    # 输入文件路径
    input_file = base_dir / "m1_final_clean.parquet"
    
    # 输出文件路径（覆盖原文件，先备份）
    output_file = base_dir / "m1_final_clean.parquet"
    backup_file = base_dir / "m1_final_clean_backup.parquet"
    
    # ==================== 检查输入文件 ====================
    logger.info(f"检查输入文件：{input_file.absolute()}")
    
    if not input_file.exists():
        logger.error(f"❌ 输入文件不存在：{input_file.absolute()}")
        logger.info("请确认文件位置，当前目录结构:")
        for p in base_dir.iterdir():
            logger.info(f"   - {p.name}")
        return
    
    logger.info(f"✅ 找到输入文件：{input_file.absolute()}")
    
    # ==================== 读取数据 ====================
    logger.info("")
    logger.info("【Step 1】读取原始数据...")
    step_start = time.time()
    
    try:
        # 懒加载读取
        df = pl.scan_parquet(input_file)
        
        # 检查必需字段
        schema = df.collect_schema()
        required_cols = ["user_id", "item_id", "behavior_type", "timestamp"]
        missing = set(required_cols) - set(schema.names())
        
        if missing:
            logger.error(f"❌ 缺少必需字段：{missing}")
            return
        
        logger.info(f"✅ Schema: {schema.names()}")
        
        elapsed = time.time() - step_start
        logger.info(f"⏱️  耗时：{elapsed:.2f} 秒")
        
    except Exception as e:
        logger.error(f"❌ 读取失败：{e}")
        return
    
    # ==================== 生成 session_id ====================
    logger.info("")
    logger.info("【Step 2】生成 session_id...")
    step_start = time.time()
    
    try:
        df_with_session = generate_session_id(df)
        
        elapsed = time.time() - step_start
        logger.info(f"✅ 会话识别逻辑构建完成，耗时：{elapsed:.2f} 秒")
        
    except Exception as e:
        logger.error(f"❌ 生成失败：{e}")
        return
    
    # ==================== 保存文件 ====================
    logger.info("")
    logger.info("【Step 3】保存文件...")
    step_start = time.time()
    
    try:
        # 备份原文件
        if output_file.exists():
            logger.info(f"备份原文件到：{backup_file}")
            import shutil
            shutil.copy2(output_file, backup_file)
        
        # 使用 sink_parquet 流式写入
        logger.info(f"保存到：{output_file.absolute()}")
        df_with_session.sink_parquet(output_file)
        
        elapsed = time.time() - step_start
        logger.info(f"✅ 保存完成，耗时：{elapsed:.2f} 秒")
        
    except Exception as e:
        logger.error(f"❌ 保存失败：{e}")
        return
    
    # ==================== 验证结果 ====================
    logger.info("")
    logger.info("【Step 4】验证结果...")
    step_start = time.time()
    
    try:
        # 读取验证
        df_verify = pl.read_parquet(output_file)
        
        # 检查字段
        if "session_id" not in df_verify.columns:
            logger.error("❌ 验证失败：session_id 字段不存在")
            return
        
        # 统计信息
        total_records = len(df_verify)
        total_sessions = df_verify["session_id"].n_unique()
        
        logger.info(f"✅ 总记录数：{total_records:,}")
        logger.info(f"✅ 总会话数：{total_sessions:,}")
        logger.info(f"✅ 平均每会话：{total_records/total_sessions:.2f} 条行为")
        
        # 检查行为类型分布
        behavior_counts = df_verify.group_by("behavior_type").len().sort("behavior_type")
        logger.info("")
        logger.info("行为类型分布:")
        for row in behavior_counts.iter_rows():
            logger.info(f"   {row[0]}: {row[1]:,}")
        
        elapsed = time.time() - step_start
        logger.info(f"⏱️  验证耗时：{elapsed:.2f} 秒")
        
    except Exception as e:
        logger.error(f"❌ 验证失败：{e}")
        return
    
    # ==================== 完成 ====================
    total_elapsed = time.time() - start_time
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ 生成完成！")
    logger.info("=" * 60)
    logger.info(f"输出文件：{output_file.absolute()}")
    logger.info(f"总耗时：{total_elapsed:.2f} 秒")
    logger.info("")
    logger.info("现在可以运行测试器验证:")
    logger.info(f"  python m1_tester.py {output_file}")


if __name__ == "__main__":
    main()
