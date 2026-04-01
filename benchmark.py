import polars as pl
import pandas as pd
import time
from pathlib import Path

# 配置路径
DATA_PATH = "m1_final_clean.parquet"

def measure_time(func):
    """计时装饰器"""
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"耗时: {end - start:.2f} 秒")
        return result
    return wrapper

@measure_time
def test_polars():
    """测试 Polars 性能"""
    df = pl.scan_parquet(DATA_PATH)
    # 复用重构后的核心逻辑，例如统计 UV
    uv_count = df.select(pl.col("user_id").n_unique()).collect().item()
    return f"Polars 完成，UV 数量: {uv_count}"

@measure_time
def test_pandas():
    """测试 Pandas 性能（如果内存足够）"""
    try:
        df = pd.read_parquet(DATA_PATH)
        uv_count = df['user_id'].nunique()
        return f"Pandas 完成，UV 数量: {uv_count}"
    except MemoryError:
        return "Pandas 内存溢出，无法完成测试"

if __name__ == "__main__":
    print("=== 测试 Polars 性能 ===")
    test_polars()
    
    print("\n=== 测试 Pandas 性能 ===")
    test_pandas()