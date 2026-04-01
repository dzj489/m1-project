import polars as pl

# 懒加载读取数据
df = pl.scan_parquet("m1_final_clean.parquet")

# 1. 校验字段（schema）
print("=== 数据字段校验 ===")
print(df.schema)

# 2. 校验总行数
print("\n=== 数据总行数校验 ===")
row_count = df.select(pl.count()).collect().item()
print(f"最终数据总行数：{row_count:,}")

# 3. 计算去重比例（原始数据约1亿行）
print("\n=== 去重比例校验 ===")
original_count = 100150807  # 原始数据总行数（1亿）
dedup_ratio = (original_count - row_count) / original_count * 100
print(f"原始数据总行数：{original_count:,}")
print(f"去重数据行数：{row_count:,}")
print(f"数据去重比例：{dedup_ratio:.2f}%")

# 4. 统计会话总数（修复版）
print("\n=== 会话总数校验 ===")
if "session_id" in df.columns:
    # 正确写法：n_unique() 必须放在 agg 里面
    session_count = df.select(pl.col("session_id").n_unique()).collect().item()
    print(f"总会话数量：{session_count:,}")
else:
    print("当前数据未包含 session_id 字段")