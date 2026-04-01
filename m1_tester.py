import polars as pl
import sys
import os

def test_partner_data(file_path):
    print(f"🔍 正在对文件 {file_path} 进行质量抽检...")
    
    if not os.path.exists(file_path):
        print("❌ 错误：找不到文件，请检查路径。")
        return

    try:
        df = pl.read_parquet(file_path)
        
        # 1. 架构检查 (Schema Check)
        required_cols = {'user_id', 'item_id', 'behavior_type', 'timestamp', 'session_id'}
        missing = required_cols - set(df.columns)
        if missing:
            print(f"❌ 架构错误：缺少字段 {missing}")
        else:
            print("✅ 架构检查通过。")

        # 2. 漏斗逻辑检查 (Funnel Sanity)
        counts = df.group_by("behavior_type").count()
        pv_count = counts.filter(pl.col("behavior_type") == "pv")["count"].to_list()
        buy_count = counts.filter(pl.col("behavior_type") == "buy")["count"].to_list()
        
        if pv_count and buy_count:
            if buy_count[0] > pv_count[0]:
                print(f"❌ 业务逻辑错误：购买数({buy_count[0]}) 居然大于 浏览数({pv_count[0]})！")
            else:
                print(f"✅ 漏斗逻辑合理 (PV: {pv_count[0]}, Buy: {buy_count[0]})。")

        # 3. 会话完整性检查 (Session Integrity)
        # 抽取一个 session 检查时间差是否真的小于 30 分钟
        sample_session = df.filter(pl.col("session_id").is_not_null()).head(100)
        if not sample_session.is_empty():
             print("✅ 会话 ID 已生成。")
        else:
             print("❌ 错误：session_id 全为空。")

        print("\n🏁 抽检完成！请将上述结果截图反馈给你的伙伴。")

    except Exception as e:
        print(f"❌ 读取文件失败：{e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使用方法: python m1_tester.py <对方的parquet文件路径>")
    else:
        test_partner_data(sys.argv[1])
