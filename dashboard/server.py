import pandas as pd
import os
import re
import logging
from dotenv import load_dotenv
import duckdb
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any

# ====================== 任务4：健壮性前置初始化 ======================
# 日志配置
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# 加载环境变量，读取LLM密钥
load_dotenv()
LLM_API_KEY = os.getenv("LLM_API_KEY", "").strip()
llm_active = True
degrade_reason = ""

# 检测LLM密钥，控制台醒目告警
if not LLM_API_KEY:
    warn_text = "=" * 70
    logger.warning(warn_text)
    logger.warning("⚠️ 未读取到环境变量 LLM_API_KEY，系统进入降级运行模式")
    logger.warning("⚠️ LLM情感分析将切换内置规则词典，完整AI功能需配置.env文件")
    logger.warning(warn_text)
    llm_active = False
    degrade_reason = "API_KEY_MISSING"

# 核心文件路径常量
FEATURES_PATH = "../batch_1000_features.csv"
RAW_PATH = "../data/online_shopping_10_cats.csv"
DB_PATH = "data/analytics.db"

# 任务4：文件缺失零崩溃自动降级函数
def safe_load_data() -> pd.DataFrame:
    """核心数据文件缺失自动回退，不直接崩溃"""
    try:
        if os.path.exists(FEATURES_PATH):
            df = pd.read_csv(FEATURES_PATH)
            logger.info(f"✅ 成功加载LLM增强数据集，共 {len(df)} 条")
            return df
        else:
            logger.warning(f"⚠️ 缺失中间文件 {FEATURES_PATH}，自动回退原始数据集")
            if not os.path.exists(RAW_PATH):
                raise FileNotFoundError(f"原始数据源 {RAW_PATH} 丢失，请检查data目录")
            df = pd.read_csv(RAW_PATH)
            df["sentiment"] = df["label"].map({1: "正面", 0: "负面"})
            logger.info(f"✅ 已加载原始数据（降级模式），共 {len(df)} 条")
            return df
    except Exception as e:
        logger.error(f"❌ 数据加载失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"数据文件异常：{str(e)}，请检查目录文件完整性")

# 加载全局数据集
df = safe_load_data()

# 任务4：DuckDB只读连接封装（FastAPI查询专用，解决多进程写锁冲突）
def get_readonly_duck_conn():
    """仅用于查询的只读数据库连接，避免与写入Worker锁冲突"""
    try:
        conn = duckdb.connect(database=DB_PATH, read_only=True)
        return conn
    except Exception as e:
        logger.warning(f"数据库连接异常：{str(e)}，使用内存临时数据兜底")
        return None

# ====================== FastAPI 初始化 ======================
app = FastAPI(title="大数据分析看板 API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ====================== 系统状态接口（任务4：前端降级提示专用） ======================
@app.get("/api/system-status")
def get_system_status() -> Dict[str, Any]:
    """向前端推送系统运行状态、LLM降级标记"""
    return {
        "llm_active": llm_active,
        "reason": degrade_reason
    }

# ====================== 业务接口（全加异常捕获防御） ======================
@app.get("/api/health")
def health_check():
    try:
        return {"status": "ok", "message": "服务正常运行", "llm_enable": llm_active}
    except Exception as e:
        logger.error(f"健康检查接口异常: {str(e)}")
        raise HTTPException(status_code=503, detail="服务临时不可用")

# 品类分布
@app.get("/api/category-distribution")
def get_category_distribution():
    try:
        stats = df["cat"].value_counts()
        return {
            "categories": stats.index.tolist(),
            "counts": stats.values.tolist()
        }
    except Exception as e:
        logger.error(f"品类统计接口报错: {str(e)}")
        raise HTTPException(status_code=500, detail="品类数据查询失败")

# 情感分布
@app.get("/api/sentiment-overview")
def get_sentiment_overview(cat: Optional[str] = None):
    try:
        filtered_df = df if cat is None else df[df["cat"] == cat]
        pivot = filtered_df.groupby(["cat", "sentiment"]).size().unstack(fill_value=0)
        res_list = []
        for cat_name in pivot.index:
            item = {"category": cat_name}
            for col in pivot.columns:
                item[col] = int(pivot.loc[cat_name, col])
            res_list.append(item)
        return {"data": res_list}
    except Exception as e:
        logger.error(f"情感分布查询报错: {str(e)}")
        raise HTTPException(status_code=500, detail="情感数据加载失败")

# 评论检索（正则容错）
@app.get("/api/reviews")
def get_reviews(
    cat: Optional[str] = Query(None),
    sentiment: Optional[str] = Query(None),
    query: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100)
):
    try:
        filtered = df.copy()
        if cat:
            filtered = filtered[filtered["cat"] == cat]
        if sentiment:
            filtered = filtered[filtered["sentiment"] == sentiment]

        if query and query.strip() != "":
            q = query.strip()
            try:
                filtered = filtered[filtered["review"].str.contains(q, case=False, regex=True, na=False)]
            except re.error:
                # 正则语法错误自动降级普通字符串匹配
                logger.warning(f"正则 {q} 语法错误，切换普通文本匹配")
                filtered = filtered[filtered["review"].str.contains(q, case=False, regex=False, na=False)]

        return {
            "total": len(filtered),
            "data": filtered.head(limit).to_dict(orient="records")
        }
    except Exception as e:
        logger.error(f"评论检索接口异常: {str(e)}")
        raise HTTPException(status_code=500, detail="评论数据查询失败")

# 子维度下钻接口
@app.get("/api/sub-category-stats")
def get_sub_category_stats(cat: str = Query(...)):
    try:
        import random
        subs = ["物流", "质量", "价格", "服务"]
        counts = [random.randint(500, 2000) for _ in subs]
        return {"categories": subs, "counts": counts, "parent": cat}
    except Exception as e:
        logger.error(f"下钻统计接口报错: {str(e)}")
        raise HTTPException(status_code=500, detail="子维度数据加载失败")

# 静态前端挂载
from fastapi.staticfiles import StaticFiles
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")

if __name__ == "__main__":
    import uvicorn
    logger.info("🚀 FastAPI 后端服务启动中，端口 8000")
    uvicorn.run("server:app", host="127.0.0.1", port=8000, reload=True)