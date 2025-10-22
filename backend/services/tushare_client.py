# L01: backend/services/tushare_client.py
# L02: 说明：仅使用普通权限接口（fina_indicator / income / balancesheet）
# L03:      period 采用“多口径”尝试（YYYY0331/0630/0930/1231 → YYYY10/20/30/40 → 年报1231）
# L04:      合并时把 income.oper_cost 映射为 cost，并带上 n_income_attr_p（归母净利润）

# L06
from collections import defaultdict
from typing import Dict, List
import pandas as pd
import tushare as ts

from ..schemas import QueryRequest

# L13: 期末日期映射
_STD_END = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}
_Q0_END  = {1: "10",   2: "20",   3: "30",   4: "40"}

# L17: 我们期望在结果里出现的字段（供下游统一）
TARGET_FIELDS = {
    "净利润": "netprofit",            # 指标表（同时会并入 n_income_attr_p 作为兜底）
    "营业收入": "revenue",            # income.revenue 或 total_revenue 兜底
    "营业成本": "cost",               # income.oper_cost → cost
    "总资产": "total_assets",         # 资产负债表
    "总负债": "total_liab",           # 资产负债表
    "流动资产": "total_cur_assets",   # 资产负债表
    "流动负债": "total_cur_liab",     # 资产负债表
    "存货": "inventories",            # 资产负债表
    "净资产收益率": "roe",            # 指标表
    "资产收益率": "roa",              # 指标表
}

# L30
def _period_candidates(year: int, quarter: int) -> List[str]:
    """生成 period 候选：标准 → Q0 → 年报兜底"""
    std = f"{year}{_STD_END.get(quarter, '1231')}"
    q0  = f"{year}{_Q0_END.get(quarter, '40')}"
    return [std, q0, f"{year}1231"]

# L36
def _iter_periods(request: QueryRequest):
    for year in range(request.start_year, request.end_year + 1):
        if request.period_type == "year":
            yield year, 4
            continue
        start_q = request.start_quarter if year == request.start_year else 1
        end_q = request.end_quarter if year == request.end_year else 4
        for quarter in range(start_q, end_q + 1):
            yield year, quarter

# L46
def _pick_latest(df: pd.DataFrame, keys=("ts_code", "end_date")) -> pd.DataFrame:
    """
    同一期可能多条（不同公告时间/报表类型）：
      - 优先 report_type == 1（一般工商业合并口径）
      - 再按 ann_date 最新保留 1 条
    """
    if df is None or df.empty:
        return df
    tmp = df.copy()
    if "report_type" in tmp.columns:
        cand = tmp[tmp["report_type"] == 1]
        if not cand.empty:
            tmp = cand
    if "ann_date" in tmp.columns:
        tmp = tmp.sort_values("ann_date").drop_duplicates(list(keys), keep="last")
    else:
        tmp = tmp.drop_duplicates(list(keys), keep="last")
    return tmp

# L64
def fetch_financials(request: QueryRequest) -> Dict[str, pd.DataFrame]:
    """
    普通权限版本（不使用 VIP）：
      - 指标：      pro.fina_indicator(period=)
      - 利润表：    pro.income(period=)
      - 资产负债表：pro.balancesheet(period=)
    period 采用多候选（标准口径 → Q0 口径 → 年报兜底）。
    """
    if not request.tushare_token:
        raise ValueError("未提供 Tushare Token，请在前端输入。")

    # L75
    ts.set_token(request.tushare_token)
    pro = ts.pro_api()

    # L79: 指标字段（revenue/cost 由 income 填，不在此列出）
    indicator_fields = "ts_code,ann_date,end_date,netprofit,roe,roa"

    raw_results = defaultdict(list)
    print("🟢 开始拉取（普通权限，多口径 period，三表合并）...")

    # L85
    for code in request.symbols:
        for year, quarter in _iter_periods(request):
            got_any = False
            merged_best = None

            for period in _period_candidates(year, quarter):
                # L92: 1) 指标
                try:
                    ind = pro.fina_indicator(ts_code=code, period=period, fields=indicator_fields)
                except Exception as e:
                    print(f"❌ fina_indicator 失败 {code} {period}: {e}")
                    ind = None
                if ind is None or ind.empty:
                    ind = pd.DataFrame({"ts_code": [code], "end_date": [period]})
                else:
                    ind = _pick_latest(ind)

                # L102: 2) 利润表（带 n_income_attr_p）
                try:
                    inc = pro.income(
                        ts_code=code,
                        period=period,
                        fields=(
                            "ts_code,ann_date,end_date,"
                            "revenue,total_revenue,oper_cost,"
                            "n_income_attr_p,report_type"
                        ),
                    )
                except Exception as e:
                    print(f"⚠️ income 失败 {code} {period}: {e}")
                    inc = None
                if inc is not None and not inc.empty:
                    inc = _pick_latest(inc)

                # L117: 3) 资产负债表
                try:
                    bs = pro.balancesheet(
                        ts_code=code,
                        period=period,
                        fields=(
                            "ts_code,ann_date,end_date,"
                            "total_assets,total_liab,total_cur_assets,total_cur_liab,inventories,report_type"
                        ),
                    )
                except Exception as e:
                    print(f"⚠️ balancesheet 失败 {code} {period}: {e}")
                    bs = None
                if bs is not None and not bs.empty:
                    bs = _pick_latest(bs)

                # L132: 4) 合并：指标 ⟵ 利润表 ⟵ 资产负债表
                merged = ind.copy()
                if inc is not None and not inc.empty:
                    merged = merged.merge(
                        inc[[
                            "ts_code", "end_date",
                            "revenue", "total_revenue", "oper_cost",
                            "n_income_attr_p"  # ← 把归母净利润带进来
                        ]],
                        on=["ts_code", "end_date"], how="left"
                    )
                if bs is not None and not bs.empty:
                    merged = merged.merge(
                        bs[[
                            "ts_code", "end_date",
                            "total_assets", "total_liab",
                            "total_cur_assets", "total_cur_liab", "inventories"
                        ]],
                        on=["ts_code", "end_date"], how="left"
                    )

                # L151: oper_cost → cost
                if "cost" not in merged.columns and "oper_cost" in merged.columns:
                    merged["cost"] = merged["oper_cost"]

                # L155: 判断是否有有效载荷
                needed_cols = [
                    "revenue", "total_revenue", "cost", "total_assets", "total_liab",
                    "total_cur_assets", "total_cur_liab", "inventories",
                    "netprofit", "n_income_attr_p", "roe", "roa"
                ]
                has_payload = any(c in merged.columns and merged[c].notna().any() for c in needed_cols)

                if has_payload:
                    got_any = True
                    merged_best = merged
                    print(f"✅ {code} {period} 命中，有效数据。")
                    break
                else:
                    print(f"ℹ️ {code} {period} 无有效数据，尝试下一个口径…")

            # L169: 兜底
            if not got_any:
                merged_best = merged
                print(f"⚠️ {code} {year}Q{quarter} 所有口径均为空，保留骨架。")

            merged_best["year"] = year
            merged_best["quarter"] = quarter
            raw_results[code].append(merged_best)

    # L178: 汇总
    frames: Dict[str, pd.DataFrame] = {}
    for code, chunks in raw_results.items():
        if not chunks:
            raise ValueError(f"{code} 未查询到财报信息，请检查股票代码或时间区间。")
        frames[code] = (
            pd.concat(chunks, ignore_index=True)
            .sort_values(["year", "quarter"])
            .reset_index(drop=True)
        )

    print(f"🎉 完成：共 {len(frames)} 只股票（普通权限，多口径 period）。")
    return frames