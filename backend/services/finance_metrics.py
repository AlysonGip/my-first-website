# L01: backend/services/finance_metrics.py
# L02: 说明：计算前端展示用的指标；缺列不崩；资产负债率 = 总负债 / 总资产

from typing import Dict
import numpy as np
import pandas as pd

# L08: —— 工具函数 —— 
def _safe_col(df: pd.DataFrame, main: str, alts=None, fill=np.nan) -> pd.Series:
    """从 df 中安全取列：先 main，再 alts，都没有则返回全 NaN 列"""
    alts = alts or []
    for col in [main] + alts:
        if col in df.columns:
            return df[col]
    return pd.Series([fill] * len(df), index=df.index, name=main)

# L16
def _num(s: pd.Series) -> pd.Series:
    """转为数值，无法转换置 NaN"""
    return pd.to_numeric(s, errors="coerce")

# L21
def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """安全相除：分母为 0/NaN 时结果为 NaN"""
    denom = denominator.replace({0: np.nan})
    return numerator / denom

# L27: —— 前端展示列 —— 
DISPLAY_COLUMNS = [
    "公司代码",
    "年份",
    "季度",
    "净利润",
    "营业收入",
    "营业成本",
    "总资产",
    "总负债",
    "毛利率(%)",
    "销售净利率(%)",
    "资产负债率",
    "流动比率",
    "速动比率",
    "净资产收益率(%)",
    "资产收益率(%)",
]

# L46: —— 主函数 —— 
def build_financial_dataset(frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    输入：每只股票一个 DataFrame（含 year/quarter + 三表合并结果）
    输出：稳定的展示表（向量化计算、缺列不崩）
    """
    all_rows = []

    for code, df in frames.items():
        if df is None or df.empty:
            continue

        data = df.copy()

        # L58: 基础标识
        year    = _num(_safe_col(data, "year"))
        quarter = _num(_safe_col(data, "quarter"))

        # L62: 指标列（带兜底）
        # 关键：净利润优先取归母净利润，其次 indicator 的 netprofit / income 的 n_income
        netprofit = _num(_safe_col(data, "n_income_attr_p", alts=["netprofit", "n_income"]))

        revenue   = _num(_safe_col(data, "revenue", alts=["total_revenue"]))
        cost      = _num(_safe_col(data, "cost", alts=["oper_cost"]))

        total_assets     = _num(_safe_col(data, "total_assets"))
        total_liab       = _num(_safe_col(data, "total_liab"))
        total_cur_assets = _num(_safe_col(data, "total_cur_assets"))
        total_cur_liab   = _num(_safe_col(data, "total_cur_liab"))
        inventories      = _num(_safe_col(data, "inventories")).fillna(0)

        roe = _num(_safe_col(data, "roe"))
        roa = _num(_safe_col(data, "roa"))

        # L77: 向量化计算（分母安全）
        gross_margin = _safe_divide(revenue - cost, revenue) * 100.0             # 毛利率(%)
        net_margin   = _safe_divide(netprofit, revenue) * 100.0                  # 销售净利率(%)
        debt_ratio   = _safe_divide(total_liab, total_assets)                    # 资产负债率 = 负债/资产
        current_ratio= _safe_divide(total_cur_assets, total_cur_liab)            # 流动比率
        quick_ratio  = _safe_divide(total_cur_assets - inventories, total_cur_liab)  # 速动比率

        # L84: 组装输出（不使用 iterrows）
        out = pd.DataFrame({
            "公司代码": code,
            "年份": year.astype("Int64"),
            "季度": quarter.astype("Int64"),
            "净利润": netprofit,
            "营业收入": revenue,
            "营业成本": cost,
            "总资产": total_assets,
            "总负债": total_liab,
            "毛利率(%)": gross_margin,
            "销售净利率(%)": net_margin,
            "资产负债率": debt_ratio,
            "流动比率": current_ratio,
            "速动比率": quick_ratio,
            "净资产收益率(%)": roe,
            "资产收益率(%)": roa,
        })

        out = out.reindex(columns=DISPLAY_COLUMNS)
        all_rows.append(out)

    if not all_rows:
        return pd.DataFrame(columns=DISPLAY_COLUMNS)

    # L105: 汇总排序 & 保留四位小数
    result = pd.concat(all_rows, ignore_index=True)
    result = result.sort_values(["公司代码", "年份", "季度"], na_position="last", kind="mergesort")

    num_cols = [c for c in DISPLAY_COLUMNS if c not in ("公司代码", "年份", "季度")]
    result[num_cols] = result[num_cols].apply(pd.to_numeric, errors="coerce").round(4)

    return result.reset_index(drop=True)