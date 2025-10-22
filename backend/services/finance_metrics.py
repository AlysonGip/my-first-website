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


# L28: —— 去累计（累计口径 -> 单季口径） ——
def _decumulate_by_year(series: pd.Series,
                        year: pd.Series,
                        quarter: pd.Series,
                        assume_cum: bool = True) -> pd.Series:
    """
    若 series 为年内累计口径，则按年份做去累计，得到单季值：
      Q1 -> Q1
      Q2 -> Q2 - Q1
      Q3 -> Q3 - Q2
      Q4 -> Q4 - Q3
    会按同一年内按“季度”排序做差分；对缺季、乱序亦鲁棒；恢复到原索引顺序。
    默认 assume_cum=True（更符合国内利润表返回习惯）。
    内置温和自检：若某年看起来不像累计，则保持原值。
    """
    s = _num(series)
    y = pd.Series(year)
    q = pd.Series(quarter)
    out = s.copy()

    def _looks_cumulative(x: pd.Series) -> bool:
        x = x.dropna()
        if len(x) <= 1:
            return assume_cum
        # 累计口径：多数情况下单调不减（或至少大多数差分>=0）
        mono_like = x.is_monotonic_increasing or (
            x.diff().fillna(0) >= 0).mean() > 0.75
        # 末值≈各期“单季值”之和（启发式，放宽阈值）
        try:
            close = (abs(x.iloc[-1] - x.sum()) / (abs(x.sum()) + 1e-9)) < 0.15
        except Exception:
            close = False
        return mono_like and (close or assume_cum)

    tmp = pd.DataFrame({"s": s, "y": y, "q": q})
    for yv, g in tmp.groupby("y"):
        gi = g.sort_values("q").index  # 确保按季度递增
        x = s.loc[gi]
        if _looks_cumulative(x):
            d = x - x.shift(1)
            d = d.where(~d.isna(), x)  # Q1：保持原值
            out.loc[gi] = d
        else:
            out.loc[gi] = x

    return out


# L52: —— 前端展示列 ——
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


# L72: —— 主函数 ——
def build_financial_dataset(frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    输入：每只股票一个 DataFrame（含 year/quarter + 三表合并结果）
    输出：稳定的展示表（向量化计算、缺列不崩）
    关键修复：
      1) 利润表类（净利润、营业收入、营业成本）若来源为年内累计口径 -> 自动转为单季口径
      2) 资产负债表类（总资产、总负债、流动资产、流动负债、存货）为时点值 -> 直接使用
    """
    all_rows = []

    for code, df in frames.items():
        if df is None or df.empty:
            continue

        data = df.copy()

        # 基础标识
        year = _num(_safe_col(data, "year"))
        quarter = _num(_safe_col(data, "quarter"))

        # 指标列（带兜底）
        # 净利润优先取归母净利润，其次 indicator 的 netprofit / income 的 n_income
        netprofit = _num(_safe_col(data, "n_income_attr_p",
                         alts=["netprofit", "n_income"]))
        revenue = _num(_safe_col(data, "revenue", alts=["total_revenue"]))
        cost = _num(_safe_col(data, "cost", alts=["oper_cost"]))

        total_assets = _num(_safe_col(data, "total_assets"))
        total_liab = _num(_safe_col(data, "total_liab"))
        total_cur_assets = _num(_safe_col(data, "total_cur_assets"))
        total_cur_liab = _num(_safe_col(data, "total_cur_liab"))
        inventories = _num(_safe_col(data, "inventories")).fillna(0)

        roe = _num(_safe_col(data, "roe"))   # 注意：数据源可能为TTM或年化
        roa = _num(_safe_col(data, "roa"))   # 注意：数据源可能为TTM或年化

        # ====== 关键修正：利润表类去累计，得到“单季数” ======
        netprofit_q = _decumulate_by_year(
            netprofit, year, quarter, assume_cum=True)
        revenue_q = _decumulate_by_year(
            revenue, year, quarter, assume_cum=True)
        cost_q = _decumulate_by_year(cost, year, quarter, assume_cum=True)

        # 向量化计算（用“单季数”计算利润率；用“时点值”计算结构性比率）
        gross_margin = _safe_divide(
            revenue_q - cost_q, revenue_q) * 100.0        # 毛利率(%)
        net_margin = _safe_divide(
            netprofit_q, revenue_q) * 100.0                 # 销售净利率(%)
        # 资产负债率 = 负债/资产（时点）
        debt_ratio = _safe_divide(total_liab, total_assets)
        current_ratio = _safe_divide(
            total_cur_assets, total_cur_liab)            # 流动比率（时点）
        quick_ratio = _safe_divide(
            total_cur_assets - inventories, total_cur_liab)  # 速动比率（时点）

        # 组装输出（不使用 iterrows）
        out = pd.DataFrame({
            "公司代码": code,
            "年份": year.astype("Int64"),
            "季度": quarter.astype("Int64"),
            "净利润": netprofit_q,
            "营业收入": revenue_q,
            "营业成本": cost_q,
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

    # L145: 汇总排序 & 保留四位小数
    result = pd.concat(all_rows, ignore_index=True)
    result = result.sort_values(
        ["公司代码", "年份", "季度"], na_position="last", kind="mergesort")

    num_cols = [c for c in DISPLAY_COLUMNS if c not in ("公司代码", "年份", "季度")]
    result[num_cols] = result[num_cols].apply(
        pd.to_numeric, errors="coerce").round(4)

    return result.reset_index(drop=True)
