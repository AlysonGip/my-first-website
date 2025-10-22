# backend/services/summary.py  —— 兼容 openai==1.26.0 的写法
import os
from textwrap import dedent
from typing import Optional, List

import pandas as pd
from openai import AsyncOpenAI
from openai import OpenAIError

DEFAULT_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")


def _get_client() -> Optional[AsyncOpenAI]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None
    base_url = os.getenv("OPENAI_BASE_URL", "").strip() or None
    return AsyncOpenAI(api_key=api_key, base_url=base_url)


def _shrink_table(df: pd.DataFrame, max_rows: int = 12) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    sort_cols: List[str] = []
    for c in ("年份", "季度", "year", "quarter"):
        if c in df.columns:
            sort_cols.append(c)
    if sort_cols:
        df = df.sort_values(sort_cols, na_position="last", kind="mergesort")
    if len(df) > max_rows:
        df = df.tail(max_rows)
    return df


def _df_to_markdown(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "(无数据)"
    preferred = [
        "公司代码", "年份", "季度",
        "净利润", "营业收入", "营业成本",
        "总资产", "总负债",
        "毛利率(%)", "销售净利率(%)",
        "资产负债率", "流动比率", "速动比率",
        "净资产收益率(%)", "资产收益率(%)",
    ]
    cols = [c for c in preferred if c in df.columns] or list(df.columns)
    small = df[cols].copy()
    for c in small.columns:
        if pd.api.types.is_numeric_dtype(small[c]):
            small[c] = pd.to_numeric(small[c], errors="coerce").round(4)
    small = _shrink_table(small, max_rows=12)
    return small.to_markdown(index=False)


def _build_prompt(df: pd.DataFrame, request_obj) -> str:
    symbols = getattr(request_obj, "symbols", [])
    period_type = getattr(request_obj, "period_type", "quarter")
    sy = getattr(request_obj, "start_year", None)
    ey = getattr(request_obj, "end_year", None)
    sq = getattr(request_obj, "start_quarter", None)
    eq = getattr(request_obj, "end_quarter", None)
    md_table = _df_to_markdown(df)
    return dedent(f"""
    你是一名卖方行研助理，请基于下表做中文要点总结（<=8条），不要给投资建议。

    【查询条件】
    - 股票: {", ".join(symbols) or "-"}
    - 口径: {period_type}
    - 区间: {sy}年Q{sq} ~ {ey}年Q{eq}

    【数据表（最多12行）】
    {md_table}

    输出要求：
    - 概括收入、成本、毛利率、净利润、净利率、资产负债率、流动/速动比率、ROE/ROA 的变化
    - 若某期缺数据，直接说明“该期缺少××数据”
    - 最后一行给一句整体结论（10～25字）
    """).strip()


async def summarize_financials(df: pd.DataFrame, request_obj) -> str:
    client = _get_client()
    if client is None:
        return "未设置 OPENAI_API_KEY 环境变量"

    model = os.getenv("OPENAI_MODEL", DEFAULT_MODEL)

    try:
        prompt = _build_prompt(df, request_obj)
        # ✅ 关键：兼容 1.26.0 —— 使用 chat.completions，而不是 responses
        resp = await client.chat.completions.create(
            model=model,
            temperature=0.2,
            messages=[
                {"role": "system", "content": "你是严格、中性、简洁的行业研究助理。"},
                {"role": "user", "content": prompt},
            ],
        )
        text = resp.choices[0].message.content.strip()
        return text or "（模型未返回内容）"
    except OpenAIError as e:
        return f"GPT 总结失败：{e}"
    except Exception as e:
        return f"GPT 总结失败：{e}"
