
# backend/services/exporter.py

from pathlib import Path
import re
import pandas as pd


def _sanitize_base_name(base_name: str) -> str:
    """
    清理用户输入的文件名：
    - 去掉前后空格
    - 去除路径分隔符和非法字符
    - 去掉结尾的 .xlsx（不区分大小写）
    """
    if not base_name:
        return ""

    name = base_name.strip()

    # 去掉路径分隔符（防止目录穿越）
    name = name.replace("/", "_").replace("\\", "_")

    # 去掉结尾的 .xlsx / .XLSX
    if name.lower().endswith(".xlsx"):
        name = name[: -len(".xlsx")]

    # 仅允许数字/字母/点/下划线/中横线
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    return name


def _unique_name_in_dir(directory: Path, base: str, ext: str = ".xlsx") -> Path:
    """
    为避免重名覆盖，如果存在同名，自动加 -1, -2, ...
    """
    candidate = directory / f"{base}{ext}"
    if not candidate.exists():
        return candidate

    i = 1
    while True:
        candidate = directory / f"{base}-{i}{ext}"
        if not candidate.exists():
            return candidate
        i += 1


def dataframe_to_excel(df: pd.DataFrame, path: Path, base_name: str) -> str:
    """
    参数：
      - df: 要写入的 DataFrame
      - path: 传入的“临时文件”完整路径，例如 tmp_exports/report-uuid.xlsx
      - base_name: 用户输入的文件名（不需要包含 .xlsx）

    行为：
      1) 先把数据写入临时文件 `path`
      2) 如果 base_name 非空，则把临时文件重命名为 {safe_base}.xlsx（遇到重名自动加序号）
         若 base_name 为空，则沿用临时文件名不变
      3) 返回“最终文件名”（仅名字，不含目录）
    """
    # 1) 先写入“临时文件”
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="数据明细")

    # 2) 决定最终文件名
    safe_base = _sanitize_base_name(base_name)
    if safe_base:
        final_path = _unique_name_in_dir(path.parent, safe_base, ".xlsx")
        path.replace(final_path)  # 把临时文件“移动/改名”为最终文件
    else:
        final_path = path  # 用户没提供名字，就保留临时名

    # 3) 返回最终文件名（给主流程当 download_token 用）
    return final_path.name
