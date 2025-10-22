from typing import List, Literal, Optional

from pydantic import BaseModel, Field, root_validator

PeriodType = Literal["year", "quarter"]


class QueryRequest(BaseModel):
    tushare_token: str = Field(..., min_length=32, max_length=64)
    symbols: List[str] = Field(..., min_items=1, max_items=10)
    period_type: PeriodType
    start_year: int
    end_year: int
    start_quarter: Optional[int] = Field(None, ge=1, le=4)
    end_quarter: Optional[int] = Field(None, ge=1, le=4)
    filename: Optional[str] = Field(None, max_length=60)

    @root_validator(pre=True)
    def _validate_quarters(cls, values):
        period_type = values.get("period_type")
        start_q = values.get("start_quarter")
        end_q = values.get("end_quarter")
        start_year = values.get("start_year")
        end_year = values.get("end_year")

        if start_year and end_year and start_year > end_year:
            raise ValueError("起始年份需小于或等于结束年份")

        if period_type == "quarter":
            if start_q is None or end_q is None:
                raise ValueError("按季度查询时需填写起始与结束季度")
            if values["start_year"] == values["end_year"] and start_q > end_q:
                raise ValueError("同一年内起始季度需小于或等于结束季度")
        return values

    class Config:
        validate_assignment = True


class QueryResponse(BaseModel):
    summary: str
    columns: List[str]
    table: List[dict]
    download_token: str
