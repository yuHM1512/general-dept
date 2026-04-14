from __future__ import annotations

from pydantic import BaseModel


class IngestResult(BaseModel):
    inserted: int
    updated: int
    skipped: int


class IngestJobResponse(BaseModel):
    job_id: str


class IngestJobStatus(BaseModel):
    job_id: str
    status: str
    total_rows: int
    processed_rows: int
    inserted: int
    skipped: int
    invalid: int
    error: str | None = None


class PayrollRowOut(BaseModel):
    year: int
    month: int
    ttbp: str
    don_vi: str
    co_so: str
    department: str
    manv: str
    full_name: str
    job_title: str
    group_name: str
    metric_vnd: int


class PayrollListResponse(BaseModel):
    year: int
    month: int
    total: int
    limit: int
    offset: int
    rows: list[PayrollRowOut]


class GroupInsight(BaseModel):
    group: str
    count: int
    min_vnd: int
    max_vnd: int
    median_vnd: int
    avg_vnd: int


class MonthPoint(BaseModel):
    month: int
    count: int
    avg_vnd: int


class InsightsResponse(BaseModel):
    year: int
    month: int | None
    target_salary_vnd: int
    view: str  # "month" | "year_avg"
    group_filter: str | None
    by_group: list[GroupInsight]
    by_month: list[MonthPoint]


class HeadcountPoint(BaseModel):
    month: int
    headcount: int


class HeadcountResponse(BaseModel):
    year: int
    group_filter: str | None
    points: list[HeadcountPoint]
    avg_headcount: int


class FilterOptionsResponse(BaseModel):
    year: int
    months: list[int]
    co_so: list[str]
    don_vi: list[str]
    departments: list[str]


class TrendDepartmentItem(BaseModel):
    department: str
    avg_vnd: int


class TrendDonViItem(BaseModel):
    don_vi: str
    avg_vnd: int
    departments: list[TrendDepartmentItem]


class MonthlyDetailsResponse(BaseModel):
    year: int
    month: int
    items: list[TrendDonViItem]


class ParticipationResponse(BaseModel):
    year: int
    month: int | None
    don_vi_count: int


class GroupStat(BaseModel):
    group: str
    total_count: int
    not_meet_count: int
    not_meet_rate: float


class StatsResponse(BaseModel):
    year: int
    month: int
    target_salary_vnd: int
    stats: list[GroupStat]


class TimeseriesPoint(BaseModel):
    year: int
    month: int
    group: str
    total_count: int
    not_meet_count: int
    not_meet_rate: float


class TimeseriesResponse(BaseModel):
    year: int
    target_salary_vnd: int
    points: list[TimeseriesPoint]
