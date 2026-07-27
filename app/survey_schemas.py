"""
Pydantic response schemas cho module Khảo sát.
"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class SurveySyncJobStart(BaseModel):
    job_id: str


class SurveySyncJobStatus(BaseModel):
    job_id: str
    status: str  # pending|running|completed|failed
    total_rows: int
    upserted: int
    skipped: int
    started_at: datetime | None = None
    finished_at: datetime | None = None
    message: str | None = None


class SurveyLastSyncInfo(BaseModel):
    """Thông tin lần đồng bộ gần nhất — hiển thị trên UI."""

    has_sync: bool
    status: str | None = None
    finished_at: datetime | None = None
    upserted: int = 0
    total_responses_in_db: int = 0


class UnitRoundScore(BaseModel):
    """Điểm số của một đơn vị trong một đợt khảo sát."""

    dot_khao_sat: str
    avg_score: float | None = None       # trung bình 1..7 (null nếu chưa có data)
    total_score: float | None = None     # avg / 7 * 100
    response_count: int = 0


class UnitComparisonRow(BaseModel):
    """Một dòng trong bảng so sánh: 1 đơn vị x N đợt."""

    don_vi: str
    scores: list[UnitRoundScore]         # theo thứ tự rounds trong response


class SurveyComparisonResponse(BaseModel):
    """Response cho bảng so sánh 4 đợt của khảo sát nội bộ."""

    rounds: list[str]                                # ["Đợt 1 - 2025", "Đợt 2 - 2025", ...]
    overall: list[UnitRoundScore]                    # điểm tổng theo đợt (tất cả đơn vị)
    rows: list[UnitComparisonRow]                    # từng đơn vị
    total_responses: int
