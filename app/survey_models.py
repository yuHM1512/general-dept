"""
Models cho module Khảo sát (nội bộ + tương lai: bên ngoài).
Table `survey_response` map 1-1 với sheet '3. DATA' của Google Sheet nguồn.
Table `survey_sync_job` theo dõi các lần đồng bộ (đã cấy pattern giống IngestJob).
"""
from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import Index
from sqlmodel import Field, SQLModel


class SurveyResponse(SQLModel, table=True):
    """Một dòng = một câu trả lời của một đơn vị khảo sát cho một câu hỏi."""

    __tablename__ = "survey_response"

    # PK dùng chính UUID trong sheet — cho phép upsert idempotent theo `id`.
    id: UUID = Field(primary_key=True)

    # Metadata form
    thoi_gian: datetime | None = Field(default=None, index=True)  # cột "Time"
    so_phieu: str = Field(default="", index=True)                 # cột "No" (ví dụ "KS0005")

    # Ai đánh giá ai
    don_vi_khao_sat: str = Field(default="", index=True)          # đơn vị đi khảo sát
    don_vi_duoc_khao_sat: str = Field(default="", index=True)     # đơn vị bị đánh giá

    # Nội dung
    noi_dung_cau_hoi: str = Field(default="")                     # full text "Câu N: ..."
    question_key: str = Field(default="", index=True)             # short form "text:đv" từ cột "Question"

    # Điểm số + phản hồi
    muc_do_hai_long: int | None = Field(default=None, index=True) # 1..7, nullable
    y_kien_dong_gop: str | None = Field(default=None)             # góp ý tự do
    ly_do_diem_thap: str | None = Field(default=None)             # lý do khi điểm thấp

    # Phân loại đợt
    dot_khao_sat: str = Field(default="", index=True)             # "Đợt 1 - 2025"
    dot_nam: int | None = Field(default=None, index=True)         # tách năm để filter/sort
    dot_so: int | None = Field(default=None, index=True)          # tách số đợt (1 hoặc 2)
    loai_khao_sat: str = Field(default="", index=True)            # "Khách hàng nội bộ" | "Khách hàng bên ngoài"

    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


# Composite indexes cho aggregation nhanh
Index(
    "ix_survey_response_dot_dv",
    SurveyResponse.dot_khao_sat,
    SurveyResponse.don_vi_duoc_khao_sat,
    SurveyResponse.loai_khao_sat,
)


class SurveySyncJob(SQLModel, table=True):
    """Job theo dõi tiến độ đồng bộ từ Google Sheets."""

    __tablename__ = "survey_sync_job"

    id: UUID = Field(default_factory=uuid4, primary_key=True)

    status: str = Field(default="pending", index=True)  # pending|running|completed|failed
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    started_at: datetime | None = Field(default=None)
    finished_at: datetime | None = Field(default=None)

    total_rows: int = 0        # tổng số dòng đọc từ sheet
    upserted: int = 0          # số dòng đã insert/update
    skipped: int = 0           # số dòng bị bỏ qua (thiếu id, sai format...)

    triggered_by: str = Field(default="")  # ma_nv của user bấm nút
    message: str | None = None             # message chi tiết / error
