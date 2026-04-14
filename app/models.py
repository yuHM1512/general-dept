from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import Index
from sqlmodel import Field, SQLModel


class PayrollRow(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)

    year: int = Field(index=True)
    month: int = Field(index=True)

    ttbp: str = Field(default="", index=True)
    don_vi: str = Field(default="", index=True)
    co_so: str = Field(default="", index=True)
    department: str = Field(index=True)
    manv: str = Field(index=True)
    full_name: str = Field(default="", index=True)
    job_title: str = Field(index=True)

    lgtrgio: int = 0
    bu_du_luong_toi_thieu: int = 0
    tien_cm_thai_7t_vspn: int = 0
    tien_f_l_h_r_gl: int = 0
    tien_e: int = 0

    metric_vnd: int = Field(index=True)
    group_name: str = Field(index=True)

    ingested_at: datetime = Field(default_factory=datetime.utcnow, index=True)


Index("ux_payrollrow_year_month_manv", PayrollRow.year, PayrollRow.month, PayrollRow.manv, unique=True)


class IngestJob(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)

    status: str = Field(default="pending", index=True)  # pending|running|completed|failed
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    started_at: datetime | None = Field(default=None, index=True)
    finished_at: datetime | None = Field(default=None, index=True)

    total_rows: int = 0
    processed_rows: int = 0
    inserted: int = 0
    skipped: int = 0
    invalid: int = 0

    error: str | None = None
