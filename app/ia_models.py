"""
Internal Audit (IA) module models.
Reuses master data from audit_5s_* tables (don_vi, bo_phan, linh_vuc, tieu_chi, ap_dung)
but stores violations (so_loi) instead of scores (diem 0/1/2).
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlmodel import Field, SQLModel


class IaDotKiemTra(SQLModel, table=True):
    __tablename__ = "ia_dot_kiem_tra"
    id: Optional[int] = Field(default=None, primary_key=True)
    ky: str = Field(default="", max_length=7, index=True)
    ngay_kiem_tra: date = Field(default_factory=date.today)
    nguoi_kiem_tra: Optional[str] = None
    ghi_chu: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class IaPhieuKiemTra(SQLModel, table=True):
    __tablename__ = "ia_phieu_kiem_tra"
    id: Optional[int] = Field(default=None, primary_key=True)
    dot_id: Optional[int] = Field(default=None, foreign_key="ia_dot_kiem_tra.id")
    bo_phan_id: int = Field(foreign_key="audit_5s_bo_phan.id")
    loai: str = Field(default="TRUC_QUAN", max_length=20)
    tong_loi: int = Field(default=0)
    nguoi_kiem_tra: Optional[str] = None
    ghi_chu: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None


class IaChiTiet(SQLModel, table=True):
    __tablename__ = "ia_chi_tiet"
    id: Optional[int] = Field(default=None, primary_key=True)
    phieu_id: int = Field(foreign_key="ia_phieu_kiem_tra.id")
    tieu_chi_id: int = Field(foreign_key="audit_5s_tieu_chi.id")
    so_loi: int = Field(default=0)
    mo_ta: Optional[str] = None
    hinh_anh: Optional[str] = None


class IaCap(SQLModel, table=True):
    __tablename__ = "ia_cap"
    id: Optional[int] = Field(default=None, primary_key=True)
    chi_tiet_id: int = Field(foreign_key="ia_chi_tiet.id", unique=True, index=True)
    phieu_id: int = Field(foreign_key="ia_phieu_kiem_tra.id", index=True)
    tieu_chi_id: int = Field(foreign_key="audit_5s_tieu_chi.id")
    hanh_dong_kp: str = Field(default="")
    nguoi_thuc_hien: str = Field(default="")
    thoi_han: Optional[date] = None
    tinh_trang: str = Field(default="Chưa tiếp nhận", max_length=20)
    created_by: str = Field(default="", max_length=16)
    updated_by: str = Field(default="", max_length=16)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
