from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from sqlmodel import Field, SQLModel


class AuditDonVi(SQLModel, table=True):
    __tablename__ = "audit_5s_don_vi"
    id: Optional[int] = Field(default=None, primary_key=True)
    ma: str = Field(max_length=50)
    ten: str = Field(max_length=200)


class AuditBoPhan(SQLModel, table=True):
    __tablename__ = "audit_5s_bo_phan"
    id: Optional[int] = Field(default=None, primary_key=True)
    don_vi_id: int = Field(foreign_key="audit_5s_don_vi.id")
    ten: str = Field(max_length=200)


class AuditLinhVuc(SQLModel, table=True):
    __tablename__ = "audit_5s_linh_vuc"
    id: Optional[int] = Field(default=None, primary_key=True)
    loai: str = Field(max_length=20)   # '5S' | 'TRUC_QUAN'
    ma: str = Field(max_length=30)
    ten: str = Field(max_length=100)
    icon: str = Field(default="", max_length=50)
    thu_tu: int


class AuditBien(SQLModel, table=True):
    __tablename__ = "audit_5s_bien"
    id: Optional[int] = Field(default=None, primary_key=True)
    linh_vuc_id: int = Field(foreign_key="audit_5s_linh_vuc.id", index=True)
    ten_goi: str = Field(max_length=300)
    mo_ta: str = Field(default="")
    kich_thuoc: str = Field(default="", max_length=100)
    so_thu_tu: int = Field(default=0)


class AuditTieuChi(SQLModel, table=True):
    __tablename__ = "audit_5s_tieu_chi"
    id: Optional[int] = Field(default=None, primary_key=True)
    linh_vuc_id: int = Field(foreign_key="audit_5s_linh_vuc.id")
    bien_id: Optional[int] = Field(default=None, foreign_key="audit_5s_bien.id")
    so_thu_tu: int
    noi_dung: str
    active: bool = Field(default=True)


class AuditApDung(SQLModel, table=True):
    __tablename__ = "audit_5s_ap_dung"
    bo_phan_id: int = Field(foreign_key="audit_5s_bo_phan.id", primary_key=True)
    tieu_chi_id: int = Field(foreign_key="audit_5s_tieu_chi.id", primary_key=True)


class AuditDotKiemTra(SQLModel, table=True):
    __tablename__ = "audit_5s_dot_kiem_tra"
    id: Optional[int] = Field(default=None, primary_key=True)
    ky: str = Field(default="", max_length=7, index=True)   # "2026-07" = YYYY-MM
    ngay_kiem_tra: date = Field(default_factory=date.today)
    nguoi_kiem_tra: Optional[str] = None
    ghi_chu: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class AuditPhieuKiemTra(SQLModel, table=True):
    __tablename__ = "audit_5s_phieu_kiem_tra"
    id: Optional[int] = Field(default=None, primary_key=True)
    dot_id: Optional[int] = Field(default=None, foreign_key="audit_5s_dot_kiem_tra.id")
    bo_phan_id: int = Field(foreign_key="audit_5s_bo_phan.id")
    loai: str = Field(max_length=20)   # '5S' | 'TRUC_QUAN' | 'DAY_DU'
    so_diem: Optional[int] = None
    tong_diem: Optional[int] = None
    ty_le: Optional[float] = None
    ket_luan: Optional[str] = None
    nguoi_kiem_tra: Optional[str] = None
    ghi_chu: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None


class AuditChiTietDiem(SQLModel, table=True):
    __tablename__ = "audit_5s_chi_tiet_diem"
    id: Optional[int] = Field(default=None, primary_key=True)
    phieu_id: int = Field(foreign_key="audit_5s_phieu_kiem_tra.id")
    tieu_chi_id: int = Field(foreign_key="audit_5s_tieu_chi.id")
    diem: int   # 0, 1, or 2
    ghi_chu: Optional[str] = None
