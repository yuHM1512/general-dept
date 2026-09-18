from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import CheckConstraint, Index, UniqueConstraint, text
from sqlmodel import Field, SQLModel


class PcccDonVi(SQLModel, table=True):
    __tablename__ = "pccc_don_vi"
    __table_args__ = (UniqueConstraint("ma", name="uq_pccc_don_vi_ma"),)

    id: int | None = Field(default=None, primary_key=True)
    ma: str = Field(max_length=50, index=True)
    ten: str = Field(max_length=200)
    thu_tu: int = Field(default=0)
    active: bool = Field(default=True, index=True)


class PcccBoPhan(SQLModel, table=True):
    __tablename__ = "pccc_bo_phan"
    __table_args__ = (
        UniqueConstraint("don_vi_id", "ten", name="uq_pccc_bo_phan_don_vi_ten"),
        CheckConstraint("si_so_tham_khao >= 0", name="ck_pccc_bo_phan_si_so_nonnegative"),
    )

    id: int | None = Field(default=None, primary_key=True)
    don_vi_id: int = Field(foreign_key="pccc_don_vi.id", index=True)
    ma: str = Field(default="", max_length=80)
    ten: str = Field(max_length=200)
    si_so_tham_khao: int = Field(default=0)
    thu_tu: int = Field(default=0)
    active: bool = Field(default=True, index=True)


class PcccDotDienTap(SQLModel, table=True):
    __tablename__ = "pccc_dot_dien_tap"

    id: int | None = Field(default=None, primary_key=True)
    ten: str = Field(max_length=200)
    ngay_dien_tap: date = Field(default_factory=date.today, index=True)
    bat_dau_du_kien: datetime | None = Field(default=None)
    bat_dau_thuc_te: datetime | None = Field(default=None)
    ket_thuc_thuc_te: datetime | None = Field(default=None)
    trang_thai: str = Field(default="MO_SI_SO", max_length=30, index=True)
    ghi_chu: str = Field(default="")
    created_by: str = Field(default="", max_length=16)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class PcccPhanCong(SQLModel, table=True):
    __tablename__ = "pccc_phan_cong"
    __table_args__ = (
        Index(
            "ux_pccc_phan_cong_unit_role",
            "don_vi_id",
            "vai_tro",
            unique=True,
            postgresql_where=text("bo_phan_id IS NULL"),
            sqlite_where=text("bo_phan_id IS NULL"),
        ),
        Index(
            "ux_pccc_phan_cong_department_role",
            "bo_phan_id",
            "vai_tro",
            unique=True,
            postgresql_where=text("bo_phan_id IS NOT NULL"),
            sqlite_where=text("bo_phan_id IS NOT NULL"),
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    don_vi_id: int = Field(foreign_key="pccc_don_vi.id", index=True)
    bo_phan_id: int | None = Field(default=None, foreign_key="pccc_bo_phan.id", index=True)
    ma_nv: str = Field(max_length=16, index=True)
    vai_tro: str = Field(default="CHINH", max_length=20)  # CHINH | DU_PHONG
    active: bool = Field(default=True, index=True)
    created_by: str = Field(default="", max_length=16)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class PcccKiemDem(SQLModel, table=True):
    __tablename__ = "pccc_kiem_dem"
    __table_args__ = (
        UniqueConstraint("dot_id", "bo_phan_id", name="uq_pccc_kiem_dem_dot_bo_phan"),
        CheckConstraint("si_so_dau_ngay IS NULL OR si_so_dau_ngay >= 0", name="ck_pccc_si_so_nonnegative"),
        CheckConstraint("thuc_te_kiem_dem IS NULL OR thuc_te_kiem_dem >= 0", name="ck_pccc_thuc_te_nonnegative"),
    )

    id: int | None = Field(default=None, primary_key=True)
    dot_id: int = Field(foreign_key="pccc_dot_dien_tap.id", index=True)
    bo_phan_id: int = Field(foreign_key="pccc_bo_phan.id", index=True)
    si_so_dau_ngay: int | None = Field(default=None)
    thuc_te_kiem_dem: int | None = Field(default=None)
    ma_ly_do: str = Field(default="", max_length=50)
    ly_do_chi_tiet: str = Field(default="")
    si_so_updated_by: str = Field(default="", max_length=16)
    si_so_updated_at: datetime | None = Field(default=None)
    kiem_dem_updated_by: str = Field(default="", max_length=16)
    kiem_dem_updated_at: datetime | None = Field(default=None)
    xac_nhan_by: str = Field(default="", max_length=16)
    xac_nhan_at: datetime | None = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class PcccXacNhanDonVi(SQLModel, table=True):
    __tablename__ = "pccc_xac_nhan_don_vi"
    __table_args__ = (
        UniqueConstraint("dot_id", "don_vi_id", name="uq_pccc_xac_nhan_dot_don_vi"),
    )

    id: int | None = Field(default=None, primary_key=True)
    dot_id: int = Field(foreign_key="pccc_dot_dien_tap.id", index=True)
    don_vi_id: int = Field(foreign_key="pccc_don_vi.id", index=True)
    xac_nhan_by: str = Field(max_length=16)
    xac_nhan_at: datetime = Field(default_factory=datetime.utcnow)
    ghi_chu: str = Field(default="")


class PcccLichSu(SQLModel, table=True):
    __tablename__ = "pccc_lich_su"

    id: int | None = Field(default=None, primary_key=True)
    dot_id: int = Field(foreign_key="pccc_dot_dien_tap.id", index=True)
    kiem_dem_id: int | None = Field(default=None, foreign_key="pccc_kiem_dem.id", index=True)
    don_vi_id: int | None = Field(default=None, foreign_key="pccc_don_vi.id", index=True)
    hanh_dong: str = Field(max_length=50)
    du_lieu_cu: str = Field(default="")
    du_lieu_moi: str = Field(default="")
    changed_by: str = Field(default="", max_length=16)
    changed_at: datetime = Field(default_factory=datetime.utcnow, index=True)


Index("ix_pccc_bo_phan_don_vi_thu_tu", PcccBoPhan.don_vi_id, PcccBoPhan.thu_tu)
Index("ix_pccc_kiem_dem_dot_bo_phan", PcccKiemDem.dot_id, PcccKiemDem.bo_phan_id)
