from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field, field_validator


PCCC_STATUSES = {"NHAP", "MO_SI_SO", "DANG_KIEM_DEM", "DA_KET_THUC"}
PCCC_ROLES = {"CHINH", "DU_PHONG"}


class PcccDrillCreate(BaseModel):
    ten: str = Field(min_length=1, max_length=200)
    ngay_dien_tap: date
    bat_dau_du_kien: datetime | None = None
    ghi_chu: str = ""


class PcccDrillStatusUpdate(BaseModel):
    trang_thai: str

    @field_validator("trang_thai")
    @classmethod
    def validate_status(cls, value: str) -> str:
        normalized = value.strip().upper()
        if normalized not in PCCC_STATUSES:
            raise ValueError("Trạng thái đợt diễn tập không hợp lệ")
        return normalized


class PcccAssignmentUpsert(BaseModel):
    don_vi_id: int
    bo_phan_id: int | None = None
    ma_nv: str = Field(min_length=1, max_length=16)
    vai_tro: str = "CHINH"
    active: bool = True

    @field_validator("ma_nv")
    @classmethod
    def normalize_employee_code(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("vai_tro")
    @classmethod
    def validate_role(cls, value: str) -> str:
        normalized = value.strip().upper()
        if normalized not in PCCC_ROLES:
            raise ValueError("Vai trò phải là CHINH hoặc DU_PHONG")
        return normalized


class PcccCountItem(BaseModel):
    bo_phan_id: int
    so_luong: int = Field(ge=0)
    ma_ly_do: str = Field(default="", max_length=50)
    ly_do_chi_tiet: str = ""


class PcccBulkCountUpdate(BaseModel):
    items: list[PcccCountItem] = Field(min_length=1)


class PcccUnitConfirm(BaseModel):
    ghi_chu: str = ""


class PcccDepartmentOut(BaseModel):
    id: int
    ma: str
    ten: str
    si_so_tham_khao: int
    thu_tu: int


class PcccUnitOut(BaseModel):
    id: int
    ma: str
    ten: str
    thu_tu: int
    bo_phans: list[PcccDepartmentOut] = Field(default_factory=list)


class PcccAssignmentOut(BaseModel):
    id: int
    don_vi_id: int
    bo_phan_id: int | None = None
    bo_phan_ten: str = ""
    ma_nv: str
    ho_ten: str = ""
    vai_tro: str
    active: bool


class PcccDrillOut(BaseModel):
    id: int
    ten: str
    ngay_dien_tap: date
    bat_dau_du_kien: datetime | None
    bat_dau_thuc_te: datetime | None
    ket_thuc_thuc_te: datetime | None
    trang_thai: str
    ghi_chu: str
    created_by: str
    created_at: datetime
    updated_at: datetime


class PcccCountRecordOut(BaseModel):
    id: int
    bo_phan_id: int
    bo_phan_ma: str
    bo_phan_ten: str
    si_so_tham_khao: int
    si_so_dau_ngay: int | None
    thuc_te_kiem_dem: int | None
    chenh_lech: int | None
    trang_thai: str
    ma_ly_do: str
    ly_do_chi_tiet: str
    si_so_updated_at: datetime | None
    kiem_dem_updated_at: datetime | None
    da_xac_nhan: bool = False
    xac_nhan_at: datetime | None = None


class PcccUnitDetailOut(BaseModel):
    dot: PcccDrillOut
    don_vi: PcccUnitOut
    records: list[PcccCountRecordOut]
    tong_si_so_dau_ngay: int
    tong_thuc_te_kiem_dem: int
    chenh_lech: int
    completed_count: int
    department_count: int
    da_xac_nhan: bool
    xac_nhan_at: datetime | None = None


class PcccOverviewUnitOut(BaseModel):
    don_vi_id: int
    don_vi_ten: str
    tong_si_so_dau_ngay: int
    tong_thuc_te_kiem_dem: int
    chenh_lech: int
    completed_count: int
    department_count: int
    trang_thai: str
    xac_nhan_at: datetime | None = None


class PcccOverviewOut(BaseModel):
    dot: PcccDrillOut
    units: list[PcccOverviewUnitOut]
    tong_si_so_dau_ngay: int
    tong_thuc_te_kiem_dem: int
    chenh_lech: int
    unit_confirmed_count: int
    unit_count: int
