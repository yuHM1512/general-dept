"""SQLModel cho module Mục tiêu chất lượng (MTCL).

Bảng `mtcl_company` = chính sách (cscl) + mục tiêu (mtcl) cấp công ty theo năm.
Bảng `mtcl_depart` = mục tiêu cấp đơn vị (`id_depart` → `department.id`).
Bảng `department` là master đơn vị đã có sẵn — chỉ map, không seed.
Bảng `sd_so_do` + `sd_so_do_details` là dữ liệu gốc để vẽ sơ đồ tổ chức / vai trò.
"""
from __future__ import annotations

from datetime import date

from sqlmodel import Field, SQLModel


class MtclDepartment(SQLModel, table=True):
    __tablename__ = "department"

    id: int | None = Field(default=None, primary_key=True)
    name: str | None = Field(default=None)
    description: str | None = Field(default=None)
    created_at: date | None = Field(default=None)
    updated_at: date | None = Field(default=None)


class MtclCompany(SQLModel, table=True):
    __tablename__ = "mtcl_company"

    id: int | None = Field(default=None, primary_key=True)
    nam: int | None = Field(default=None, index=True)
    cscl: str | None = Field(default=None)
    mtcl: str | None = Field(default=None)
    note: str | None = Field(default=None)
    created_at: date | None = Field(default=None)
    updated_at: date | None = Field(default=None)


class MtclDepart(SQLModel, table=True):
    __tablename__ = "mtcl_depart"

    id: int | None = Field(default=None, primary_key=True)
    nam: int | None = Field(default=None, index=True)
    id_depart: int | None = Field(default=None, foreign_key="department.id", index=True)
    content: str | None = Field(default=None)
    note: str | None = Field(default=None)
    created_at: date | None = Field(default=None)
    updated_at: date | None = Field(default=None)


class SdLoaiVaiTro(SQLModel, table=True):
    __tablename__ = "loai_vai_tro"

    id: int | None = Field(default=None, primary_key=True)
    name: str | None = Field(default=None)
    created_at: date | None = Field(default=None)


class SdEmployee(SQLModel, table=True):
    __tablename__ = "sd_employees"

    id: int | None = Field(default=None, primary_key=True)
    id_depart: int | None = Field(default=None, index=True)
    name: str | None = Field(default=None)
    code: str | None = Field(default=None)
    created_at: date | None = Field(default=None)
    updated_at: date | None = Field(default=None)


class SdChucVuVaiTro(SQLModel, table=True):
    __tablename__ = "sd_chuc_vu_vai_tro"

    id: int | None = Field(default=None, primary_key=True)
    name: str | None = Field(default=None)
    id_loai_vai_tro: int | None = Field(default=None, index=True)  # 1 = chức vụ, 2 = vai trò
    created_at: date | None = Field(default=None)
    updated_at: date | None = Field(default=None)


class SdSoDo(SQLModel, table=True):
    __tablename__ = "sd_so_do"

    id: int | None = Field(default=None, primary_key=True)
    id_depart: int | None = Field(default=None, index=True)
    id_sd_employees: int | None = Field(default=None, index=True)
    note: str | None = Field(default=None)
    created_at: date | None = Field(default=None)
    updated_at: date | None = Field(default=None)


class SdSoDoDetail(SQLModel, table=True):
    __tablename__ = "sd_so_do_details"

    id: int | None = Field(default=None, primary_key=True)
    id_depart: int | None = Field(default=None, index=True)
    id_sd_so_do: int | None = Field(default=None, index=True)
    id_loai_so_do: int | None = Field(default=None, index=True)
    id_chuc_vu_vai_tro: int | None = Field(default=None)
    id_vai_tro: int | None = Field(default=None, index=True)  # FK sd_chuc_vu_vai_tro.id (loai=2), hiển thị dưới chức vụ
    # id_bao_cao/id_ho_tro/id_cap_bac trỏ tới sd_so_do.id (id_sd_so_do) của NGƯỜI đích —
    # quan hệ theo người, không theo từng chức vụ, vì 1 người/1 ô có thể giữ nhiều chức vụ.
    id_bao_cao: int | None = Field(default=None)
    id_ho_tro: int | None = Field(default=None)
    id_cap_bac: int | None = Field(default=None)
    cap: int | None = Field(default=None)
    pos_x: float | None = Field(default=None)
    pos_y: float | None = Field(default=None)
    z_index: int | None = Field(default=None)  # thứ tự chồng lớp của khối (đưa lên trên/xuống dưới)
    created_at: date | None = Field(default=None)
    updated_at: date | None = Field(default=None)


class SdSoDoArrow(SQLModel, table=True):
    """Mũi tên / đoạn thẳng / ô vuông trên canvas — lưu tọa độ để tự vẽ."""
    __tablename__ = "sd_so_do_arrows"

    id: int | None = Field(default=None, primary_key=True)
    id_depart: int | None = Field(default=None, index=True)
    id_loai_so_do: int | None = Field(default=None, index=True)
    kind: str = Field(default="cap", max_length=20)  # giữ tương thích dữ liệu cũ: cap | bao_cao | ho_tro
    shape_type: str = Field(default="arrow", max_length=10)  # arrow | line | rect | note
    content: str | None = Field(default=None)  # nội dung ghi chú (shape_type = note)
    color: str = Field(default="#1A1C1D", max_length=16)
    dash: str | None = Field(default=None, max_length=20)  # vd "6 5"; rỗng/None = nét liền
    z_index: int = Field(default=0)  # thứ tự chồng lớp (đưa lên trên/xuống dưới) trong nhóm mũi tên/hình vẽ
    from_node_id: int = Field(default=0, index=True)  # = sd_so_do.id; 0 nếu là hình vẽ tự do không gắn khối
    to_node_id: int = Field(default=0, index=True)  # = sd_so_do.id; 0 nếu là hình vẽ tự do không gắn khối
    x1: float = Field(default=0)
    y1: float = Field(default=0)
    x2: float = Field(default=0)
    y2: float = Field(default=0)
    points_json: str | None = Field(default=None)  # JSON list [[x,y],...] điểm gập khúc giữa (x1,y1)→(x2,y2)
    created_at: date | None = Field(default=None)
    updated_at: date | None = Field(default=None)


class SdSoDoLink(SQLModel, table=True):
    """Link URL gắn vào 1 khối sơ đồ — 1 khối có thể nhiều link."""
    __tablename__ = "sd_so_do_links"

    id: int | None = Field(default=None, primary_key=True)
    so_do_id: int = Field(index=True)
    id_depart: int = Field(index=True)
    id_loai_so_do: int = Field(default=1)
    url: str = Field(default="")
    label: str = Field(default="")
    created_at: date | None = Field(default=None)
    updated_at: date | None = Field(default=None)
