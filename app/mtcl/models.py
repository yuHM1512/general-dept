"""SQLModel cho module Mục tiêu chất lượng (MTCL).

Bảng `mtcl_company` = chính sách (cscl) + mục tiêu (mtcl) cấp công ty theo năm.
Bảng `mtcl_depart` = mục tiêu cấp đơn vị (`id_depart` → `department.id`).
Bảng `department` là master đơn vị đã có sẵn — chỉ map, không seed.
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
