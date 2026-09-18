"""Pydantic schemas cho module Mục tiêu chất lượng."""
from __future__ import annotations

from pydantic import BaseModel, Field


class MtclCompanyIn(BaseModel):
    nam: int = Field(..., ge=2000, le=2100)
    cscl: str = ""
    mtcl: str = ""
    note: str = ""


class MtclDepartIn(BaseModel):
    nam: int = Field(..., ge=2000, le=2100)
    id_depart: int
    content: str = ""
    note: str = ""


class MtclCompanyNode(BaseModel):
    id: int
    cscl: str
    mtcl: str
    note: str = ""


class MtclCsclGroup(BaseModel):
    cscl: str
    muc_tieu: list[MtclCompanyNode]


class MtclDepartNode(BaseModel):
    id: int
    id_depart: int
    ten: str
    mo_ta: str = ""
    content: str
    note: str = ""


class MtclDepartmentOption(BaseModel):
    id: int
    name: str
    description: str = ""


class MtclDiagramResponse(BaseModel):
    nam: int
    years: list[int]
    chien_luoc: list[MtclCsclGroup]
    don_vi: list[MtclDepartNode]
    depart_columns: list[list[MtclDepartNode]] = []
    departments: list[MtclDepartmentOption]
    company_count: int = 0
    depart_count: int = 0
