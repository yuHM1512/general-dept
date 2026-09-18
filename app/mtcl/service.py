"""Aggregation cho sơ đồ Mục tiêu chất lượng."""
from __future__ import annotations

from datetime import date

from sqlmodel import Session, func, select

from app.mtcl.models import MtclCompany, MtclDepart, MtclDepartment
from app.mtcl.schemas import (
    MtclCompanyNode,
    MtclCsclGroup,
    MtclDepartNode,
    MtclDepartmentOption,
    MtclDiagramResponse,
)


def _clean(value: str | None) -> str:
    return str(value or "").strip()


def _card_weight(node: MtclDepartNode) -> int:
    """Ước lượng chiều cao thẻ để chia đều các nhánh."""
    text = node.content or ""
    line_breaks = text.count("\n") + 1
    wrapped = max(1, (len(text) + 41) // 42)
    return 56 + 18 * max(line_breaks, wrapped)


def balance_depart_columns(items: list[MtclDepartNode], ncols: int) -> list[list[MtclDepartNode]]:
    """Chia đơn vị vào cột ngắn nhất (thẻ dài trước) để các nhánh cân bằng."""
    if not items:
        return []
    ncols = max(1, min(ncols, len(items)))
    columns: list[list[MtclDepartNode]] = [[] for _ in range(ncols)]
    heights = [0] * ncols
    ranked = sorted(items, key=_card_weight, reverse=True)
    for node in ranked:
        idx = min(range(ncols), key=lambda col: (heights[col], col))
        columns[idx].append(node)
        heights[idx] += _card_weight(node) + 16
    return columns


def list_years(session: Session) -> list[int]:
    years: set[int] = set()
    for nam in session.exec(select(MtclCompany.nam)).all():
        if nam:
            years.add(int(nam))
    for nam in session.exec(select(MtclDepart.nam)).all():
        if nam:
            years.add(int(nam))
    return sorted(years, reverse=True)


def list_departments(session: Session) -> list[MtclDepartmentOption]:
    rows = session.exec(select(MtclDepartment).order_by(MtclDepartment.id)).all()
    return [
        MtclDepartmentOption(
            id=int(row.id or 0),
            name=_clean(row.name) or f"Đơn vị #{row.id}",
            description=_clean(row.description),
        )
        for row in rows
        if row.id
    ]


def build_diagram(session: Session, nam: int) -> MtclDiagramResponse:
    companies = session.exec(
        select(MtclCompany).where(MtclCompany.nam == nam).order_by(MtclCompany.id)
    ).all()
    depart_rows = session.exec(
        select(MtclDepart).where(MtclDepart.nam == nam).order_by(MtclDepart.id)
    ).all()
    dept_map = {row.id: row for row in session.exec(select(MtclDepartment)).all() if row.id}

    groups: dict[str, list[MtclCompanyNode]] = {}
    order: list[str] = []
    for row in companies:
        if not row.id:
            continue
        cscl = _clean(row.cscl) or "Chưa có chính sách chất lượng"
        if cscl not in groups:
            groups[cscl] = []
            order.append(cscl)
        groups[cscl].append(
            MtclCompanyNode(
                id=int(row.id),
                cscl=_clean(row.cscl),
                mtcl=_clean(row.mtcl) or "Chưa có mục tiêu chất lượng",
                note=_clean(row.note),
            )
        )

    don_vi: list[MtclDepartNode] = []
    for row in depart_rows:
        if not row.id:
            continue
        dept = dept_map.get(row.id_depart)
        don_vi.append(
            MtclDepartNode(
                id=int(row.id),
                id_depart=int(row.id_depart or 0),
                ten=_clean(dept.name if dept else "") or f"Đơn vị #{row.id_depart}",
                mo_ta=_clean(dept.description if dept else ""),
                content=_clean(row.content),
                note=_clean(row.note),
            )
        )

    return MtclDiagramResponse(
        nam=nam,
        years=list_years(session),
        chien_luoc=[MtclCsclGroup(cscl=key, muc_tieu=groups[key]) for key in order],
        don_vi=don_vi,
        depart_columns=balance_depart_columns(don_vi, min(4, len(don_vi) or 1)),
        departments=list_departments(session),
        company_count=len(companies),
        depart_count=len(depart_rows),
    )


def resolve_year(session: Session, nam: int | None) -> int:
    years = list_years(session)
    if nam and nam in years:
        return nam
    if nam and nam >= 2000:
        return nam
    if years:
        return years[0]
    return date.today().year


def next_id(session: Session, model) -> int:
    current = session.exec(select(func.max(model.id))).one()
    return int(current or 0) + 1


def touch_dates(row: MtclCompany | MtclDepart) -> None:
    today = date.today()
    if row.created_at is None:
        row.created_at = today
    row.updated_at = today
