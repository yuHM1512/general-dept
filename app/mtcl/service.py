"""Aggregation cho sơ đồ Mục tiêu chất lượng."""
from __future__ import annotations

import json
from datetime import date
from typing import TypeVar

from sqlalchemy import or_
from sqlmodel import Session, col, func, select

from app.mtcl.models import (
    MtclCompany,
    MtclDepart,
    MtclDepartment,
    SdChucVuVaiTro,
    SdEmployee,
    SdSoDo,
    SdSoDoArrow,
    SdSoDoDetail,
    SdSoDoLink,
)
from app.mtcl.schemas import (
    MtclCompanyNode,
    MtclCsclGroup,
    MtclDepartDetail,
    MtclDepartNode,
    MtclDepartmentOption,
    MtclDiagramResponse,
    MtclOrgArrow,
    MtclOrgArrowsUpdateIn,
    MtclOrgLinkIn,
    MtclOrgNode,
    MtclOrgPositionsIn,
    MtclOrgRole,
    MtclOrgVaiTroTag,
    MtclNodeLink,
)

LOAI_SO_DO_TO_CHUC = 1
LOAI_SO_DO_VAI_TRO = 2

# sd_chuc_vu_vai_tro dùng chung 1 bảng cho cả chức vụ và vai trò, phân biệt bằng id_loai_vai_tro.
LOAI_VAI_TRO_CHUC_VU = 1
LOAI_VAI_TRO_VAI_TRO = 2

_TModel = TypeVar("_TModel")


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
    org_flags, role_flags = _diagram_flags(session)

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
        id_depart = int(row.id_depart or 0)
        don_vi.append(
            MtclDepartNode(
                id=int(row.id),
                id_depart=id_depart,
                ten=_clean(dept.name if dept else "") or f"Đơn vị #{row.id_depart}",
                mo_ta=_clean(dept.description if dept else ""),
                content=_clean(row.content),
                note=_clean(row.note),
                has_to_chuc=id_depart in org_flags,
                has_vai_tro=id_depart in role_flags,
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


def _diagram_flags(session: Session) -> tuple[set[int], set[int]]:
    org_flags: set[int] = set()
    role_flags: set[int] = set()
    for id_depart, loai in session.exec(
        select(SdSoDoDetail.id_depart, SdSoDoDetail.id_loai_so_do)
    ).all():
        if not id_depart:
            continue
        if loai == LOAI_SO_DO_TO_CHUC:
            org_flags.add(int(id_depart))
        elif loai == LOAI_SO_DO_VAI_TRO:
            role_flags.add(int(id_depart))
    return org_flags, role_flags


def _fetch_by_ids(session: Session, model: type[_TModel], ids: set[int]) -> dict[int, _TModel]:
    clean_ids = {int(i) for i in ids if i}
    if not clean_ids:
        return {}
    rows = session.exec(select(model).where(col(model.id).in_(list(clean_ids)))).all()  # type: ignore[attr-defined]
    return {int(row.id): row for row in rows if getattr(row, "id", None)}


def _detail_cap(detail: SdSoDoDetail) -> int | None:
    if detail.cap is None:
        return None
    try:
        value = int(detail.cap)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _build_org_trees(session: Session, id_depart: int, loai_so_do: int) -> list[MtclOrgNode]:
    """Danh sách phẳng các ô + tọa độ canvas. Quan hệ: cap_parent / reports_to / ho_tro."""
    details = session.exec(
        select(SdSoDoDetail)
        .where(
            SdSoDoDetail.id_depart == id_depart,
            SdSoDoDetail.id_loai_so_do == loai_so_do,
        )
        .order_by(SdSoDoDetail.id)
    ).all()
    if not details:
        return []

    so_map = _fetch_by_ids(session, SdSoDo, {d.id_sd_so_do for d in details if d.id_sd_so_do})
    emp_map = _fetch_by_ids(
        session,
        SdEmployee,
        {row.id_sd_employees for row in so_map.values() if row.id_sd_employees},
    )
    cv_ids = {
        int(i)
        for d in details
        for i in (d.id_chuc_vu_vai_tro, d.id_bao_cao, d.id_ho_tro, d.id_cap_bac)
        if i
    }
    cv_map = _fetch_by_ids(session, SdChucVuVaiTro, cv_ids)
    vt_ids = {int(d.id_vai_tro) for d in details if d.id_vai_tro}
    vt_map = _fetch_by_ids(session, SdChucVuVaiTro, vt_ids)

    def _cv_name(cid: int | None) -> str:
        if not cid:
            return "Chưa gán chức vụ"
        chuc_vu = cv_map.get(int(cid))
        return _clean(chuc_vu.name if chuc_vu else "") or f"Chức vụ #{cid}"

    def _vt_name(vid: int | None) -> str:
        if not vid:
            return ""
        vt = vt_map.get(int(vid))
        return _clean(vt.name if vt else "") or f"Vai trò #{vid}"

    people_acc: dict[int, dict] = {}
    for detail in details:
        so_do_id = int(detail.id_sd_so_do or 0)
        rec = people_acc.setdefault(
            so_do_id,
            {
                "so_do_id": so_do_id,
                "roles": [],
                "vai_tro_tags": [],
                "detail_id": None,
                "bao_cao": None,
                "cap_bac": None,
                "ho_tro": [],
                "cap": None,
                "pos_x": None,
                "pos_y": None,
                "z_index": None,
                "detail_ids": [],
            },
        )
        chuc_vu_id = int(detail.id_chuc_vu_vai_tro) if detail.id_chuc_vu_vai_tro else None
        vai_tro_id = int(detail.id_vai_tro) if detail.id_vai_tro else None
        if chuc_vu_id:
            # Dòng có chức vụ: vai_tro_id (nếu có) là vai trò ghép riêng cho đúng chức vụ này (luồng cũ).
            rec["roles"].append({
                "detail_id": int(detail.id) if detail.id else None,
                "chuc_vu_id": chuc_vu_id,
                "vai_tro_id": vai_tro_id,
            })
        elif vai_tro_id:
            # Dòng chỉ có vai trò (tick checkbox, không gắn chức vụ nào) — hiển thị như nhãn chung của cả ô.
            rec["vai_tro_tags"].append({
                "detail_id": int(detail.id) if detail.id else None,
                "vai_tro_id": vai_tro_id,
            })
        if detail.id:
            rec["detail_ids"].append(int(detail.id))
            if rec["detail_id"] is None:
                rec["detail_id"] = int(detail.id)
        # Quan hệ (báo cáo/cấp bậc/hỗ trợ) là theo NGƯỜI — trỏ tới so_do_id đích,
        # lấy từ dòng detail đầu tiên có giá trị (mọi dòng của cùng 1 người luôn được ghi đồng bộ khi lưu nối).
        if rec["bao_cao"] is None and detail.id_bao_cao:
            rec["bao_cao"] = int(detail.id_bao_cao)
        if rec["cap_bac"] is None and detail.id_cap_bac:
            rec["cap_bac"] = int(detail.id_cap_bac)
        if detail.id_ho_tro:
            ho_tro_id = int(detail.id_ho_tro)
            if ho_tro_id not in rec["ho_tro"]:
                rec["ho_tro"].append(ho_tro_id)
        row_cap = _detail_cap(detail)
        if row_cap is not None and (rec["cap"] is None or row_cap < rec["cap"]):
            rec["cap"] = row_cap
        if detail.pos_x is not None and rec["pos_x"] is None:
            rec["pos_x"] = float(detail.pos_x)
        if detail.pos_y is not None and rec["pos_y"] is None:
            rec["pos_y"] = float(detail.pos_y)
        if detail.z_index is not None and rec["z_index"] is None:
            rec["z_index"] = int(detail.z_index)

    needed_positions = {rec["bao_cao"] for rec in people_acc.values() if rec["bao_cao"]}
    needed_positions.update(hid for rec in people_acc.values() for hid in rec["ho_tro"])
    needed_positions.update(rec["cap_bac"] for rec in people_acc.values() if rec["cap_bac"])
    missing_parents = {so_do_id for so_do_id in needed_positions if so_do_id not in people_acc}
    if missing_parents:
        extra_details = session.exec(
            select(SdSoDoDetail)
            .where(col(SdSoDoDetail.id_sd_so_do).in_(list(missing_parents)))
            .order_by(SdSoDoDetail.id)
        ).all()
        extra_so = _fetch_by_ids(session, SdSoDo, {d.id_sd_so_do for d in extra_details if d.id_sd_so_do})
        so_map.update(extra_so)
        extra_emp = _fetch_by_ids(
            session,
            SdEmployee,
            {row.id_sd_employees for row in extra_so.values() if row.id_sd_employees},
        )
        emp_map.update(extra_emp)
        extra_cv = _fetch_by_ids(
            session,
            SdChucVuVaiTro,
            {int(d.id_chuc_vu_vai_tro) for d in extra_details if d.id_chuc_vu_vai_tro}
            | {int(d.id_vai_tro) for d in extra_details if d.id_vai_tro},
        )
        cv_map.update(extra_cv)
        vt_map.update(extra_cv)
        extra_details = sorted(
            extra_details,
            key=lambda d: (0 if d.id_depart == id_depart else 1, d.id or 0),
        )
        for detail in extra_details:
            so_do_id = int(detail.id_sd_so_do or 0)
            if not so_do_id or so_do_id in people_acc:
                continue
            chuc_vu_id = int(detail.id_chuc_vu_vai_tro) if detail.id_chuc_vu_vai_tro else None
            vai_tro_id = int(detail.id_vai_tro) if detail.id_vai_tro else None
            roles = [{"detail_id": int(detail.id) if detail.id else None, "chuc_vu_id": chuc_vu_id, "vai_tro_id": None}] if chuc_vu_id else []
            vai_tro_tags = [{"detail_id": int(detail.id) if detail.id else None, "vai_tro_id": vai_tro_id}] if not chuc_vu_id and vai_tro_id else []
            people_acc[so_do_id] = {
                "so_do_id": so_do_id,
                "roles": roles,
                "vai_tro_tags": vai_tro_tags,
                "detail_id": int(detail.id) if detail.id else None,
                "bao_cao": None,
                "cap_bac": None,
                "ho_tro": [],
                "cap": _detail_cap(detail),
                "pos_x": float(detail.pos_x) if detail.pos_x is not None else None,
                "pos_y": float(detail.pos_y) if detail.pos_y is not None else None,
                "z_index": int(detail.z_index) if detail.z_index is not None else None,
                "detail_ids": [int(detail.id)] if detail.id else [],
            }

    def _person_fields(so_do_id: int) -> tuple[str, str]:
        so_do = so_map.get(so_do_id)
        emp = emp_map.get(int(so_do.id_sd_employees)) if so_do and so_do.id_sd_employees else None
        return (
            _clean(emp.name if emp else "") or "—",
            _clean(emp.code if emp else ""),
        )

    # Fetch links cho tất cả nodes
    all_links = session.exec(
        select(SdSoDoLink).where(
            SdSoDoLink.id_depart == id_depart,
            SdSoDoLink.id_loai_so_do == loai_so_do,
        )
    ).all()
    links_by_node: dict[int, list[MtclNodeLink]] = {}
    for lnk in all_links:
        links_by_node.setdefault(lnk.so_do_id, []).append(
            MtclNodeLink(id=lnk.id, url=lnk.url or "", label=lnk.label or "")  # type: ignore[arg-type]
        )

    nodes: dict[int, MtclOrgNode] = {}
    rec_by_node: dict[int, dict] = {}
    for rec in people_acc.values():
        node_id = rec["so_do_id"]
        name, code = _person_fields(rec["so_do_id"])
        roles = [
            MtclOrgRole(
                detail_id=role["detail_id"],
                chuc_vu_id=role["chuc_vu_id"],
                chuc_vu=_cv_name(role["chuc_vu_id"]),
                vai_tro_id=role["vai_tro_id"],
                vai_tro=_vt_name(role["vai_tro_id"]),
            )
            for role in rec["roles"]
        ]
        vai_tro_tags = [
            MtclOrgVaiTroTag(
                detail_id=tag["detail_id"],
                vai_tro_id=tag["vai_tro_id"],
                vai_tro=_vt_name(tag["vai_tro_id"]),
            )
            for tag in rec["vai_tro_tags"]
        ]
        nodes[node_id] = MtclOrgNode(
            id=node_id,
            detail_id=rec["detail_id"],
            name=name,
            code=code,
            roles=roles,
            vai_tro_tags=vai_tro_tags,
            links=links_by_node.get(node_id, []),
            cap=int(rec["cap"] or 1),
            ho_tro=[hid for hid in rec["ho_tro"] if hid != node_id],
            children=[],
            x=float(rec["pos_x"] or 0),
            y=float(rec["pos_y"] or 0),
            z_index=int(rec["z_index"] or 0),
        )
        rec_by_node[node_id] = rec

    def _sort_key(node_id: int) -> tuple[int, str]:
        rec = rec_by_node[node_id]
        return (rec["so_do_id"], nodes[node_id].name)

    bao_cao_of: dict[int, int | None] = {}
    for node_id, rec in rec_by_node.items():
        target = rec["bao_cao"]
        bao_cao_of[node_id] = target if target and target != node_id and target in nodes else None
        nodes[node_id].reports_to = bao_cao_of[node_id]

    missing_cap = [nid for nid, rec in rec_by_node.items() if rec["cap"] is None]
    if missing_cap:
        pending = list(rec_by_node)
        changed = True
        while pending and changed:
            changed = False
            still: list[int] = []
            for node_id in pending:
                rec = rec_by_node[node_id]
                if rec["cap"] is not None:
                    continue
                parent_id = bao_cao_of.get(node_id)
                if parent_id is None:
                    rec["cap"] = 1
                    nodes[node_id].cap = 1
                    changed = True
                    continue
                parent_cap = rec_by_node[parent_id]["cap"]
                if parent_cap is None:
                    still.append(node_id)
                    continue
                rec["cap"] = int(parent_cap) + 1
                nodes[node_id].cap = rec["cap"]
                changed = True
            pending = still
        for node_id in pending:
            rec_by_node[node_id]["cap"] = 1
            nodes[node_id].cap = 1

    def _cap_parent(node_id: int) -> int | None:
        """Chỉ dùng nối cấp bậc user tự vẽ (id_cap_bac) — không suy luận từ cây cũ."""
        rec = rec_by_node[node_id]
        target = rec.get("cap_bac")
        return target if target and target != node_id and target in nodes else None

    for node_id in rec_by_node:
        nodes[node_id].cap_parent = _cap_parent(node_id)

    # Tọa độ: giữ DB; nếu chưa có thì xếp lưới phẳng (không theo cây)
    need_persist = False
    ordered_ids = sorted(nodes, key=_sort_key)
    cols = max(3, int(len(ordered_ids) ** 0.5) or 1)
    for idx, node_id in enumerate(ordered_ids):
        rec = rec_by_node[node_id]
        if rec["pos_x"] is None or rec["pos_y"] is None:
            nodes[node_id].x = 48.0 + (idx % cols) * 300
            nodes[node_id].y = 48.0 + (idx // cols) * 160
            need_persist = True
        else:
            nodes[node_id].x = float(rec["pos_x"])
            nodes[node_id].y = float(rec["pos_y"])

    if need_persist:
        for node_id, rec in rec_by_node.items():
            node = nodes[node_id]
            for did in rec["detail_ids"]:
                row = session.get(SdSoDoDetail, did)
                if not row:
                    continue
                if row.pos_x is None:
                    row.pos_x = node.x
                if row.pos_y is None:
                    row.pos_y = node.y
                session.add(row)
        session.commit()

    return [nodes[nid] for nid in ordered_ids]


def _details_for_node(
    session: Session,
    id_depart: int,
    loai_so_do: int,
    so_do_id: int,
) -> list[SdSoDoDetail]:
    """Mọi dòng chức vụ/vai trò của 1 người (1 ô = 1 người, có thể nhiều dòng)."""
    return list(
        session.exec(
            select(SdSoDoDetail).where(
                SdSoDoDetail.id_depart == id_depart,
                SdSoDoDetail.id_loai_so_do == loai_so_do,
                SdSoDoDetail.id_sd_so_do == so_do_id,
            )
        ).all()
    )


CARD_W = 260.0
CARD_H = 92.0


def _arrow_endpoints(fx: float, fy: float, tx: float, ty: float) -> tuple[float, float, float, float]:
    """Điểm neo giữa 2 ô (tâm → mép gần)."""
    fcx, fcy = fx + CARD_W / 2, fy + CARD_H / 2
    tcx, tcy = tx + CARD_W / 2, ty + CARD_H / 2
    dx, dy = tcx - fcx, tcy - fcy
    if abs(dx) >= abs(dy):
        x1 = fx + CARD_W if dx > 0 else fx
        y1 = fcy
        x2 = tx if dx > 0 else tx + CARD_W
        y2 = tcy
    else:
        x1 = fcx
        y1 = fy + CARD_H if dy > 0 else fy
        x2 = tcx
        y2 = ty if dy > 0 else ty + CARD_H
    return x1, y1, x2, y2


def _simple_pos_map(session: Session, id_depart: int, loai_so_do: int) -> dict[int, tuple[float, float]]:
    details = session.exec(
        select(SdSoDoDetail).where(
            SdSoDoDetail.id_depart == id_depart,
            SdSoDoDetail.id_loai_so_do == loai_so_do,
        )
    ).all()
    out: dict[int, tuple[float, float]] = {}
    for d in details:
        if not d.id_sd_so_do:
            continue
        nid = int(d.id_sd_so_do)
        if nid not in out:
            out[nid] = (float(d.pos_x or 0), float(d.pos_y or 0))
    return out


def _parse_arrow_points(raw: str | None) -> list[list[float]]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return []
    if not isinstance(data, list):
        return []
    points: list[list[float]] = []
    for item in data:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            try:
                points.append([float(item[0]), float(item[1])])
            except (TypeError, ValueError):
                continue
    return points


def _dump_arrow_points(points: list) -> str | None:
    clean = [[float(p[0]), float(p[1])] for p in points if len(p) == 2]
    return json.dumps(clean) if clean else None


def list_org_arrows(session: Session, id_depart: int, loai_so_do: int) -> list[MtclOrgArrow]:
    rows = session.exec(
        select(SdSoDoArrow)
        .where(
            SdSoDoArrow.id_depart == id_depart,
            SdSoDoArrow.id_loai_so_do == loai_so_do,
        )
        .order_by(col(SdSoDoArrow.z_index), SdSoDoArrow.id)
    ).all()
    return [
        MtclOrgArrow(
            id=int(r.id),
            kind=_clean(r.kind) or "cap",
            shape_type=_clean(r.shape_type) or "arrow",
            color=_clean(r.color) or "#1A1C1D",
            dash=_clean(r.dash),
            z_index=int(r.z_index or 0),
            content=_clean(r.content),
            from_node_id=int(r.from_node_id),
            to_node_id=int(r.to_node_id),
            x1=float(r.x1 or 0),
            y1=float(r.y1 or 0),
            x2=float(r.x2 or 0),
            y2=float(r.y2 or 0),
            points=_parse_arrow_points(r.points_json),
        )
        for r in rows
        if r.id
    ]


def _refresh_arrow_coords(
    session: Session,
    id_depart: int,
    loai_so_do: int,
    pos_map: dict[int, tuple[float, float]] | None = None,
) -> None:
    if pos_map is None:
        pos_map = _simple_pos_map(session, id_depart, loai_so_do)
    rows = session.exec(
        select(SdSoDoArrow).where(
            SdSoDoArrow.id_depart == id_depart,
            SdSoDoArrow.id_loai_so_do == loai_so_do,
        )
    ).all()
    today = date.today()
    for row in rows:
        fp = pos_map.get(int(row.from_node_id))
        tp = pos_map.get(int(row.to_node_id))
        if not fp or not tp:
            continue
        x1, y1, x2, y2 = _arrow_endpoints(fp[0], fp[1], tp[0], tp[1])
        row.x1, row.y1, row.x2, row.y2 = x1, y1, x2, y2
        row.updated_at = today
        session.add(row)


def save_org_positions(session: Session, payload: MtclOrgPositionsIn) -> dict:
    """Lưu vị trí ô; dịch theo delta các đầu mũi tên gắn với ô đó (giữ chỉnh tay)."""
    pos_before = _simple_pos_map(session, payload.id_depart, payload.loai_so_do)
    updated = 0
    today = date.today()
    for item in payload.positions:
        so_do_id = int(item.node_id)
        old = pos_before.get(so_do_id)
        rows = _details_for_node(session, payload.id_depart, payload.loai_so_do, so_do_id)
        for row in rows:
            row.pos_x = float(item.x)
            row.pos_y = float(item.y)
            session.add(row)
            updated += 1
        if old:
            dx = float(item.x) - float(old[0])
            dy = float(item.y) - float(old[1])
            if dx or dy:
                linked = session.exec(
                    select(SdSoDoArrow).where(
                        SdSoDoArrow.id_depart == payload.id_depart,
                        SdSoDoArrow.id_loai_so_do == payload.loai_so_do,
                        or_(
                            SdSoDoArrow.from_node_id == int(item.node_id),
                            SdSoDoArrow.to_node_id == int(item.node_id),
                        ),
                    )
                ).all()
                for arrow in linked:
                    moved = False
                    if int(arrow.from_node_id) == int(item.node_id):
                        arrow.x1 = float(arrow.x1 or 0) + dx
                        arrow.y1 = float(arrow.y1 or 0) + dy
                        moved = True
                    if int(arrow.to_node_id) == int(item.node_id):
                        arrow.x2 = float(arrow.x2 or 0) + dx
                        arrow.y2 = float(arrow.y2 or 0) + dy
                        moved = True
                    if moved:
                        points = _parse_arrow_points(arrow.points_json)
                        if points:
                            arrow.points_json = _dump_arrow_points(
                                [[px + dx, py + dy] for px, py in points]
                            )
                    arrow.updated_at = today
                    session.add(arrow)
    session.commit()
    return {"ok": True, "updated": updated}


def save_org_arrow_coords(session: Session, payload: MtclOrgArrowsUpdateIn) -> dict:
    """Cập nhật tọa độ mũi tên do user kéo tay."""
    today = date.today()
    updated = 0
    for item in payload.arrows:
        row = session.get(SdSoDoArrow, int(item.id))
        if not row or row.id_depart != payload.id_depart or row.id_loai_so_do != payload.loai_so_do:
            continue
        row.x1 = float(item.x1)
        row.y1 = float(item.y1)
        row.x2 = float(item.x2)
        row.y2 = float(item.y2)
        row.points_json = _dump_arrow_points(item.points)
        if item.color is not None:
            row.color = _clean(item.color) or row.color
        if item.dash is not None:
            row.dash = _clean(item.dash) or None
        row.updated_at = today
        session.add(row)
        updated += 1
    session.commit()
    return {"ok": True, "updated": updated}


def save_org_link(session: Session, payload: MtclOrgLinkIn) -> dict:
    from_so = int(payload.from_node_id)
    to_so = int(payload.to_node_id)
    kind = payload.kind
    if kind != "clear" and payload.from_node_id == payload.to_node_id:
        raise ValueError("Không nối một ô với chính nó")
    rows = _details_for_node(session, payload.id_depart, payload.loai_so_do, from_so)
    if not rows:
        raise ValueError("Không tìm thấy ô nguồn")
    if kind != "clear":
        to_rows = _details_for_node(session, payload.id_depart, payload.loai_so_do, to_so)
        if not to_rows:
            raise ValueError("Không tìm thấy ô đích")
    # Quan hệ là theo NGƯỜI: ghi to_so (so_do_id đích) lên MỌI dòng chức vụ/vai trò của người nguồn.
    for row in rows:
        if kind == "cap":
            row.id_cap_bac = to_so
        elif kind == "bao_cao":
            row.id_bao_cao = to_so
        elif kind == "ho_tro":
            row.id_ho_tro = to_so
        elif kind == "clear":
            row.id_cap_bac = None
            row.id_bao_cao = None
            row.id_ho_tro = None
        session.add(row)

    today = date.today()
    if kind == "clear":
        old = session.exec(
            select(SdSoDoArrow).where(
                SdSoDoArrow.id_depart == payload.id_depart,
                SdSoDoArrow.id_loai_so_do == payload.loai_so_do,
                SdSoDoArrow.from_node_id == payload.from_node_id,
            )
        ).all()
        removed_ids = [int(a.id) for a in old if a.id]
        for arrow in old:
            session.delete(arrow)
        session.commit()
        return {"ok": True, "kind": kind, "removed_arrow_ids": removed_ids}

    # 1 nguồn có thể có NHIỀU mũi tên cùng loại tới nhiều đích khác nhau (không giới hạn);
    # chỉ khi trùng đúng (nguồn, đích, loại) mới cập nhật lại mũi tên cũ thay vì tạo trùng.
    dup = session.exec(
        select(SdSoDoArrow).where(
            SdSoDoArrow.id_depart == payload.id_depart,
            SdSoDoArrow.id_loai_so_do == payload.loai_so_do,
            SdSoDoArrow.from_node_id == payload.from_node_id,
            SdSoDoArrow.to_node_id == payload.to_node_id,
            SdSoDoArrow.kind == kind,
        )
    ).all()
    removed_ids = [int(a.id) for a in dup if a.id]
    for arrow in dup:
        session.delete(arrow)

    pos_map = _simple_pos_map(session, payload.id_depart, payload.loai_so_do)
    fp = pos_map.get(int(payload.from_node_id), (0.0, 0.0))
    tp = pos_map.get(int(payload.to_node_id), (0.0, 0.0))
    if payload.x1 is not None and payload.y1 is not None and payload.x2 is not None and payload.y2 is not None:
        x1, y1, x2, y2 = float(payload.x1), float(payload.y1), float(payload.x2), float(payload.y2)
    else:
        x1, y1, x2, y2 = _arrow_endpoints(fp[0], fp[1], tp[0], tp[1])

    color = _clean(payload.color) or "#1A1C1D"
    dash = _clean(payload.dash) or None
    new_id = next_id(session, SdSoDoArrow)
    session.add(
        SdSoDoArrow(
            id=new_id,
            id_depart=payload.id_depart,
            id_loai_so_do=payload.loai_so_do,
            kind=kind,
            shape_type="arrow",
            color=color,
            dash=dash,
            from_node_id=int(payload.from_node_id),
            to_node_id=int(payload.to_node_id),
            x1=x1,
            y1=y1,
            x2=x2,
            y2=y2,
            points_json=_dump_arrow_points(payload.points),
            created_at=today,
            updated_at=today,
        )
    )
    session.commit()
    return {
        "ok": True,
        "kind": kind,
        "removed_arrow_ids": removed_ids,
        "arrow": {
            "id": new_id,
            "kind": kind,
            "shape_type": "arrow",
            "color": color,
            "dash": dash or "",
            "z_index": 0,
            "from_node_id": int(payload.from_node_id),
            "to_node_id": int(payload.to_node_id),
            "x1": x1,
            "y1": y1,
            "x2": x2,
            "y2": y2,
            "points": payload.points,
        },
    }


def delete_org_arrow(session: Session, id_depart: int, loai_so_do: int, arrow_id: int) -> dict:
    """Xóa 1 mũi tên (bấm chọn trên canvas + phím Delete), giữ nguyên các nối khác của ô."""
    arrow = session.get(SdSoDoArrow, arrow_id)
    if not arrow or arrow.id_depart != id_depart or arrow.id_loai_so_do != loai_so_do:
        raise ValueError("Không tìm thấy mũi tên cần xóa")

    from_so = int(arrow.from_node_id)
    to_so = int(arrow.to_node_id)
    rows = _details_for_node(session, id_depart, loai_so_do, from_so)
    for row in rows:
        if arrow.kind == "cap" and row.id_cap_bac == to_so:
            row.id_cap_bac = None
        elif arrow.kind == "bao_cao" and row.id_bao_cao == to_so:
            row.id_bao_cao = None
        elif arrow.kind == "ho_tro" and row.id_ho_tro == to_so:
            row.id_ho_tro = None
        session.add(row)

    session.delete(arrow)
    session.commit()
    return {"ok": True}


def create_org_shape(session: Session, payload) -> dict:
    """Vẽ 1 đoạn thẳng / ô vuông / mũi tên tự do trên canvas — không gắn vào khối nào."""
    today = date.today()
    color = _clean(payload.color) or "#1A1C1D"
    dash = _clean(payload.dash) or None
    new_id = next_id(session, SdSoDoArrow)
    session.add(
        SdSoDoArrow(
            id=new_id,
            id_depart=payload.id_depart,
            id_loai_so_do=payload.loai_so_do,
            kind="clear",  # không mang nghĩa quan hệ báo cáo/cấp bậc/hỗ trợ
            shape_type=payload.shape_type,
            color=color,
            dash=dash,
            from_node_id=0,
            to_node_id=0,
            x1=float(payload.x1),
            y1=float(payload.y1),
            x2=float(payload.x2),
            y2=float(payload.y2),
            created_at=today,
            updated_at=today,
        )
    )
    session.commit()
    return {
        "ok": True,
        "arrow": {
            "id": new_id,
            "kind": "clear",
            "shape_type": payload.shape_type,
            "color": color,
            "dash": dash or "",
            "z_index": 0,
            "from_node_id": 0,
            "to_node_id": 0,
            "x1": payload.x1,
            "y1": payload.y1,
            "x2": payload.x2,
            "y2": payload.y2,
            "points": [],
        },
    }


def create_org_notes(session: Session, payload) -> dict:
    """Thêm nhiều ghi chú cùng lúc — hiển thị trong bảng chú thích cố định, không có tọa độ trên canvas."""
    if not payload.notes:
        raise ValueError("Chưa nhập nội dung ghi chú nào")
    today = date.today()
    arrows_out = []
    for item in payload.notes:
        content = _clean(item.content)
        if not content:
            continue
        color = _clean(item.color) or "#1A1C1D"
        new_id = next_id(session, SdSoDoArrow)
        session.add(
            SdSoDoArrow(
                id=new_id,
                id_depart=payload.id_depart,
                id_loai_so_do=payload.loai_so_do,
                kind="clear",
                shape_type="note",
                color=color,
                content=content,
                from_node_id=0,
                to_node_id=0,
                x1=0,
                y1=0,
                x2=0,
                y2=0,
                created_at=today,
                updated_at=today,
            )
        )
        session.flush()  # next_id() dùng max(id) trong DB — flush để ghi chú kế tiếp không trùng id
        arrows_out.append({
            "id": new_id,
            "kind": "clear",
            "shape_type": "note",
            "color": color,
            "dash": "",
            "z_index": 0,
            "content": content,
            "from_node_id": 0,
            "to_node_id": 0,
            "x1": 0,
            "y1": 0,
            "x2": 0,
            "y2": 0,
            "points": [],
        })
    if not arrows_out:
        raise ValueError("Chưa nhập nội dung ghi chú nào")
    session.commit()
    return {"ok": True, "arrows": arrows_out}


def update_org_note(session: Session, payload) -> dict:
    """Sửa nội dung / màu 1 ghi chú."""
    row = session.exec(
        select(SdSoDoArrow).where(
            SdSoDoArrow.id == payload.note_id,
            SdSoDoArrow.id_depart == payload.id_depart,
            SdSoDoArrow.id_loai_so_do == payload.loai_so_do,
            SdSoDoArrow.shape_type == "note",
        )
    ).first()
    if not row:
        raise ValueError("Không tìm thấy ghi chú")
    content = _clean(payload.content)
    if not content:
        raise ValueError("Nội dung ghi chú không được để trống")
    row.content = content
    row.color = _clean(payload.color) or "#1A1C1D"
    row.updated_at = date.today()
    session.add(row)
    session.commit()
    return {
        "ok": True,
        "arrow": {
            "id": row.id,
            "kind": row.kind,
            "shape_type": "note",
            "color": row.color,
            "dash": row.dash or "",
            "z_index": row.z_index or 0,
            "content": row.content,
            "from_node_id": 0,
            "to_node_id": 0,
            "x1": 0, "y1": 0, "x2": 0, "y2": 0,
            "points": [],
        },
    }


def update_org_zorder(session: Session, payload) -> dict:
    """Đưa 1 khối hoặc 1 mũi tên/hình vẽ lên trên cùng / xuống dưới cùng / lên 1 bậc / xuống 1 bậc."""
    if payload.target_type == "node":
        rows = _details_for_node(session, payload.id_depart, payload.loai_so_do, payload.target_id)
        if not rows:
            raise ValueError("Không tìm thấy ô")
        siblings = session.exec(
            select(SdSoDoDetail.id_sd_so_do, SdSoDoDetail.z_index).where(
                SdSoDoDetail.id_depart == payload.id_depart,
                SdSoDoDetail.id_loai_so_do == payload.loai_so_do,
            )
        ).all()
        by_node: dict[int, int] = {}
        for so_do_id, z in siblings:
            if so_do_id is None:
                continue
            so_do_id = int(so_do_id)
            by_node[so_do_id] = max(by_node.get(so_do_id, 0), int(z or 0))
        current = by_node.get(payload.target_id, 0)
        new_z = _resolve_zorder(payload.action, current, list(by_node.values()))
        for row in rows:
            row.z_index = new_z
            session.add(row)
        session.commit()
        return {"ok": True, "z_index": new_z}

    arrow = session.get(SdSoDoArrow, payload.target_id)
    if not arrow or arrow.id_depart != payload.id_depart or arrow.id_loai_so_do != payload.loai_so_do:
        raise ValueError("Không tìm thấy mũi tên/hình vẽ")
    siblings = session.exec(
        select(SdSoDoArrow.z_index).where(
            SdSoDoArrow.id_depart == payload.id_depart,
            SdSoDoArrow.id_loai_so_do == payload.loai_so_do,
        )
    ).all()
    new_z = _resolve_zorder(payload.action, int(arrow.z_index or 0), [int(z or 0) for z in siblings])
    arrow.z_index = new_z
    arrow.updated_at = date.today()
    session.add(arrow)
    session.commit()
    return {"ok": True, "z_index": new_z}


def _resolve_zorder(action: str, current: int, all_values: list[int]) -> int:
    lo = min(all_values) if all_values else 0
    hi = max(all_values) if all_values else 0
    if action == "front":
        return hi + 1
    if action == "back":
        return lo - 1
    if action == "forward":
        return current + 1
    if action == "backward":
        return current - 1
    return current


def list_org_catalog(session: Session, id_depart: int) -> dict:
    """Danh sách nhân viên + chức vụ để chọn khi thêm ô (đầy đủ, không cắt)."""
    from app.mtcl.schemas import MtclOrgCatalog, MtclOrgOption

    emps = list(session.exec(select(SdEmployee).order_by(SdEmployee.name)).all())
    emps.sort(
        key=lambda e: (
            0 if e.id_depart == id_depart else 1,
            (_clean(e.name) or "").casefold(),
            (_clean(e.code) or "").casefold(),
        )
    )

    cvs = list(
        session.exec(
            select(SdChucVuVaiTro)
            .where(SdChucVuVaiTro.id_loai_vai_tro == LOAI_VAI_TRO_CHUC_VU)
            .order_by(SdChucVuVaiTro.name)
        ).all()
    )

    vts = list(
        session.exec(
            select(SdChucVuVaiTro)
            .where(SdChucVuVaiTro.id_loai_vai_tro == LOAI_VAI_TRO_VAI_TRO)
            .order_by(SdChucVuVaiTro.name)
        ).all()
    )

    return MtclOrgCatalog(
        employees=[
            MtclOrgOption(id=int(e.id), name=_clean(e.name) or f"NV #{e.id}", code=_clean(e.code))
            for e in emps
            if e.id
        ],
        chuc_vu=[
            MtclOrgOption(id=int(c.id), name=_clean(c.name) or f"CV #{c.id}")
            for c in cvs
            if c.id
        ],
        vai_tro=[
            MtclOrgOption(id=int(v.id), name=_clean(v.name) or f"Vai trò #{v.id}")
            for v in vts
            if v.id
        ],
    ).model_dump()


def create_org_node(session: Session, payload) -> dict:
    """Tạo ô mới trên canvas: ghi sd_employees (nếu cần), sd_chuc_vu, sd_so_do, sd_so_do_details."""
    from app.mtcl.schemas import MtclOrgNodeCreateIn

    data: MtclOrgNodeCreateIn = payload
    today = date.today()

    # --- Nhân viên ---
    emp_id = data.employee_id
    if emp_id:
        emp = session.get(SdEmployee, emp_id)
        if not emp:
            raise ValueError("Không tìm thấy nhân viên")
    else:
        name = _clean(data.employee_name)
        if not name:
            raise ValueError("Chọn nhân viên hoặc nhập tên mới")
        code = _clean(data.employee_code)
        existing = None
        if code:
            existing = session.exec(
                select(SdEmployee).where(SdEmployee.code == code).order_by(SdEmployee.id)
            ).first()
        if existing and existing.id:
            emp_id = int(existing.id)
            emp = existing
        else:
            emp_id = next_id(session, SdEmployee)
            emp = SdEmployee(
                id=emp_id,
                id_depart=data.id_depart,
                name=name,
                code=code or None,
                created_at=today,
                updated_at=today,
            )
            session.add(emp)

    # --- Chức vụ (sd_chuc_vu_vai_tro, id_loai_vai_tro=1) ---
    cv_id = data.chuc_vu_id
    if cv_id:
        cv = session.get(SdChucVuVaiTro, cv_id)
        if not cv:
            raise ValueError("Không tìm thấy chức vụ")
    else:
        cv_name = _clean(data.chuc_vu_name)
        if not cv_name:
            raise ValueError("Chọn chức vụ hoặc nhập tên chức vụ mới")
        existing_cv = session.exec(
            select(SdChucVuVaiTro)
            .where(
                SdChucVuVaiTro.name == cv_name,
                SdChucVuVaiTro.id_loai_vai_tro == LOAI_VAI_TRO_CHUC_VU,
            )
            .order_by(SdChucVuVaiTro.id)
        ).first()
        if existing_cv and existing_cv.id:
            cv_id = int(existing_cv.id)
        else:
            cv_id = next_id(session, SdChucVuVaiTro)
            session.add(
                SdChucVuVaiTro(
                    id=cv_id,
                    name=cv_name,
                    id_loai_vai_tro=LOAI_VAI_TRO_CHUC_VU,
                    created_at=today,
                    updated_at=today,
                )
            )
            # next_id() dựa trên max(id) trong DB — flush để ô vai trò mới (cùng bảng) không trùng id.
            session.flush()

    # --- Vai trò (tùy chọn, hiển thị dưới chức vụ; sd_chuc_vu_vai_tro, id_loai_vai_tro=2) ---
    vt_id = data.vai_tro_id
    if vt_id:
        vt = session.get(SdChucVuVaiTro, vt_id)
        if not vt:
            raise ValueError("Không tìm thấy vai trò")
    else:
        vt_name = _clean(data.vai_tro_name)
        if vt_name:
            existing_vt = session.exec(
                select(SdChucVuVaiTro)
                .where(
                    SdChucVuVaiTro.name == vt_name,
                    SdChucVuVaiTro.id_loai_vai_tro == LOAI_VAI_TRO_VAI_TRO,
                )
                .order_by(SdChucVuVaiTro.id)
            ).first()
            if existing_vt and existing_vt.id:
                vt_id = int(existing_vt.id)
            else:
                vt_id = next_id(session, SdChucVuVaiTro)
                session.add(
                    SdChucVuVaiTro(
                        id=vt_id,
                        name=vt_name,
                        id_loai_vai_tro=LOAI_VAI_TRO_VAI_TRO,
                        created_at=today,
                        updated_at=today,
                    )
                )

    # --- sd_so_do (1 dòng / nhân viên / đơn vị) ---
    so_do = session.exec(
        select(SdSoDo).where(
            SdSoDo.id_depart == data.id_depart,
            SdSoDo.id_sd_employees == emp_id,
        ).order_by(SdSoDo.id)
    ).first()
    if so_do and so_do.id:
        so_do_id = int(so_do.id)
    else:
        so_do_id = next_id(session, SdSoDo)
        so_do = SdSoDo(
            id=so_do_id,
            id_depart=data.id_depart,
            id_sd_employees=emp_id,
            created_at=today,
            updated_at=today,
        )
        session.add(so_do)

    # Tránh trùng cùng chức vụ trên cùng loại sơ đồ
    dup = session.exec(
        select(SdSoDoDetail).where(
            SdSoDoDetail.id_depart == data.id_depart,
            SdSoDoDetail.id_loai_so_do == data.loai_so_do,
            SdSoDoDetail.id_sd_so_do == so_do_id,
            SdSoDoDetail.id_chuc_vu_vai_tro == cv_id,
        )
    ).first()
    if dup:
        raise ValueError("Ô này đã có trên sơ đồ (cùng người + chức vụ)")

    # 1 ô = 1 người: nếu người này đã có ô trên sơ đồ (chức vụ khác), thêm chức vụ/vai trò mới
    # vào ĐÚNG vị trí ô cũ thay vì tạo ô rời.
    existing_node = _details_for_node(session, data.id_depart, data.loai_so_do, so_do_id)
    if existing_node:
        pos_x = existing_node[0].pos_x if existing_node[0].pos_x is not None else float(data.x)
        pos_y = existing_node[0].pos_y if existing_node[0].pos_y is not None else float(data.y)
    else:
        pos_x, pos_y = float(data.x), float(data.y)

    detail_id = next_id(session, SdSoDoDetail)
    detail = SdSoDoDetail(
        id=detail_id,
        id_depart=data.id_depart,
        id_sd_so_do=so_do_id,
        id_loai_so_do=data.loai_so_do,
        id_chuc_vu_vai_tro=cv_id,
        id_vai_tro=vt_id or None,
        cap=int(data.cap),
        pos_x=pos_x,
        pos_y=pos_y,
        created_at=today,
        updated_at=today,
    )
    session.add(detail)
    session.commit()
    return {
        "ok": True,
        "node_id": so_do_id,
        "detail_id": detail_id,
        "so_do_id": so_do_id,
        "chuc_vu_id": cv_id,
        "employee_id": emp_id,
    }


def _resolve_or_create_cv_vt(
    session: Session, ids: list[int], names: list[str], id_loai_vai_tro: int, label: str
) -> list[int]:
    """Ghép id có sẵn (tick checkbox) + tên mới (gõ thêm) thành danh sách id sd_chuc_vu_vai_tro, không trùng."""
    today = date.today()
    resolved: list[int] = []
    seen: set[int] = set()
    for raw_id in ids:
        cid = int(raw_id)
        if cid and cid not in seen:
            seen.add(cid)
            resolved.append(cid)
    for raw_name in names:
        name = _clean(raw_name)
        if not name:
            continue
        existing = session.exec(
            select(SdChucVuVaiTro)
            .where(SdChucVuVaiTro.name == name, SdChucVuVaiTro.id_loai_vai_tro == id_loai_vai_tro)
            .order_by(SdChucVuVaiTro.id)
        ).first()
        if existing and existing.id:
            cid = int(existing.id)
        else:
            cid = next_id(session, SdChucVuVaiTro)
            session.add(
                SdChucVuVaiTro(
                    id=cid,
                    name=name,
                    id_loai_vai_tro=id_loai_vai_tro,
                    created_at=today,
                    updated_at=today,
                )
            )
            session.flush()  # next_id() dùng max(id) trong DB — flush để id mới kế tiếp không trùng
        if cid not in seen:
            seen.add(cid)
            resolved.append(cid)
    if not resolved and (ids or names):
        raise ValueError(f"Không tìm thấy {label}")
    return resolved


def create_org_node_batch(session: Session, payload) -> dict:
    """Thêm 1 người vào sơ đồ, tick nhiều chức vụ + nhiều vai trò cùng lúc (checkbox).

    Mỗi chức vụ tick tạo 1 dòng riêng (không kèm vai trò); mỗi vai trò tick tạo 1 dòng
    riêng không gắn chức vụ nào — hiển thị như nhãn chung của cả ô.
    """
    from app.mtcl.schemas import MtclOrgNodeBatchCreateIn

    data: MtclOrgNodeBatchCreateIn = payload
    today = date.today()

    # --- Nhân viên (giống create_org_node) ---
    emp_id = data.employee_id
    if emp_id:
        emp = session.get(SdEmployee, emp_id)
        if not emp:
            raise ValueError("Không tìm thấy nhân viên")
    else:
        name = _clean(data.employee_name)
        if not name:
            raise ValueError("Chọn nhân viên hoặc nhập tên mới")
        code = _clean(data.employee_code)
        existing = None
        if code:
            existing = session.exec(
                select(SdEmployee).where(SdEmployee.code == code).order_by(SdEmployee.id)
            ).first()
        if existing and existing.id:
            emp_id = int(existing.id)
        else:
            emp_id = next_id(session, SdEmployee)
            session.add(
                SdEmployee(
                    id=emp_id,
                    id_depart=data.id_depart,
                    name=name,
                    code=code or None,
                    created_at=today,
                    updated_at=today,
                )
            )
            session.flush()

    cv_ids = _resolve_or_create_cv_vt(
        session, data.chuc_vu_ids, data.chuc_vu_names, LOAI_VAI_TRO_CHUC_VU, "chức vụ"
    )
    if not cv_ids:
        raise ValueError("Chọn ít nhất 1 chức vụ")
    vt_ids = _resolve_or_create_cv_vt(
        session, data.vai_tro_ids, data.vai_tro_names, LOAI_VAI_TRO_VAI_TRO, "vai trò"
    )

    # --- sd_so_do (1 dòng / nhân viên / đơn vị) ---
    so_do = session.exec(
        select(SdSoDo).where(
            SdSoDo.id_depart == data.id_depart,
            SdSoDo.id_sd_employees == emp_id,
        ).order_by(SdSoDo.id)
    ).first()
    if so_do and so_do.id:
        so_do_id = int(so_do.id)
    else:
        so_do_id = next_id(session, SdSoDo)
        session.add(
            SdSoDo(
                id=so_do_id,
                id_depart=data.id_depart,
                id_sd_employees=emp_id,
                created_at=today,
                updated_at=today,
            )
        )
        session.flush()

    # 1 ô = 1 người: nếu người này đã có ô trên sơ đồ, giữ đúng vị trí cũ.
    existing_rows = _details_for_node(session, data.id_depart, data.loai_so_do, so_do_id)
    if existing_rows and existing_rows[0].pos_x is not None and existing_rows[0].pos_y is not None:
        pos_x, pos_y = float(existing_rows[0].pos_x), float(existing_rows[0].pos_y)
    else:
        pos_x, pos_y = float(data.x), float(data.y)
    existing_cv = {int(r.id_chuc_vu_vai_tro) for r in existing_rows if r.id_chuc_vu_vai_tro}
    existing_vt_only = {int(r.id_vai_tro) for r in existing_rows if r.id_vai_tro and not r.id_chuc_vu_vai_tro}

    detail_ids: list[int] = []
    for cv_id in cv_ids:
        if cv_id in existing_cv:
            continue
        detail_id = next_id(session, SdSoDoDetail)
        session.add(
            SdSoDoDetail(
                id=detail_id,
                id_depart=data.id_depart,
                id_sd_so_do=so_do_id,
                id_loai_so_do=data.loai_so_do,
                id_chuc_vu_vai_tro=cv_id,
                cap=int(data.cap),
                pos_x=pos_x,
                pos_y=pos_y,
                created_at=today,
                updated_at=today,
            )
        )
        session.flush()
        existing_cv.add(cv_id)
        detail_ids.append(detail_id)

    for vt_id in vt_ids:
        if vt_id in existing_vt_only:
            continue
        detail_id = next_id(session, SdSoDoDetail)
        session.add(
            SdSoDoDetail(
                id=detail_id,
                id_depart=data.id_depart,
                id_sd_so_do=so_do_id,
                id_loai_so_do=data.loai_so_do,
                id_chuc_vu_vai_tro=None,
                id_vai_tro=vt_id,
                cap=int(data.cap),
                pos_x=pos_x,
                pos_y=pos_y,
                created_at=today,
                updated_at=today,
            )
        )
        session.flush()
        existing_vt_only.add(vt_id)
        detail_ids.append(detail_id)

    session.commit()
    return {
        "ok": True,
        "node_id": so_do_id,
        "so_do_id": so_do_id,
        "employee_id": emp_id,
        "created": len(detail_ids),
        "detail_ids": detail_ids,
    }


def update_org_node(session: Session, payload) -> dict:
    """Sửa toàn bộ nội dung 1 khối: đổi tên/mã NV (nếu có) + thay thế hẳn danh sách chức vụ/vai trò.

    Không cho đổi sang nhân viên khác (tránh gộp nhầm 2 ô) — chỉ sửa thông tin hiển thị của
    đúng người đang giữ ô này.
    """
    so_do_id = int(payload.so_do_id)
    rows = _details_for_node(session, payload.id_depart, payload.loai_so_do, so_do_id)
    if not rows:
        raise ValueError("Không tìm thấy ô cần sửa")

    today = date.today()
    so_do = session.get(SdSoDo, so_do_id)
    name = _clean(payload.employee_name)
    if so_do and so_do.id_sd_employees and name:
        emp = session.get(SdEmployee, so_do.id_sd_employees)
        if emp:
            emp.name = name
            emp.code = _clean(payload.employee_code) or None
            emp.updated_at = today
            session.add(emp)

    cv_ids = _resolve_or_create_cv_vt(
        session, payload.chuc_vu_ids, payload.chuc_vu_names, LOAI_VAI_TRO_CHUC_VU, "chức vụ"
    )
    if not cv_ids:
        raise ValueError("Chọn ít nhất 1 chức vụ")
    vt_ids = _resolve_or_create_cv_vt(
        session, payload.vai_tro_ids, payload.vai_tro_names, LOAI_VAI_TRO_VAI_TRO, "vai trò"
    )

    # Các trường quan hệ/tọa độ nằm theo NGƯỜI (lặp lại trên mọi dòng chức vụ/vai trò) —
    # giữ nguyên giá trị cũ khi xóa dòng cũ + tạo dòng mới cho danh sách chức vụ/vai trò đã sửa.
    first = rows[0]
    shared = {
        "cap": first.cap,
        "pos_x": first.pos_x,
        "pos_y": first.pos_y,
        "z_index": first.z_index,
        "id_bao_cao": first.id_bao_cao,
        "id_cap_bac": first.id_cap_bac,
        "id_ho_tro": first.id_ho_tro,
    }

    for row in rows:
        session.delete(row)
    session.flush()

    for cv_id in cv_ids:
        detail_id = next_id(session, SdSoDoDetail)
        session.add(
            SdSoDoDetail(
                id=detail_id,
                id_depart=payload.id_depart,
                id_sd_so_do=so_do_id,
                id_loai_so_do=payload.loai_so_do,
                id_chuc_vu_vai_tro=cv_id,
                **shared,
                created_at=today,
                updated_at=today,
            )
        )
        session.flush()

    for vt_id in vt_ids:
        detail_id = next_id(session, SdSoDoDetail)
        session.add(
            SdSoDoDetail(
                id=detail_id,
                id_depart=payload.id_depart,
                id_sd_so_do=so_do_id,
                id_loai_so_do=payload.loai_so_do,
                id_chuc_vu_vai_tro=None,
                id_vai_tro=vt_id,
                **shared,
                created_at=today,
                updated_at=today,
            )
        )
        session.flush()

    session.commit()
    return {"ok": True, "so_do_id": so_do_id}


def delete_org_node(session: Session, id_depart: int, loai_so_do: int, node_id: int) -> dict:
    so_do_id = int(node_id)
    rows = _details_for_node(session, id_depart, loai_so_do, so_do_id)
    if not rows:
        raise ValueError("Không tìm thấy ô cần xóa")
    for row in rows:
        session.delete(row)
    arrows = session.exec(
        select(SdSoDoArrow).where(
            SdSoDoArrow.id_depart == id_depart,
            SdSoDoArrow.id_loai_so_do == loai_so_do,
            or_(SdSoDoArrow.from_node_id == node_id, SdSoDoArrow.to_node_id == node_id),
        )
    ).all()
    for arrow in arrows:
        session.delete(arrow)
    session.commit()
    return {"ok": True, "deleted": len(rows)}


def delete_org_node_details(
    session: Session, id_depart: int, loai_so_do: int, so_do_id: int, detail_ids: list[int]
) -> dict:
    """Xóa các dòng chức vụ/vai trò cụ thể của 1 người (undo Thêm ô) — không đụng các dòng khác của người đó."""
    if not detail_ids:
        return {"ok": True, "deleted": 0}
    rows = session.exec(
        select(SdSoDoDetail).where(
            col(SdSoDoDetail.id).in_(detail_ids),
            SdSoDoDetail.id_depart == id_depart,
            SdSoDoDetail.id_loai_so_do == loai_so_do,
            SdSoDoDetail.id_sd_so_do == so_do_id,
        )
    ).all()
    for row in rows:
        session.delete(row)
    session.commit()
    return {"ok": True, "deleted": len(rows)}


# ── Node links CRUD ──────────────────────────────────────────────

def create_node_link(
    session: Session, id_depart: int, loai_so_do: int, so_do_id: int, url: str, label: str,
) -> dict:
    row = SdSoDoLink(
        so_do_id=so_do_id,
        id_depart=id_depart,
        id_loai_so_do=loai_so_do,
        url=url.strip(),
        label=label.strip(),
        created_at=date.today(),
        updated_at=date.today(),
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return {"ok": True, "id": row.id, "url": row.url, "label": row.label}


def update_node_link(
    session: Session, id_depart: int, loai_so_do: int, link_id: int, url: str, label: str,
) -> dict:
    row = session.exec(
        select(SdSoDoLink).where(
            SdSoDoLink.id == link_id,
            SdSoDoLink.id_depart == id_depart,
            SdSoDoLink.id_loai_so_do == loai_so_do,
        )
    ).first()
    if not row:
        return {"ok": False, "error": "Link không tồn tại"}
    row.url = url.strip()
    row.label = label.strip()
    row.updated_at = date.today()
    session.add(row)
    session.commit()
    return {"ok": True, "id": row.id, "url": row.url, "label": row.label}


def delete_node_link(
    session: Session, id_depart: int, loai_so_do: int, link_id: int,
) -> dict:
    row = session.exec(
        select(SdSoDoLink).where(
            SdSoDoLink.id == link_id,
            SdSoDoLink.id_depart == id_depart,
            SdSoDoLink.id_loai_so_do == loai_so_do,
        )
    ).first()
    if not row:
        return {"ok": False, "error": "Link không tồn tại"}
    session.delete(row)
    session.commit()
    return {"ok": True}


def build_depart_detail(session: Session, nam: int, id_depart: int) -> MtclDepartDetail | None:
    dept = session.get(MtclDepartment, id_depart)
    if not dept or not dept.id:
        return None
    mtcl = session.exec(
        select(MtclDepart)
        .where(MtclDepart.nam == nam, MtclDepart.id_depart == id_depart)
        .order_by(MtclDepart.id)
    ).first()
    return MtclDepartDetail(
        id_depart=int(dept.id),
        ten=_clean(dept.name) or f"Đơn vị #{dept.id}",
        mo_ta=_clean(dept.description),
        nam=nam,
        mtcl_id=int(mtcl.id) if mtcl and mtcl.id else None,
        content=_clean(mtcl.content if mtcl else ""),
        note=_clean(mtcl.note if mtcl else ""),
        so_do_to_chuc=_build_org_trees(session, id_depart, LOAI_SO_DO_TO_CHUC),
        so_do_vai_tro=_build_org_trees(session, id_depart, LOAI_SO_DO_VAI_TRO),
        arrows_to_chuc=list_org_arrows(session, id_depart, LOAI_SO_DO_TO_CHUC),
        arrows_vai_tro=list_org_arrows(session, id_depart, LOAI_SO_DO_VAI_TRO),
    )
