from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlmodel import Session, select

from app.db import get_session
from app.models import GeneralEmployee
from app.pccc.models import (
    PcccBoPhan,
    PcccDonVi,
    PcccDotDienTap,
    PcccKiemDem,
    PcccLichSu,
    PcccPhanCong,
    PcccXacNhanDonVi,
)
from app.pccc.schemas import (
    PcccAssignmentOut,
    PcccAssignmentUpsert,
    PcccBulkCountUpdate,
    PcccDrillCreate,
    PcccDrillOut,
    PcccDrillStatusUpdate,
    PcccOverviewOut,
    PcccUnitConfirm,
    PcccUnitDetailOut,
    PcccUnitOut,
)
from app.pccc.service import (
    build_overview,
    build_unit_detail,
    confirm_unit,
    current_employee_code,
    drill_out,
    ensure_drill_records,
    is_admin,
    organization_unit_ids,
    require_unit_access,
    save_counts,
    visible_department_ids,
    visible_unit_ids,
)


router = APIRouter(prefix="/api/pccc", tags=["PCCC"])


def _current_user(request: Request) -> dict | None:
    try:
        return request.session.get("user")  # type: ignore[attr-defined]
    except Exception:
        return None


def _require_admin(request: Request) -> dict:
    user = _current_user(request)
    if not is_admin(user):
        raise HTTPException(status_code=403, detail="Chỉ admin mới có thể thực hiện thao tác này")
    return user or {}


def _get_drill(session: Session, drill_id: int) -> PcccDotDienTap:
    drill = session.get(PcccDotDienTap, drill_id)
    if not drill:
        raise HTTPException(status_code=404, detail="Không tìm thấy đợt diễn tập PCCC")
    return drill


def _unit_payload(
    session: Session,
    unit: PcccDonVi,
    department_ids: set[int] | None = None,
) -> PcccUnitOut:
    departments = session.exec(
        select(PcccBoPhan)
        .where(PcccBoPhan.don_vi_id == unit.id, PcccBoPhan.active == True)  # noqa: E712
        .order_by(PcccBoPhan.thu_tu, PcccBoPhan.id)
    ).all()
    if department_ids is not None:
        departments = [row for row in departments if int(row.id) in department_ids]
    return PcccUnitOut(
        id=int(unit.id),
        ma=unit.ma,
        ten=unit.ten,
        thu_tu=unit.thu_tu,
        bo_phans=[
            {
                "id": int(dep.id),
                "ma": dep.ma,
                "ten": dep.ten,
                "si_so_tham_khao": dep.si_so_tham_khao,
                "thu_tu": dep.thu_tu,
            }
            for dep in departments
        ],
    )


@router.get("/units", response_model=list[PcccUnitOut])
def list_units(
    request: Request,
    session: Session = Depends(get_session),
) -> list[PcccUnitOut]:
    allowed = visible_unit_ids(_current_user(request), session)
    query = select(PcccDonVi).where(PcccDonVi.active == True)  # noqa: E712
    if allowed is not None:
        if not allowed:
            return []
        query = query.where(PcccDonVi.id.in_(list(allowed)))
    units = session.exec(query.order_by(PcccDonVi.thu_tu, PcccDonVi.id)).all()
    result = []
    user = _current_user(request)
    for unit in units:
        department_ids = visible_department_ids(user, session, int(unit.id))
        if department_ids is not None and not department_ids:
            continue
        result.append(_unit_payload(session, unit, department_ids))
    return result


@router.get("/assignments", response_model=list[PcccAssignmentOut])
def list_assignments(
    request: Request,
    don_vi_id: int | None = Query(default=None),
    session: Session = Depends(get_session),
) -> list[PcccAssignmentOut]:
    _require_admin(request)
    query = select(PcccPhanCong)
    if don_vi_id is not None:
        query = query.where(PcccPhanCong.don_vi_id == don_vi_id)
    rows = session.exec(query.order_by(PcccPhanCong.don_vi_id, PcccPhanCong.vai_tro)).all()
    employees = {
        employee.ma_nv: employee.ho_ten
        for employee in session.exec(
            select(GeneralEmployee).where(GeneralEmployee.ma_nv.in_([row.ma_nv for row in rows]))
        ).all()
    } if rows else {}
    departments = {
        int(department.id): department.ten
        for department in session.exec(
            select(PcccBoPhan).where(
                PcccBoPhan.id.in_([row.bo_phan_id for row in rows if row.bo_phan_id is not None])
            )
        ).all()
    } if any(row.bo_phan_id is not None for row in rows) else {}
    return [
        PcccAssignmentOut(
            id=int(row.id),
            don_vi_id=row.don_vi_id,
            bo_phan_id=row.bo_phan_id,
            bo_phan_ten=departments.get(int(row.bo_phan_id), "") if row.bo_phan_id is not None else "",
            ma_nv=row.ma_nv,
            ho_ten=employees.get(row.ma_nv, ""),
            vai_tro=row.vai_tro,
            active=row.active,
        )
        for row in rows
    ]


@router.get("/employees/{employee_code}")
def assignment_employee(employee_code: str, request: Request, session: Session = Depends(get_session)) -> dict:
    _require_admin(request)
    employee = session.get(GeneralEmployee, employee_code.strip().upper())
    if not employee:
        raise HTTPException(status_code=404, detail="Mã nhân viên chưa tồn tại trong danh mục nhân sự")
    return {
        "ma_nv": employee.ma_nv,
        "ho_ten": employee.ho_ten,
        "don_vi": employee.don_vi,
        "bo_phan": employee.bo_phan,
        "unit_ids": sorted(organization_unit_ids({"don_vi": employee.don_vi}, session)),
    }


@router.delete("/assignments/{assignment_id}")
def revoke_assignment(assignment_id: int, request: Request, session: Session = Depends(get_session)) -> dict:
    _require_admin(request)
    row = session.get(PcccPhanCong, assignment_id)
    if not row:
        raise HTTPException(status_code=404, detail="Không tìm thấy phân công")
    row.active = False
    row.updated_at = datetime.utcnow()
    session.add(row)
    session.commit()
    return {"ok": True}


@router.put("/assignments", response_model=PcccAssignmentOut)
def upsert_assignment(
    request: Request,
    payload: PcccAssignmentUpsert,
    session: Session = Depends(get_session),
) -> PcccAssignmentOut:
    user = _require_admin(request)
    unit = session.get(PcccDonVi, payload.don_vi_id)
    if not unit or not unit.active:
        raise HTTPException(status_code=422, detail="Đơn vị PCCC không tồn tại")
    department = None
    if payload.bo_phan_id is not None:
        department = session.get(PcccBoPhan, payload.bo_phan_id)
        if not department or not department.active or department.don_vi_id != payload.don_vi_id:
            raise HTTPException(status_code=422, detail="Bộ phận không thuộc đơn vị PCCC đã chọn")
    employee = session.get(GeneralEmployee, payload.ma_nv)
    if not employee:
        raise HTTPException(status_code=422, detail="Mã nhân viên chưa tồn tại trong hệ thống")
    employee_units = organization_unit_ids(
        {
            "ma_nv": employee.ma_nv,
            "don_vi": employee.don_vi,
            "bo_phan": employee.bo_phan,
            "role": employee.role,
        },
        session,
    )
    if employee_units is not None and payload.don_vi_id not in employee_units:
        raise HTTPException(
            status_code=422,
            detail="Nhân viên không thuộc phạm vi tổ chức của đơn vị PCCC này",
        )

    duplicate_employee = session.exec(
        select(PcccPhanCong).where(
            PcccPhanCong.don_vi_id == payload.don_vi_id,
            PcccPhanCong.bo_phan_id == payload.bo_phan_id,
            PcccPhanCong.ma_nv == payload.ma_nv,
            PcccPhanCong.vai_tro != payload.vai_tro,
        )
    ).first()
    if duplicate_employee:
        raise HTTPException(status_code=409, detail="Nhân viên đã được phân công vai trò khác trong đơn vị")

    row = session.exec(
        select(PcccPhanCong).where(
            PcccPhanCong.don_vi_id == payload.don_vi_id,
            PcccPhanCong.bo_phan_id == payload.bo_phan_id,
            PcccPhanCong.vai_tro == payload.vai_tro,
        )
    ).first()
    now = datetime.utcnow()
    if row:
        row.ma_nv = payload.ma_nv
        row.active = payload.active
        row.updated_at = now
    else:
        row = PcccPhanCong(
            don_vi_id=payload.don_vi_id,
            bo_phan_id=payload.bo_phan_id,
            ma_nv=payload.ma_nv,
            vai_tro=payload.vai_tro,
            active=payload.active,
            created_by=current_employee_code(user),
            created_at=now,
            updated_at=now,
        )
    session.add(row)
    session.commit()
    session.refresh(row)
    return PcccAssignmentOut(
        id=int(row.id),
        don_vi_id=row.don_vi_id,
        bo_phan_id=row.bo_phan_id,
        bo_phan_ten=department.ten if department else "",
        ma_nv=row.ma_nv,
        ho_ten=employee.ho_ten,
        vai_tro=row.vai_tro,
        active=row.active,
    )


@router.get("/drills", response_model=list[PcccDrillOut])
def list_drills(
    request: Request,
    active_only: bool = Query(default=False),
    session: Session = Depends(get_session),
) -> list[PcccDrillOut]:
    query = select(PcccDotDienTap)
    if active_only or not is_admin(_current_user(request)):
        query = query.where(PcccDotDienTap.trang_thai != "DA_KET_THUC")
    rows = session.exec(query.order_by(PcccDotDienTap.ngay_dien_tap.desc(), PcccDotDienTap.id.desc())).all()
    return [drill_out(row) for row in rows]


@router.get("/drills/active", response_model=PcccDrillOut | None)
def active_drill(session: Session = Depends(get_session)) -> PcccDrillOut | None:
    rows = session.exec(
        select(PcccDotDienTap)
        .where(PcccDotDienTap.trang_thai != "DA_KET_THUC")
        .order_by(PcccDotDienTap.ngay_dien_tap.desc(), PcccDotDienTap.id.desc())
    ).all()
    priority = {"DANG_KIEM_DEM": 0, "MO_SI_SO": 1, "NHAP": 2}
    row = min(
        rows,
        key=lambda item: (priority.get(item.trang_thai, 9), -item.ngay_dien_tap.toordinal(), -int(item.id)),
        default=None,
    )
    return drill_out(row) if row else None


@router.post("/drills", response_model=PcccDrillOut, status_code=201)
def create_drill(
    request: Request,
    payload: PcccDrillCreate,
    session: Session = Depends(get_session),
) -> PcccDrillOut:
    user = _require_admin(request)
    row = PcccDotDienTap(
        ten=payload.ten.strip(),
        ngay_dien_tap=payload.ngay_dien_tap,
        bat_dau_du_kien=payload.bat_dau_du_kien,
        ghi_chu=payload.ghi_chu.strip(),
        created_by=current_employee_code(user),
    )
    session.add(row)
    session.flush()
    ensure_drill_records(session, int(row.id))
    session.commit()
    session.refresh(row)
    return drill_out(row)


@router.patch("/drills/{drill_id}/status", response_model=PcccDrillOut)
def update_drill_status(
    drill_id: int,
    request: Request,
    payload: PcccDrillStatusUpdate,
    session: Session = Depends(get_session),
) -> PcccDrillOut:
    user = _require_admin(request)
    drill = _get_drill(session, drill_id)
    transitions = {
        "NHAP": {"MO_SI_SO", "DA_KET_THUC"},
        "MO_SI_SO": {"DANG_KIEM_DEM", "DA_KET_THUC"},
        "DANG_KIEM_DEM": {"DA_KET_THUC"},
        "DA_KET_THUC": set(),
    }
    if payload.trang_thai == drill.trang_thai:
        return drill_out(drill)
    if payload.trang_thai not in transitions.get(drill.trang_thai, set()):
        raise HTTPException(
            status_code=409,
            detail=f"Không thể chuyển trạng thái từ {drill.trang_thai} sang {payload.trang_thai}",
        )
    if payload.trang_thai == "MO_SI_SO":
        other_operational = session.exec(
            select(PcccDotDienTap).where(
                PcccDotDienTap.id != drill_id,
                PcccDotDienTap.trang_thai.in_(["MO_SI_SO", "DANG_KIEM_DEM"]),
            )
        ).first()
        if other_operational:
            raise HTTPException(
                status_code=409,
                detail=f"Đợt '{other_operational.ten}' đang hoạt động; hãy kết thúc đợt đó trước",
            )
    now = datetime.utcnow()
    old_status = drill.trang_thai
    drill.trang_thai = payload.trang_thai
    drill.updated_at = now
    if payload.trang_thai == "DANG_KIEM_DEM":
        drill.bat_dau_thuc_te = now
    elif payload.trang_thai == "DA_KET_THUC":
        drill.ket_thuc_thuc_te = now
    ensure_drill_records(session, drill_id)
    session.add(drill)
    session.add(
        PcccLichSu(
            dot_id=drill_id,
            don_vi_id=None,
            hanh_dong="DOI_TRANG_THAI_DOT",
            du_lieu_cu=old_status,
            du_lieu_moi=payload.trang_thai,
            changed_by=current_employee_code(user),
        )
    )
    session.commit()
    session.refresh(drill)
    return drill_out(drill)


@router.get("/drills/{drill_id}/units/{unit_id}", response_model=PcccUnitDetailOut)
def unit_detail(
    drill_id: int,
    unit_id: int,
    request: Request,
    session: Session = Depends(get_session),
) -> PcccUnitDetailOut:
    drill = _get_drill(session, drill_id)
    user = _current_user(request)
    unit = require_unit_access(user, session, unit_id)
    department_ids = visible_department_ids(user, session, unit_id)
    if department_ids is not None and not department_ids:
        raise HTTPException(status_code=403, detail="Không có bộ phận PCCC phù hợp với hồ sơ nhân sự")
    detail = build_unit_detail(session, drill, unit, department_ids=department_ids)
    session.commit()
    return detail


@router.put("/drills/{drill_id}/units/{unit_id}/baseline", response_model=PcccUnitDetailOut)
def update_baseline(
    drill_id: int,
    unit_id: int,
    request: Request,
    payload: PcccBulkCountUpdate,
    session: Session = Depends(get_session),
) -> PcccUnitDetailOut:
    user = _current_user(request)
    drill = _get_drill(session, drill_id)
    if drill.trang_thai not in {"MO_SI_SO", "DANG_KIEM_DEM"}:
        raise HTTPException(status_code=409, detail="Đợt diễn tập chưa mở báo số")
    unit = require_unit_access(user, session, unit_id)
    department_ids = visible_department_ids(user, session, unit_id)
    if department_ids is not None and not department_ids:
        raise HTTPException(status_code=403, detail="Không có bộ phận PCCC phù hợp với hồ sơ nhân sự")
    save_counts(
        session,
        drill,
        unit,
        payload.items,
        current_employee_code(user),
        mode="baseline",
        allowed_department_ids=department_ids,
    )
    return build_unit_detail(session, drill, unit, department_ids=department_ids)


@router.put("/drills/{drill_id}/units/{unit_id}/actual", response_model=PcccUnitDetailOut)
def update_actual_count(
    drill_id: int,
    unit_id: int,
    request: Request,
    payload: PcccBulkCountUpdate,
    session: Session = Depends(get_session),
) -> PcccUnitDetailOut:
    user = _current_user(request)
    drill = _get_drill(session, drill_id)
    if drill.trang_thai not in {"MO_SI_SO", "DANG_KIEM_DEM"}:
        raise HTTPException(status_code=409, detail="Đợt diễn tập chưa mở báo số")
    if drill.bat_dau_thuc_te is None:
        drill.bat_dau_thuc_te = datetime.utcnow()
        session.add(drill)
    unit = require_unit_access(user, session, unit_id)
    department_ids = visible_department_ids(user, session, unit_id)
    if department_ids is not None and not department_ids:
        raise HTTPException(status_code=403, detail="Không có bộ phận PCCC phù hợp với hồ sơ nhân sự")
    save_counts(
        session,
        drill,
        unit,
        payload.items,
        current_employee_code(user),
        mode="actual",
        allowed_department_ids=department_ids,
    )
    return build_unit_detail(session, drill, unit, department_ids=department_ids)


@router.post("/drills/{drill_id}/units/{unit_id}/confirm")
def confirm_unit_result(
    drill_id: int,
    unit_id: int,
    request: Request,
    payload: PcccUnitConfirm,
    session: Session = Depends(get_session),
) -> dict:
    user = _current_user(request)
    drill = _get_drill(session, drill_id)
    if drill.trang_thai not in {"MO_SI_SO", "DANG_KIEM_DEM"}:
        raise HTTPException(status_code=409, detail="Đợt diễn tập chưa mở báo số")
    unit = require_unit_access(user, session, unit_id)
    department_ids = visible_department_ids(user, session, unit_id)
    if department_ids is not None and not department_ids:
        raise HTTPException(status_code=403, detail="Không có bộ phận PCCC phù hợp với hồ sơ nhân sự")
    confirmation = confirm_unit(
        session,
        drill,
        unit,
        current_employee_code(user),
        payload.ghi_chu,
        department_ids=department_ids,
    )
    detail = build_unit_detail(session, drill, unit, department_ids=department_ids)
    return {
        "ok": True,
        "id": confirmation.id if confirmation else None,
        "xac_nhan_at": detail.xac_nhan_at,
    }


@router.post("/drills/{drill_id}/units/{unit_id}/reopen")
def reopen_unit_result(
    drill_id: int,
    unit_id: int,
    request: Request,
    session: Session = Depends(get_session),
) -> dict:
    user = _require_admin(request)
    drill = _get_drill(session, drill_id)
    unit = session.get(PcccDonVi, unit_id)
    if not unit:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn vị PCCC")
    confirmation = session.exec(
        select(PcccXacNhanDonVi).where(
            PcccXacNhanDonVi.dot_id == drill_id,
            PcccXacNhanDonVi.don_vi_id == unit_id,
        )
    ).first()
    if not confirmation:
        reopened = False
    else:
        session.delete(confirmation)
        reopened = True
    department_ids = session.exec(
        select(PcccBoPhan.id).where(PcccBoPhan.don_vi_id == unit_id)
    ).all()
    rows = session.exec(
        select(PcccKiemDem).where(
            PcccKiemDem.dot_id == drill_id,
            PcccKiemDem.bo_phan_id.in_(department_ids),
        )
    ).all()
    for row in rows:
        if row.xac_nhan_at is not None:
            reopened = True
        row.xac_nhan_by = ""
        row.xac_nhan_at = None
        session.add(row)
    session.add(
        PcccLichSu(
            dot_id=drill_id,
            don_vi_id=unit_id,
            hanh_dong="MO_LAI_DON_VI",
            du_lieu_cu="DA_XAC_NHAN",
            du_lieu_moi="DANG_KIEM_DEM",
            changed_by=current_employee_code(user),
        )
    )
    session.commit()
    return {"ok": True, "reopened": reopened}


@router.get("/drills/{drill_id}/overview", response_model=PcccOverviewOut)
def drill_overview(
    drill_id: int,
    request: Request,
    session: Session = Depends(get_session),
) -> PcccOverviewOut:
    _require_admin(request)
    drill = _get_drill(session, drill_id)
    overview = build_overview(session, drill)
    session.commit()
    return overview
