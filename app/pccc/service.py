from __future__ import annotations

import json
import unicodedata
from datetime import datetime

from fastapi import HTTPException
from sqlmodel import Session, select

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
    PcccCountItem,
    PcccCountRecordOut,
    PcccDepartmentOut,
    PcccDrillOut,
    PcccOverviewOut,
    PcccOverviewUnitOut,
    PcccUnitDetailOut,
    PcccUnitOut,
)


UNIT_ALIASES = {
    "ban thiết bị may": {"btbm", "ban thiết bị may", "ban thiet bi may"},
    "lab": {"lab", "phòng lab", "phong lab"},
    "phòng kdxnk": {"p.kdxnk", "kdxnk", "phòng kdxnk", "phong kdxnk"},
    "phòng ktcđ đt&mt": {"p.ktcd", "ktcđ", "ktcd", "phòng ktcđ đt&mt", "phong ktcd dt&mt"},
    "phòng kế toán": {"p.kt", "phòng kế toán", "phong ke toan"},
    "phòng kỹ thuật công nghệ": {"p.ktcn", "phòng kỹ thuật công nghệ", "phong ky thuat cong nghe"},
    "phòng quản lý chất lượng": {"p.qlcl", "phòng quản lý chất lượng", "phong quan ly chat luong"},
    "phòng quản trị đời sống": {"p.qtds", "phòng quản trị đời sống", "phong quan tri doi song"},
    "phòng tổng hợp": {"p.th", "phòng tổng hợp", "phong tong hop"},
    "trạm y tế": {"tyt", "trạm y tế", "tram y te", "y tế", "y te"},
    "xnm1-v1": {"xnm1-v1", "xn1-v1"},
    "xnm2": {"xnm2", "xn2"},
    "xnm3": {"xnm3", "xn3"},
    "xnv2": {"xnv2", "xn v2"},
}


def normalize_text(value: object) -> str:
    return " ".join(str(value or "").strip().lower().split())


def normalize_key(value: object) -> str:
    raw = normalize_text(value).replace("đ", "d")
    return "".join(ch for ch in unicodedata.normalize("NFD", raw) if unicodedata.category(ch) != "Mn")


def is_admin(user: dict | None) -> bool:
    return normalize_text((user or {}).get("role")) == "admin"


def current_employee_code(user: dict | None) -> str:
    return str((user or {}).get("ma_nv") or "").strip().upper()


def organization_unit_ids(user: dict | None, session: Session) -> set[int]:
    ids: set[int] = set()

    raw_unit = normalize_text((user or {}).get("don_vi"))
    if raw_unit:
        units = session.exec(select(PcccDonVi).where(PcccDonVi.active == True)).all()  # noqa: E712
        for unit in units:
            names = {normalize_text(unit.ma), normalize_text(unit.ten)}
            names.update(UNIT_ALIASES.get(normalize_text(unit.ten), set()))
            if raw_unit in names:
                ids.add(int(unit.id))
    return ids


def visible_unit_ids(user: dict | None, session: Session) -> set[int] | None:
    if is_admin(user):
        return None
    assigned = set(session.exec(select(PcccPhanCong.don_vi_id).where(
        PcccPhanCong.ma_nv == current_employee_code(user),
        PcccPhanCong.active == True,
    )).all())
    return organization_unit_ids(user, session) & assigned


def visible_department_ids(
    user: dict | None,
    session: Session,
    unit_id: int,
) -> set[int] | None:
    if is_admin(user):
        return None
    allowed_units = visible_unit_ids(user, session)
    if allowed_units is None or unit_id not in allowed_units:
        return set()
    departments = session.exec(
        select(PcccBoPhan).where(
            PcccBoPhan.don_vi_id == unit_id,
            PcccBoPhan.active == True,  # noqa: E712
        )
    ).all()
    assignments = session.exec(select(PcccPhanCong).where(
        PcccPhanCong.ma_nv == current_employee_code(user),
        PcccPhanCong.don_vi_id == unit_id,
        PcccPhanCong.active == True,  # noqa: E712
    )).all()
    assigned_department_ids = {
        int(row.bo_phan_id) for row in assignments if row.bo_phan_id is not None
    }
    if assigned_department_ids:
        active_department_ids = {int(row.id) for row in departments}
        return assigned_department_ids & active_department_ids
    if not any(row.bo_phan_id is None for row in assignments):
        return set()
    raw_department = normalize_key((user or {}).get("bo_phan"))
    if not raw_department:
        return {int(row.id) for row in departments}

    matches: set[int] = set()
    for department in departments:
        candidates = {normalize_key(department.ma), normalize_key(department.ten)}
        if any(
            raw_department == candidate
            or (len(raw_department) >= 4 and raw_department in candidate)
            or (len(candidate) >= 4 and candidate in raw_department)
            for candidate in candidates
            if candidate
        ):
            matches.add(int(department.id))
    if matches:
        return matches
    # Một số phòng chỉ có một dòng kiểm đếm nhưng hồ sơ nhân sự lưu tên nhóm nội bộ.
    if len(departments) == 1:
        return {int(departments[0].id)}
    return set()


def require_unit_access(user: dict | None, session: Session, unit_id: int) -> PcccDonVi:
    unit = session.get(PcccDonVi, unit_id)
    if not unit or not unit.active:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn vị PCCC")
    allowed = visible_unit_ids(user, session)
    if allowed is not None and unit_id not in allowed:
        raise HTTPException(status_code=403, detail="Bạn không được phân công khai báo cho đơn vị này")
    return unit


def ensure_drill_records(session: Session, drill_id: int) -> None:
    existing_ids = set(
        session.exec(select(PcccKiemDem.bo_phan_id).where(PcccKiemDem.dot_id == drill_id)).all()
    )
    departments = session.exec(select(PcccBoPhan).where(PcccBoPhan.active == True)).all()  # noqa: E712
    for department in departments:
        if department.id not in existing_ids:
            session.add(PcccKiemDem(dot_id=drill_id, bo_phan_id=int(department.id)))
    session.flush()


def drill_out(row: PcccDotDienTap) -> PcccDrillOut:
    return PcccDrillOut(
        id=int(row.id),
        ten=row.ten,
        ngay_dien_tap=row.ngay_dien_tap,
        bat_dau_du_kien=row.bat_dau_du_kien,
        bat_dau_thuc_te=row.bat_dau_thuc_te,
        ket_thuc_thuc_te=row.ket_thuc_thuc_te,
        trang_thai=row.trang_thai,
        ghi_chu=row.ghi_chu,
        created_by=row.created_by,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _unit_out(unit: PcccDonVi, departments: list[PcccBoPhan]) -> PcccUnitOut:
    return PcccUnitOut(
        id=int(unit.id),
        ma=unit.ma,
        ten=unit.ten,
        thu_tu=unit.thu_tu,
        bo_phans=[
            PcccDepartmentOut(
                id=int(dep.id),
                ma=dep.ma,
                ten=dep.ten,
                si_so_tham_khao=dep.si_so_tham_khao,
                thu_tu=dep.thu_tu,
            )
            for dep in departments
        ],
    )


def _difference(record: PcccKiemDem) -> int | None:
    if record.si_so_dau_ngay is None or record.thuc_te_kiem_dem is None:
        return None
    return record.thuc_te_kiem_dem - record.si_so_dau_ngay


def _record_status(record: PcccKiemDem) -> str:
    difference = _difference(record)
    if record.si_so_dau_ngay is None:
        return "CHUA_KHAI_BAO"
    if record.thuc_te_kiem_dem is None:
        return "DA_CO_SI_SO"
    if difference == 0:
        return "DU"
    return "DU_THUA" if difference and difference > 0 else "THIEU"


def build_unit_detail(
    session: Session,
    drill: PcccDotDienTap,
    unit: PcccDonVi,
    department_ids: set[int] | None = None,
) -> PcccUnitDetailOut:
    ensure_drill_records(session, int(drill.id))
    departments = session.exec(
        select(PcccBoPhan)
        .where(PcccBoPhan.don_vi_id == unit.id, PcccBoPhan.active == True)  # noqa: E712
        .order_by(PcccBoPhan.thu_tu, PcccBoPhan.id)
    ).all()
    if department_ids is not None:
        departments = [row for row in departments if int(row.id) in department_ids]
    dep_ids = [int(dep.id) for dep in departments]
    rows = session.exec(
        select(PcccKiemDem).where(
            PcccKiemDem.dot_id == drill.id,
            PcccKiemDem.bo_phan_id.in_(dep_ids),
        )
    ).all() if dep_ids else []
    records_by_department = {row.bo_phan_id: row for row in rows}
    record_outputs: list[PcccCountRecordOut] = []
    for department in departments:
        row = records_by_department[int(department.id)]
        record_outputs.append(
            PcccCountRecordOut(
                id=int(row.id),
                bo_phan_id=int(department.id),
                bo_phan_ma=department.ma,
                bo_phan_ten=department.ten,
                si_so_tham_khao=department.si_so_tham_khao,
                si_so_dau_ngay=row.si_so_dau_ngay,
                thuc_te_kiem_dem=row.thuc_te_kiem_dem,
                chenh_lech=_difference(row),
                trang_thai=_record_status(row),
                ma_ly_do=row.ma_ly_do,
                ly_do_chi_tiet=row.ly_do_chi_tiet,
                si_so_updated_at=row.si_so_updated_at,
                kiem_dem_updated_at=row.kiem_dem_updated_at,
                da_xac_nhan=row.xac_nhan_at is not None,
                xac_nhan_at=row.xac_nhan_at,
            )
        )
    baseline_total = sum(item.si_so_dau_ngay or 0 for item in record_outputs)
    actual_total = sum(item.thuc_te_kiem_dem or 0 for item in record_outputs)
    difference_total = sum(
        item.chenh_lech or 0
        for item in record_outputs
        if item.chenh_lech is not None
    )
    completed = sum(item.thuc_te_kiem_dem is not None for item in record_outputs)
    return PcccUnitDetailOut(
        dot=drill_out(drill),
        don_vi=_unit_out(unit, departments),
        records=record_outputs,
        tong_si_so_dau_ngay=baseline_total,
        tong_thuc_te_kiem_dem=actual_total,
        # Chỉ cộng các dòng đã kiểm đếm để tránh hiển thị "thiếu" giả trong
        # giai đoạn mới khai báo sĩ số hoặc khi người dùng đang nhập dở.
        chenh_lech=difference_total,
        completed_count=completed,
        department_count=len(record_outputs),
        da_xac_nhan=bool(record_outputs) and all(item.da_xac_nhan for item in record_outputs),
        xac_nhan_at=max(
            (item.xac_nhan_at for item in record_outputs if item.xac_nhan_at is not None),
            default=None,
        ),
    )


def _validate_items_for_unit(
    session: Session,
    unit_id: int,
    items: list[PcccCountItem],
    allowed_department_ids: set[int] | None = None,
) -> tuple[dict[int, PcccBoPhan], dict[int, PcccCountItem]]:
    item_map = {item.bo_phan_id: item for item in items}
    if len(item_map) != len(items):
        raise HTTPException(status_code=422, detail="Danh sách có bộ phận bị lặp")
    departments = session.exec(
        select(PcccBoPhan).where(
            PcccBoPhan.don_vi_id == unit_id,
            PcccBoPhan.active == True,  # noqa: E712
        )
    ).all()
    department_map = {int(dep.id): dep for dep in departments}
    invalid = set(item_map) - set(department_map)
    if invalid:
        raise HTTPException(status_code=422, detail="Có bộ phận không thuộc đơn vị được khai báo")
    if allowed_department_ids is not None and not set(item_map).issubset(allowed_department_ids):
        raise HTTPException(status_code=403, detail="Bạn chỉ được khai báo các bộ phận thuộc phạm vi của mình")
    return department_map, item_map


def save_counts(
    session: Session,
    drill: PcccDotDienTap,
    unit: PcccDonVi,
    items: list[PcccCountItem],
    actor: str,
    *,
    mode: str,
    allowed_department_ids: set[int] | None = None,
) -> None:
    _, item_map = _validate_items_for_unit(
        session,
        int(unit.id),
        items,
        allowed_department_ids=allowed_department_ids,
    )
    ensure_drill_records(session, int(drill.id))
    now = datetime.utcnow()
    rows = session.exec(
        select(PcccKiemDem).where(
            PcccKiemDem.dot_id == drill.id,
            PcccKiemDem.bo_phan_id.in_(list(item_map)),
        )
    ).all()
    for row in rows:
        if row.xac_nhan_at is not None and mode != "baseline":
            raise HTTPException(
                status_code=409,
                detail="Kết quả bộ phận đã được xác nhận; admin cần mở lại trước khi sửa",
            )
        item = item_map[row.bo_phan_id]
        if mode == "baseline":
            old = row.si_so_dau_ngay
            row.si_so_dau_ngay = item.so_luong
            row.si_so_updated_by = actor
            row.si_so_updated_at = now
            action = "CAP_NHAT_SI_SO"
            new = row.si_so_dau_ngay
        else:
            if row.si_so_dau_ngay is None:
                raise HTTPException(
                    status_code=422,
                    detail="Phải khai báo sĩ số đầu ngày trước khi nhập thực tế kiểm đếm",
                )
            old = {
                "thuc_te_kiem_dem": row.thuc_te_kiem_dem,
                "ma_ly_do": row.ma_ly_do,
                "ly_do_chi_tiet": row.ly_do_chi_tiet,
            }
            row.thuc_te_kiem_dem = item.so_luong
            row.ma_ly_do = item.ma_ly_do.strip().upper()
            row.ly_do_chi_tiet = item.ly_do_chi_tiet.strip()
            row.kiem_dem_updated_by = actor
            row.kiem_dem_updated_at = now
            action = "CAP_NHAT_KIEM_DEM"
            new = {
                "thuc_te_kiem_dem": row.thuc_te_kiem_dem,
                "ma_ly_do": row.ma_ly_do,
                "ly_do_chi_tiet": row.ly_do_chi_tiet,
            }
        row.updated_at = now
        session.add(row)
        session.flush()
        if old != new:
            session.add(
                PcccLichSu(
                    dot_id=int(drill.id),
                    kiem_dem_id=int(row.id),
                    don_vi_id=int(unit.id),
                    hanh_dong=action,
                    du_lieu_cu=json.dumps(old, ensure_ascii=False),
                    du_lieu_moi=json.dumps(new, ensure_ascii=False),
                    changed_by=actor,
                    changed_at=now,
                )
            )
    drill.updated_at = now
    session.add(drill)
    session.commit()


def confirm_unit(
    session: Session,
    drill: PcccDotDienTap,
    unit: PcccDonVi,
    actor: str,
    note: str,
    department_ids: set[int] | None = None,
) -> PcccXacNhanDonVi | None:
    detail = build_unit_detail(session, drill, unit, department_ids=department_ids)
    if not detail.records:
        raise HTTPException(status_code=422, detail="Đơn vị chưa có bộ phận để kiểm đếm")
    missing = [item.bo_phan_ten for item in detail.records if item.thuc_te_kiem_dem is None]
    if missing:
        raise HTTPException(status_code=422, detail=f"Chưa kiểm đếm đủ bộ phận: {', '.join(missing)}")
    no_baseline = [item.bo_phan_ten for item in detail.records if item.si_so_dau_ngay is None]
    if no_baseline:
        raise HTTPException(status_code=422, detail=f"Chưa có sĩ số đầu ngày: {', '.join(no_baseline)}")
    unexplained = [
        item.bo_phan_ten
        for item in detail.records
        if item.chenh_lech not in (None, 0) and not item.ly_do_chi_tiet.strip()
    ]
    if unexplained:
        raise HTTPException(status_code=422, detail=f"Chưa giải trình chênh lệch: {', '.join(unexplained)}")
    now = datetime.utcnow()
    target_ids = [item.id for item in detail.records]
    target_rows = session.exec(
        select(PcccKiemDem).where(PcccKiemDem.id.in_(target_ids))
    ).all()
    for row in target_rows:
        row.xac_nhan_by = actor
        row.xac_nhan_at = now
        row.updated_at = now
        session.add(row)

    all_department_ids = set(
        session.exec(
            select(PcccBoPhan.id).where(
                PcccBoPhan.don_vi_id == unit.id,
                PcccBoPhan.active == True,  # noqa: E712
            )
        ).all()
    )
    all_rows = session.exec(
        select(PcccKiemDem).where(
            PcccKiemDem.dot_id == drill.id,
            PcccKiemDem.bo_phan_id.in_(list(all_department_ids)),
        )
    ).all()
    confirmed_ids = {
        row.bo_phan_id
        for row in all_rows
        if row.xac_nhan_at is not None or int(row.id) in target_ids
    }
    confirmation = session.exec(
        select(PcccXacNhanDonVi).where(
            PcccXacNhanDonVi.dot_id == drill.id,
            PcccXacNhanDonVi.don_vi_id == unit.id,
        )
    ).first()
    if all_department_ids and confirmed_ids == all_department_ids and not confirmation:
        confirmation = PcccXacNhanDonVi(
            dot_id=int(drill.id),
            don_vi_id=int(unit.id),
            xac_nhan_by=actor,
            xac_nhan_at=now,
            ghi_chu=note.strip(),
        )
        session.add(confirmation)
    session.add(
        PcccLichSu(
            dot_id=int(drill.id),
            don_vi_id=int(unit.id),
            hanh_dong="XAC_NHAN_DON_VI",
            du_lieu_moi=json.dumps({"ghi_chu": note.strip()}, ensure_ascii=False),
            changed_by=actor,
        )
    )
    session.commit()
    if confirmation:
        session.refresh(confirmation)
    return confirmation


def build_overview(session: Session, drill: PcccDotDienTap) -> PcccOverviewOut:
    units = session.exec(
        select(PcccDonVi).where(PcccDonVi.active == True).order_by(PcccDonVi.thu_tu, PcccDonVi.id)  # noqa: E712
    ).all()
    outputs: list[PcccOverviewUnitOut] = []
    for unit in units:
        detail = build_unit_detail(session, drill, unit)
        if detail.da_xac_nhan:
            status = "DA_XAC_NHAN"
        elif detail.completed_count == detail.department_count and detail.department_count:
            status = "DU" if detail.chenh_lech == 0 else "CO_CHENH_LECH"
        elif detail.completed_count:
            status = "DANG_KIEM_DEM"
        elif any(item.si_so_dau_ngay is not None for item in detail.records):
            status = "DA_CO_SI_SO"
        else:
            status = "CHUA_KHAI_BAO"
        outputs.append(
            PcccOverviewUnitOut(
                don_vi_id=int(unit.id),
                don_vi_ten=unit.ten,
                tong_si_so_dau_ngay=detail.tong_si_so_dau_ngay,
                tong_thuc_te_kiem_dem=detail.tong_thuc_te_kiem_dem,
                chenh_lech=detail.chenh_lech,
                completed_count=detail.completed_count,
                department_count=detail.department_count,
                trang_thai=status,
                xac_nhan_at=detail.xac_nhan_at,
            )
        )
    return PcccOverviewOut(
        dot=drill_out(drill),
        units=outputs,
        tong_si_so_dau_ngay=sum(item.tong_si_so_dau_ngay for item in outputs),
        tong_thuc_te_kiem_dem=sum(item.tong_thuc_te_kiem_dem for item in outputs),
        chenh_lech=sum(item.chenh_lech for item in outputs),
        unit_confirmed_count=sum(item.trang_thai == "DA_XAC_NHAN" for item in outputs),
        unit_count=len(outputs),
    )
