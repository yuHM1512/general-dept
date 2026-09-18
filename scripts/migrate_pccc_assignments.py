"""Create/update the approved PCCC reporting assignments.

Run after deploying the application code:
    python -m scripts.migrate_pccc_assignments

The migration is idempotent. It uses DATABASE_URL from the application environment.
"""
from __future__ import annotations

from datetime import UTC, datetime

from sqlmodel import Session, select

from app.db import create_db_and_tables, engine
from app.models import GeneralEmployee
from app.pccc.models import PcccBoPhan, PcccDonVi, PcccPhanCong


# unit code, department code, employee name, employee code, HR unit, HR department
ASSIGNMENTS = [
    ("BTBM", None, "Huỳnh Văn Toàn", "T2548", "BTBM", ""),
    ("LAB", None, "Nguyễn Thị Thuý Vân", "V0044", "Lab", ""),
    ("P.KDXNK", "VP", "Huỳnh Thị Kim Sinh", "S0106", "P.KDXNK", "KHỐI VP KDXNK"),
    (
        "P.KDXNK",
        "KHO_NL",
        "Dương Minh Quốc",
        "Q0205",
        "P.KDXNK",
        "KHO NGUYÊN LIỆU, XÉN VIỀN & KIỂM VẢI",
    ),
    ("P.KDXNK", "KHO_PL", "Lê Trọng Châu", "C0008", "P.KDXNK", "KHO PHỤ LIỆU"),
    ("P.KDXNK", "KHO_CARTON", "Huỳnh Trọng", "T0134", "P.KDXNK", "KHO CARTON & VẬT TƯ"),
    ("P.KDXNK", "KHO_TP", "Trương Hùng Đức", "Đ0238", "P.KDXNK", "KHO THÀNH PHẨM"),
    ("P.KDXNK", "BOC_VAC", "Nguyễn Quốc Vũ", "V0532", "P.KDXNK", "BỐC VÁC"),
    ("P.KTCD", None, "Phan Thị Ngọc Trâm", "T3996", "P.KTCD", ""),
    ("P.KT", None, "Nguyễn Thị Kim Liên", "L0768", "P.KT", ""),
    ("P.KTCN", None, "Nguyễn Thị Thu Sơn", "S0493", "P.KTCN", ""),
    ("P.QLCL", None, "Huỳnh Thúy Quyên", "Q0386", "P.QLCL", ""),
    ("P.QTDS", None, "Trần Thị Xuân", "X0023", "P.QTDS", ""),
    ("P.TH", None, "Dương Công Hiền", "H3644", "P.TH", ""),
    ("TYT", None, "Nguyễn Thị Thanh Giang", "G0026", "TYT", ""),
    ("XNM1-V1", None, "Đặng Thị Kim Lý", "L1847", "XN1-V1", ""),
    ("XNM2", None, "Trương Thị Hà", "H0710", "XN2", ""),
    ("XNM3", None, "Huỳnh Thị Hoài Thu", "T1729", "XN3", ""),
    ("XNV2", None, "Thái Thùy Trang", "T3787", "XNV2", ""),
]


def migrate() -> dict[str, int]:
    create_db_and_tables()
    now = datetime.now(UTC).replace(tzinfo=None)
    created_employees = 0
    created_assignments = 0
    updated_assignments = 0

    with Session(engine) as session:
        units = {
            unit.ma: unit
            for unit in session.exec(select(PcccDonVi).where(PcccDonVi.active == True)).all()  # noqa: E712
        }
        departments = {
            (unit.ma, department.ma): department
            for unit in units.values()
            for department in session.exec(select(PcccBoPhan).where(
                PcccBoPhan.don_vi_id == unit.id,
                PcccBoPhan.active == True,  # noqa: E712
            )).all()
        }

        for unit_code, department_code, full_name, employee_code, hr_unit, hr_department in ASSIGNMENTS:
            unit = units.get(unit_code)
            if unit is None:
                raise RuntimeError(f"Missing active PCCC unit: {unit_code}")
            department = departments.get((unit_code, department_code)) if department_code else None
            if department_code and department is None:
                raise RuntimeError(f"Missing PCCC department: {unit_code}/{department_code}")

            employee = session.get(GeneralEmployee, employee_code)
            if employee is None:
                employee = GeneralEmployee(
                    ma_nv=employee_code,
                    role="user",
                    station=[],
                    don_vi=hr_unit,
                    bo_phan=hr_department,
                )
                created_employees += 1
            employee.ho_ten = full_name
            if unit_code in {"LAB", "P.KDXNK"} or not employee.don_vi:
                employee.don_vi = hr_unit
                employee.bo_phan = hr_department
            session.add(employee)

            assignment = session.exec(select(PcccPhanCong).where(
                PcccPhanCong.don_vi_id == unit.id,
                PcccPhanCong.bo_phan_id == (department.id if department else None),
                PcccPhanCong.vai_tro == "CHINH",
            )).first()
            if assignment is None:
                assignment = PcccPhanCong(
                    don_vi_id=int(unit.id),
                    bo_phan_id=int(department.id) if department else None,
                    ma_nv=employee_code,
                    vai_tro="CHINH",
                    active=True,
                    created_by="ADMIN_SETUP",
                    created_at=now,
                    updated_at=now,
                )
                created_assignments += 1
            else:
                assignment.ma_nv = employee_code
                assignment.active = True
                assignment.updated_at = now
                updated_assignments += 1
            session.add(assignment)

        kdxnk = units["P.KDXNK"]
        stale_unit_assignment = session.exec(select(PcccPhanCong).where(
            PcccPhanCong.don_vi_id == kdxnk.id,
            PcccPhanCong.bo_phan_id == None,  # noqa: E711
            PcccPhanCong.active == True,  # noqa: E712
        )).first()
        if stale_unit_assignment:
            stale_unit_assignment.active = False
            stale_unit_assignment.updated_at = now
            session.add(stale_unit_assignment)

        session.commit()

    return {
        "created_employees": created_employees,
        "created_assignments": created_assignments,
        "updated_assignments": updated_assignments,
        "approved_assignments": len(ASSIGNMENTS),
    }


if __name__ == "__main__":
    print(migrate())
