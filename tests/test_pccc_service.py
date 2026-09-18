from __future__ import annotations

import unittest
from datetime import date

from fastapi import HTTPException
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

from app.pccc.models import (
    PcccBoPhan,
    PcccDonVi,
    PcccDotDienTap,
    PcccKiemDem,
    PcccLichSu,
    PcccPhanCong,
    PcccXacNhanDonVi,
)
from app.pccc.schemas import PcccCountItem
from app.pccc.service import (
    build_overview,
    build_unit_detail,
    confirm_unit,
    ensure_drill_records,
    save_counts,
    visible_department_ids,
    visible_unit_ids,
)


class PcccServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        tables = [
            PcccDonVi.__table__,
            PcccBoPhan.__table__,
            PcccDotDienTap.__table__,
            PcccPhanCong.__table__,
            PcccKiemDem.__table__,
            PcccXacNhanDonVi.__table__,
            PcccLichSu.__table__,
        ]
        SQLModel.metadata.create_all(self.engine, tables=tables)

    def test_counting_flow_and_confirmation_rules(self) -> None:
        with Session(self.engine) as session:
            unit = PcccDonVi(ma="TEST", ten="Đơn vị test", thu_tu=1)
            session.add(unit)
            session.flush()
            dep_a = PcccBoPhan(don_vi_id=unit.id, ma="A", ten="Bộ phận A", thu_tu=1)
            dep_b = PcccBoPhan(don_vi_id=unit.id, ma="B", ten="Bộ phận B", thu_tu=2)
            drill = PcccDotDienTap(
                ten="Diễn tập test",
                ngay_dien_tap=date(2026, 9, 18),
                trang_thai="DANG_KIEM_DEM",
            )
            session.add(dep_a)
            session.add(dep_b)
            session.add(drill)
            session.commit()
            session.refresh(unit)
            session.refresh(dep_a)
            session.refresh(dep_b)
            session.refresh(drill)

            ensure_drill_records(session, drill.id)
            session.commit()
            self.assertEqual(
                len(session.exec(select(PcccKiemDem).where(PcccKiemDem.dot_id == drill.id)).all()),
                2,
            )

            save_counts(
                session,
                drill,
                unit,
                [
                    PcccCountItem(bo_phan_id=dep_a.id, so_luong=10),
                    PcccCountItem(bo_phan_id=dep_b.id, so_luong=8),
                ],
                "NV01",
                mode="baseline",
            )
            self.assertEqual(build_unit_detail(session, drill, unit).chenh_lech, 0)
            save_counts(
                session,
                drill,
                unit,
                [
                    PcccCountItem(bo_phan_id=dep_a.id, so_luong=10),
                    PcccCountItem(bo_phan_id=dep_b.id, so_luong=7),
                ],
                "NV01",
                mode="actual",
            )

            with self.assertRaises(HTTPException) as ctx:
                confirm_unit(session, drill, unit, "NV01", "")
            self.assertEqual(ctx.exception.status_code, 422)

            save_counts(
                session,
                drill,
                unit,
                [
                    PcccCountItem(
                        bo_phan_id=dep_b.id,
                        so_luong=7,
                        ly_do_chi_tiet="01 người làm nhiệm vụ PCCC",
                    )
                ],
                "NV01",
                mode="actual",
            )
            confirmation = confirm_unit(session, drill, unit, "NV01", "Đã đối chiếu")
            self.assertIsNotNone(confirmation.id)

            detail = build_unit_detail(session, drill, unit)
            self.assertEqual(detail.tong_si_so_dau_ngay, 18)
            self.assertEqual(detail.tong_thuc_te_kiem_dem, 17)
            self.assertEqual(detail.chenh_lech, -1)
            self.assertEqual(detail.records[1].ma_ly_do, "")
            self.assertTrue(detail.da_xac_nhan)

            overview = build_overview(session, drill)
            self.assertEqual(overview.unit_confirmed_count, 1)
            self.assertEqual(overview.units[0].trang_thai, "DA_XAC_NHAN")

    def test_organization_profile_limits_unit_and_department(self) -> None:
        with Session(self.engine) as session:
            assigned = PcccDonVi(ma="XNM2", ten="XNM2", thu_tu=1)
            named = PcccDonVi(ma="P.TH", ten="Phòng Tổng hợp", thu_tu=2)
            session.add(assigned)
            session.add(named)
            session.flush()
            session.add(PcccPhanCong(don_vi_id=assigned.id, ma_nv="NV01", vai_tro="CHINH"))
            session.commit()

            allowed = visible_unit_ids(
                {"ma_nv": "NV01", "don_vi": "Phòng Tổng hợp", "role": "user"},
                session,
            )
            self.assertEqual(allowed, set())
            matching_assignment = PcccPhanCong(don_vi_id=named.id, ma_nv="NV01", vai_tro="CHINH")
            session.add(matching_assignment)
            session.commit()
            profile = {"ma_nv": "NV01", "don_vi": "Phòng Tổng hợp", "role": "user"}
            self.assertEqual(visible_unit_ids(profile, session), {named.id})
            matching_assignment.active = False
            session.add(matching_assignment)
            session.commit()
            self.assertEqual(visible_unit_ids(profile, session), set())
            self.assertEqual(visible_unit_ids({**profile, "ma_nv": "UNASSIGNED"}, session), set())
            self.assertIsNone(visible_unit_ids({"role": "admin"}, session))

            scoped_unit = PcccDonVi(ma="P.KDXNK", ten="Phòng KDXNK", thu_tu=3)
            session.add(scoped_unit)
            session.flush()
            kho_nl = PcccBoPhan(
                don_vi_id=scoped_unit.id,
                ma="KHO_NL",
                ten="KHO NGUYÊN LIỆU, XÉN VIỀN & KIỂM VẢI",
                thu_tu=1,
            )
            kho_pl = PcccBoPhan(
                don_vi_id=scoped_unit.id,
                ma="KHO_PL",
                ten="KHO PHỤ LIỆU",
                thu_tu=2,
            )
            session.add(kho_nl)
            session.add(kho_pl)
            session.add(PcccPhanCong(don_vi_id=scoped_unit.id, ma_nv="NV02", vai_tro="CHINH"))
            session.commit()
            scoped = visible_department_ids(
                {"ma_nv": "NV02", "don_vi": "P.KDXNK", "bo_phan": "Kho nguyên liệu", "role": "user"},
                session,
                scoped_unit.id,
            )
            self.assertEqual(scoped, {kho_nl.id})

    def test_lab_is_separate_and_kdxnk_can_be_assigned_by_department(self) -> None:
        with Session(self.engine) as session:
            lab_unit = PcccDonVi(ma="LAB", ten="Lab")
            kdxnk_unit = PcccDonVi(ma="P.KDXNK", ten="Phòng KDXNK")
            session.add(lab_unit)
            session.add(kdxnk_unit)
            session.flush()
            lab = PcccBoPhan(don_vi_id=lab_unit.id, ma="LAB", ten="Phòng LAB")
            office = PcccBoPhan(don_vi_id=kdxnk_unit.id, ma="VP", ten="Khối VP KDXNK")
            material = PcccBoPhan(don_vi_id=kdxnk_unit.id, ma="KHO_NL", ten="Kho nguyên liệu")
            session.add(lab)
            session.add(office)
            session.add(material)
            session.flush()
            session.add(PcccPhanCong(don_vi_id=lab_unit.id, ma_nv="LAB01"))
            session.add(PcccPhanCong(
                don_vi_id=kdxnk_unit.id,
                bo_phan_id=material.id,
                ma_nv="KHO01",
            ))
            session.commit()
            lab_profile = {"ma_nv": "LAB01", "don_vi": "Lab", "bo_phan": "", "role": "user"}
            self.assertEqual(visible_unit_ids(lab_profile, session), {lab_unit.id})
            self.assertEqual(visible_department_ids(lab_profile, session, lab_unit.id), {lab.id})
            warehouse_profile = {
                "ma_nv": "KHO01",
                "don_vi": "P.KDXNK",
                "bo_phan": "",
                "role": "user",
            }
            self.assertEqual(visible_unit_ids(warehouse_profile, session), {kdxnk_unit.id})
            self.assertEqual(
                visible_department_ids(warehouse_profile, session, kdxnk_unit.id),
                {material.id},
            )

    def test_end_preparation_and_hide_closed_drills_from_users(self) -> None:
        from starlette.requests import Request
        from app.pccc.routes import update_drill_status, list_drills, active_drill
        from app.pccc.schemas import PcccDrillStatusUpdate
        admin = Request({"type": "http", "session": {"user": {"role": "admin", "ma_nv": "ADMIN"}}})
        user = Request({"type": "http", "session": {"user": {"role": "user"}}})
        with Session(self.engine) as session:
            for status in ("NHAP", "MO_SI_SO", "DANG_KIEM_DEM"):
                drill = PcccDotDienTap(ten=status, trang_thai=status)
                session.add(drill)
                session.commit()
                result = update_drill_status(drill.id, admin, PcccDrillStatusUpdate(trang_thai="DA_KET_THUC"), session)
                self.assertEqual(result.trang_thai, "DA_KET_THUC")
                self.assertIsNotNone(result.ket_thuc_thuc_te)
            self.assertEqual(list_drills(user, False, session), [])
            self.assertEqual(len(list_drills(admin, False, session)), 3)
            self.assertIsNone(active_drill(session))

    def test_new_drill_is_open_for_reporting_immediately(self) -> None:
        from starlette.requests import Request

        from app.pccc.routes import create_drill
        from app.pccc.schemas import PcccDrillCreate

        admin = Request({"type": "http", "session": {"user": {"role": "admin", "ma_nv": "ADMIN"}}})
        with Session(self.engine) as session:
            unit = PcccDonVi(ma="TEST", ten="Đơn vị test")
            session.add(unit)
            session.flush()
            session.add(PcccBoPhan(don_vi_id=unit.id, ma="BP", ten="Bộ phận test"))
            session.commit()

            result = create_drill(
                admin,
                PcccDrillCreate(ten="Đợt báo ngay", ngay_dien_tap=date(2026, 9, 18)),
                session,
            )

            self.assertEqual(result.trang_thai, "MO_SI_SO")
            records = session.exec(select(PcccKiemDem).where(PcccKiemDem.dot_id == result.id)).all()
            self.assertEqual(len(records), 1)

    def test_baseline_and_actual_are_available_in_same_open_status(self) -> None:
        from starlette.requests import Request
        from app.pccc.routes import confirm_unit_result, update_actual_count, update_baseline
        from app.pccc.schemas import PcccBulkCountUpdate, PcccUnitConfirm

        with Session(self.engine) as session:
            unit = PcccDonVi(ma="P.KDXNK", ten="Phòng KDXNK")
            session.add(unit)
            session.flush()
            department = PcccBoPhan(don_vi_id=unit.id, ma="LAB", ten="Phòng LAB")
            drill = PcccDotDienTap(ten="Đợt mở chung", trang_thai="MO_SI_SO")
            session.add(department)
            session.add(drill)
            session.add(PcccPhanCong(don_vi_id=unit.id, ma_nv="NV01", vai_tro="CHINH"))
            session.commit()
            session.refresh(unit)
            session.refresh(department)
            session.refresh(drill)
            request = Request({
                "type": "http",
                "session": {"user": {
                    "ma_nv": "NV01",
                    "don_vi": "P.KDXNK",
                    "bo_phan": "",
                    "role": "user",
                }},
            })
            baseline = PcccBulkCountUpdate(items=[PcccCountItem(bo_phan_id=department.id, so_luong=3)])
            actual = PcccBulkCountUpdate(items=[PcccCountItem(bo_phan_id=department.id, so_luong=3)])
            update_baseline(drill.id, unit.id, request, baseline, session)
            detail = update_actual_count(drill.id, unit.id, request, actual, session)
            self.assertEqual(detail.records[0].thuc_te_kiem_dem, 3)
            self.assertIsNotNone(session.get(PcccDotDienTap, drill.id).bat_dau_thuc_te)
            result = confirm_unit_result(drill.id, unit.id, request, PcccUnitConfirm(), session)
            self.assertTrue(result["ok"])

    def test_confirmation_is_scoped_to_employee_department(self) -> None:
        with Session(self.engine) as session:
            unit = PcccDonVi(ma="P.KDXNK", ten="Phong KDXNK", thu_tu=1)
            session.add(unit)
            session.flush()
            kho_nl = PcccBoPhan(don_vi_id=unit.id, ma="KHO_NL", ten="Kho nguyen lieu", thu_tu=1)
            kho_pl = PcccBoPhan(don_vi_id=unit.id, ma="KHO_PL", ten="Kho phu lieu", thu_tu=2)
            drill = PcccDotDienTap(
                ten="Dien tap phan quyen",
                ngay_dien_tap=date(2026, 9, 18),
                trang_thai="DANG_KIEM_DEM",
            )
            session.add(kho_nl)
            session.add(kho_pl)
            session.add(drill)
            session.commit()
            session.refresh(unit)
            session.refresh(kho_nl)
            session.refresh(kho_pl)
            session.refresh(drill)

            ensure_drill_records(session, drill.id)
            session.commit()
            save_counts(
                session,
                drill,
                unit,
                [PcccCountItem(bo_phan_id=kho_nl.id, so_luong=12)],
                "NV-KHO-NL",
                mode="baseline",
                allowed_department_ids={kho_nl.id},
            )
            save_counts(
                session,
                drill,
                unit,
                [PcccCountItem(bo_phan_id=kho_nl.id, so_luong=12)],
                "NV-KHO-NL",
                mode="actual",
                allowed_department_ids={kho_nl.id},
            )

            confirmation = confirm_unit(
                session,
                drill,
                unit,
                "NV-KHO-NL",
                "",
                department_ids={kho_nl.id},
            )
            self.assertIsNone(confirmation)
            scoped_detail = build_unit_detail(session, drill, unit, department_ids={kho_nl.id})
            full_detail = build_unit_detail(session, drill, unit)
            self.assertTrue(scoped_detail.da_xac_nhan)
            self.assertFalse(full_detail.da_xac_nhan)
            self.assertEqual([row.bo_phan_ma for row in scoped_detail.records], ["KHO_NL"])
            self.assertEqual(session.exec(select(PcccXacNhanDonVi)).all(), [])


if __name__ == "__main__":
    unittest.main()
