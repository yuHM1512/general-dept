from __future__ import annotations

from sqlalchemy import inspect, text
from sqlmodel import Session, SQLModel, create_engine

from app.settings import settings
from app.services import classify_group
import app.audit_models as _audit_models  # noqa: F401 – registers audit tables in SQLModel.metadata

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    connect_args={"connect_timeout": settings.db_connect_timeout},
)


def create_db_and_tables() -> None:
    _apply_rename_migrations()   # rename old table names → new names BEFORE create_all
    SQLModel.metadata.create_all(engine)
    _apply_light_migrations()
    _seed_users()
    from app.audit_seed import seed_if_empty
    seed_if_empty(engine)


def _apply_rename_migrations() -> None:
    """
    Rename tables from old naming scheme to new.
    Each rename is idempotent: runs only when old name exists AND new name does not.
    """
    renames = [
        # RCP tables
        ("payrollrow",      "rcp_payrollrow"),
        ("ingestjob",       "rcp_ingestjob"),
        ("hanging_line",    "rcp_hanging_line"),
        # Audit 5S tables
        ("audit_don_vi",        "audit_5s_don_vi"),
        ("audit_bo_phan",       "audit_5s_bo_phan"),
        ("audit_linh_vuc",      "audit_5s_linh_vuc"),
        ("audit_tieu_chi",      "audit_5s_tieu_chi"),
        ("audit_ap_dung",       "audit_5s_ap_dung"),
        ("audit_dot_kiem_tra",  "audit_5s_dot_kiem_tra"),
        ("audit_phieu_kiem_tra","audit_5s_phieu_kiem_tra"),
        ("audit_chi_tiet_diem", "audit_5s_chi_tiet_diem"),
    ]
    with engine.begin() as conn:
        for old, new in renames:
            conn.execute(text(f"""
                DO $$ BEGIN
                  IF EXISTS (SELECT FROM pg_tables WHERE schemaname='public' AND tablename='{old}')
                  AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname='public' AND tablename='{new}')
                  THEN ALTER TABLE {old} RENAME TO {new};
                  END IF;
                END $$;
            """))


def _apply_light_migrations() -> None:
    """
    SQLModel's create_all() does not alter existing tables. This project uses a
    small, non-destructive migration helper so new columns can be added without
    requiring a full Alembic setup.
    """
    insp = inspect(engine)
    with engine.begin() as conn:
        if "rcp_ingestjob" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("rcp_ingestjob")}
            if "processed_rows" not in cols:
                conn.execute(
                    text(
                        "ALTER TABLE rcp_ingestjob "
                        "ADD COLUMN processed_rows INTEGER NOT NULL DEFAULT 0"
                    )
                )
        if "rcp_payrollrow" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("rcp_payrollrow")}
            if "ttbp" not in cols:
                conn.execute(text("ALTER TABLE rcp_payrollrow ADD COLUMN ttbp TEXT NOT NULL DEFAULT ''"))
            if "full_name" not in cols:
                conn.execute(text("ALTER TABLE rcp_payrollrow ADD COLUMN full_name TEXT NOT NULL DEFAULT ''"))
            if "don_vi" not in cols:
                conn.execute(text("ALTER TABLE rcp_payrollrow ADD COLUMN don_vi TEXT NOT NULL DEFAULT ''"))
                if "ttbp" in cols:
                    conn.execute(text("UPDATE rcp_payrollrow SET don_vi = ttbp WHERE don_vi = ''"))
            if "co_so" not in cols:
                conn.execute(text("ALTER TABLE rcp_payrollrow ADD COLUMN co_so TEXT NOT NULL DEFAULT ''"))
                if "ttbp" in cols:
                    conn.execute(
                        text(
                            "UPDATE rcp_payrollrow "
                            "SET co_so = CASE WHEN ttbp = 'DT' THEN 'Duy Trung' ELSE 'Mẹ Nhu' END "
                            "WHERE co_so = ''"
                        )
                    )

            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ux_payrollrow_year_month_manv "
                    "ON rcp_payrollrow(year, month, manv)"
                )
            )
            rows = conn.execute(
                text("SELECT id, department, job_title, group_name FROM rcp_payrollrow")
            ).mappings()
            updates = []
            for row in rows:
                group_name = classify_group(row["department"] or "", row["job_title"] or "")
                if (row["group_name"] or "") != group_name:
                    updates.append({"id": row["id"], "group_name": group_name})
            if updates:
                conn.execute(
                    text("UPDATE rcp_payrollrow SET group_name = :group_name WHERE id = :id"),
                    updates,
                )

        if "rcp_hanging_line" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("rcp_hanging_line")}
            if "ngay_ap_dung" not in cols:
                conn.execute(text("ALTER TABLE rcp_hanging_line ADD COLUMN ngay_ap_dung DATE NOT NULL DEFAULT CURRENT_DATE"))
            if "created_at" not in cols:
                conn.execute(text("ALTER TABLE rcp_hanging_line ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT NOW()"))
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ux_hanging_line_don_vi_department "
                    "ON rcp_hanging_line(don_vi, department)"
                )
            )

        # Add ky (period) column to audit_5s_dot_kiem_tra if missing
        if "audit_5s_dot_kiem_tra" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("audit_5s_dot_kiem_tra")}
            if "ky" not in cols:
                conn.execute(
                    text("ALTER TABLE audit_5s_dot_kiem_tra ADD COLUMN ky VARCHAR(7) NOT NULL DEFAULT ''")
                )
            if "ma_nv_nguoi_kiem_tra" in cols:
                conn.execute(
                    text(
                        "UPDATE audit_5s_dot_kiem_tra "
                        "SET nguoi_kiem_tra = ma_nv_nguoi_kiem_tra "
                        "WHERE ma_nv_nguoi_kiem_tra IS NOT NULL AND ma_nv_nguoi_kiem_tra <> ''"
                    )
                )
                conn.execute(
                    text("ALTER TABLE audit_5s_dot_kiem_tra DROP COLUMN ma_nv_nguoi_kiem_tra")
                )

        if "audit_5s_phieu_kiem_tra" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("audit_5s_phieu_kiem_tra")}
            if "ma_nv_nguoi_kiem_tra" in cols:
                conn.execute(
                    text(
                        "UPDATE audit_5s_phieu_kiem_tra "
                        "SET nguoi_kiem_tra = ma_nv_nguoi_kiem_tra "
                        "WHERE ma_nv_nguoi_kiem_tra IS NOT NULL AND ma_nv_nguoi_kiem_tra <> ''"
                    )
                )
                conn.execute(
                    text("ALTER TABLE audit_5s_phieu_kiem_tra DROP COLUMN ma_nv_nguoi_kiem_tra")
                )

        # Backfill dot_id for phieus that predate the dot_kiem_tra link
        if ("audit_5s_phieu_kiem_tra" in insp.get_table_names()
                and "audit_5s_dot_kiem_tra" in insp.get_table_names()):
            orphans = conn.execute(
                text("SELECT id, created_at FROM audit_5s_phieu_kiem_tra WHERE dot_id IS NULL")
            ).fetchall()
            for row in orphans:
                ca = row.created_at
                ky = f"{ca.year}-{ca.month:02d}"
                existing = conn.execute(
                    text("SELECT id FROM audit_5s_dot_kiem_tra WHERE ky = :ky"), {"ky": ky}
                ).fetchone()
                if existing:
                    dot_id = existing[0]
                else:
                    dot_id = conn.execute(
                        text(
                            "INSERT INTO audit_5s_dot_kiem_tra (ky, ngay_kiem_tra, created_at) "
                            "VALUES (:ky, :ngay, NOW()) RETURNING id"
                        ),
                        {"ky": ky, "ngay": ca.date()},
                    ).fetchone()[0]
                conn.execute(
                    text("UPDATE audit_5s_phieu_kiem_tra SET dot_id = :dot_id WHERE id = :id"),
                    {"dot_id": dot_id, "id": row.id},
                )

        # Add active column to audit_5s_tieu_chi if missing
        if "audit_5s_tieu_chi" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("audit_5s_tieu_chi")}
            if "active" not in cols:
                conn.execute(
                    text("ALTER TABLE audit_5s_tieu_chi ADD COLUMN active BOOLEAN NOT NULL DEFAULT TRUE")
                )

        # Add role column to general_employees if missing
        if "general_employees" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("general_employees")}
            if "role" not in cols:
                conn.execute(
                    text("ALTER TABLE general_employees ADD COLUMN role VARCHAR(20) NOT NULL DEFAULT 'user'")
                )


def _seed_users() -> None:
    """Upsert all known users on every startup. Safe on fresh DB and idempotent on existing."""
    with engine.begin() as conn:
        # All 4 users in one batch.
        # For T3656/H3839/N1785: always sync name+role from seed.
        # For P0872: insert with known info on fresh DB; on conflict only update role
        # (preserves any manual edits to their profile on an existing DB).
        conn.execute(
            text(
                "INSERT INTO general_employees (ma_nv, ho_ten, chuc_vu, don_vi, bo_phan, station, role) "
                "VALUES (:ma_nv, :ho_ten, :chuc_vu, :don_vi, :bo_phan, '[]'::jsonb, :role) "
                "ON CONFLICT (ma_nv) DO UPDATE SET "
                "  ho_ten  = CASE WHEN general_employees.ma_nv = 'P0872' THEN general_employees.ho_ten  ELSE EXCLUDED.ho_ten  END, "
                "  chuc_vu = CASE WHEN general_employees.ma_nv = 'P0872' THEN general_employees.chuc_vu ELSE EXCLUDED.chuc_vu END, "
                "  don_vi  = CASE WHEN general_employees.ma_nv = 'P0872' THEN general_employees.don_vi  ELSE EXCLUDED.don_vi  END, "
                "  bo_phan = CASE WHEN general_employees.ma_nv = 'P0872' THEN general_employees.bo_phan ELSE EXCLUDED.bo_phan END, "
                "  role    = EXCLUDED.role"
            ),
            [
                {"ma_nv": "P0872", "ho_ten": "Hồ Anh Phát",        "chuc_vu": "Phó phòng",        "don_vi": "P.TH", "bo_phan": "KSHT", "role": "admin"},
                {"ma_nv": "T3656", "ho_ten": "Phạm Ngọc Minh Trí", "chuc_vu": "KSHT - Tuân thủ", "don_vi": "P.TH", "bo_phan": "KSHT", "role": "user"},
                {"ma_nv": "H3839", "ho_ten": "Nguyễn Thị Ngọc Hoa","chuc_vu": "KSHT - Tuân thủ", "don_vi": "P.TH", "bo_phan": "KSHT", "role": "user"},
                {"ma_nv": "N1785", "ho_ten": "Lê Thị Kim Ngân",     "chuc_vu": "KSHT - Tuân thủ", "don_vi": "P.TH", "bo_phan": "KSHT", "role": "user"},
            ],
        )


def get_session():
    with Session(engine) as session:
        yield session
