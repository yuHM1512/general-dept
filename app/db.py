from __future__ import annotations

from sqlalchemy import inspect, text
from sqlmodel import Session, SQLModel, create_engine

from app.settings import settings
from app.services import classify_group

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    connect_args={"connect_timeout": settings.db_connect_timeout},
)


def create_db_and_tables() -> None:
    SQLModel.metadata.create_all(engine)
    _apply_light_migrations()


def _apply_light_migrations() -> None:
    """
    SQLModel's create_all() does not alter existing tables. This project uses a
    small, non-destructive migration helper so new columns can be added without
    requiring a full Alembic setup.
    """
    insp = inspect(engine)
    with engine.begin() as conn:
        if "ingestjob" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("ingestjob")}
            if "processed_rows" not in cols:
                conn.execute(
                    text(
                        "ALTER TABLE ingestjob "
                        "ADD COLUMN processed_rows INTEGER NOT NULL DEFAULT 0"
                    )
                )
        if "payrollrow" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("payrollrow")}
            if "ttbp" not in cols:
                conn.execute(text("ALTER TABLE payrollrow ADD COLUMN ttbp TEXT NOT NULL DEFAULT ''"))
            if "full_name" not in cols:
                conn.execute(text("ALTER TABLE payrollrow ADD COLUMN full_name TEXT NOT NULL DEFAULT ''"))
            if "don_vi" not in cols:
                conn.execute(text("ALTER TABLE payrollrow ADD COLUMN don_vi TEXT NOT NULL DEFAULT ''"))
                # Backfill from ttbp when available.
                if "ttbp" in cols:
                    conn.execute(text("UPDATE payrollrow SET don_vi = ttbp WHERE don_vi = ''"))
            if "co_so" not in cols:
                conn.execute(text("ALTER TABLE payrollrow ADD COLUMN co_so TEXT NOT NULL DEFAULT ''"))
                # Backfill based on TTBP rule: DT => Duy Trung; else Mẹ Nhu.
                if "ttbp" in cols:
                    conn.execute(
                        text(
                            "UPDATE payrollrow "
                            "SET co_so = CASE WHEN ttbp = 'DT' THEN 'Duy Trung' ELSE 'Mẹ Nhu' END "
                            "WHERE co_so = ''"
                        )
                    )

            # Ensure dedupe constraint exists for repeated imports.
            # (create_all won't add indexes/constraints to existing tables)
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ux_payrollrow_year_month_manv "
                    "ON payrollrow(year, month, manv)"
                )
            )
            rows = conn.execute(
                text("SELECT id, department, job_title, group_name FROM payrollrow")
            ).mappings()
            updates = []
            for row in rows:
                group_name = classify_group(row["department"] or "", row["job_title"] or "")
                if (row["group_name"] or "") != group_name:
                    updates.append({"id": row["id"], "group_name": group_name})
            if updates:
                conn.execute(
                    text("UPDATE payrollrow SET group_name = :group_name WHERE id = :id"),
                    updates,
                )

        if "hanging_line" in insp.get_table_names():
            cols = {c["name"] for c in insp.get_columns("hanging_line")}
            if "ngay_ap_dung" not in cols:
                conn.execute(text("ALTER TABLE hanging_line ADD COLUMN ngay_ap_dung DATE NOT NULL DEFAULT CURRENT_DATE"))
            if "created_at" not in cols:
                conn.execute(text("ALTER TABLE hanging_line ADD COLUMN created_at TIMESTAMP NOT NULL DEFAULT NOW()"))
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ux_hanging_line_don_vi_department "
                    "ON hanging_line(don_vi, department)"
                )
            )


def get_session():
    with Session(engine) as session:
        yield session
