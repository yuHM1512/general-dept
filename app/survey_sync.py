"""
Sync data từ Google Sheets (sheet '3. DATA') xuống bảng `survey_response`.

Chạy trong background thread khi user bấm nút "Đồng bộ dữ liệu".
Sử dụng service account credentials trong file JSON (default: credentials_m29.json).
"""
from __future__ import annotations

import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlmodel import Session

from app.db import engine
from app.settings import settings
from app.survey_models import SurveyResponse, SurveySyncJob


# ----- Helpers -----------------------------------------------------------------


_DOT_RE = re.compile(r"[Đđ]ợt\s*(\d+)\s*-\s*(\d{4})")


def _parse_dot(dot_khao_sat: str) -> tuple[int | None, int | None]:
    """Tách 'Đợt 1 - 2025' → (dot_so=1, dot_nam=2025)."""
    if not dot_khao_sat:
        return None, None
    m = _DOT_RE.search(dot_khao_sat)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def _to_int_score(value: Any) -> int | None:
    """Score 1..7. Sheet có thể trả về float — cast an toàn."""
    if value is None or value == "":
        return None
    try:
        f = float(value)
        i = int(round(f))
        if 1 <= i <= 7:
            return i
    except (TypeError, ValueError):
        return None
    return None


def _to_datetime(value: Any) -> datetime | None:
    """Sheet trả 'Time' về dạng string hoặc datetime tuỳ thư viện."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    return None


def _parse_uuid(value: Any) -> UUID | None:
    if not value:
        return None
    try:
        return UUID(str(value).strip())
    except (ValueError, TypeError):
        return None


def _clean_str(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _clean_opt_str(value: Any) -> str | None:
    s = _clean_str(value)
    return s if s else None


# ----- Sheet fetch --------------------------------------------------------------


def fetch_sheet_rows() -> list[dict[str, Any]]:
    """
    Đọc toàn bộ dòng data từ sheet '3. DATA' qua Google Sheets API.

    Raises:
        RuntimeError: nếu credentials thiếu / permissions sai.
    """
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "Thiếu thư viện gspread/google-auth. Chạy: pip install gspread google-auth"
        ) from exc

    cred_path = Path(settings.survey_credentials_path)
    if not cred_path.exists():
        raise RuntimeError(f"Không tìm thấy credentials tại: {cred_path}")

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_file(str(cred_path), scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(settings.survey_sheet_id)

    target = None
    for ws in sh.worksheets():
        if ws.title.strip() == settings.survey_data_tab.strip():
            target = ws
            break
    if target is None:
        raise RuntimeError(
            f"Không thấy tab {settings.survey_data_tab!r} trong sheet {settings.survey_sheet_id}"
        )

    # get_all_records() dùng header hàng 1 làm keys — không cần map thủ công
    records: list[dict[str, Any]] = target.get_all_records()
    return records


# ----- Upsert -------------------------------------------------------------------


# Mapping header cột trong sheet → field trong SurveyResponse
_HEADER_MAP = {
    "ID": "id",
    "Time": "thoi_gian",
    "No": "so_phieu",
    "Đơn vị khảo sát": "don_vi_khao_sat",
    "Đơn vị được khảo sát": "don_vi_duoc_khao_sat",
    "Nội dung khảo sát": "noi_dung_cau_hoi",
    "Mức độ hài lòng": "muc_do_hai_long",
    "Ý kiến đóng góp khác": "y_kien_dong_gop",
    "Lý do điểm thấp": "ly_do_diem_thap",
    "Question": "question_key",
    "Đợt khảo sát": "dot_khao_sat",
    "Loại khảo sát": "loai_khao_sat",
}


def _row_to_payload(rec: dict[str, Any]) -> dict[str, Any] | None:
    """Chuyển 1 dict record từ sheet → dict payload cho INSERT/UPSERT."""
    row_id = _parse_uuid(rec.get("ID"))
    if row_id is None:
        return None

    dot_khao_sat = _clean_str(rec.get("Đợt khảo sát"))
    dot_so, dot_nam = _parse_dot(dot_khao_sat)

    now = datetime.utcnow()
    return {
        "id": row_id,
        "thoi_gian": _to_datetime(rec.get("Time")),
        "so_phieu": _clean_str(rec.get("No")),
        "don_vi_khao_sat": _clean_str(rec.get("Đơn vị khảo sát")),
        "don_vi_duoc_khao_sat": _clean_str(rec.get("Đơn vị được khảo sát")),
        "noi_dung_cau_hoi": _clean_str(rec.get("Nội dung khảo sát")),
        "question_key": _clean_str(rec.get("Question")),
        "muc_do_hai_long": _to_int_score(rec.get("Mức độ hài lòng")),
        "y_kien_dong_gop": _clean_opt_str(rec.get("Ý kiến đóng góp khác")),
        "ly_do_diem_thap": _clean_opt_str(rec.get("Lý do điểm thấp")),
        "dot_khao_sat": dot_khao_sat,
        "dot_so": dot_so,
        "dot_nam": dot_nam,
        "loai_khao_sat": _clean_str(rec.get("Loại khảo sát")),
        "created_at": now,
        "updated_at": now,
    }


def _upsert_batch(session: Session, batch: list[dict[str, Any]]) -> None:
    """Upsert theo PK `id`. Chunk 500 để tránh query quá lớn."""
    if not batch:
        return
    table = SurveyResponse.__table__
    stmt = pg_insert(table).values(batch)
    update_cols = {
        c.name: stmt.excluded[c.name]
        for c in table.columns
        if c.name not in ("id", "created_at")
    }
    stmt = stmt.on_conflict_do_update(index_elements=["id"], set_=update_cols)
    session.exec(stmt)  # type: ignore[arg-type]


# ----- Job runner ---------------------------------------------------------------


def run_sync_job(job_id: UUID) -> None:
    """Chạy toàn bộ pipeline sync cho 1 job. Cập nhật status trong DB."""
    with Session(engine) as session:
        job = session.get(SurveySyncJob, job_id)
        if job is None:
            return
        job.status = "running"
        job.started_at = datetime.utcnow()
        session.add(job)
        session.commit()

    try:
        records = fetch_sheet_rows()
        total = len(records)
        upserted = 0
        skipped = 0

        with Session(engine) as session:
            batch: list[dict[str, Any]] = []
            BATCH_SIZE = 500
            for rec in records:
                payload = _row_to_payload(rec)
                if payload is None:
                    skipped += 1
                    continue
                batch.append(payload)
                if len(batch) >= BATCH_SIZE:
                    _upsert_batch(session, batch)
                    upserted += len(batch)
                    batch.clear()
            if batch:
                _upsert_batch(session, batch)
                upserted += len(batch)
            session.commit()

            # Update job as completed
            job = session.get(SurveySyncJob, job_id)
            if job is not None:
                job.status = "completed"
                job.total_rows = total
                job.upserted = upserted
                job.skipped = skipped
                job.finished_at = datetime.utcnow()
                job.message = f"Đã đồng bộ {upserted}/{total} dòng, bỏ qua {skipped}."
                session.add(job)
                session.commit()

    except Exception as exc:  # pragma: no cover — báo lỗi ra UI
        with Session(engine) as session:
            job = session.get(SurveySyncJob, job_id)
            if job is not None:
                job.status = "failed"
                job.finished_at = datetime.utcnow()
                job.message = f"Lỗi đồng bộ: {exc}"
                session.add(job)
                session.commit()


def start_sync_in_background(job_id: UUID) -> None:
    """Spawn thread chạy sync (không block request)."""
    t = threading.Thread(target=run_sync_job, args=(job_id,), daemon=True)
    t.start()
