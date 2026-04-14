from __future__ import annotations

from collections.abc import Callable
from typing import BinaryIO

from openpyxl import load_workbook
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session

from app.models import PayrollRow
from app.services import classify_group, normalize_header, to_int_money


class IngestCounters:
    def __init__(self, *, total_rows: int, inserted: int, skipped: int, invalid: int):
        self.total_rows = total_rows
        self.inserted = inserted
        self.skipped = skipped
        self.invalid = invalid


TARGET_SHEET_NAME = "Luong ky nhan thang tong "


def _resolve_column_indices(ws) -> dict[str, int]:
    header_row = 1
    headers = {}
    for col in range(1, ws.max_column + 1):
        value = ws.cell(header_row, col).value
        key = normalize_header(value)
        if key:
            headers[key] = col

    required = {
        "THANG": "THANG",
        "NAM": "NAM",
        "MANV": "MANV",
        "LGTRGIO": "LGTRGIO",
        "Bu du luong toi thieu": "Bu du luong toi thieu",
        "Tien CM thai 7T VSPN": "Tien CM thai 7T VSPN",
        "Tien F L H R GL": "Tien F L H R GL",
        "TIEN E": "TIEN E",
    }

    col_index: dict[str, int] = {}
    missing = []
    for out_key, header_key in required.items():
        if header_key not in headers:
            missing.append(header_key)
            continue
        col_index[out_key] = headers[header_key]

    if missing:
        raise ValueError(f"Missing required headers: {', '.join(missing)}")

    # Positional columns in the current file.
    # D = TTBP, E = Department name, G = Full name, I = Job title.
    col_index["TTBP"] = 4
    col_index["DEPARTMENT"] = 5
    col_index["FULL_NAME"] = 7
    col_index["JOB_TITLE"] = 9

    # Optional: if Excel provides an explicit "don_vi" column, use it.
    # We'll accept a few common header spellings (case-insensitive).
    header_lc = {k.lower(): v for k, v in headers.items()}
    for key in ["don_vi", "don vi", "đơn vị", "donvi"]:
        if key in header_lc:
            col_index["DON_VI"] = header_lc[key]
            break
    return col_index


def ingest_workbook(file_obj: BinaryIO, session: Session) -> IngestCounters:
    return ingest_workbook_with_progress(file_obj, session, progress=None)


# Column indices are 0-based when iterating rows as tuples.
_BATCH_SIZE = 2000


def ingest_workbook_with_progress(
    file_obj: BinaryIO,
    session: Session,
    *,
    progress: Callable[[int, int, int], None] | None,
) -> IngestCounters:
    wb = load_workbook(file_obj, data_only=True, read_only=True)
    if TARGET_SHEET_NAME not in wb.sheetnames:
        raise ValueError(f"Sheet not found: {TARGET_SHEET_NAME!r}")

    ws = wb[TARGET_SHEET_NAME]
    # Resolve 1-based column indices from header mapping, then convert to 0-based
    # for fast tuple-based row iteration (avoids per-cell ws.cell() overhead).
    col1 = _resolve_column_indices(ws)  # 1-based
    c = {k: v - 1 for k, v in col1.items()}  # 0-based

    total_rows = 0
    inserted = 0
    skipped = 0
    invalid = 0
    batch: list[dict] = []

    def flush_batch() -> int:
        nonlocal batch
        if not batch:
            return 0
        stmt = insert(PayrollRow).values(batch)
        stmt = stmt.on_conflict_do_nothing(index_elements=["year", "month", "manv"])
        result = session.exec(stmt)
        session.commit()
        rowcount = int(getattr(result, "rowcount", 0) or 0)
        batch = []
        return rowcount

    # Iterate rows as value-tuples — significantly faster than ws.cell() per cell.
    row_iter = ws.iter_rows(min_row=2, values_only=True)
    for values in row_iter:
        total_rows += 1

        # Guard against short rows (sparse sheets).
        def _get(idx: int):
            return values[idx] if idx < len(values) else None

        manv = _get(c["MANV"])
        if manv is None or str(manv).strip() == "":
            invalid += 1
            if progress and (total_rows % 500 == 0):
                progress(total_rows, inserted, invalid)
            continue

        year = _get(c["NAM"])
        month = _get(c["THANG"])
        if year is None or month is None:
            invalid += 1
            if progress and (total_rows % 500 == 0):
                progress(total_rows, inserted, invalid)
            continue

        ttbp = _get(c["TTBP"]) or ""
        department = _get(c["DEPARTMENT"]) or ""
        full_name = _get(c["FULL_NAME"]) or ""
        job_title = _get(c["JOB_TITLE"]) or ""
        co_so = "Duy Trung" if str(ttbp).strip() == "DT" else "Mẹ Nhu"
        don_vi_idx = c.get("DON_VI")
        don_vi = _get(don_vi_idx) if don_vi_idx is not None else None
        if don_vi is None:
            don_vi = str(ttbp).strip()

        lgtrgio = to_int_money(_get(c["LGTRGIO"]))
        bu = to_int_money(_get(c["Bu du luong toi thieu"]))
        cm = to_int_money(_get(c["Tien CM thai 7T VSPN"]))
        flhr = to_int_money(_get(c["Tien F L H R GL"]))
        tien_e = to_int_money(_get(c["TIEN E"]))

        metric = lgtrgio + bu + cm + flhr + tien_e
        group_name = classify_group(str(department), str(job_title))

        manv_str = str(manv).strip()
        try:
            year_int = int(float(year))
            month_int = int(float(month))
        except (TypeError, ValueError):
            invalid += 1
            continue

        batch.append(
            {
                "year": year_int,
                "month": month_int,
                "ttbp": str(ttbp).strip(),
                "don_vi": str(don_vi).strip(),
                "co_so": co_so,
                "department": str(department).strip(),
                "manv": manv_str,
                "full_name": str(full_name).strip(),
                "job_title": str(job_title).strip(),
                "lgtrgio": lgtrgio,
                "bu_du_luong_toi_thieu": bu,
                "tien_cm_thai_7t_vspn": cm,
                "tien_f_l_h_r_gl": flhr,
                "tien_e": tien_e,
                "metric_vnd": metric,
                "group_name": group_name,
            }
        )
        if len(batch) >= _BATCH_SIZE:
            inserted += flush_batch()
            if progress:
                progress(total_rows, inserted, invalid)
        elif progress and (total_rows % 500 == 0):
            progress(total_rows, inserted, invalid)

    inserted += flush_batch()
    if progress:
        progress(total_rows, inserted, invalid)
    # duplicates = total valid - inserted
    valid = total_rows - invalid
    skipped = max(0, valid - inserted)
    return IngestCounters(total_rows=total_rows, inserted=inserted, skipped=skipped, invalid=invalid)
