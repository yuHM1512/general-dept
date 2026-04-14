from __future__ import annotations

import math
import re
import unicodedata


def _strip_to_none(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    return str(value).strip() or None


def normalize_header(value: object) -> str:
    text = _strip_to_none(value) or ""
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def remove_diacritics(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join([c for c in nfkd if not unicodedata.combining(c)])


def to_int_money(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int,)):
        return int(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return 0
        return int(round(value))
    if isinstance(value, str):
        cleaned = value.strip().replace(".", "").replace(",", "")
        if cleaned == "":
            return 0
        try:
            return int(round(float(cleaned)))
        except ValueError:
            return 0
    return 0


def classify_group(department: str, job_title: str) -> str:
    department_trim = (department or "").strip()
    if department_trim in {"Cắt", "Tổ Cắt"}:
        return "Cắt/Tổ Cắt"

    title = (job_title or "").strip()
    title_low = title.lower()
    title_low_ascii = remove_diacritics(title_low)
    if ("vệ sinh công nghiệp" in title_low) or ("ve sinh cong nghiep" in title_low_ascii):
        return "Vệ sinh công nghiệp"

    return "Khác"

