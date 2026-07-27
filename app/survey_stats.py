"""
Aggregation queries cho module Khảo sát nội bộ.
"""
from __future__ import annotations

from collections import defaultdict

from sqlalchemy import func
from sqlmodel import Session, select

from app.survey_models import SurveyResponse
from app.survey_schemas import (
    SurveyComparisonResponse,
    UnitComparisonRow,
    UnitRoundScore,
)


LOAI_NOI_BO = "Khách hàng nội bộ"


def _score_from_avg(avg: float | None) -> float | None:
    """Công thức tổng điểm theo PowerBI: avg(1..7) / 7 * 100."""
    if avg is None:
        return None
    return round(avg / 7.0 * 100.0, 1)


def _sort_round_key(dot_khao_sat: str) -> tuple[int, int]:
    """
    Sort đợt theo (năm, đợt_số) tăng dần.
    Fallback (9999, 9) nếu không parse được (đẩy xuống cuối).
    """
    # Import cục bộ để tránh vòng import
    from app.survey_sync import _parse_dot
    dot_so, dot_nam = _parse_dot(dot_khao_sat)
    return (dot_nam or 9999, dot_so or 9)


def internal_comparison(session: Session) -> SurveyComparisonResponse:
    """
    Bảng so sánh điểm tổng theo (đơn vị được khảo sát × đợt khảo sát) cho loại Nội bộ.

    Trả về:
      - rounds: danh sách các đợt (sort theo năm/đợt tăng dần)
      - overall: điểm tổng của toàn công ty theo từng đợt
      - rows: mỗi đơn vị được khảo sát, kèm mảng điểm theo thứ tự rounds
    """
    # Truy vấn 1: aggregate theo (dot, don_vi_duoc_khao_sat)
    q_by_unit = (
        select(
            SurveyResponse.dot_khao_sat,
            SurveyResponse.don_vi_duoc_khao_sat,
            func.avg(SurveyResponse.muc_do_hai_long).label("avg_score"),
            func.count(SurveyResponse.muc_do_hai_long).label("cnt"),
        )
        .where(SurveyResponse.loai_khao_sat == LOAI_NOI_BO)
        .where(SurveyResponse.muc_do_hai_long.is_not(None))
        .group_by(SurveyResponse.dot_khao_sat, SurveyResponse.don_vi_duoc_khao_sat)
    )
    by_unit_rows = session.exec(q_by_unit).all()

    # Truy vấn 2: overall theo dot
    q_overall = (
        select(
            SurveyResponse.dot_khao_sat,
            func.avg(SurveyResponse.muc_do_hai_long).label("avg_score"),
            func.count(SurveyResponse.muc_do_hai_long).label("cnt"),
        )
        .where(SurveyResponse.loai_khao_sat == LOAI_NOI_BO)
        .where(SurveyResponse.muc_do_hai_long.is_not(None))
        .group_by(SurveyResponse.dot_khao_sat)
    )
    overall_rows = session.exec(q_overall).all()

    # Total responses (kể cả null score)
    q_total = (
        select(func.count())
        .select_from(SurveyResponse)
        .where(SurveyResponse.loai_khao_sat == LOAI_NOI_BO)
    )
    total_responses = int(session.exec(q_total).one() or 0)

    # ---- Ghép dữ liệu ----
    all_rounds = sorted({r[0] for r in by_unit_rows} | {r[0] for r in overall_rows},
                        key=_sort_round_key)
    all_units = sorted({r[1] for r in by_unit_rows})

    # Lookup: (dot, unit) → (avg, cnt)
    unit_lookup: dict[tuple[str, str], tuple[float, int]] = {
        (dot, unit): (float(avg) if avg is not None else 0.0, int(cnt or 0))
        for dot, unit, avg, cnt in by_unit_rows
    }
    overall_lookup: dict[str, tuple[float, int]] = {
        dot: (float(avg) if avg is not None else 0.0, int(cnt or 0))
        for dot, avg, cnt in overall_rows
    }

    overall = [
        UnitRoundScore(
            dot_khao_sat=dot,
            avg_score=round(overall_lookup[dot][0], 3) if dot in overall_lookup else None,
            total_score=_score_from_avg(overall_lookup[dot][0]) if dot in overall_lookup else None,
            response_count=overall_lookup.get(dot, (0.0, 0))[1],
        )
        for dot in all_rounds
    ]

    rows: list[UnitComparisonRow] = []
    for unit in all_units:
        scores: list[UnitRoundScore] = []
        for dot in all_rounds:
            key = (dot, unit)
            if key in unit_lookup:
                avg, cnt = unit_lookup[key]
                scores.append(UnitRoundScore(
                    dot_khao_sat=dot,
                    avg_score=round(avg, 3),
                    total_score=_score_from_avg(avg),
                    response_count=cnt,
                ))
            else:
                scores.append(UnitRoundScore(
                    dot_khao_sat=dot,
                    avg_score=None,
                    total_score=None,
                    response_count=0,
                ))
        rows.append(UnitComparisonRow(don_vi=unit, scores=scores))

    return SurveyComparisonResponse(
        rounds=all_rounds,
        overall=overall,
        rows=rows,
        total_responses=total_responses,
    )
