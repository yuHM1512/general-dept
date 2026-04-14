from __future__ import annotations

from collections import defaultdict

from sqlmodel import Session, select
from sqlalchemy import func

from app.models import PayrollRow


def _exclude_management_departments(query):
    """
    Exclude departments that contain leadership keywords only.
    Match is case-insensitive (lower()).
    """
    dept_lc = func.lower(PayrollRow.department)
    return query.where(~(dept_lc.like("%lãnh đạo%") | dept_lc.like("%lanh dao%")))


def month_stats(session: Session, *, year: int, month: int, target_salary_vnd: int) -> list[dict]:
    q = select(PayrollRow).where(PayrollRow.year == year, PayrollRow.month == month)
    q = _exclude_management_departments(q)
    rows = session.exec(q).all()

    buckets: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "not_meet": 0})
    for row in rows:
        buckets[row.group_name]["total"] += 1
        if row.metric_vnd < target_salary_vnd:
            buckets[row.group_name]["not_meet"] += 1

    results = []
    for group in sorted(buckets.keys()):
        total = buckets[group]["total"]
        not_meet = buckets[group]["not_meet"]
        rate = (not_meet / total) if total else 0.0
        results.append(
            {"group": group, "total_count": total, "not_meet_count": not_meet, "not_meet_rate": rate}
        )
    return results


def year_timeseries(session: Session, *, year: int, target_salary_vnd: int) -> list[dict]:
    q = select(PayrollRow).where(PayrollRow.year == year)
    q = _exclude_management_departments(q)
    rows = session.exec(q).all()

    buckets: dict[tuple[int, str], dict[str, int]] = defaultdict(lambda: {"total": 0, "not_meet": 0})
    for row in rows:
        key = (row.month, row.group_name)
        buckets[key]["total"] += 1
        if row.metric_vnd < target_salary_vnd:
            buckets[key]["not_meet"] += 1

    results = []
    for (month, group) in sorted(buckets.keys()):
        total = buckets[(month, group)]["total"]
        not_meet = buckets[(month, group)]["not_meet"]
        rate = (not_meet / total) if total else 0.0
        results.append(
            {
                "year": year,
                "month": month,
                "group": group,
                "total_count": total,
                "not_meet_count": not_meet,
                "not_meet_rate": rate,
            }
        )
    return results


def metric_insights(
    session: Session,
    *,
    year: int,
    month: int | None,
    target_salary_vnd: int,
    group_filter: str | None,
    co_so: str | None,
    don_vi: str | None,
    department: str | None,
) -> tuple[list[dict], list[dict]]:
    """
    Returns:
    - by_group: stats per group for the selected scope
    - by_month: avg per month for the selected scope (only when month is None)
    """
    base = select(PayrollRow).where(PayrollRow.year == year)
    base = _exclude_management_departments(base)
    if month is not None:
        base = base.where(PayrollRow.month == month)
    if group_filter:
        base = base.where(PayrollRow.group_name == group_filter)
    if co_so:
        base = base.where(PayrollRow.co_so == co_so)
    if don_vi:
        base = base.where(PayrollRow.don_vi == don_vi)
    if department:
        base = base.where(PayrollRow.department == department)

    subq = base.subquery()

    median_expr = func.percentile_cont(0.5).within_group(subq.c.metric_vnd)
    by_group_rows = session.exec(
        select(
            subq.c.group_name.label("group"),
            func.count().label("count"),
            func.min(subq.c.metric_vnd).label("min_vnd"),
            func.max(subq.c.metric_vnd).label("max_vnd"),
            median_expr.label("median_vnd"),
            func.avg(subq.c.metric_vnd).label("avg_vnd"),
        ).group_by(subq.c.group_name)
    ).all()

    by_group = []
    for r in by_group_rows:
        by_group.append(
            {
                "group": r.group,
                "count": int(r.count or 0),
                "min_vnd": int(r.min_vnd or 0),
                "max_vnd": int(r.max_vnd or 0),
                "median_vnd": int(round(float(r.median_vnd or 0))),
                "avg_vnd": int(round(float(r.avg_vnd or 0))),
            }
        )

    by_month: list[dict] = []
    if month is None:
        q = select(
            PayrollRow.month.label("month"),
            func.count().label("count"),
            func.avg(PayrollRow.metric_vnd).label("avg_vnd"),
        ).where(PayrollRow.year == year)
        q = _exclude_management_departments(q)
        if group_filter:
            q = q.where(PayrollRow.group_name == group_filter)
        if co_so:
            q = q.where(PayrollRow.co_so == co_so)
        if don_vi:
            q = q.where(PayrollRow.don_vi == don_vi)
        if department:
            q = q.where(PayrollRow.department == department)
        q = q.group_by(PayrollRow.month).order_by(PayrollRow.month)
        by_month_rows = session.exec(q).all()
        for r in by_month_rows:
            by_month.append(
                {
                    "month": int(r.month or 0),
                    "count": int(r.count or 0),
                    "avg_vnd": int(round(float(r.avg_vnd or 0))),
                }
            )

    return by_group, by_month


def headcount_by_month(
    session: Session,
    *,
    year: int,
    group_filter: str | None,
    co_so: str | None,
    don_vi: str | None,
    department: str | None,
) -> list[dict]:
    query = select(
        PayrollRow.month.label("month"),
        func.count(func.distinct(PayrollRow.manv)).label("headcount"),
    ).where(PayrollRow.year == year)
    query = _exclude_management_departments(query)
    if group_filter:
        query = query.where(PayrollRow.group_name == group_filter)
    if co_so:
        query = query.where(PayrollRow.co_so == co_so)
    if don_vi:
        query = query.where(PayrollRow.don_vi == don_vi)
    if department:
        query = query.where(PayrollRow.department == department)
    query = query.group_by(PayrollRow.month).order_by(PayrollRow.month)
    rows = session.exec(query).all()
    out: list[dict] = []
    for r in rows:
        out.append({"month": int(r.month or 0), "headcount": int(r.headcount or 0)})
    return out

