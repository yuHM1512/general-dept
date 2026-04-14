from __future__ import annotations

import io
import threading
from pathlib import Path
from datetime import datetime
from uuid import UUID
from concurrent.futures import ThreadPoolExecutor

from fastapi import Depends, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func
from sqlmodel import Session, select
from starlette.requests import Request

from app.db import create_db_and_tables, get_session
from app.ingest import ingest_workbook_with_progress
from app.models import IngestJob, PayrollRow
from app.schemas import (
    IngestJobResponse,
    IngestJobStatus,
    FilterOptionsResponse,
    MonthlyDetailsResponse,
    ParticipationResponse,
    HeadcountResponse,
    InsightsResponse,
    PayrollListResponse,
    PayrollRowOut,
    StatsResponse,
    TimeseriesResponse,
)
from app.settings import settings
from app.stats import month_stats, year_timeseries
from app.stats import headcount_by_month, metric_insights

app = FastAPI(title=settings.app_name)
executor = ThreadPoolExecutor(max_workers=2)
CO_SO_OPTIONS = ["Mẹ Nhu", "Duy Trung"]

BASE_DIR = Path(__file__).resolve().parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
static_dir = BASE_DIR / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

uploads_dir = BASE_DIR / "data" / "uploads"
uploads_dir.mkdir(parents=True, exist_ok=True)


def _ensure_no_active_ingest(session: Session) -> None:
    active = session.exec(
        select(func.count())
        .select_from(IngestJob)
        .where(IngestJob.status.in_(["pending", "running"]))
    ).one()
    if int(active or 0) > 0:
        raise HTTPException(status_code=409, detail="Ingest is still running. Please wait until it completes.")


@app.on_event("startup")
def _startup() -> None:
    if settings.create_tables_on_startup:
        create_db_and_tables()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "env": settings.app_env}


@app.get("/", response_class=HTMLResponse)
def homepage(request: Request) -> HTMLResponse:
    # Department homepage (portal)
    return templates.TemplateResponse(
        "homepage.html",
        {
            "request": request,
            "app_name": settings.app_name,
            "target_salary_vnd_fmt": _fmt_vnd(settings.target_salary_vnd),
            "now_year": datetime.utcnow().year,
            "active_nav": "homepage",
            "user": None,
            "department_name": "Phòng Tổng hợp",
        },
    )


@app.get("/rcp", response_class=HTMLResponse)
def rcp_home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "home_rcp.html",
        {
            "request": request,
            "app_name": settings.app_name,
            "target_salary_vnd_fmt": _fmt_vnd(settings.target_salary_vnd),
            "now_year": datetime.utcnow().year,
            "active_nav": "rcp",
        },
    )


@app.get("/rcp/data", response_class=HTMLResponse)
def data_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "data_rcp.html",
        {
            "request": request,
            "app_name": settings.app_name,
            "target_salary_vnd_fmt": _fmt_vnd(settings.target_salary_vnd),
            "now_year": datetime.utcnow().year,
            "active_nav": "rcp_data",
        },
    )


@app.get("/rcp/dashboard", response_class=HTMLResponse)
def dashboard_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "dashboard_rcp.html",
        {
            "request": request,
            "app_name": settings.app_name,
            "target_salary_vnd_fmt": _fmt_vnd(settings.target_salary_vnd),
            "now_year": datetime.utcnow().year,
            "active_nav": "rcp_dashboard",
            "target_salary_vnd": settings.target_salary_vnd,
        },
    )


@app.get("/data")
def legacy_data_redirect() -> RedirectResponse:
    return RedirectResponse(url="/rcp/data", status_code=307)


@app.get("/dashboard")
def legacy_dashboard_redirect() -> RedirectResponse:
    return RedirectResponse(url="/rcp/dashboard", status_code=307)


def _fmt_vnd(value: int) -> str:
    try:
        return f"{int(value):,}".replace(",", ".")
    except Exception:
        return str(value)


def _run_ingest_job(job_id: UUID, xlsx_path: Path) -> None:
    from sqlmodel import Session

    from app.db import engine

    with Session(engine) as session:
        job = session.get(IngestJob, job_id)
        if not job:
            return
        job.status = "running"
        job.started_at = datetime.utcnow()
        session.add(job)
        session.commit()

        try:
            # Set total rows early for UI.
            try:
                from openpyxl import load_workbook

                wb = load_workbook(xlsx_path, data_only=True, read_only=True)
                if "Luong ky nhan thang tong " in wb.sheetnames:
                    expected_total = max(0, wb["Luong ky nhan thang tong "].max_row - 1)
                    job.total_rows = expected_total
                    session.add(job)
                    session.commit()
            except Exception:
                # Non-fatal: progress can still work without total_rows.
                pass

            def _progress(processed_rows: int, inserted: int, invalid: int) -> None:
                # Persist progress using a separate session to avoid interfering with bulk inserts.
                try:
                    with Session(engine) as s2:
                        current = s2.get(IngestJob, job_id)
                        if not current:
                            return
                        current.processed_rows = processed_rows
                        current.inserted = inserted
                        current.invalid = invalid
                        s2.add(current)
                        s2.commit()
                except Exception:
                    return

            with xlsx_path.open("rb") as f:
                counters = ingest_workbook_with_progress(f, session, progress=_progress)
            job.total_rows = counters.total_rows
            job.processed_rows = counters.total_rows
            job.inserted = counters.inserted
            job.skipped = counters.skipped
            job.invalid = counters.invalid
            job.status = "completed"
            job.finished_at = datetime.utcnow()
            job.error = None
            session.add(job)
            session.commit()
        except Exception as exc:
            job.status = "failed"
            job.finished_at = datetime.utcnow()
            job.error = str(exc)
            session.add(job)
            session.commit()


@app.post("/api/ingest/excel", response_model=IngestJobResponse)
async def ingest_excel(
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
) -> IngestJobResponse:
    # Ensure tables exist even if CREATE_TABLES_ON_STARTUP is off.
    create_db_and_tables()
    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Please upload a .xlsx file")

    content = await file.read()
    job = IngestJob(status="pending")
    session.add(job)
    session.commit()
    session.refresh(job)

    xlsx_path = uploads_dir / f"{job.id}.xlsx"
    xlsx_path.write_bytes(content)

    executor.submit(_run_ingest_job, job.id, xlsx_path)
    return IngestJobResponse(job_id=str(job.id))


@app.post("/api/ingest/local", response_model=IngestJobResponse)
def ingest_local(
    path: str = Query(..., description="Local path to .xlsx"),
    session: Session = Depends(get_session),
) -> IngestJobResponse:
    if not settings.allow_local_ingest:
        raise HTTPException(status_code=403, detail="Local ingest disabled (ALLOW_LOCAL_INGEST=false)")
    create_db_and_tables()
    xlsx = Path(path)
    if not xlsx.exists() or xlsx.suffix.lower() != ".xlsx":
        raise HTTPException(status_code=400, detail="Invalid path")
    job = IngestJob(status="pending")
    session.add(job)
    session.commit()
    session.refresh(job)
    executor.submit(_run_ingest_job, job.id, xlsx)
    return IngestJobResponse(job_id=str(job.id))


@app.get("/api/ingest/jobs/{job_id}", response_model=IngestJobStatus)
def ingest_job_status(job_id: str, session: Session = Depends(get_session)) -> IngestJobStatus:
    create_db_and_tables()
    try:
        uid = UUID(job_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid job_id") from exc
    job = session.get(IngestJob, uid)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return IngestJobStatus(
        job_id=str(job.id),
        status=job.status,
        total_rows=job.total_rows,
        processed_rows=job.processed_rows,
        inserted=job.inserted,
        skipped=job.skipped,
        invalid=job.invalid,
        error=job.error,
    )


@app.get("/api/stats", response_model=StatsResponse)
def stats(
    year: int = Query(..., ge=2000, le=2100),
    month: int = Query(..., ge=1, le=12),
    session: Session = Depends(get_session),
) -> StatsResponse:
    _ensure_no_active_ingest(session)
    stats_rows = month_stats(session, year=year, month=month, target_salary_vnd=settings.target_salary_vnd)
    return StatsResponse(year=year, month=month, target_salary_vnd=settings.target_salary_vnd, stats=stats_rows)


@app.get("/api/timeseries", response_model=TimeseriesResponse)
def timeseries(
    year: int = Query(..., ge=2000, le=2100),
    session: Session = Depends(get_session),
) -> TimeseriesResponse:
    _ensure_no_active_ingest(session)
    points = year_timeseries(session, year=year, target_salary_vnd=settings.target_salary_vnd)
    return TimeseriesResponse(year=year, target_salary_vnd=settings.target_salary_vnd, points=points)


@app.get("/api/insights", response_model=InsightsResponse)
def insights(
    year: int = Query(..., ge=2000, le=2100),
    month: int | None = Query(None, ge=1, le=12),
    view: str = Query("month", pattern="^(month|year_avg)$"),
    group_name: str | None = Query(None),
    co_so: str | None = Query(None),
    don_vi: str | None = Query(None),
    department: str | None = Query(None),
    session: Session = Depends(get_session),
) -> InsightsResponse:
    _ensure_no_active_ingest(session)
    if view == "year_avg":
        month = None

    by_group, by_month = metric_insights(
        session,
        year=year,
        month=month,
        target_salary_vnd=settings.target_salary_vnd,
        group_filter=group_name,
        co_so=co_so,
        don_vi=don_vi,
        department=department,
    )

    return InsightsResponse(
        year=year,
        month=month,
        target_salary_vnd=settings.target_salary_vnd,
        view=view,
        group_filter=group_name,
        by_group=by_group,
        by_month=by_month,
    )


@app.get("/api/headcount", response_model=HeadcountResponse)
def headcount(
    year: int = Query(..., ge=2000, le=2100),
    group_name: str | None = Query(None),
    co_so: str | None = Query(None),
    don_vi: str | None = Query(None),
    department: str | None = Query(None),
    session: Session = Depends(get_session),
) -> HeadcountResponse:
    _ensure_no_active_ingest(session)
    points = headcount_by_month(
        session, year=year, group_filter=group_name, co_so=co_so, don_vi=don_vi, department=department
    )
    avg = int(round(sum(p["headcount"] for p in points) / len(points))) if points else 0
    return HeadcountResponse(year=year, group_filter=group_name, points=points, avg_headcount=avg)




@app.get("/api/available-months")
def available_months(session: Session = Depends(get_session)) -> list[dict]:
    rows = session.exec(select(PayrollRow.year, PayrollRow.month).distinct().order_by(PayrollRow.year, PayrollRow.month)).all()
    return [{"year": y, "month": m} for (y, m) in rows]


@app.get("/api/trend/monthly-details", response_model=MonthlyDetailsResponse)
def monthly_trend_details(
    year: int = Query(..., ge=2000, le=2100),
    month: int = Query(..., ge=1, le=12),
    group_name: str | None = Query(None),
    co_so: str | None = Query(None),
    don_vi: str | None = Query(None),
    department: str | None = Query(None),
    session: Session = Depends(get_session),
) -> MonthlyDetailsResponse:
    """
    Drilldown for a month: Đơn vị -> Bộ phận, with avg metric_vnd.
    Uses the same exclusion rule for lãnh đạo departments only.
    """
    _ensure_no_active_ingest(session)

    query = (
        select(
            PayrollRow.don_vi.label("don_vi"),
            PayrollRow.department.label("department"),
            func.count().label("count"),
            func.avg(PayrollRow.metric_vnd).label("avg_vnd"),
        )
        .where(PayrollRow.year == year, PayrollRow.month == month)
    )

    dept_lc = func.lower(PayrollRow.department)
    query = query.where(
        ~(
            dept_lc.like("%lãnh đạo%")
            | dept_lc.like("%lanh dao%")
        )
    )
    if group_name:
        query = query.where(PayrollRow.group_name == group_name)
    if co_so:
        query = query.where(PayrollRow.co_so == co_so)
    if don_vi:
        query = query.where(PayrollRow.don_vi == don_vi)
    if department:
        query = query.where(PayrollRow.department == department)

    query = query.group_by(PayrollRow.don_vi, PayrollRow.department).order_by(PayrollRow.don_vi, PayrollRow.department)
    rows = session.exec(query).all()

    # Build hierarchical structure: don_vi -> departments
    by_dv: dict[str, dict] = {}
    for r in rows:
        dv = (r.don_vi or "").strip()
        dep = (r.department or "").strip()
        count = int(r.count or 0)
        avg_vnd = float(r.avg_vnd or 0.0)

        if dv not in by_dv:
            by_dv[dv] = {"sum": 0.0, "count": 0, "departments": []}
        by_dv[dv]["sum"] += avg_vnd * count
        by_dv[dv]["count"] += count
        by_dv[dv]["departments"].append({"department": dep, "avg_vnd": int(round(avg_vnd))})

    items = []
    for dv in sorted(by_dv.keys()):
        total_count = int(by_dv[dv]["count"] or 0)
        weighted_avg = int(round((by_dv[dv]["sum"] / total_count) if total_count else 0.0))
        items.append(
            {
                "don_vi": dv,
                "avg_vnd": weighted_avg,
                "departments": by_dv[dv]["departments"],
            }
        )

    return MonthlyDetailsResponse(year=year, month=month, items=items)


@app.get("/api/participation", response_model=ParticipationResponse)
def participation(
    year: int = Query(..., ge=2000, le=2100),
    month: int | None = Query(None, ge=1, le=12),
    group_name: str | None = Query(None),
    co_so: str | None = Query(None),
    don_vi: str | None = Query(None),
    department: str | None = Query(None),
    session: Session = Depends(get_session),
) -> ParticipationResponse:
    """
    Count how many distinct `don_vi` participate in the selected scope.
    Excludes departments that contain "lãnh đạo" (case-insensitive).
    """
    _ensure_no_active_ingest(session)
    query = select(func.count(func.distinct(PayrollRow.don_vi))).where(PayrollRow.year == year)
    if month is not None:
        query = query.where(PayrollRow.month == month)

    dept_lc = func.lower(PayrollRow.department)
    query = query.where(~(dept_lc.like("%lãnh đạo%") | dept_lc.like("%lanh dao%")))

    if group_name:
        query = query.where(PayrollRow.group_name == group_name)
    if co_so:
        query = query.where(PayrollRow.co_so == co_so)
    if don_vi:
        query = query.where(PayrollRow.don_vi == don_vi)
    if department:
        query = query.where(PayrollRow.department == department)

    count = session.exec(query).one()
    return ParticipationResponse(year=year, month=month, don_vi_count=int(count or 0))


@app.get("/api/filters", response_model=FilterOptionsResponse)
def filter_options(
    year: int = Query(..., ge=2000, le=2100),
    group_name: str | None = Query(None),
    co_so: str | None = Query(None),
    don_vi: str | None = Query(None),
    department: str | None = Query(None),
    session: Session = Depends(get_session),
) -> FilterOptionsResponse:
    _ensure_no_active_ingest(session)

    # Faceted filter options:
    # - Months: apply all selected filters (except month itself; month not a param here)
    # - Each dimension's option list ignores its own current selection so users can switch
    #   without having to reset to "Tất cả" first (e.g., co_so list still shows both).
    def _base() -> any:
        q = select(PayrollRow).where(PayrollRow.year == year)
        dept_lc = func.lower(PayrollRow.department)
        q = q.where(
            ~(
                dept_lc.like("%lãnh đạo%")
                | dept_lc.like("%lanh dao%")
            )
        )
        if group_name:
            q = q.where(PayrollRow.group_name == group_name)
        return q

    def _distinct_list(q: any, field_name: str) -> list:
        subq = q.subquery()
        col = getattr(subq.c, field_name)
        return session.exec(select(col).distinct().order_by(col)).all()

    # months uses all selections
    q_months = _base()
    if co_so:
        q_months = q_months.where(PayrollRow.co_so == co_so)
    if don_vi:
        q_months = q_months.where(PayrollRow.don_vi == don_vi)
    if department:
        q_months = q_months.where(PayrollRow.department == department)
    months = _distinct_list(q_months, "month")

    # co_so is a derived dimension with only two values.
    # Always return both so users can switch directly without resetting other filters.
    co_so_values = CO_SO_OPTIONS

    # don_vi ignores don_vi selection
    q_don_vi = _base()
    if co_so:
        q_don_vi = q_don_vi.where(PayrollRow.co_so == co_so)
    if department:
        q_don_vi = q_don_vi.where(PayrollRow.department == department)
    don_vi_values = _distinct_list(q_don_vi, "don_vi")

    # departments ignores department selection
    q_dept = _base()
    if co_so:
        q_dept = q_dept.where(PayrollRow.co_so == co_so)
    if don_vi:
        q_dept = q_dept.where(PayrollRow.don_vi == don_vi)
    dept_values = _distinct_list(q_dept, "department")

    return FilterOptionsResponse(
        year=year,
        months=[int(m) for m in months if m is not None],
        co_so=[str(x) for x in co_so_values if x],
        don_vi=[str(x) for x in don_vi_values if x],
        departments=[str(x) for x in dept_values if x],
    )


@app.get("/api/payroll", response_model=PayrollListResponse)
def payroll_rows(
    year: int = Query(..., ge=2000, le=2100),
    month: int = Query(..., ge=1, le=12),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    group_name: str | None = Query(None),
    manv: str | None = Query(None, description="Contains"),
    department: str | None = Query(None, description="Contains"),
    co_so: str | None = Query(None),
    don_vi: str | None = Query(None),
    session: Session = Depends(get_session),
) -> PayrollListResponse:
    _ensure_no_active_ingest(session)
    query = select(PayrollRow).where(PayrollRow.year == year, PayrollRow.month == month)
    dept_lc = func.lower(PayrollRow.department)
    query = query.where(
        ~(
            dept_lc.like("%lãnh đạo%")
            | dept_lc.like("%lanh dao%")
        )
    )
    if group_name:
        query = query.where(PayrollRow.group_name == group_name)
    if co_so:
        query = query.where(PayrollRow.co_so == co_so)
    if don_vi:
        query = query.where(PayrollRow.don_vi == don_vi)
    if manv:
        query = query.where(PayrollRow.manv.ilike(f"%{manv.strip()}%"))
    if department:
        query = query.where(PayrollRow.department.ilike(f"%{department.strip()}%"))

    total = session.exec(select(func.count()).select_from(query.subquery())).one()
    rows = session.exec(query.order_by(PayrollRow.manv).offset(offset).limit(limit)).all()
    out = [
        PayrollRowOut(
            year=r.year,
            month=r.month,
            ttbp=r.ttbp,
            don_vi=r.don_vi,
            co_so=r.co_so,
            department=r.department,
            manv=r.manv,
            full_name=r.full_name,
            job_title=r.job_title,
            group_name=r.group_name,
            metric_vnd=r.metric_vnd,
        )
        for r in rows
    ]
    return PayrollListResponse(year=year, month=month, total=int(total or 0), limit=limit, offset=offset, rows=out)


@app.get("/api/debug/db")
def debug_db(session: Session = Depends(get_session)) -> dict:
    """
    Lightweight diagnostics for connectivity + whether any rows were ingested.
    """
    create_db_and_tables()
    row_count = session.exec(select(func.count()).select_from(PayrollRow)).one()
    # Avoid returning rows; just counts and distinct months.
    months = session.exec(
        select(PayrollRow.year, PayrollRow.month).distinct().order_by(PayrollRow.year, PayrollRow.month)
    ).all()
    return {"ok": True, "row_count": int(row_count or 0), "months": [{"year": y, "month": m} for (y, m) in months]}
