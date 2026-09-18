"""Routes HTML + JSON cho module Mục tiêu chất lượng."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Session

from app.db import create_db_and_tables, get_session
from app.mtcl.models import MtclCompany, MtclDepart, MtclDepartment
from app.mtcl.schemas import MtclCompanyIn, MtclDepartIn, MtclDiagramResponse
from app.mtcl.service import build_diagram, next_id, resolve_year, touch_dates
from app.settings import settings

BASE_DIR = Path(__file__).resolve().parent.parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

router = APIRouter()


def _current_user(request: Request) -> dict | None:
    try:
        return request.session.get("user")  # type: ignore[attr-defined]
    except Exception:
        return None


def _is_admin(request: Request) -> bool:
    user = _current_user(request)
    role = str((user or {}).get("role") or "user").strip().lower() or "user"
    return user is not None and role == "admin"


def _page_context(request: Request, diagram: MtclDiagramResponse) -> dict:
    return {
        "request": request,
        "app_name": settings.app_name,
        "now_year": datetime.utcnow().year,
        "user": _current_user(request),
        "is_admin": _is_admin(request),
        "diagram": diagram,
    }


@router.get("/mtcl", response_class=HTMLResponse)
def mtcl_list(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "MTCL/list.html",
        {
            "request": request,
            "app_name": settings.app_name,
            "now_year": datetime.utcnow().year,
            "user": _current_user(request),
        },
    )


def _diagram_page(
    request: Request,
    session: Session,
    nam: int | None,
    *,
    view: str,
    page_title: str,
    base_path: str,
    tab: str = "so-do",
) -> HTMLResponse:
    create_db_and_tables()
    year = resolve_year(session, nam)
    diagram = build_diagram(session, year)
    if tab not in {"so-do", "cong-ty", "don-vi"}:
        tab = "so-do"
    ctx = _page_context(request, diagram)
    ctx.update({"view": view, "page_title": page_title, "base_path": base_path, "tab": tab})
    return templates.TemplateResponse("MTCL/diagram.html", ctx)


@router.get("/mtcl/so-do", response_class=HTMLResponse)
def mtcl_so_do(
    request: Request,
    nam: int | None = Query(default=None),
    tab: str = Query(default="so-do"),
    session: Session = Depends(get_session),
) -> HTMLResponse:
    return _diagram_page(
        request,
        session,
        nam,
        view="all",
        page_title="Mục tiêu chất lượng & chính sách chất lượng",
        base_path="/mtcl/so-do",
        tab=tab,
    )


@router.get("/mtcl/muc-tieu")
def mtcl_muc_tieu_redirect(nam: int | None = Query(default=None)) -> RedirectResponse:
    url = "/mtcl/so-do" if not nam else f"/mtcl/so-do?nam={nam}"
    return RedirectResponse(url=url, status_code=303)


@router.get("/mtcl/chinh-sach")
def mtcl_chinh_sach_redirect(nam: int | None = Query(default=None)) -> RedirectResponse:
    url = "/mtcl/so-do" if not nam else f"/mtcl/so-do?nam={nam}"
    return RedirectResponse(url=url, status_code=303)


@router.get("/api/mtcl/diagram", response_model=MtclDiagramResponse)
def api_mtcl_diagram(
    nam: int | None = Query(default=None),
    session: Session = Depends(get_session),
) -> MtclDiagramResponse:
    create_db_and_tables()
    return build_diagram(session, resolve_year(session, nam))


@router.post("/api/mtcl/company")
def api_mtcl_company_create(
    request: Request,
    payload: MtclCompanyIn,
    session: Session = Depends(get_session),
) -> dict:
    if not _is_admin(request):
        raise HTTPException(status_code=403, detail="Chỉ admin mới có thể thêm mục tiêu công ty")
    create_db_and_tables()
    row = MtclCompany(
        id=next_id(session, MtclCompany),
        nam=payload.nam,
        cscl=payload.cscl.strip(),
        mtcl=payload.mtcl.strip(),
        note=payload.note.strip(),
    )
    touch_dates(row)
    session.add(row)
    session.commit()
    session.refresh(row)
    return {"ok": True, "id": row.id}


@router.put("/api/mtcl/company/{item_id}")
def api_mtcl_company_update(
    item_id: int,
    request: Request,
    payload: MtclCompanyIn,
    session: Session = Depends(get_session),
) -> dict:
    if not _is_admin(request):
        raise HTTPException(status_code=403, detail="Chỉ admin mới có thể sửa mục tiêu công ty")
    row = session.get(MtclCompany, item_id)
    if not row:
        raise HTTPException(status_code=404, detail="Không tìm thấy mục tiêu công ty")
    row.nam = payload.nam
    row.cscl = payload.cscl.strip()
    row.mtcl = payload.mtcl.strip()
    row.note = payload.note.strip()
    touch_dates(row)
    session.add(row)
    session.commit()
    return {"ok": True, "id": item_id}


@router.delete("/api/mtcl/company/{item_id}")
def api_mtcl_company_delete(
    item_id: int,
    request: Request,
    session: Session = Depends(get_session),
) -> dict:
    if not _is_admin(request):
        raise HTTPException(status_code=403, detail="Chỉ admin mới có thể xóa mục tiêu công ty")
    row = session.get(MtclCompany, item_id)
    if not row:
        raise HTTPException(status_code=404, detail="Không tìm thấy mục tiêu công ty")
    session.delete(row)
    session.commit()
    return {"ok": True, "deleted_id": item_id}


@router.post("/api/mtcl/depart")
def api_mtcl_depart_create(
    request: Request,
    payload: MtclDepartIn,
    session: Session = Depends(get_session),
) -> dict:
    if not _is_admin(request):
        raise HTTPException(status_code=403, detail="Chỉ admin mới có thể thêm mục tiêu đơn vị")
    create_db_and_tables()
    dept = session.get(MtclDepartment, payload.id_depart)
    if not dept:
        raise HTTPException(status_code=422, detail="Đơn vị không tồn tại")
    row = MtclDepart(
        id=next_id(session, MtclDepart),
        nam=payload.nam,
        id_depart=payload.id_depart,
        content=payload.content.strip(),
        note=payload.note.strip(),
    )
    touch_dates(row)
    session.add(row)
    session.commit()
    session.refresh(row)
    return {"ok": True, "id": row.id}


@router.put("/api/mtcl/depart/{item_id}")
def api_mtcl_depart_update(
    item_id: int,
    request: Request,
    payload: MtclDepartIn,
    session: Session = Depends(get_session),
) -> dict:
    if not _is_admin(request):
        raise HTTPException(status_code=403, detail="Chỉ admin mới có thể sửa mục tiêu đơn vị")
    row = session.get(MtclDepart, item_id)
    if not row:
        raise HTTPException(status_code=404, detail="Không tìm thấy mục tiêu đơn vị")
    dept = session.get(MtclDepartment, payload.id_depart)
    if not dept:
        raise HTTPException(status_code=422, detail="Đơn vị không tồn tại")
    row.nam = payload.nam
    row.id_depart = payload.id_depart
    row.content = payload.content.strip()
    row.note = payload.note.strip()
    touch_dates(row)
    session.add(row)
    session.commit()
    return {"ok": True, "id": item_id}


@router.delete("/api/mtcl/depart/{item_id}")
def api_mtcl_depart_delete(
    item_id: int,
    request: Request,
    session: Session = Depends(get_session),
) -> dict:
    if not _is_admin(request):
        raise HTTPException(status_code=403, detail="Chỉ admin mới có thể xóa mục tiêu đơn vị")
    row = session.get(MtclDepart, item_id)
    if not row:
        raise HTTPException(status_code=404, detail="Không tìm thấy mục tiêu đơn vị")
    session.delete(row)
    session.commit()
    return {"ok": True, "deleted_id": item_id}
