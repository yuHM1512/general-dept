# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the app

```bash
# Development (auto-reload)
python -m uvicorn app.main:app --host 0.0.0.0 --port 8012 --reload

# Or via the run module
python -m app.run

# Quick smoke test (no live server needed)
python -c "from starlette.testclient import TestClient; from app.main import app; r = TestClient(app, follow_redirects=False).get('/health'); print(r.json())"
```

Default port is `8012` (set in `.env` → `PORT`). If that port is stuck in Windows TCP TIME_WAIT, use `--port 8013` or higher.

## Environment

Copy `.env.example` → `.env` and fill in `DATABASE_URL`. Minimum required:

```
DATABASE_URL=postgresql+psycopg://user:pass@localhost:5432/general_dept
CREATE_TABLES_ON_STARTUP=true
SESSION_SECRET=<random string>
```

Tables are created/migrated automatically on first request when `CREATE_TABLES_ON_STARTUP=true`. No Alembic — see migration pattern below.

## Architecture

**Stack:** FastAPI + Jinja2 SSR templates, SQLModel/SQLAlchemy ORM, PostgreSQL (psycopg v3), Starlette `SessionMiddleware` for auth, Tailwind CSS v3 Play CDN.

### Modules

| Path prefix | Module | Templates |
|---|---|---|
| `/` | Homepage portal | `homepage.html` |
| `/rcp/*` | RCP living wage audit | `home_rcp.html`, `dashboard_rcp.html`, `data_rcp.html`, … |
| `/internal-audit/*` | 5S internal audit | `home_internal.html`, `5s_*.html` |
| `/api/*` | JSON API (no HTML) | — |

### Auth flow

All routes except `/health`, `/static/*`, `/login`, `/logout`, `/rcp/login`, `/rcp/logout` require a session. Unauthenticated requests redirect to `/login?next=<url>`. Login only checks `ma_nv` against `general_employees` table — no password. Session stores `{ma_nv, ho_ten, chuc_vu, don_vi, bo_phan, role}`. Roles: `admin` (full access + settings), `user` (audit + read).

Helper functions in `main.py`: `_current_user(request)` → dict|None, `_is_admin(request)` → bool.

### Database schema

**RCP tables** (`rcp_` prefix):
- `rcp_payrollrow` — one row per employee per month (ingested from Excel)
- `rcp_ingestjob` — async upload job tracking
- `rcp_hanging_line` — per-unit salary ceiling config

**Audit 5S tables** (`audit_5s_` prefix):
- `audit_5s_don_vi` → `audit_5s_bo_phan` — org structure (unit → department)
- `audit_5s_linh_vuc` + `audit_5s_tieu_chi` — criteria library (domain → criteria)
- `audit_5s_ap_dung` — M2M: which criteria apply to which departments
- `audit_5s_dot_kiem_tra` — audit period, keyed by `ky` (e.g. `"2026-07"`)
- `audit_5s_phieu_kiem_tra` — audit form result, links to `dot_id` + `bo_phan_id`
- `audit_5s_chi_tiet_diem` — per-criterion scores (0/1/2)

**Auth table:** `general_employees` — `ma_nv` (PK), `ho_ten`, `chuc_vu`, `don_vi`, `bo_phan`, `role`, `station` (JSONB).

### Migration pattern

No Alembic. `db.py` runs four steps on `create_db_and_tables()`:

1. `_apply_rename_migrations()` — idempotent `ALTER TABLE … RENAME` for old table names, runs **before** `create_all`
2. `SQLModel.metadata.create_all()` — creates new tables only
3. `_apply_light_migrations()` — idempotent `ALTER TABLE … ADD COLUMN IF NOT EXISTS` + backfills
4. `_seed_users()` + `seed_if_empty()` — upsert known users + seed static audit data

To add a new column: add the field to the model, then add an idempotent `ALTER TABLE … ADD COLUMN` block in `_apply_light_migrations()`.

### Excel ingest (RCP module)

`app/ingest.py` reads the sheet named `"Luong ky nhan thang tong "` (trailing space is intentional). Column mapping uses Vietnamese header normalization via `services.normalize_header()`. Upload is async — a background thread runs `ingest_workbook_with_progress()` and updates `rcp_ingestjob`. Poll `/api/jobs/{id}` for status.

## Design system ("Ethical Architect")

All templates use Tailwind v3 Play CDN with a custom config block. Key tokens — **do not deviate**:

```js
colors: {
  "primary": "#002c50",
  "primary-container": "#005A9C",
  "secondary": "#1b6d24",
  "tertiary": "#00312a",
  "surface": "#F9F9FA",
  "on-surface": "#1A1C1D",
}
borderRadius: { "full": "0.75rem" }  // NOT 9999px — use style="border-radius:50%" for true circles
```

Rules: no 1px dividers (use surface-color shifts), no pure black, no heavy card borders. Headlines use **Manrope**, body uses **Inter**.

## Key files

| File | Purpose |
|---|---|
| `app/main.py` | All routes + middleware + auth |
| `app/db.py` | Engine, migrations, seeding |
| `app/models.py` | RCP + GeneralEmployee ORM models |
| `app/audit_models.py` | 5S audit ORM models |
| `app/audit_seed.py` | Static seed data for audit org/criteria |
| `app/ingest.py` | Excel → DB pipeline |
| `app/stats.py` | Payroll aggregation queries |
| `app/schemas.py` | Pydantic response schemas for API |
| `app/settings.py` | Pydantic settings (reads `.env`) |
| `templates/stitch/equitas_core/DESIGN.md` | Full design system spec |
