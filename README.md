# RCP salary audit (FastAPI)

## Setup

1. Create a virtualenv and install deps:
   - `python -m venv .venv`
   - `.\.venv\Scripts\pip install -r requirements.txt`
2. Create `.env` from `.env.example` and set `DATABASE_URL`.
   - Also set `SESSION_SECRET` (any random string).
3. Run the app:
   - `.\.venv\Scripts\python app.py`

## Login (RCP)

- Các trang thuộc module RCP (`/rcp`, `/rcp/data`, `/rcp/dashboard` và các `/api/...`) yêu cầu đăng nhập.
- Bảng quyền truy cập: `general_employees`.
- Seed user mẫu:
  - Run `psql` (hoặc pgAdmin query tool) với file `scripts/seed_general_employees.sql`.

## Ingest

- Upload Excel:
  - `POST /api/ingest/excel` (multipart file)
- Optional local ingest (disabled by default):
  - set `ALLOW_LOCAL_INGEST=true`
  - `POST /api/ingest/local?path=...`

## Stats

- `GET /api/stats?year=2026&month=1`
- `GET /api/timeseries?year=2026`
