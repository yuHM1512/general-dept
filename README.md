# RCP salary audit (FastAPI)

## Setup

1. Create a virtualenv and install deps:
   - `python -m venv .venv`
   - `.\.venv\Scripts\pip install -r requirements.txt`
2. Create `.env` from `.env.example` and set `DATABASE_URL`.
3. Run the app:
   - `.\.venv\Scripts\python app.py`

## Ingest

- Upload Excel:
  - `POST /api/ingest/excel` (multipart file)
- Optional local ingest (disabled by default):
  - set `ALLOW_LOCAL_INGEST=true`
  - `POST /api/ingest/local?path=...`

## Stats

- `GET /api/stats?year=2026&month=1`
- `GET /api/timeseries?year=2026`
