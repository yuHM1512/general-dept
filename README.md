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

## Email nhắc HĐKP

Hệ thống có thể gửi một email tổng hợp cho từng đơn vị vào 08:00 hằng ngày. Email gồm tất cả
HĐKP chưa tiếp nhận và các HĐKP đang thực hiện đã quá thời hạn. Người nhận là các nhân viên có
email và `don_vi` trùng với đơn vị của HĐKP; các bộ phận trực thuộc được gom trong cùng email.

1. Điền nhóm biến `AUDIT_REMINDER_*` và `SMTP_*` theo mẫu trong `.env.example`.
2. Với Gmail/Google Workspace, dùng App Password tại `SMTP_PASSWORD`, không dùng mật khẩu đăng nhập.
3. Kiểm tra dữ liệu và người nhận mà không gửi email:
   - `.\.venv\Scripts\python -m app.audit_reminders --dry-run`
4. Đổi `AUDIT_REMINDER_ENABLED=true` và khởi động lại service.

Mỗi đơn vị chỉ được gửi một lần trong ngày. Lịch sử gửi được lưu trong
`public.audit_5s_reminder_log` để tránh gửi trùng khi service restart hoặc chạy nhiều process.
