# CLAUDE.md

Reference cho Claude Code khi làm việc trong repo `general-dept` — công cụ số hoá nội bộ cho Phòng Tổng Hợp.

## Tech stack

- **Backend:** FastAPI 0.115 + Uvicorn, Python 3.11+
- **ORM:** SQLModel 0.0.24 + SQLAlchemy, PostgreSQL (psycopg v3)
- **Templates:** Jinja2 SSR (không dùng React/Vue)
- **CSS:** Tailwind v3 Play CDN (config inline trong `base.html`), font Manrope (headline) + Inter (body), icon Material Symbols
- **Auth:** Starlette `SessionMiddleware` (cookie-based, chỉ check `ma_nv`, không password)
- **Excel:** openpyxl 3.1.5 + pandas 2.2.3
- **Config:** pydantic-settings đọc `.env`
- **Không dùng:** Alembic (migrations tự viết trong `db.py`), Node build (Tailwind chạy CDN)

## Cấu trúc thư mục

```
general-dept/
├── app/                    # Backend Python
│   ├── main.py             # TẤT CẢ routes + middleware auth (~2740 dòng, 54 endpoints)
│   ├── db.py               # Engine, migrations idempotent, seeding
│   ├── models.py           # SQLModel: PayrollRow, GeneralEmployee, IngestJob, HangingLine
│   ├── audit_models.py     # SQLModel: 9 bảng audit 5S
│   ├── audit_seed.py       # Seed data cứng cho đơn vị/bộ phận/tiêu chí 5S
│   ├── ingest.py           # Pipeline Excel → DB (async, có progress)
│   ├── stats.py            # Aggregation queries cho dashboard RCP
│   ├── services.py         # Helpers: normalize_header, to_int_money, classify_group
│   ├── schemas.py          # Pydantic response models cho /api/*
│   ├── settings.py         # Pydantic Settings (đọc .env)
│   └── run.py              # Entry point uvicorn
├── templates/              # Jinja2 templates (tiếng Việt)
│   ├── base.html           # Layout chung + Tailwind config
│   ├── homepage.html       # Portal chính
│   ├── home_rcp.html, dashboard_rcp.html, data_rcp.html, login_rcp.html, preview_below_target.html
│   ├── home_internal.html, ewb_guide.html
│   ├── 5s_admin.html, 5s_new.html, 5s_checklist.html, 5s_result.html, 5s_settings.html, 5s_hdkp.html
│   └── stitch/, HDKP/, admin ui mockup/, stitch_garment_internal_audit_app/   # Mockup Stitch (chỉ tham khảo, không dùng runtime)
├── static/                 # Ảnh + CSS
│   ├── app.css, Logo.png, GLWC_Logo.png, Marex.jpg
│   └── bien_preview/       # 20 ảnh biển báo 5S
├── data/uploads/           # File Excel người dùng upload (gitignored)
├── scripts/
│   └── seed_general_employees.sql   # SQL seed user thủ công qua psql/pgAdmin
├── app.py                  # Wrapper gọi app.run.main()
├── requirements.txt
├── .env.example
└── CLAUDE.md
```

## Chạy dev

```bash
# 1. Cài venv + deps
python -m venv .venv
.\.venv\Scripts\pip install -r requirements.txt

# 2. Config
copy .env.example .env
# Điền DATABASE_URL và SESSION_SECRET

# 3. Chạy
python app.py
# hoặc dev auto-reload:
python -m uvicorn app.main:app --host 0.0.0.0 --port 8012 --reload
```

Port mặc định `8012`. Nếu Windows TIME_WAIT giữ port, đổi `--port 8013`.

Smoke test không cần server:
```bash
python -c "from starlette.testclient import TestClient; from app.main import app; print(TestClient(app, follow_redirects=False).get('/health').json())"
```

Không có build step. Không có test suite.

## Environment (.env)

Bắt buộc: `DATABASE_URL`, `SESSION_SECRET`. `CREATE_TABLES_ON_STARTUP=true` sẽ tự tạo/migrate schema ở request đầu tiên. `ALLOW_LOCAL_INGEST=true` mở endpoint ingest từ file local (mặc định tắt).

## Auth flow

Middleware `_require_login` trong `main.py` bảo vệ mọi route trừ `/health`, `/static/*`, `/login`, `/logout`, `/rcp/login`, `/rcp/logout`. Unauthenticated → redirect `/login?next=<url>`.

Login chỉ check `ma_nv` trong bảng `general_employees` — không có password. Session lưu `{ma_nv, ho_ten, chuc_vu, don_vi, bo_phan, role}`.

Roles: `admin` (full + settings), `user` (audit + read).

Helpers: `_current_user(request)`, `_is_admin(request)`.

## Modules chính

| Prefix | Chức năng | Templates chính |
|---|---|---|
| `/` | Portal homepage | `homepage.html` |
| `/rcp/*` | Audit lương RCP: upload Excel, xem dashboard, tìm nhân viên dưới mức lương tối thiểu, cấu hình hanging line theo đơn vị | `home_rcp.html`, `dashboard_rcp.html`, `data_rcp.html`, `preview_below_target.html` |
| `/internal-audit/*` | Audit 5S nội bộ: chọn đơn vị/bộ phận → chấm điểm checklist → xem kết quả → quản lý hành động khắc phục (HDKP) | `home_internal.html`, `5s_*.html`, `ewb_guide.html` |
| `/api/*` | JSON API (stats, filters, ingest jobs, hanging-lines, export xlsx, CRUD audit) | — |

### RCP (audit lương tối thiểu vùng)

Excel input đọc sheet `"Luong ky nhan thang tong "` (có khoảng trắng cuối — cố ý). `ingest.py` chạy background thread, cập nhật `rcp_ingestjob`, poll qua `/api/ingest/jobs/{id}`. Header Excel normalize qua `services.normalize_header()`.

Endpoints chính: `/api/stats`, `/api/timeseries`, `/api/insights`, `/api/headcount`, `/api/employees/below-target`, `/api/below-target/breakdown`, `/api/hanging-lines`, `/api/export/below-target.xlsx`.

### 5S Internal Audit

Quy trình: `AuditDotKiemTra` (đợt, khoá `"YYYY-MM"`) → `AuditPhieuKiemTra` (một phiếu / bộ phận / đợt) → `AuditChiTietDiem` (điểm 0/1/2 theo tiêu chí) → `AuditHdkp` (hành động khắc phục cho điểm < 2).

Tiêu chí gom theo `AuditLinhVuc` (loại `5S` hoặc `TRUC_QUAN`), có thể gán ảnh biển (`AuditBien`). Bảng M2M `AuditApDung` quyết định tiêu chí nào áp dụng cho bộ phận nào.

Seed data cứng ở `audit_seed.py` — chạy `seed_if_empty(engine)` mỗi startup, chỉ seed khi bảng rỗng.

## Database

### Naming convention bảng

- `rcp_*` — module RCP
- `audit_5s_*` — module audit 5S
- `general_*` — dùng chung (auth)

### Naming convention cột

Tiếng Việt không dấu, snake_case (`ma_nv`, `ho_ten`, `don_vi`, `bo_phan`, `chuc_vu`, `so_diem`, `ky`, `ngay_kiem_tra`). Timestamps `created_at`, `updated_at`, `completed_at`. Money luôn là `int` VND (không dùng float).

### Migration pattern (không dùng Alembic)

`db.py::create_db_and_tables()` chạy 4 bước idempotent mỗi startup:

1. `_apply_rename_migrations()` — `ALTER TABLE ... RENAME` cho các bảng đã đổi tên (chạy TRƯỚC `create_all`)
2. `SQLModel.metadata.create_all()` — chỉ tạo bảng mới
3. `_apply_light_migrations()` — `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` + backfill
4. `_seed_users()` + `audit_seed.seed_if_empty()`

**Thêm cột mới:** update model + thêm block `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` trong `_apply_light_migrations()`.

## Convention code

- **Type hints đầy đủ** (Python 3.11+ syntax: `str | None`, không `Optional[str]` trong code mới; `audit_models.py` còn dùng `Optional` — không refactor trừ khi được yêu cầu).
- **Pydantic models** cho mọi request/response API — không trả `dict` thô cho endpoint JSON.
- **HTTPException với message tiếng Việt** cho lỗi user-facing.
- **Comment nghiệp vụ tiếng Việt, comment kỹ thuật tiếng Anh.**
- **Không viết raw SQL** trừ khi query phức tạp — dùng SQLModel/SQLAlchemy `select()`.
- **CSV multi-filter:** dùng helper `_split_multi()` + `_apply_csv_in()` (có trong `main.py` và `stats.py`).

## Design system (Tailwind config trong `base.html`)

Tokens chính (KHÔNG deviate):

```js
colors: { primary: "#002c50", "primary-container": "#005A9C", secondary: "#1b6d24",
          tertiary: "#00312a", surface: "#F9F9FA", "on-surface": "#1A1C1D" }
borderRadius: { full: "0.75rem" }   // KHÔNG 9999px — hình tròn dùng style="border-radius:50%"
```

Rules: không dùng divider 1px (dùng shift màu surface), không dùng pure black, không dùng viền card đậm. Headline **Manrope**, body **Inter**. Mobile-first, print-friendly CSS khi cần in biên bản.

Spec đầy đủ: `templates/stitch/equitas_core/DESIGN.md`. Các folder mockup Stitch (`templates/stitch/`, `templates/HDKP/`, `templates/admin ui mockup/`, `templates/stitch_garment_internal_audit_app/`) chỉ là reference/mockup — **không import** trong runtime templates.

## File tham chiếu nhanh

| Cần làm | Đọc file |
|---|---|
| Thêm route | `app/main.py` |
| Thêm bảng/cột | `app/models.py` hoặc `app/audit_models.py` + `app/db.py::_apply_light_migrations` |
| Thay đổi seed 5S | `app/audit_seed.py` |
| Thay đổi mapping cột Excel | `app/ingest.py` + `app/services.py::normalize_header` |
| Thêm aggregation dashboard | `app/stats.py` |
| Response schema mới | `app/schemas.py` |
| Config app | `app/settings.py` + `.env` |
| Design token | `templates/base.html` (block `tailwind-config`) |
