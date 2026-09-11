from __future__ import annotations

import argparse
import html
import logging
import smtplib
import threading
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from email.message import EmailMessage
from email.utils import formataddr
from urllib.parse import urlencode
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import text
from sqlmodel import Session, select

from app.audit_models import AuditReminderLog, AuditReminderSetting
from app.db import engine
from app.models import GeneralEmployee
from app.settings import settings

logger = logging.getLogger(__name__)

_REMINDER_LOCK_ID = 5_051_202_609
_scheduler_lock = threading.Lock()
_scheduler_thread: threading.Thread | None = None
_DON_VI_ALIASES = {"XNDT", "XN Duy Trung", "Duy Trung"}
_LOAI_LABEL = {"5S": "5S", "TRUC_QUAN": "Trực quan", "DAY_DU": "Đầy đủ"}


def _timezone(name: str | None = None):
    timezone_name = name or settings.audit_reminder_timezone
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        if timezone_name == "Asia/Bangkok":
            return timezone(timedelta(hours=7), name="Asia/Bangkok")
        raise


def _scheduled_time() -> time:
    try:
        return time.fromisoformat(settings.audit_reminder_time)
    except ValueError as exc:
        raise ValueError("AUDIT_REMINDER_TIME phải có định dạng HH:MM") from exc


def smtp_is_configured() -> bool:
    sender = settings.smtp_from_email.strip() or settings.smtp_username.strip()
    return bool(
        settings.smtp_host.strip()
        and sender
        and (not settings.smtp_username.strip() or settings.smtp_password)
    )


def get_reminder_setting(session: Session) -> AuditReminderSetting:
    setting = session.get(AuditReminderSetting, 1)
    if setting:
        return setting
    session.execute(text("""
        INSERT INTO audit_5s_reminder_setting
            (id, enabled, frequency, weekday, send_time, timezone, updated_by, updated_at)
        VALUES
            (1, :enabled, :frequency, :weekday, :send_time, :timezone, '', NOW())
        ON CONFLICT (id) DO NOTHING
    """), {
        "enabled": settings.audit_reminder_enabled,
        "frequency": settings.audit_reminder_frequency,
        "weekday": settings.audit_reminder_weekday,
        "send_time": settings.audit_reminder_time,
        "timezone": settings.audit_reminder_timezone,
    })
    session.commit()
    setting = session.get(AuditReminderSetting, 1)
    if not setting:
        raise RuntimeError("Không thể khởi tạo cấu hình nhắc email")
    return setting


def reminder_setting_payload(setting: AuditReminderSetting) -> dict:
    return {
        "enabled": setting.enabled,
        "frequency": setting.frequency,
        "weekday": setting.weekday,
        "send_time": setting.send_time,
        "timezone": setting.timezone,
        "updated_by": setting.updated_by,
        "updated_at": setting.updated_at.isoformat() if setting.updated_at else None,
        "smtp_configured": smtp_is_configured(),
    }


def schedule_matches(setting: AuditReminderSetting, now: datetime) -> bool:
    scheduled = time.fromisoformat(setting.send_time)
    matches_day = (
        setting.frequency == "daily"
        or (setting.frequency == "weekly" and now.weekday() == setting.weekday)
    )
    return matches_day and now.time() >= scheduled


def _split_emails(value: str) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for part in value.replace(";", ",").split(","):
        email = part.strip()
        key = email.lower()
        if email and "@" in email and key not in seen:
            seen.add(key)
            result.append(email)
    return result


def _recipient_map(session: Session) -> dict[str, list[str]]:
    recipients: dict[str, list[str]] = defaultdict(list)
    seen: dict[str, set[str]] = defaultdict(set)
    employees = session.exec(select(GeneralEmployee).where(GeneralEmployee.email != "")).all()
    for employee in employees:
        email = (employee.email or "").strip()
        don_vi = (employee.don_vi or "").strip()
        if not email or not don_vi or email.lower() in seen[don_vi]:
            continue
        recipients[don_vi].append(email)
        seen[don_vi].add(email.lower())
    return recipients


def _emails_for_unit(recipient_map: dict[str, list[str]], ma: str, ten: str) -> list[str]:
    values = {ma.strip(), ten.strip()}
    if values & _DON_VI_ALIASES:
        values |= _DON_VI_ALIASES
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        for email in recipient_map.get(value, []):
            if email.lower() not in seen:
                seen.add(email.lower())
                result.append(email)
    return sorted(result, key=str.lower)


def collect_due_reminders(session: Session, today: date) -> list[dict]:
    rows = session.execute(text("""
        SELECT
            cd.id AS chi_tiet_diem_id,
            cd.phieu_id,
            COALESCE(p.completed_at, p.created_at)::date AS phieu_ngay,
            dv.id AS don_vi_id,
            dv.ma AS don_vi_ma,
            dv.ten AS don_vi_ten,
            bp.ten AS bo_phan_ten,
            lv.loai,
            tc.noi_dung AS tieu_chi_noi_dung,
            COALESCE(cd.ghi_chu, '') AS ghi_chu,
            COALESCE(h.hanh_dong_kp, '') AS hanh_dong_kp,
            COALESCE(h.nguoi_thuc_hien, '') AS nguoi_thuc_hien,
            h.thoi_han,
            COALESCE(h.tinh_trang, 'Chưa tiếp nhận') AS tinh_trang
        FROM audit_5s_chi_tiet_diem cd
        JOIN audit_5s_phieu_kiem_tra p ON p.id = cd.phieu_id
        JOIN audit_5s_bo_phan bp ON bp.id = p.bo_phan_id
        JOIN audit_5s_don_vi dv ON dv.id = bp.don_vi_id
        JOIN audit_5s_tieu_chi tc ON tc.id = cd.tieu_chi_id
        JOIN audit_5s_linh_vuc lv ON lv.id = tc.linh_vuc_id
        LEFT JOIN audit_5s_hdkp h ON h.chi_tiet_diem_id = cd.id
        WHERE cd.diem = 0
          AND (
            h.id IS NULL
            OR h.tinh_trang IN ('Chưa tiếp nhận', 'Chưa được tiếp nhận')
            OR (h.tinh_trang = 'Đang thực hiện' AND h.thoi_han IS NOT NULL AND h.thoi_han < :today)
          )
        ORDER BY dv.ma, bp.ten, phieu_ngay, cd.id
    """), {"today": today}).mappings().all()
    return [dict(row) for row in rows]


def _format_date(value) -> str:
    return value.strftime("%d/%m/%Y") if value else "—"


def _build_message(unit: dict, items: list[dict], recipients: list[str], cc: list[str], today: date) -> EmailMessage:
    unit_label = f"{unit['don_vi_ma']} – {unit['don_vi_ten']}"
    pending_count = sum(item["tinh_trang"] in {"Chưa tiếp nhận", "Chưa được tiếp nhận"} for item in items)
    overdue_count = len(items) - pending_count
    subject = f"[HĐKP] Nhắc xử lý {len(items)} hành động – {unit['don_vi_ma']} – {_format_date(today)}"
    query = urlencode({"don_vi_id": unit["don_vi_id"]})
    action_url = f"{settings.audit_reminder_base_url.rstrip('/')}/internal-audit/5s/hdkp?{query}"

    rows_html = []
    plain_rows = []
    for index, item in enumerate(items, 1):
        is_overdue = item["tinh_trang"] == "Đang thực hiện"
        status_color = "#b42318" if is_overdue else "#9a6700"
        criterion = html.escape(item["tieu_chi_noi_dung"] or "")
        note = html.escape(item["ghi_chu"] or "")
        criterion_html = criterion + (f"<br><span style='color:#667085;font-size:12px'>{note}</span>" if note else "")
        rows_html.append(f"""
          <tr>
            <td style="{_td_style()}text-align:center">{index}</td>
            <td style="{_td_style()}">{html.escape(item['bo_phan_ten'] or '')}</td>
            <td style="{_td_style()}white-space:nowrap">{_format_date(item['phieu_ngay'])}</td>
            <td style="{_td_style()}">{html.escape(_LOAI_LABEL.get(item['loai'], item['loai'] or ''))}</td>
            <td style="{_td_style()}">{criterion_html}</td>
            <td style="{_td_style()}">{html.escape(item['hanh_dong_kp'] or '—')}</td>
            <td style="{_td_style()}">{html.escape(item['nguoi_thuc_hien'] or '—')}</td>
            <td style="{_td_style()}white-space:nowrap">{_format_date(item['thoi_han'])}</td>
            <td style="{_td_style()}font-weight:700;color:{status_color}">{html.escape(item['tinh_trang'])}{' (quá hạn)' if is_overdue else ''}</td>
          </tr>""")
        plain_rows.append(
            f"{index}. {item['bo_phan_ten']} | {item['tieu_chi_noi_dung']} | "
            f"Hạn: {_format_date(item['thoi_han'])} | {item['tinh_trang']}{' (quá hạn)' if is_overdue else ''}"
        )

    html_body = f"""<!doctype html>
<html lang="vi"><body style="margin:0;background:#f5f7fa;font-family:Arial,sans-serif;color:#1f2937">
  <div style="max-width:1180px;margin:0 auto;padding:24px">
    <div style="background:#fff;border:1px solid #e4e7ec;border-radius:10px;padding:28px">
      <p>Kính gửi <strong>Quý đơn vị {html.escape(unit_label)}</strong>,</p>
      <p>Hệ thống ghi nhận <strong>{len(items)} HĐKP cần xử lý</strong>, gồm
        <strong>{pending_count}</strong> mục chưa tiếp nhận và <strong>{overdue_count}</strong> mục đang thực hiện đã quá hạn.</p>
      <table role="presentation" style="width:100%;border-collapse:collapse;margin:20px 0;font-size:13px">
        <thead><tr style="background:#003b68;color:#fff">
          <th style="{_th_style()}">STT</th><th style="{_th_style()}">Bộ phận</th>
          <th style="{_th_style()}">Lần ĐG</th><th style="{_th_style()}">Loại</th>
          <th style="{_th_style()}">Tiêu chí / ghi chú lỗi</th><th style="{_th_style()}">Hành động KP</th>
          <th style="{_th_style()}">Người thực hiện</th><th style="{_th_style()}">Thời hạn</th>
          <th style="{_th_style()}">Tình trạng</th>
        </tr></thead>
        <tbody>{''.join(rows_html)}</tbody>
      </table>
      <p>Kính đề nghị quý đơn vị truy cập
        <a href="{html.escape(action_url)}" style="color:#005a9c;font-weight:700">TỔNG HỢP HÀNH ĐỘNG KHẮC PHỤC</a>
        để tiếp nhận, thực hiện và cập nhật tình trạng.</p>
      <p style="color:#d92d20;font-weight:700">LƯU Ý: Đây là email tự động từ hệ thống, vui lòng không phản hồi email này.</p>
      <p>Trân trọng,<br><strong>Hệ thống Đánh giá nội bộ</strong></p>
    </div>
  </div>
</body></html>"""
    plain_body = (
        f"Kính gửi Quý đơn vị {unit_label},\n\n"
        f"Hệ thống ghi nhận {len(items)} HĐKP cần xử lý: {pending_count} chưa tiếp nhận, "
        f"{overdue_count} đang thực hiện đã quá hạn.\n\n" + "\n".join(plain_rows) +
        f"\n\nTruy cập: {action_url}\n\nĐây là email tự động, vui lòng không phản hồi."
    )

    message = EmailMessage()
    sender = settings.smtp_from_email.strip() or settings.smtp_username.strip()
    message["Subject"] = subject
    message["From"] = formataddr((settings.smtp_from_name, sender))
    message["To"] = ", ".join(recipients)
    if cc:
        message["Cc"] = ", ".join(cc)
    message.set_content(plain_body)
    message.add_alternative(html_body, subtype="html")
    return message


def _td_style() -> str:
    return "border:1px solid #d0d5dd;padding:9px 8px;vertical-align:top;line-height:1.4;"


def _th_style() -> str:
    return "border:1px solid #fff;padding:10px 8px;text-align:left;vertical-align:middle;"


def _validate_smtp_settings() -> None:
    sender = settings.smtp_from_email.strip() or settings.smtp_username.strip()
    missing = []
    if not settings.smtp_host.strip():
        missing.append("SMTP_HOST")
    if not sender:
        missing.append("SMTP_FROM_EMAIL hoặc SMTP_USERNAME")
    if settings.smtp_username.strip() and not settings.smtp_password:
        missing.append("SMTP_PASSWORD")
    if missing:
        raise RuntimeError("Thiếu cấu hình email: " + ", ".join(missing))


def _send_message(message: EmailMessage) -> None:
    smtp_class = smtplib.SMTP_SSL if settings.smtp_use_ssl else smtplib.SMTP
    with smtp_class(settings.smtp_host, settings.smtp_port, timeout=30) as smtp:
        if not settings.smtp_use_ssl:
            smtp.ehlo()
            if settings.smtp_use_tls:
                smtp.starttls()
                smtp.ehlo()
        if settings.smtp_username:
            smtp.login(settings.smtp_username, settings.smtp_password)
        smtp.send_message(message)


def run_audit_reminders(*, dry_run: bool = False, today: date | None = None) -> dict:
    reminder_date = today or datetime.now(_timezone()).date()
    if not dry_run:
        _validate_smtp_settings()
        AuditReminderLog.__table__.create(engine, checkfirst=True)

    lock_connection = None
    if not dry_run:
        lock_connection = engine.connect()
        acquired = lock_connection.execute(
            text("SELECT pg_try_advisory_lock(:lock_id)"), {"lock_id": _REMINDER_LOCK_ID}
        ).scalar_one()
        if not acquired:
            lock_connection.close()
            return {"date": reminder_date.isoformat(), "status": "locked", "sent": 0}

    summary = {"date": reminder_date.isoformat(), "status": "ok", "sent": 0, "skipped": 0, "errors": [], "units": []}
    try:
        with Session(engine) as session:
            rows = collect_due_reminders(session, reminder_date)
            grouped: dict[int, list[dict]] = defaultdict(list)
            for row in rows:
                grouped[row["don_vi_id"]].append(row)
            recipient_map = _recipient_map(session)
            cc = _split_emails(settings.audit_reminder_cc)

            for don_vi_id, items in grouped.items():
                unit = items[0]
                recipients = _emails_for_unit(recipient_map, unit["don_vi_ma"], unit["don_vi_ten"])
                unit_summary = {
                    "don_vi_id": don_vi_id,
                    "don_vi_ma": unit["don_vi_ma"],
                    "items": len(items),
                    "recipients": recipients,
                }
                summary["units"].append(unit_summary)
                if not recipients:
                    unit_summary["status"] = "no_recipients"
                    summary["skipped"] += 1
                    continue
                if not dry_run:
                    already_sent = session.exec(
                        select(AuditReminderLog).where(
                            AuditReminderLog.reminder_date == reminder_date,
                            AuditReminderLog.don_vi_id == don_vi_id,
                        )
                    ).first()
                    if already_sent:
                        unit_summary["status"] = "already_sent"
                        summary["skipped"] += 1
                        continue

                message = _build_message(unit, items, recipients, cc, reminder_date)
                unit_summary["subject"] = str(message["Subject"])
                if dry_run:
                    unit_summary["status"] = "dry_run"
                    continue
                try:
                    _send_message(message)
                    session.add(AuditReminderLog(
                        reminder_date=reminder_date,
                        don_vi_id=don_vi_id,
                        recipient_emails=", ".join(recipients),
                        item_count=len(items),
                        subject=str(message["Subject"]),
                    ))
                    session.commit()
                    unit_summary["status"] = "sent"
                    summary["sent"] += 1
                except Exception as exc:
                    session.rollback()
                    unit_summary["status"] = "error"
                    unit_summary["error"] = str(exc)
                    summary["errors"].append({"don_vi_ma": unit["don_vi_ma"], "error": str(exc)})
                    logger.exception("Không thể gửi email nhắc HĐKP cho %s", unit["don_vi_ma"])
        return summary
    finally:
        if lock_connection is not None:
            try:
                lock_connection.execute(text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": _REMINDER_LOCK_ID})
            finally:
                lock_connection.close()


def send_test_reminder(test_email: str, *, unit_code: str = "P.KDXNK", today: date | None = None) -> dict:
    """Send one real-format reminder only to the requested test mailbox, without writing the send log."""
    recipients = _split_emails(test_email)
    if len(recipients) != 1:
        raise ValueError("--test-email phải là một địa chỉ email hợp lệ")
    _validate_smtp_settings()
    reminder_date = today or datetime.now(_timezone()).date()

    with Session(engine) as session:
        rows = collect_due_reminders(session, reminder_date)
    matching = [row for row in rows if str(row["don_vi_ma"]).casefold() == unit_code.strip().casefold()]
    if not matching:
        available = sorted({str(row["don_vi_ma"]) for row in rows})
        raise ValueError(
            f"Không có HĐKP cần nhắc cho đơn vị {unit_code}. "
            f"Đơn vị đang có dữ liệu: {', '.join(available) or 'không có'}"
        )

    message = _build_message(matching[0], matching, recipients, [], reminder_date)
    message.replace_header("Subject", "[TEST] " + str(message["Subject"]))
    _send_message(message)
    return {
        "status": "test_sent",
        "unit": matching[0]["don_vi_ma"],
        "items": len(matching),
        "recipient": recipients[0],
        "subject": str(message["Subject"]),
        "send_log_written": False,
    }


def _scheduler_loop() -> None:
    last_attempt_key: tuple | None = None
    poll_seconds = max(15, settings.audit_reminder_poll_seconds)
    logger.info("Scheduler nhắc HĐKP đã sẵn sàng")
    while True:
        try:
            with Session(engine) as session:
                reminder_setting = get_reminder_setting(session)
            if reminder_setting.enabled:
                now = datetime.now(_timezone(reminder_setting.timezone))
                attempt_key = (
                    now.date(), reminder_setting.frequency, reminder_setting.weekday,
                    reminder_setting.send_time, reminder_setting.timezone,
                )
                if schedule_matches(reminder_setting, now) and last_attempt_key != attempt_key:
                    last_attempt_key = attempt_key
                    result = run_audit_reminders(today=now.date())
                    logger.info("Kết quả nhắc HĐKP: %s", result)
        except Exception:
            logger.exception("Job nhắc HĐKP thất bại")
        threading.Event().wait(poll_seconds)


def start_audit_reminder_scheduler() -> None:
    global _scheduler_thread
    with _scheduler_lock:
        if _scheduler_thread and _scheduler_thread.is_alive():
            return
        AuditReminderLog.__table__.create(engine, checkfirst=True)
        AuditReminderSetting.__table__.create(engine, checkfirst=True)
        with Session(engine) as session:
            get_reminder_setting(session)
        _scheduler_thread = threading.Thread(
            target=_scheduler_loop,
            name="audit-hdkp-reminder",
            daemon=True,
        )
        _scheduler_thread.start()


def main() -> None:
    parser = argparse.ArgumentParser(description="Gửi email nhắc HĐKP theo đơn vị")
    parser.add_argument("--dry-run", action="store_true", help="Chỉ xem các nhóm sẽ gửi, không gửi email")
    parser.add_argument("--test-email", help="Gửi một email mẫu duy nhất tới địa chỉ test")
    parser.add_argument("--unit", default="P.KDXNK", help="Mã đơn vị dùng cho email test (mặc định: P.KDXNK)")
    args = parser.parse_args()
    if args.dry_run and args.test_email:
        parser.error("Chỉ dùng một trong hai tùy chọn --dry-run hoặc --test-email")
    if args.test_email:
        print(send_test_reminder(args.test_email, unit_code=args.unit))
    else:
        print(run_audit_reminders(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
