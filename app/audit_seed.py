"""
Hard-coded master data for the 5S / Trực quan internal audit module.
Call seed_if_empty(engine) once after create_all() on each startup.
Data source: DỰ THẢO - Checklist kiểm tra 5S, Trực quan - Toàn công ty (26-06-2026).xlsx
"""
from __future__ import annotations

from sqlalchemy import inspect as sa_inspect, text

# ─── Đơn vị ─────────────────────────────────────────────────────────────────
# (id, ma, ten)
DON_VI_DATA = [
    (1,  "BTB",       "Ban Tổng Bảo"),
    (2,  "P.KDXNK",   "Phòng Kinh Doanh Xuất Nhập Khẩu"),
    (3,  "P.KT",      "Phòng Kế Toán"),
    (4,  "P.QLCL",    "Phòng Quản Lý Chất Lượng"),
    (5,  "P.KTCĐ",    "Phòng Kỹ Thuật Cơ Điện"),
    (6,  "P.TH",      "Phòng Tổng Hợp"),
    (7,  "Trạm Y tế", "Trạm Y tế"),
    (8,  "P.KTCN",    "Phòng Kỹ Thuật Công Nghệ"),
    (9,  "Duy Trung",  "Cơ sở Duy Trung"),
    (10, "P.QTĐS",    "Phòng Quản Trị Đời Sống"),
    (11, "XNSX",      "Xí Nghiệp Sản Xuất"),
]

# ─── Bộ phận ─────────────────────────────────────────────────────────────────
# (id, don_vi_id, ten)
BO_PHAN_DATA = [
    (1,  1,  "Kho máy"),
    (2,  2,  "Kho nguyên liệu"),
    (3,  2,  "Kho phụ liệu"),
    (4,  2,  "Kho thành phẩm"),
    (5,  2,  "Kho thùng carton"),
    (6,  2,  "Kho khăn bông"),
    (7,  2,  "Kho vật tư"),
    (8,  2,  "Kho tồn"),
    (9,  5,  "Kho hóa chất"),
    (10, 9,  "Kho NPL, TP, MMTB"),
    (11, 2,  "Văn phòng"),
    (12, 3,  "Văn phòng"),
    (13, 4,  "Văn phòng"),
    (14, 5,  "Văn phòng"),
    (15, 6,  "Văn phòng"),
    (16, 7,  "Văn phòng"),
    (17, 8,  "Văn phòng"),
    (18, 8,  "May mẫu, cử gá lắp, cắt laser"),
    (19, 5,  "Hệ thống XLNT"),
    (20, 5,  "Kho CTNH"),
    (21, 5,  "Kho CTSH"),
    (22, 5,  "Lò hơi, máy nén khí, mộc, nề, cơ khí, SCL"),
    (23, 9,  "Lò hơi, máy nén khí"),
    (24, 10, "Nhà ăn A"),
    (25, 10, "Nhà ăn C"),
    (26, 10, "Nhà ăn Duy Trung"),
    (27, 11, "Cắt"),
    (28, 11, "May"),
    (29, 11, "Hoàn thành"),
    (30, 11, "Kỹ thuật"),
    (31, 11, "Bảo trì"),
    (32, 11, "Văn phòng"),
]

# ─── Lĩnh vực ─────────────────────────────────────────────────────────────────
# (id, loai, ma, ten, icon, thu_tu)
LINH_VUC_DATA = [
    (1, "5S",       "S1",     "S1 – Sàng lọc",                    "filter_alt",      1),
    (2, "5S",       "S2",     "S2 – Sắp xếp",                     "grid_view",       2),
    (3, "5S",       "S3",     "S3 – Sạch sẽ",                     "cleaning_services",3),
    (4, "TRUC_QUAN","HTQLCL", "Hệ thống Quản lý Chất lượng",      "verified",        1),
    (5, "TRUC_QUAN","6S",     "6S – An toàn",                     "security",        2),
    (6, "TRUC_QUAN","ATVSLD", "ATVSLĐ – PCCC",                    "health_and_safety",3),
    (7, "TRUC_QUAN","NQLD",   "Nội quy Lao động",                 "groups",          4),
]

# ─── Tiêu chí ─────────────────────────────────────────────────────────────────
# (id, linh_vuc_id, so_thu_tu, noi_dung)
TIEU_CHI_DATA = [
    # S1 — 4 criteria
    (1,  1, 1, "Khu vực sản xuất/phục vụ sản xuất không có vật dụng linh tinh, đồ đạc cá nhân, đồ đạc không liên quan đến sản xuất/phục vụ sản xuất."),
    (2,  1, 2, "Tài liệu, hồ sơ, sổ sách liên quan đến mã hàng cũ/khách hàng cũ được lưu trữ tại văn phòng làm việc/nơi lưu trữ, không còn tồn tại tại khu vực sản xuất."),
    (3,  1, 3, "MMTB/NPL/BTP/TP liên quan đến mã hàng cũ/khách hàng cũ phải được để trong khu vực cách ly riêng biệt."),
    (4,  1, 4, "Đồ dùng, trang thiết bị hư hỏng, không thể sửa chữa, sử dụng hoặc dự phòng được đưa ra ngoài khu vực sản xuất/phục vụ sản xuất và có bảng nhận diện."),
    # S2 — 8 criteria
    (5,  2, 1, "Vạch kẻ lối thoát hiểm/vạch đánh dấu vị trí để hàng hóa, vật dụng... rõ ràng, không bị bong tróc/bạc màu."),
    (6,  2, 2, "Biển báo/bảng thông tin/ký hiệu nhận diện rõ ràng, không bị hư hỏng, xuống cấp."),
    (7,  2, 3, "NPL/BTP/TP của từng khách hàng/mã hàng được để ở khu vực riêng biệt và có nhận dạng rõ ràng."),
    (8,  2, 4, "NPL/BTP/TP được bao bọc, đóng gói cẩn thận để hạn chế hư hỏng/xuống cấp."),
    (9,  2, 5, "NPL/BTP/TP có nhãn, mác thông tin và danh sách được cập nhật đầy đủ, thường xuyên."),
    (10, 2, 6, "Tài liệu kỹ thuật/hồ sơ/sổ sách ghi chép đang sản xuất/đang sử dụng được để đúng nơi quy định, sắp xếp gọn gàng, ngăn nắp, thuận tiện cho việc sử dụng."),
    (11, 2, 7, "Công cụ, dụng cụ, trang thiết bị, máy móc đang sản xuất/đang sử dụng được để đúng nơi quy định, sắp xếp gọn gàng, ngăn nắp, thuận tiện cho việc sử dụng."),
    (12, 2, 8, "MMTB/NPL/BTP/TP trong khu vực được phân loại rõ ràng theo quy định hiện hành."),
    # S3 — 1 criterion
    (13, 3, 1, "Mọi vật dụng/khu vực sạch sẽ, đáp ứng các tiêu chí: không bám bụi, khô ráo, không ẩm mốc, không có mùi khó chịu, không có côn trùng/động vật, không có vết ố/bẩn."),
    # HTQLCL — 5 criteria (shared text pattern for all Trực quan groups)
    (14, 4, 1, "Đầy đủ và đúng khu vực áp dụng."),
    (15, 4, 2, "Đúng nội dung, màu sắc, kích thước theo quy định."),
    (16, 4, 3, "Đúng vị trí, dễ nhìn, dễ nhận biết."),
    (17, 4, 4, "Tình trạng còn tốt, sử dụng được."),
    (18, 4, 5, "Người dùng hiểu đúng và có duy trì định kỳ."),
    # 6S — 5 criteria
    (19, 5, 1, "Đầy đủ và đúng khu vực áp dụng."),
    (20, 5, 2, "Đúng nội dung, màu sắc, kích thước theo quy định."),
    (21, 5, 3, "Đúng vị trí, dễ nhìn, dễ nhận biết."),
    (22, 5, 4, "Tình trạng còn tốt, sử dụng được."),
    (23, 5, 5, "Người dùng hiểu đúng và có duy trì định kỳ."),
    # ATVSLĐ — 5 criteria
    (24, 6, 1, "Đầy đủ và đúng khu vực áp dụng."),
    (25, 6, 2, "Đúng nội dung, màu sắc, kích thước theo quy định."),
    (26, 6, 3, "Đúng vị trí, dễ nhìn, dễ nhận biết."),
    (27, 6, 4, "Tình trạng còn tốt, sử dụng được."),
    (28, 6, 5, "Người dùng hiểu đúng và có duy trì định kỳ."),
    # NQLĐ — 5 criteria
    (29, 7, 1, "Đầy đủ và đúng khu vực áp dụng."),
    (30, 7, 2, "Đúng nội dung, màu sắc, kích thước theo quy định."),
    (31, 7, 3, "Đúng vị trí, dễ nhìn, dễ nhận biết."),
    (32, 7, 4, "Tình trạng còn tốt, sử dụng được."),
    (33, 7, 5, "Người dùng hiểu đúng và có duy trì định kỳ."),
]

# ─── Áp dụng (per-department applicability) ──────────────────────────────────
# Derived from Excel 'X' marks. Verified against TỔNG ĐIỂM column.

_TV_HTQLCL    = [14, 15, 16, 17, 18]
_TV_6S        = [19, 20, 21, 22, 23]
_TV_ATVSLD    = [24, 25, 26, 27, 28]
_TV_NQLD      = [29, 30, 31, 32, 33]
_TV_ALL       = _TV_HTQLCL + _TV_6S + _TV_ATVSLD + _TV_NQLD     # all 4 Trực quan groups
_TV_NO_HTQLCL = _TV_6S + _TV_ATVSLD + _TV_NQLD                   # 3 Trực quan groups

# Applicability patterns  (tieu_chi IDs)
_T1 = [1, 4, 5, 6, 8, 10, 11, 12, 13] + _TV_NO_HTQLCL           # BTB Kho máy         (24, max 48)
_T2 = list(range(1, 14)) + _TV_ALL                                # Full w/ HTQLCL       (33, max 66)
_T3 = [1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13] + _TV_NO_HTQLCL     # Kho nhỏ no HTQLCL   (26, max 52)
_T4 = [1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13] + _TV_ALL            # DT Kho NPL TP MMTB  (31, max 62)
_T5 = [2, 4, 6, 10, 11, 13] + _TV_NO_HTQLCL                      # Văn phòng           (21, max 42)
_T6 = list(range(1, 14)) + _TV_NO_HTQLCL                          # May mẫu/full 5S     (28, max 56)
_T7 = [1, 4, 5, 6, 10, 11, 13] + _TV_NO_HTQLCL                   # Utility/Nhà ăn      (22, max 44)
_T8 = [1, 2, 3, 4, 5, 6, 8, 10, 11, 12, 13] + _TV_ALL            # XNSX KT/BT          (31, max 62)

_BO_PHAN_TYPES: dict[int, list[int]] = {
    1: _T1,               # BTB – Kho máy
    2: _T2,  3: _T2,  4: _T2,  5: _T2,      # P.KDXNK kho lớn
    6: _T3,  7: _T3,  8: _T3,  9: _T3,      # P.KDXNK/KTCĐ kho nhỏ
    10: _T4,                                  # Duy Trung – Kho NPL TP MMTB
    11: _T5, 12: _T5, 13: _T5, 14: _T5,
    15: _T5, 16: _T5, 17: _T5,              # Các văn phòng
    18: _T6,                                  # P.KTCN – May mẫu
    19: _T7, 20: _T7, 21: _T7, 22: _T7,
    23: _T7, 24: _T7, 25: _T7, 26: _T7,    # Utility & nhà ăn
    27: _T2, 28: _T2, 29: _T2,             # XNSX – Cắt/May/Hoàn thành
    30: _T8, 31: _T8,                       # XNSX – Kỹ thuật/Bảo trì
    32: _T5,                                 # XNSX – Văn phòng
}

AP_DUNG_DATA: list[tuple[int, int]] = [
    (bp_id, tc_id)
    for bp_id, tc_ids in _BO_PHAN_TYPES.items()
    for tc_id in tc_ids
]


def seed_if_empty(engine) -> None:
    insp = sa_inspect(engine)
    if "audit_5s_don_vi" not in insp.get_table_names():
        return

    with engine.begin() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM audit_5s_don_vi")).scalar()
        if count and count > 0:
            return

        conn.execute(
            text("INSERT INTO audit_5s_don_vi (id, ma, ten) VALUES (:id, :ma, :ten)"),
            [{"id": i, "ma": m, "ten": t} for i, m, t in DON_VI_DATA],
        )
        conn.execute(
            text("INSERT INTO audit_5s_bo_phan (id, don_vi_id, ten) VALUES (:id, :don_vi_id, :ten)"),
            [{"id": i, "don_vi_id": d, "ten": t} for i, d, t in BO_PHAN_DATA],
        )
        conn.execute(
            text(
                "INSERT INTO audit_5s_linh_vuc (id, loai, ma, ten, icon, thu_tu) "
                "VALUES (:id, :loai, :ma, :ten, :icon, :thu_tu)"
            ),
            [{"id": i, "loai": lo, "ma": m, "ten": t, "icon": ic, "thu_tu": tt}
             for i, lo, m, t, ic, tt in LINH_VUC_DATA],
        )
        conn.execute(
            text(
                "INSERT INTO audit_5s_tieu_chi (id, linh_vuc_id, so_thu_tu, noi_dung) "
                "VALUES (:id, :linh_vuc_id, :so_thu_tu, :noi_dung)"
            ),
            [{"id": i, "linh_vuc_id": lv, "so_thu_tu": n, "noi_dung": nd}
             for i, lv, n, nd in TIEU_CHI_DATA],
        )
        conn.execute(
            text(
                "INSERT INTO audit_5s_ap_dung (bo_phan_id, tieu_chi_id) "
                "VALUES (:bo_phan_id, :tieu_chi_id)"
            ),
            [{"bo_phan_id": bp, "tieu_chi_id": tc} for bp, tc in AP_DUNG_DATA],
        )

        # Reset sequences so future auto-increments don't collide with seeded IDs
        for tbl, col in [
            ("audit_5s_don_vi", "id"),
            ("audit_5s_bo_phan", "id"),
            ("audit_5s_linh_vuc", "id"),
            ("audit_5s_tieu_chi", "id"),
        ]:
            conn.execute(
                text(
                    f"SELECT setval(pg_get_serial_sequence('{tbl}', '{col}'), "
                    f"(SELECT MAX({col}) FROM {tbl}))"
                )
            )
