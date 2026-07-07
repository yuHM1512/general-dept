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
    (1, "5S",       "S1",     "S1 – Sàng lọc",                    "filter_alt",       1),
    (2, "5S",       "S2",     "S2 – Sắp xếp",                     "grid_view",        2),
    (3, "5S",       "S3",     "S3 – Sạch sẽ",                     "cleaning_services",3),
    (4, "TRUC_QUAN","HTQLCL", "Hệ thống Quản lý Chất lượng",      "verified",         1),
    (5, "TRUC_QUAN","6S",     "6S – An toàn",                     "security",         2),
    (6, "TRUC_QUAN","ATVSLD", "ATVSLĐ – PCCC",                    "health_and_safety",3),
    (7, "TRUC_QUAN","NQLD",   "Nội quy Lao động",                 "groups",           4),
]

# ─── Biển (Trực quan signs) ──────────────────────────────────────────────────
# (id, linh_vuc_id, ten_goi, mo_ta, kich_thuoc, so_thu_tu)
BIEN_DATA = [
    # HTQLCL (linh_vuc_id=4)
    (1, 4,
     "Biển trạng thái hàng – BTP CHƯA HOÀN CHỈNH",
     "Nền xanh dương, chữ trắng. Dùng cho NPL/BTP/TP/MMTB đã đạt yêu cầu chất lượng nhưng chưa đủ số lượng hoặc chưa đồng bộ (chưa đủ bộ), chưa sẵn sàng chuyển công đoạn tiếp theo.",
     "200 x 100", 1),
    (2, 4,
     "Biển trạng thái hàng – HÀNG CHỜ KIỂM / HÀNG CHỜ ỦI",
     "Nền vàng, chữ đỏ. Dùng cho NPL/BTP/TP/MMTB chưa đạt yêu cầu chất lượng, đang chờ xử lý cần thiết (sửa, phân loại, đánh giá lại...) trước khi quyết định chuyển tiếp/giữ lại.",
     "200 x 100", 2),
    (3, 4,
     "Biển trạng thái hàng – HÀNG ĐẠT",
     "Nền xanh lá, chữ trắng. Dùng cho NPL/BTP/TP/MMTB đạt yêu cầu chất lượng và đủ điều kiện chuyển sang công đoạn/công việc tiếp theo.",
     "200 x 100", 3),
    (4, 4,
     "Biển trạng thái hàng – HÀNG LỖI / CHỜ XỬ LÝ",
     "Nền đỏ, chữ trắng. Dùng cho NPL/BTP/TP/MMTB không đạt yêu cầu chất lượng, cần cách ly và xử lý theo quy định (sửa/tái chế/loại bỏ).",
     "200 x 100", 4),
    # 6S – An toàn (linh_vuc_id=5)
    (5, 5,
     "Biển thông tin khu vực",
     "Khu vực chính (phạm vi lớn): nền xanh dương, chữ trắng. Khu vực phụ (phạm vi nhỏ): nền trắng, chữ xanh dương. Thể hiện tên bộ phận/khu vực và thông tin nhận diện cần thiết (mã khu vực, người phụ trách, quy định đặc thù nếu có).",
     "200 x 100", 1),
    (6, 5,
     "Biển thông tin vật dụng",
     "Nền vàng/trắng/cam/hồng, chữ đen (đồng bộ màu nền cho từng nhóm đối tượng/mục đích, khác với các màu đã quy định ở các nhóm trên). Dùng định danh vật dụng/đồ đạc/vị trí, giúp nhận biết và sắp xếp đúng chuẩn 5S.",
     "100 x 100", 2),
    (7, 5,
     "Bảng năng suất – chất lượng",
     "Đặt tại khu vực cuối chuyền may. Thể hiện kết quả năng suất, chất lượng so với mục tiêu. Quy ước màu: Đỏ = không đạt mục tiêu; Xanh lá đậm = đạt mục tiêu.",
     "Theo quy cách màn hình Tivi", 3),
    (8, 5,
     "Đèn tín hiệu (Andon) trên chuyền",
     "Lắp tại các khu vực trên chuyền may. Báo hiệu nhu cầu hỗ trợ: Đỏ = cần thợ máy; Vàng = cần tổ trưởng/tổ phó; Xanh = cần kỹ thuật.",
     "Theo quy cách có sẵn từ nhà cung cấp", 4),
    # ATVSLĐ – PCCC (linh_vuc_id=6)
    (9,  6,
     "Thông tin PCCC & CNCH",
     "Nền trắng, chữ xanh/đỏ. Cung cấp thông tin/nhận diện liên quan đến PCCC & CNCH tại khu vực.",
     "100 x 50", 1),
    (10, 6,
     "Biển CẤM",
     "Nền trắng, biểu tượng cấm hình tròn, viền đỏ, chữ đen/đỏ. Quy định các hành vi nghiêm cấm trong khu vực áp dụng (xem Phụ lục 1 – Danh mục biển cấm).",
     "50 x 50", 2),
    (11, 6,
     "Biển CẢNH BÁO NGUY HIỂM",
     "Nền vàng, biểu tượng cảnh báo hình tam giác, nội dung/biểu tượng đen. Cảnh báo các mối nguy hiểm hữu tại khu vực/máy thiết bị (xem Phụ lục 2 – Danh mục biển bắt buộc).",
     "50 x 50", 3),
    (12, 6,
     "Biển BẮT BUỘC",
     "Nền xanh dương, biểu tượng bắt buộc hình tròn, nội dung trắng hoặc đen (chọn 1 kiểu thống nhất). Quy định các yêu cầu an toàn bắt buộc phải tuân thủ (PPE, thao tác an toàn...) (xem Phụ lục 3 – Danh mục biển cảnh báo).",
     "50 x 50", 4),
    (13, 6,
     "Biển KHẨN CẤP / THOÁT HIỂM",
     "Nền xanh lá, chữ trắng. Chỉ dẫn lối thoát hiểm/thoát nạn và nhận diện các trang thiết bị khẩn cấp (xem Phụ lục 4 – Danh mục biển khẩn cấp).",
     "200 x 100", 5),
    (14, 6,
     "Mũi tên chỉ hướng thoát hiểm",
     "Nền đỏ hoặc vàng. Dán/vẽ để chỉ hướng lối đi thoát hiểm. Số lượng, mật độ, kích thước bố trí tùy theo phạm vi áp dụng.",
     "150 x 100", 6),
    (15, 6,
     'Vạch "không để vật cản" trước thiết bị khẩn cấp',
     "Nền vàng. Dán/vẽ tại khu vực bố trí thiết bị PCCC/túi thuốc/cầu thang thoát hiểm để đảm bảo luôn thông thoáng.",
     "Độ rộng bản 50–60", 7),
    (16, 6,
     "Vạch phân cách lối thoát hiểm",
     "Nền vàng. Dán/vẽ trên các lối đi thoát hiểm để phân định phạm vi lối đi.",
     "Độ rộng bản 50–60", 8),
    (17, 6,
     "Khu vực không để vật dụng cản trở lối vào/ra",
     "Nền đỏ, sọc xéo. Dán/vẽ tại cửa chính lớn của một số bộ phận để giữ thông thoáng lối ra vào.",
     "Độ rộng bản 50–60", 9),
    (18, 6,
     "Khu vực đậu/đỗ xe",
     "Nền vàng, sọc xéo. Dán/vẽ tại khu vực xe chờ bốc xếp hàng hoặc đang xếp hàng theo quy định.",
     "Độ rộng bản 50–60", 10),
    # Nội quy Lao động (linh_vuc_id=7)
    (19, 7,
     "Thẻ có dây đeo",
     "Hình chữ nhật, gắn dây đeo cổ. Thể hiện thông tin cơ bản của người đeo và phạm vi/quyền hạn sử dụng thẻ (nếu có), kèm lưu ý quan trọng trong quá trình sử dụng (xem tài liệu CTPAT – Quy trình kiểm soát sự tiếp cận công ty).",
     "80 x 50", 1),
    (20, 7,
     "Khu vực cấm/hạn chế",
     "Nền trắng, chữ xanh/đỏ. Thể hiện nội dung cấm hoặc hạn chế tiếp cận đối với một số đối tượng theo quy định (xem tài liệu CTPAT – Quy trình kiểm soát sự tiếp cận công ty).",
     "300 x 100", 2),
]

# ─── Tiêu chí ─────────────────────────────────────────────────────────────────
# (id, linh_vuc_id, bien_id, so_thu_tu, noi_dung)
# 5S criteria: bien_id=None
# TRUC_QUAN criteria: bien_id = bien's id; generated from BIEN_DATA below

_5S_TIEU_CHI = [
    # S1 — Sàng lọc
    (1,  1, None, 1, "Khu vực sản xuất/phục vụ sản xuất không có vật dụng linh tinh, đồ đạc cá nhân, đồ đạc không liên quan đến sản xuất/phục vụ sản xuất."),
    (2,  1, None, 2, "Tài liệu, hồ sơ, sổ sách liên quan đến mã hàng cũ/khách hàng cũ được lưu trữ tại văn phòng làm việc/nơi lưu trữ, không còn tồn tại tại khu vực sản xuất."),
    (3,  1, None, 3, "MMTB/NPL/BTP/TP liên quan đến mã hàng cũ/khách hàng cũ phải được để trong khu vực cách ly riêng biệt."),
    (4,  1, None, 4, "Đồ dùng, trang thiết bị hư hỏng, không thể sửa chữa, sử dụng hoặc dự phòng được đưa ra ngoài khu vực sản xuất/phục vụ sản xuất và có bảng nhận diện."),
    # S2 — Sắp xếp
    (5,  2, None, 1, "Vạch kẻ lối thoát hiểm/vạch đánh dấu vị trí để hàng hóa, vật dụng... rõ ràng, không bị bong tróc/bạc màu."),
    (6,  2, None, 2, "Biển báo/bảng thông tin/ký hiệu nhận diện rõ ràng, không bị hư hỏng, xuống cấp."),
    (7,  2, None, 3, "NPL/BTP/TP của từng khách hàng/mã hàng được để ở khu vực riêng biệt và có nhận dạng rõ ràng."),
    (8,  2, None, 4, "NPL/BTP/TP được bao bọc, đóng gói cẩn thận để hạn chế hư hỏng/xuống cấp."),
    (9,  2, None, 5, "NPL/BTP/TP có nhãn, mác thông tin và danh sách được cập nhật đầy đủ, thường xuyên."),
    (10, 2, None, 6, "Tài liệu kỹ thuật/hồ sơ/sổ sách ghi chép đang sản xuất/đang sử dụng được để đúng nơi quy định, sắp xếp gọn gàng, ngăn nắp, thuận tiện cho việc sử dụng."),
    (11, 2, None, 7, "Công cụ, dụng cụ, trang thiết bị, máy móc đang sản xuất/đang sử dụng được để đúng nơi quy định, sắp xếp gọn gàng, ngăn nắp, thuận tiện cho việc sử dụng."),
    (12, 2, None, 8, "MMTB/NPL/BTP/TP trong khu vực được phân loại rõ ràng theo quy định hiện hành."),
    # S3 — Sạch sẽ
    (13, 3, None, 1, "Mọi vật dụng/khu vực sạch sẽ, đáp ứng các tiêu chí: không bám bụi, khô ráo, không ẩm mốc, không có mùi khó chịu, không có côn trùng/động vật, không có vết ố/bẩn."),
]

# 5 criteria that repeat for every biển
_BIEN_CRITERIA = [
    "Đầy đủ và đúng khu vực áp dụng.",
    "Đúng nội dung, màu sắc, kích thước theo quy định.",
    "Đúng vị trí, dễ nhìn, dễ nhận biết.",
    "Tình trạng còn tốt, sử dụng được.",
    "Người dùng hiểu đúng và có duy trì định kỳ.",
]

# Generate TRUC_QUAN tieu_chi: IDs 14–113 (20 biển × 5 criteria = 100 rows)
_tv_tc: list[tuple] = []
_tc_id = 14
for _b_id, _lv_id, *_ in BIEN_DATA:
    for _stt, _nd in enumerate(_BIEN_CRITERIA, start=1):
        _tv_tc.append((_tc_id, _lv_id, _b_id, _stt, _nd))
        _tc_id += 1

TIEU_CHI_DATA: list[tuple] = _5S_TIEU_CHI + _tv_tc

# ─── Áp dụng (per-department applicability) ──────────────────────────────────
# TRUC_QUAN groups now cover all tieu_chi for each bien set (5 per bien)
_TV_HTQLCL = list(range(14, 34))    # bien 1–4  × 5 = IDs 14–33
_TV_6S     = list(range(34, 54))    # bien 5–8  × 5 = IDs 34–53
_TV_ATVSLD = list(range(54, 104))   # bien 9–18 × 5 = IDs 54–103
_TV_NQLD   = list(range(104, 114))  # bien 19–20 × 5 = IDs 104–113
_TV_ALL       = _TV_HTQLCL + _TV_6S + _TV_ATVSLD + _TV_NQLD
_TV_NO_HTQLCL = _TV_6S + _TV_ATVSLD + _TV_NQLD

_T1 = [1, 4, 5, 6, 8, 10, 11, 12, 13] + _TV_NO_HTQLCL
_T2 = list(range(1, 14)) + _TV_ALL
_T3 = [1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13] + _TV_NO_HTQLCL
_T4 = [1, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13] + _TV_ALL
_T5 = [2, 4, 6, 10, 11, 13] + _TV_NO_HTQLCL
_T6 = list(range(1, 14)) + _TV_NO_HTQLCL
_T7 = [1, 4, 5, 6, 10, 11, 13] + _TV_NO_HTQLCL
_T8 = [1, 2, 3, 4, 5, 6, 8, 10, 11, 12, 13] + _TV_ALL

_BO_PHAN_TYPES: dict[int, list[int]] = {
    1: _T1,
    2: _T2,  3: _T2,  4: _T2,  5: _T2,
    6: _T3,  7: _T3,  8: _T3,  9: _T3,
    10: _T4,
    11: _T5, 12: _T5, 13: _T5, 14: _T5,
    15: _T5, 16: _T5, 17: _T5,
    18: _T6,
    19: _T7, 20: _T7, 21: _T7, 22: _T7,
    23: _T7, 24: _T7, 25: _T7, 26: _T7,
    27: _T2, 28: _T2, 29: _T2,
    30: _T8, 31: _T8,
    32: _T5,
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
                "INSERT INTO audit_5s_bien (id, linh_vuc_id, ten_goi, mo_ta, kich_thuoc, so_thu_tu) "
                "VALUES (:id, :linh_vuc_id, :ten_goi, :mo_ta, :kich_thuoc, :so_thu_tu)"
            ),
            [{"id": i, "linh_vuc_id": lv, "ten_goi": tg, "mo_ta": mt, "kich_thuoc": kt, "so_thu_tu": st}
             for i, lv, tg, mt, kt, st in BIEN_DATA],
        )
        conn.execute(
            text(
                "INSERT INTO audit_5s_tieu_chi (id, linh_vuc_id, bien_id, so_thu_tu, noi_dung, active) "
                "VALUES (:id, :linh_vuc_id, :bien_id, :so_thu_tu, :noi_dung, TRUE)"
            ),
            [{"id": i, "linh_vuc_id": lv, "bien_id": bi, "so_thu_tu": n, "noi_dung": nd}
             for i, lv, bi, n, nd in TIEU_CHI_DATA],
        )
        conn.execute(
            text(
                "INSERT INTO audit_5s_ap_dung (bo_phan_id, tieu_chi_id) "
                "VALUES (:bo_phan_id, :tieu_chi_id)"
            ),
            [{"bo_phan_id": bp, "tieu_chi_id": tc} for bp, tc in AP_DUNG_DATA],
        )

        for tbl, col in [
            ("audit_5s_don_vi",    "id"),
            ("audit_5s_bo_phan",   "id"),
            ("audit_5s_linh_vuc",  "id"),
            ("audit_5s_bien",      "id"),
            ("audit_5s_tieu_chi",  "id"),
        ]:
            conn.execute(
                text(
                    f"SELECT setval(pg_get_serial_sequence('{tbl}', '{col}'), "
                    f"(SELECT MAX({col}) FROM {tbl}))"
                )
            )
