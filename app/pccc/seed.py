"""Dữ liệu nền PCCC lấy từ báo cáo diễn tập tháng 06/2025."""
from __future__ import annotations

from sqlalchemy import inspect as sa_inspect, text


# (id, ma, ten, thu_tu)
DON_VI_DATA = [
    (1, "BTBM", "Ban Thiết bị may", 1),
    (2, "LAB", "Lab", 2),
    (3, "P.KDXNK", "Phòng KDXNK", 3),
    (4, "P.KTCD", "Phòng KTCĐ ĐT&MT", 4),
    (5, "P.KT", "Phòng Kế toán", 5),
    (6, "P.KTCN", "Phòng Kỹ thuật công nghệ", 6),
    (7, "P.QLCL", "Phòng Quản lý chất lượng", 7),
    (8, "P.QTDS", "Phòng Quản trị đời sống", 8),
    (9, "P.TH", "Phòng Tổng hợp", 9),
    (10, "TYT", "Trạm Y tế", 10),
    (11, "XNM1-V1", "XNM1-V1", 11),
    (12, "XNM2", "XNM2", 12),
    (13, "XNM3", "XNM3", 13),
    (14, "XNV2", "XNV2", 14),
]


# (id, don_vi_id, ma, ten, si_so_tham_khao, thu_tu)
BO_PHAN_DATA = [
    (1, 1, "BTBM", "BAN THIẾT BỊ MAY", 9, 1),
    (2, 2, "LAB", "Phòng LAB", 3, 1),
    (3, 3, "VP", "KHỐI VP KDXNK", 47, 1),
    (4, 3, "KHO_NL", "KHO NGUYÊN LIỆU, XÉN VIỀN & KIỂM VẢI", 22, 2),
    (5, 3, "KHO_PL", "KHO PHỤ LIỆU", 22, 3),
    (6, 3, "KHO_CARTON", "KHO CARTON & VẬT TƯ", 2, 4),
    (7, 3, "KHO_TP", "KHO THÀNH PHẨM", 7, 5),
    (8, 3, "BOC_VAC", "BỐC VÁC", 12, 6),
    (9, 4, "KTCD", "PHÒNG KTCĐ ĐT&MT", 28, 1),
    (10, 5, "KT", "PHÒNG KẾ TOÁN", 9, 1),
    (11, 6, "DON_HANG", "Đơn hàng", 12, 1),
    (12, 6, "CU_GA", "Cử gá", 3, 2),
    (13, 6, "SO_DO_LAZE", "Sơ đồ, Laze", 8, 3),
    (14, 6, "RAP", "Rập", 3, 4),
    (15, 6, "IE", "IE", 8, 5),
    (16, 6, "MAY_MAU", "Tổ may mẫu", 16, 6),
    (17, 7, "QLCL", "PHÒNG QUẢN LÝ CHẤT LƯỢNG", 17, 1),
    (18, 8, "QTDS", "PHÒNG QUẢN TRỊ ĐỜI SỐNG", 27, 1),
    (19, 9, "TH", "PHÒNG TỔNG HỢP", 18, 1),
    (20, 10, "YT", "TRẠM Y TẾ", 6, 1),
    (21, 11, "MAY_1", "TỔ MAY 1", 43, 1),
    (22, 11, "MAY_2", "TỔ MAY 2", 50, 2),
    (23, 11, "MAY_3", "TỔ MAY 3", 52, 3),
    (24, 11, "MAY_4", "TỔ MAY 4", 39, 4),
    (25, 11, "MAY_5", "TỔ MAY 5", 36, 5),
    (26, 11, "MAY_6", "TỔ MAY 6", 40, 6),
    (27, 11, "MAY_7", "TỔ MAY 7", 43, 7),
    (28, 11, "MAY_8", "TỔ MAY 8", 44, 8),
    (29, 11, "MAY_9", "TỔ MAY 9", 43, 9),
    (30, 11, "CAT", "TỔ CẮT", 34, 10),
    (31, 11, "HOAN_THANH", "TỔ HOÀN THÀNH", 5, 11),
    (32, 11, "QUAN_LY", "QUẢN LÝ", 33, 12),
    (33, 12, "QUAN_LY", "QUẢN LÝ", 23, 1),
    (34, 12, "HOAN_THANH", "TỔ HOÀN THÀNH", 6, 2),
    (35, 12, "CAT", "TỔ CẮT", 22, 3),
    (36, 12, "MAY_1", "TỔ MAY 1", 37, 4),
    (37, 12, "MAY_2_8", "TỔ MAY 2 & 8", 48, 5),
    (38, 12, "MAY_3_5", "TỔ MAY 3 & 5", 70, 6),
    (39, 12, "MAY_4_7", "TỔ 4 & 7", 52, 7),
    (40, 12, "MAY_6", "TỔ 6", 41, 8),
    (41, 13, "MAY_1", "TỔ MAY 1", 31, 1),
    (42, 13, "MAY_2", "TỔ MAY 2", 34, 2),
    (43, 13, "MAY_3", "TỔ MAY 3", 38, 3),
    (44, 13, "MAY_4", "TỔ MAY 4", 35, 4),
    (45, 13, "MAY_5", "TỔ MAY 5", 42, 5),
    (46, 13, "MAY_6", "TỔ MAY 6", 39, 6),
    (47, 13, "MAY_7", "TỔ MAY 7", 28, 7),
    (48, 13, "MAY_8", "TỔ MAY 8", 36, 8),
    (49, 13, "KT_HT_QL", "KỸ THUẬT, HOÀN THÀNH VÀ QUẢN LÝ", 32, 9),
    (50, 13, "CAT", "TỔ CẮT", 25, 10),
    (51, 14, "QLPV", "QUẢN LÝ PHỤC VỤ", 36, 1),
    (52, 14, "CAT", "TỔ CẮT", 45, 2),
    (53, 14, "HOAN_THANH", "HOÀN THÀNH", 34, 3),
    (54, 14, "MAY_1A", "TỔ MAY 1A", 48, 4),
    (55, 14, "MAY_1B", "TỔ MAY 1B", 34, 5),
    (56, 14, "MAY_1C", "TỔ MAY 1C", 48, 6),
    (57, 14, "MAY_1D", "TỔ MAY 1D", 52, 7),
    (58, 14, "MAY_2", "TỔ MAY 2", 57, 8),
    (59, 14, "MAY_4", "TỔ MAY 4", 44, 9),
    (60, 14, "KCS", "TỔ KCS", 28, 10),
    (61, 14, "MAY_MAU", "TỔ MAY MẪU", 17, 11),
]


def sync_master_data(engine) -> None:
    insp = sa_inspect(engine)
    tables = set(insp.get_table_names())
    if not {"pccc_don_vi", "pccc_bo_phan"}.issubset(tables):
        return

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO pccc_don_vi (id, ma, ten, thu_tu, active) "
                "VALUES (:id, :ma, :ten, :thu_tu, TRUE) "
                "ON CONFLICT (id) DO UPDATE SET "
                "ma=EXCLUDED.ma, ten=EXCLUDED.ten, thu_tu=EXCLUDED.thu_tu, active=TRUE"
            ),
            [{"id": i, "ma": ma, "ten": ten, "thu_tu": tt} for i, ma, ten, tt in DON_VI_DATA],
        )
        conn.execute(
            text(
                "INSERT INTO pccc_bo_phan "
                "(id, don_vi_id, ma, ten, si_so_tham_khao, thu_tu, active) "
                "VALUES (:id, :don_vi_id, :ma, :ten, :si_so, :thu_tu, TRUE) "
                "ON CONFLICT (id) DO UPDATE SET "
                "don_vi_id=EXCLUDED.don_vi_id, ma=EXCLUDED.ma, ten=EXCLUDED.ten, "
                "si_so_tham_khao=EXCLUDED.si_so_tham_khao, thu_tu=EXCLUDED.thu_tu"
            ),
            [
                {"id": i, "don_vi_id": dv, "ma": ma, "ten": ten, "si_so": ss, "thu_tu": tt}
                for i, dv, ma, ten, ss, tt in BO_PHAN_DATA
            ],
        )

        for table in ("pccc_don_vi", "pccc_bo_phan"):
            conn.execute(
                text(
                    f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), "
                    f"GREATEST(COALESCE((SELECT MAX(id) FROM {table}), 1), 1))"
                )
            )
