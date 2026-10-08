-- Seed phân công PCCC – chạy lại khi cần khôi phục dữ liệu
-- Cập nhật: 2026-09-19

BEGIN;

DELETE FROM pccc_phan_cong;

-- Reset sequence
SELECT setval('pccc_phan_cong_id_seq', 1, false);

INSERT INTO pccc_phan_cong (don_vi_id, bo_phan_id, ma_nv, vai_tro, active, created_by, created_at, updated_at)
VALUES
  -- Đơn vị cấp đơn vị (1 người / đơn vị, bo_phan_id = NULL)
  (1,  NULL, 'T2548', 'CHINH', true, 'ADMIN_SETUP', '2026-09-18 08:09:19.728096', '2026-09-18 08:09:19.728096'),  -- Ban Thiết bị may
  (2,  NULL, 'V0044', 'CHINH', true, 'ADMIN_SETUP', '2026-09-18 08:09:19.728096', '2026-09-18 08:09:19.728096'),  -- Lab
  (4,  NULL, 'T3996', 'CHINH', true, 'ADMIN_SETUP', '2026-09-18 08:09:19.728096', '2026-09-18 08:09:19.728096'),  -- P.KTCĐ ĐT&MT
  (5,  NULL, 'L0768', 'CHINH', true, 'ADMIN_SETUP', '2026-09-18 08:09:19.728096', '2026-09-18 08:09:19.728096'),  -- P.Kế toán
  (6,  NULL, 'S0493', 'CHINH', true, 'ADMIN_SETUP', '2026-09-18 08:09:19.728096', '2026-09-18 08:09:19.728096'),  -- P.Kỹ thuật CN
  (7,  NULL, 'Q0386', 'CHINH', true, 'ADMIN_SETUP', '2026-09-18 08:09:19.728096', '2026-09-18 08:09:19.728096'),  -- P.QLCL
  (8,  NULL, 'X0023', 'CHINH', true, 'ADMIN_SETUP', '2026-09-18 08:09:19.728096', '2026-09-18 08:09:19.728096'),  -- P.QTDS
  (9,  NULL, 'H3644', 'CHINH', true, 'ADMIN_SETUP', '2026-09-18 08:09:19.728096', '2026-09-18 08:09:19.728096'),  -- P.Tổng hợp
  (10, NULL, 'G0026', 'CHINH', true, 'ADMIN_SETUP', '2026-09-18 08:09:19.728096', '2026-09-18 08:09:19.728096'),  -- Trạm Y tế
  (11, NULL, 'L1847', 'CHINH', true, 'ADMIN_SETUP', '2026-09-18 08:09:19.728096', '2026-09-18 08:09:19.728096'),  -- XNM1-V1
  (12, NULL, 'H0710', 'CHINH', true, 'ADMIN_SETUP', '2026-09-18 08:09:19.728096', '2026-09-18 08:09:19.728096'),  -- XNM2
  (13, NULL, 'T1729', 'CHINH', true, 'ADMIN_SETUP', '2026-09-18 08:09:19.728096', '2026-09-18 08:09:19.728096'),  -- XNM3
  (14, NULL, 'T3787', 'CHINH', true, 'ADMIN_SETUP', '2026-09-18 08:09:19.728096', '2026-09-18 08:09:19.728096'),  -- XNV2

  -- P.KDXNK: S0106 khai cho 6 bộ phận
  (3, 3, 'S0106', 'CHINH', true, 'ADMIN_SETUP', '2026-09-19 08:01:50.316818', '2026-09-19 08:01:50.316818'),  -- Khối VP KDXNK
  (3, 4, 'S0106', 'CHINH', true, 'ADMIN_SETUP', '2026-09-19 08:01:50.316818', '2026-09-19 08:01:50.316818'),  -- Kho Nguyên liệu
  (3, 5, 'S0106', 'CHINH', true, 'ADMIN_SETUP', '2026-09-19 08:01:50.316818', '2026-09-19 08:01:50.316818'),  -- Kho Phụ liệu
  (3, 6, 'S0106', 'CHINH', true, 'ADMIN_SETUP', '2026-09-19 08:01:50.316818', '2026-09-19 08:01:50.316818'),  -- Kho Carton & VT
  (3, 7, 'S0106', 'CHINH', true, 'ADMIN_SETUP', '2026-09-19 08:01:50.316818', '2026-09-19 08:01:50.316818'),  -- Kho Thành phẩm
  (3, 8, 'S0106', 'CHINH', true, 'ADMIN_SETUP', '2026-09-19 08:01:50.316818', '2026-09-19 08:01:50.316818'); -- Bốc vác

COMMIT;
