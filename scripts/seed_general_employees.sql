-- Seed initial user(s) for RCP module login
-- Safe to run multiple times (uses ON CONFLICT DO NOTHING).

CREATE TABLE IF NOT EXISTS public.general_employees (
  ma_nv character varying(16) PRIMARY KEY,
  ho_ten text NOT NULL DEFAULT '',
  chuc_vu text NOT NULL DEFAULT '',
  don_vi text NOT NULL DEFAULT '',
  bo_phan text NOT NULL DEFAULT '',
  station jsonb NOT NULL DEFAULT '[]'::jsonb
);

INSERT INTO public.general_employees (ma_nv, ho_ten, chuc_vu, don_vi, bo_phan, station)
VALUES ('P0872', 'Hồ Anh Phát', 'Phó phòng', 'P.TH', 'KSHT', '[]'::jsonb)
ON CONFLICT (ma_nv) DO NOTHING;

