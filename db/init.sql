-- Inisialisasi skema Postgres untuk dashboard (Minggu 5).
-- File ini di-mount ke /docker-entrypoint-initdb.d/ -> dijalankan OTOMATIS oleh image
-- postgres SEKALI saat volume data pertama kali dibuat (sebelum Postgres menerima koneksi
-- eksternal). Flink JDBC sink TIDAK membuat tabel sendiri -> tabel harus sudah ada di sini.
--
-- Kolom skor sengaja lowercase + nama Indonesia (waktu/amount/kelas) agar:
--   1. menghindari reserved word & case-folding identifier Postgres (Time/Class),
--   2. cocok 1:1 dengan nama kolom tabel `pg_scores` di Flink (JDBC sink meng-generate
--      INSERT memakai NAMA kolom, bukan posisi).
-- Kolom `id` & `ingested_at` diisi default oleh Postgres -> Flink cukup INSERT 6 kolom skor.

CREATE TABLE IF NOT EXISTS scores (
    id          BIGSERIAL PRIMARY KEY,
    waktu       DOUBLE PRECISION,   -- fitur "Time" transaksi (detik sejak transaksi pertama)
    amount      DOUBLE PRECISION,   -- nominal transaksi
    kelas       INTEGER,            -- label asli dataset (0=normal, 1=fraud) — utk evaluasi
    skor_ecod   DOUBLE PRECISION,   -- skor anomali model ECOD (baseline)
    skor_hst    DOUBLE PRECISION,   -- skor anomali model Half-Space Trees (online, pemenang M2)
    is_anomali  BOOLEAN,            -- keputusan akhir (= flag HST pada ambang 0.914)
    ingested_at TIMESTAMPTZ DEFAULT now()  -- waktu baris tiba di Postgres (utk grafik live)
);

-- Dashboard mengurutkan & mengambil baris terbaru via id; index eksplisit (PK sudah index,
-- tapi ditulis jelas untuk query ORDER BY id DESC LIMIT n).
CREATE INDEX IF NOT EXISTS idx_scores_id_desc ON scores (id DESC);
