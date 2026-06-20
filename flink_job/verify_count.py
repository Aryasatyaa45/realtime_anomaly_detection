"""Cek isi tabel Paimon `scores` (batch read) — verifikasi cepat sink 4e bekerja.

Jalankan dari dalam container Flink:
    flink run -py /opt/flink_job/verify_count.py

Mencetak jumlah baris, jumlah yang ditandai anomali, dan beberapa contoh baris. Dipakai
sebagai pemeriksaan runnable untuk DoD Minggu 4e (hasil skor benar-benar tersimpan).
"""

from __future__ import annotations

import os

from pyflink.table import EnvironmentSettings, TableEnvironment


def main() -> None:
    warehouse = os.environ.get("PAIMON_WAREHOUSE", "file:///opt/paimon")
    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    t_env.execute_sql(
        f"CREATE CATALOG paimon WITH ('type'='paimon', 'warehouse'='{warehouse}')"
    )
    print("=== ringkasan tabel paimon.default.scores ===")
    t_env.execute_sql(
        """
        SELECT
            COUNT(*)                              AS total_baris,
            SUM(CASE WHEN is_anomali THEN 1 ELSE 0 END) AS jml_anomali,
            ROUND(AVG(skor_hst), 4)               AS rata_skor_hst,
            ROUND(MAX(skor_ecod), 2)              AS maks_skor_ecod
        FROM paimon.`default`.scores
        """
    ).print()

    print("=== 5 baris bertanda anomali (jika ada) ===")
    t_env.execute_sql(
        """
        SELECT `Time`, Amount, `Class`, skor_ecod, skor_hst, is_anomali
        FROM paimon.`default`.scores
        WHERE is_anomali
        LIMIT 5
        """
    ).print()


if __name__ == "__main__":
    main()
