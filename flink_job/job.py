"""PyFlink job: konsumsi stream dari Iggy, hitung fitur window, panggil scorer, sink ke Paimon.

Fitur window contoh: rata-rata Amount per 10 detik, frekuensi transaksi per window.

Implementasi penuh dikerjakan di **Minggu 4** (ROADMAP §4).
"""

from __future__ import annotations


def main() -> None:
    """Bangun & jalankan pipeline Flink."""
    # TODO(Minggu 4):
    #   1. StreamExecutionEnvironment + source dari Iggy.
    #   2. Window 10 detik → fitur agregat (avg Amount, count).
    #   3. Map per-event → ml.scorer.score_event.
    #   4. Sink hasil ke tabel Paimon.
    raise NotImplementedError("Flink job diimplementasikan pada Minggu 4.")


if __name__ == "__main__":
    main()
