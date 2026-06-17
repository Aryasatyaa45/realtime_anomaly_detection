"""Service skoring real-time: skor tiap transaksi dengan ECOD + Half-Space Trees.

Dipanggil dari Flink job per-event. Mengembalikan skor kedua model + flag anomali.

Implementasi penuh dikerjakan di **Minggu 4** (ROADMAP §4).
"""

from __future__ import annotations

from typing import Any


def score_event(event: dict[str, Any]) -> dict[str, Any]:
    """Skor satu transaksi dengan kedua model.

    Args:
        event: Satu transaksi (dict fitur V1..V28, Amount, dst).

    Returns:
        Dict berisi transaksi asli + skor_ecod, skor_hst, is_anomali.
    """
    # TODO(Minggu 4):
    #   1. Load ecod.pkl (sekali, di init) → ecod.decision_function.
    #   2. HS-Trees: hst.score_one(event) lalu hst.learn_one(event).
    #   3. Bandingkan skor dengan ambang → set is_anomali.
    raise NotImplementedError("scorer.py diimplementasikan pada Minggu 4.")
