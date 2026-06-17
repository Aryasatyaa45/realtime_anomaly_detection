"""Dashboard Streamlit: visualisasi transaksi live + anomali terdeteksi.

Membaca hasil dari Postgres (lokal) / Supabase (online saat deploy Streamlit Cloud).

Implementasi penuh dikerjakan di **Minggu 5** (ROADMAP §4).
"""

from __future__ import annotations

import streamlit as st


def main() -> None:
    """Render dashboard."""
    st.set_page_config(page_title="Real-Time Anomaly Detection", page_icon="🛰️")
    st.title("🛰️ Real-Time Anomaly Detection")
    st.info("Dashboard diimplementasikan pada Minggu 5 (ROADMAP §4).")
    # TODO(Minggu 5):
    #   1. Koneksi ke Postgres/Supabase.
    #   2. Grafik transaksi masuk (live refresh) + titik merah anomali.
    #   3. Tabel transaksi terbaru + skor ECOD & HS-Trees berdampingan.
    #   4. Metrik ringkas: total transaksi, total anomali, % anomali.


if __name__ == "__main__":
    main()
