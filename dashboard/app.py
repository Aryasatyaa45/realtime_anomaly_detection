"""Dashboard Streamlit: visualisasi transaksi live + anomali terdeteksi (Minggu 5).

Membaca tabel `scores` dari Postgres (lokal) / Supabase (online saat deploy Streamlit Cloud).
Sumber data diisi pipeline Flink (sink JDBC kedua, di samping Paimon). Auto-refresh memakai
`st.fragment(run_every=...)` — native Streamlit, tanpa dependency tambahan.

Jalankan lokal (via compose): `docker compose up dashboard` -> buka http://127.0.0.1:8501
(pakai 127.0.0.1, bukan localhost — di Windows localhost resolve ke IPv6 dulu = lambat).
"""

from __future__ import annotations

import logging
import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Ambang model (hasil tuning Minggu 2). Ditampilkan sebagai garis acuan di grafik skor.
AMBANG_HST = 0.914
AMBANG_ECOD = 120.58

# Alert ke file (ponytail: minimal — banner + log file. Telegram ditunda sampai diminta).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("/app/alerts.log"), logging.StreamHandler()],
)
logger = logging.getLogger("alert")


def db_url() -> str:
    """URL koneksi Postgres: pakai DASHBOARD_DB_URL bila diset (online), else bangun dari env."""
    url = os.environ.get("DASHBOARD_DB_URL")
    if url:
        return url
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "anomaly")
    user = os.environ.get("POSTGRES_USER", "anomaly")
    pwd = os.environ.get("POSTGRES_PASSWORD", "anomaly")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


@st.cache_resource
def get_engine() -> Engine:
    """Engine SQLAlchemy dibuat sekali (cache lintas rerun). pool_pre_ping anti koneksi mati."""
    return create_engine(db_url(), pool_pre_ping=True)


def load_summary(engine: Engine) -> dict:
    """Hitung ringkasan agregat di sisi DB (murah walau tabel besar)."""
    q = text(
        "SELECT count(*) AS total, "
        "COALESCE(SUM(CASE WHEN is_anomali THEN 1 ELSE 0 END), 0) AS anomali FROM scores"
    )
    with engine.connect() as c:
        row = c.execute(q).mappings().first()
    total, anomali = int(row["total"]), int(row["anomali"])
    pct = (anomali / total * 100) if total else 0.0
    return {"total": total, "anomali": anomali, "pct": pct}


def load_recent(engine: Engine, limit: int = 200) -> pd.DataFrame:
    """Ambil N transaksi terakhir (urut id menaik untuk grafik kronologis)."""
    q = text(
        "SELECT id, waktu, amount, kelas, skor_ecod, skor_hst, is_anomali, ingested_at "
        "FROM scores ORDER BY id DESC LIMIT :lim"
    )
    df = pd.read_sql(q, engine, params={"lim": limit})
    return df.sort_values("id").reset_index(drop=True)


def log_anomali_baru(anomalies: pd.DataFrame) -> None:
    """Catat anomali yang BELUM pernah di-log (lacak id terakhir di session_state)."""
    last = st.session_state.get("last_logged_id", 0)
    baru = anomalies[anomalies["id"] > last]
    for _, r in baru.iterrows():
        logger.warning(
            "ANOMALI id=%s amount=%.2f skor_hst=%.4f skor_ecod=%.2f",
            r["id"], r["amount"], r["skor_hst"], r["skor_ecod"],
        )
    if not anomalies.empty:
        st.session_state["last_logged_id"] = int(anomalies["id"].max())


def grafik_amount(df: pd.DataFrame) -> go.Figure:
    """Garis Amount per transaksi + titik MERAH pada anomali."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["id"], y=df["amount"], mode="lines",
                             name="Amount", line=dict(color="#4C9BE8", width=1.5)))
    anom = df[df["is_anomali"]]
    if not anom.empty:
        fig.add_trace(go.Scatter(x=anom["id"], y=anom["amount"], mode="markers",
                                 name="Anomali", marker=dict(color="red", size=10, symbol="x")))
    fig.update_layout(height=300, margin=dict(l=10, r=10, t=30, b=10),
                      title="Amount transaksi (titik merah = anomali)",
                      xaxis_title="id transaksi", yaxis_title="Amount")
    return fig


def grafik_skor(df: pd.DataFrame) -> go.Figure:
    """Skor 2 model per transaksi + garis ambang masing-masing."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["id"], y=df["skor_hst"], mode="lines",
                             name="HST", line=dict(color="#E8A33C")))
    fig.add_trace(go.Scatter(x=df["id"], y=df["skor_ecod"], mode="lines",
                             name="ECOD", line=dict(color="#9B8CFF"), yaxis="y2"))
    fig.add_hline(y=AMBANG_HST, line_dash="dash", line_color="#E8A33C",
                  annotation_text=f"ambang HST {AMBANG_HST}")
    fig.update_layout(
        height=300, margin=dict(l=10, r=10, t=30, b=10),
        title="Skor anomali per transaksi (HST vs ECOD)",
        xaxis_title="id transaksi",
        yaxis=dict(title="skor HST [0..1]"),
        yaxis2=dict(title="skor ECOD", overlaying="y", side="right"),
        legend=dict(orientation="h"),
    )
    return fig


@st.fragment(run_every=2)
def bagian_live() -> None:
    """Blok yang auto-refresh tiap 2 detik: metrik, grafik, tabel, alert."""
    engine = get_engine()
    try:
        summary = load_summary(engine)
        df = load_recent(engine, 200)
    except Exception as e:  # noqa: BLE001 — tampilkan ramah, jangan crash UI
        st.warning(f"Belum bisa baca Postgres (mungkin pipeline belum jalan): {e}")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Total transaksi", f"{summary['total']:,}")
    c2.metric("Total anomali", f"{summary['anomali']:,}")
    c3.metric("% anomali", f"{summary['pct']:.3f}%")

    if summary["total"] == 0:
        st.info("Tabel `scores` masih kosong — tunggu pipeline mengisi data.")
        return

    anomalies = df[df["is_anomali"]]
    if not anomalies.empty:
        st.error(f"🚨 {len(anomalies)} anomali pada 200 transaksi terbaru — cek log alert.")
        log_anomali_baru(anomalies)

    st.plotly_chart(grafik_amount(df), use_container_width=True)
    st.plotly_chart(grafik_skor(df), use_container_width=True)

    st.subheader("Transaksi terbaru (skor 2 model berdampingan)")
    tampil = df.sort_values("id", ascending=False)[
        ["id", "amount", "kelas", "skor_ecod", "skor_hst", "is_anomali", "ingested_at"]
    ]
    st.dataframe(tampil, use_container_width=True, hide_index=True)


def main() -> None:
    """Render dashboard."""
    st.set_page_config(page_title="Real-Time Anomaly Detection", page_icon="🛰️", layout="wide")
    st.title("🛰️ Real-Time Anomaly Detection")
    st.caption(
        "Fraud transaksi kartu kredit — skoring streaming 2 model (ECOD baseline vs "
        "Half-Space Trees online). Data live dari pipeline Flink via Postgres. Auto-refresh 2 dtk."
    )
    bagian_live()


if __name__ == "__main__":
    main()
