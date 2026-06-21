"""Dashboard Streamlit: monitor transaksi live + deteksi dataset + log alert (Minggu 5-6).

Tiga tab (native st.tabs, tanpa router tambahan):
  1. Monitor Live   — baca tabel `scores` (Postgres/Supabase), auto-refresh 2 dtk.
  2. Deteksi Dataset — upload CSV -> skor via service scorer (?learn=false, tak ubah state live).
  3. Log Alert       — isi /app/alerts.log.

Sumber data live diisi pipeline Flink (sink JDBC kedua, di samping Paimon).

Jalankan lokal (via compose): `docker compose up dashboard` -> buka http://127.0.0.1:8501
(pakai 127.0.0.1, bukan localhost — di Windows localhost resolve ke IPv6 dulu = lambat).
"""

from __future__ import annotations

import logging
import os

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Ambang model (hasil tuning Minggu 2). Ditampilkan sebagai garis acuan di grafik skor.
AMBANG_HST = 0.914
AMBANG_ECOD = 120.58

# Service scorer di jaringan compose (nama service, bukan localhost). Dipakai tab upload.
SCORER_URL = os.environ.get("SCORER_URL", "http://scorer:8000")
# ponytail: batas baris di-skor per upload (1 HTTP request/baris ke scorer). Naikkan kalau perlu.
MAKS_UPLOAD = 2000
LOG_PATH = "/app/alerts.log"

# Alert ke file (ponytail: minimal — banner + log file. Telegram ditunda sampai diminta).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
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


@st.cache_resource
def scorer_session() -> requests.Session:
    """Session requests reusable (keep-alive) untuk skoring batch upload."""
    return requests.Session()


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


def load_recent(engine: Engine, limit: int) -> pd.DataFrame:
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


# --------------------------------------------------------------------------- grafik

def grafik_amount(df: pd.DataFrame) -> go.Figure:
    """Garis nilai transaksi (Amount) + titik MERAH pada anomali."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["id"], y=df["amount"], mode="lines", name="Amount",
        line=dict(color="#4C9BE8", width=1.5),
        hovertemplate="id %{x}<br>Amount %{y:.2f}<extra></extra>",
    ))
    anom = df[df["is_anomali"]]
    if not anom.empty:
        fig.add_trace(go.Scatter(
            x=anom["id"], y=anom["amount"], mode="markers", name="Anomali",
            marker=dict(color="#E54848", size=11, symbol="x", line=dict(width=1)),
            hovertemplate="ANOMALI id %{x}<br>Amount %{y:.2f}<extra></extra>",
        ))
    fig.update_layout(
        height=300, margin=dict(l=10, r=10, t=40, b=10),
        title="💸 Nilai transaksi (Amount) — tanda ✕ merah = anomali",
        xaxis_title="urutan transaksi", yaxis_title="Amount",
        legend=dict(orientation="h", y=1.12, x=0), hovermode="x unified",
    )
    return fig


def grafik_skor(df: pd.DataFrame) -> go.Figure:
    """Skor 2 model per transaksi + garis ambang masing-masing (HST kiri, ECOD kanan)."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["id"], y=df["skor_hst"], mode="lines", name="HST (model utama)",
        line=dict(color="#E8A33C", width=2),
        hovertemplate="id %{x}<br>skor HST %{y:.4f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df["id"], y=df["skor_ecod"], mode="lines", name="ECOD (baseline)",
        line=dict(color="#9B8CFF", width=1), yaxis="y2",
        hovertemplate="id %{x}<br>skor ECOD %{y:.2f}<extra></extra>",
    ))
    # Tandai titik anomali di kurva HST agar mudah dilihat.
    anom = df[df["is_anomali"]]
    if not anom.empty:
        fig.add_trace(go.Scatter(
            x=anom["id"], y=anom["skor_hst"], mode="markers", name="Anomali",
            marker=dict(color="#E54848", size=10, symbol="circle-open", line=dict(width=2)),
            hoverinfo="skip",
        ))
    # Ambang HST (sumbu kiri) + ambang ECOD (sumbu kanan, via shape y2).
    fig.add_hline(y=AMBANG_HST, line_dash="dash", line_color="#E8A33C",
                  annotation_text=f"ambang HST {AMBANG_HST}", annotation_position="top left")
    if len(df):
        fig.add_shape(type="line", xref="x", yref="y2",
                      x0=df["id"].min(), x1=df["id"].max(), y0=AMBANG_ECOD, y1=AMBANG_ECOD,
                      line=dict(color="#9B8CFF", dash="dot", width=1))
    fig.update_layout(
        height=320, margin=dict(l=10, r=10, t=40, b=10),
        title="📈 Skor anomali per transaksi (di atas garis ambang = mencurigakan)",
        xaxis_title="urutan transaksi",
        yaxis=dict(title="skor HST [0..1]"),
        yaxis2=dict(title="skor ECOD", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", y=1.12, x=0), hovermode="x unified",
    )
    return fig


def tabel_transaksi(df: pd.DataFrame, height: int = 420) -> None:
    """Tabel transaksi (id terbaru di atas), baris anomali disorot merah."""
    tampil = df.sort_values("id", ascending=False)[
        ["id", "amount", "kelas", "skor_ecod", "skor_hst", "is_anomali", "ingested_at"]
    ].rename(columns={
        "amount": "Amount", "kelas": "Class (label)", "skor_ecod": "skor ECOD",
        "skor_hst": "skor HST", "is_anomali": "anomali?", "ingested_at": "masuk",
    })
    sty = tampil.style.apply(
        lambda r: ["background-color: #5c1f1f" if r["anomali?"] else "" for _ in r], axis=1
    ).format({"Amount": "{:.2f}", "skor ECOD": "{:.2f}", "skor HST": "{:.4f}"})
    st.dataframe(sty, use_container_width=True, hide_index=True, height=height)


# --------------------------------------------------------------------------- tab: live

@st.fragment(run_every=2)
def bagian_live() -> None:
    """Tab Monitor Live: auto-refresh tiap 2 detik."""
    engine = get_engine()
    try:
        summary = load_summary(engine)
    except Exception as e:  # noqa: BLE001 — tampilkan ramah, jangan crash UI
        st.warning(f"Belum bisa baca Postgres (mungkin pipeline belum jalan): {e}")
        return

    c1, c2, c3 = st.columns(3)
    c1.metric("Total transaksi diproses", f"{summary['total']:,}")
    c2.metric("Anomali terdeteksi", f"{summary['anomali']:,}")
    c3.metric("Rasio anomali", f"{summary['pct']:.3f}%")

    if summary["total"] == 0:
        st.info("Tabel `scores` masih kosong — tunggu pipeline mengisi data.")
        return

    n = st.slider("Jumlah transaksi terbaru ditampilkan", 100, 5000, 500, step=100,
                  help="Grafik & tabel memuat N transaksi terakhir. Naikkan untuk lihat lebih banyak.")
    df = load_recent(engine, n)

    anomalies = df[df["is_anomali"]]
    if not anomalies.empty:
        st.error(f"🚨 {len(anomalies)} anomali pada {len(df)} transaksi terbaru — tercatat di tab Log.")
        log_anomali_baru(anomalies)

    st.plotly_chart(grafik_amount(df), use_container_width=True)
    st.plotly_chart(grafik_skor(df), use_container_width=True)

    st.subheader(f"Tabel transaksi — {len(df)} terbaru")
    tabel_transaksi(df)


# --------------------------------------------------------------------------- tab: upload

def skor_dataframe(df: pd.DataFrame) -> pd.DataFrame | None:
    """Skor tiap baris via service scorer (learn=false). Kembalikan df + kolom skor, atau None."""
    sess = scorer_session()
    records = df.to_dict("records")
    n = len(records)
    skor_ecod, skor_hst, is_anom = [], [], []
    prog = st.progress(0.0, text="Menskor transaksi...")
    try:
        for i, row in enumerate(records):
            r = sess.post(f"{SCORER_URL}/score?learn=false", json=row, timeout=10)
            r.raise_for_status()
            d = r.json()
            skor_ecod.append(d["skor_ecod"])
            skor_hst.append(d["skor_hst"])
            is_anom.append(bool(d["is_anomali"]))
            if i % 25 == 0 or i == n - 1:
                prog.progress((i + 1) / n, text=f"Menskor {i + 1}/{n} transaksi...")
    except Exception as e:  # noqa: BLE001
        prog.empty()
        st.error(f"Gagal menghubungi scorer di {SCORER_URL}: {e}\n\n"
                 "Pastikan service scorer jalan: `docker compose up -d scorer`.")
        return None
    prog.empty()

    out = df.copy()
    out["id"] = range(1, len(out) + 1)
    out["amount"] = df["Amount"] if "Amount" in df.columns else df.get("amount", 0.0)
    out["skor_ecod"] = skor_ecod
    out["skor_hst"] = skor_hst
    out["is_anomali"] = is_anom
    return out


def bagian_upload() -> None:
    """Tab Deteksi Dataset: upload CSV -> skor batch -> visualisasi + unduh hasil."""
    st.markdown(
        "Upload **CSV transaksi** (kolom `V1`..`V28`, `Amount` — format dataset Kaggle Credit "
        "Card Fraud). Tiap baris diskor oleh **kedua model**. Skoring memakai `learn=false` "
        "sehingga **tidak mengubah** state model pipeline live."
    )
    file = st.file_uploader("Pilih file CSV", type=["csv"])
    if file is None:
        st.caption("Belum ada file. Contoh: potongan `data/creditcard.csv`.")
        return

    try:
        df = pd.read_csv(file)
    except Exception as e:  # noqa: BLE001
        st.error(f"Gagal membaca CSV: {e}")
        return

    st.write(f"File berisi **{len(df):,}** baris, {len(df.columns)} kolom.")
    if len(df) > MAKS_UPLOAD:
        st.warning(f"Hanya {MAKS_UPLOAD:,} baris pertama yang diskor (batas demo).")
        df = df.head(MAKS_UPLOAD)

    if not st.button("🔍 Deteksi anomali", type="primary"):
        return

    hasil = skor_dataframe(df)
    if hasil is None:
        return

    n_anom = int(hasil["is_anomali"].sum())
    c1, c2, c3 = st.columns(3)
    c1.metric("Baris diskor", f"{len(hasil):,}")
    c2.metric("Anomali terdeteksi", f"{n_anom:,}")
    c3.metric("Rasio anomali", f"{(n_anom / len(hasil) * 100):.2f}%")

    st.plotly_chart(grafik_amount(hasil), use_container_width=True)
    st.plotly_chart(grafik_skor(hasil), use_container_width=True)

    st.subheader(f"Hasil deteksi — {len(hasil)} baris")
    tabel_transaksi(hasil, height=420)

    st.download_button(
        "⬇️ Unduh hasil (CSV)",
        hasil.to_csv(index=False).encode("utf-8"),
        file_name="hasil_deteksi.csv", mime="text/csv",
    )


# --------------------------------------------------------------------------- tab: log

def bagian_log() -> None:
    """Tab Log Alert: tampilkan baris terakhir /app/alerts.log."""
    st.markdown(f"Isi `{LOG_PATH}` — alert anomali & aktivitas dashboard (terbaru di bawah).")
    n = st.slider("Jumlah baris terakhir", 50, 1000, 200, step=50)
    if st.button("🔄 Muat ulang log"):
        st.rerun()
    try:
        with open(LOG_PATH, encoding="utf-8") as f:
            lines = f.readlines()
        teks = "".join(lines[-n:]) or "(log masih kosong)"
        st.caption(f"Menampilkan {min(n, len(lines))} dari {len(lines)} baris.")
    except FileNotFoundError:
        teks = "(file log belum ada — belum ada aktivitas)"
    st.code(teks, language="log")


# --------------------------------------------------------------------------- main

def main() -> None:
    """Render dashboard."""
    st.set_page_config(page_title="Real-Time Anomaly Detection", page_icon="🛰️", layout="wide")
    st.title("🛰️ Real-Time Anomaly Detection")
    st.caption(
        "Fraud transaksi kartu kredit — skoring streaming 2 model (ECOD baseline vs "
        "Half-Space Trees online). Data live dari pipeline Flink via Postgres."
    )
    with st.expander("ℹ️ Cara baca dashboard"):
        st.markdown(
            "- **HST (Half-Space Trees)** = model utama (online, belajar per transaksi). "
            f"Skor [0..1], anomali bila ≥ **{AMBANG_HST}**.\n"
            f"- **ECOD** = baseline statistik. Skor terbuka, anomali bila ≥ **{AMBANG_ECOD}**.\n"
            "- **Tanda merah** di grafik & tabel = transaksi yang ditandai anomali oleh model utama.\n"
            "- Tab **Deteksi Dataset**: upload CSV-mu sendiri untuk diskor tanpa mengubah model live.\n"
            "- Tab **Log Alert**: jejak anomali yang tercatat."
        )

    tab_live, tab_upload, tab_log = st.tabs(
        ["📡 Monitor Live", "📤 Deteksi Dataset", "📋 Log Alert"]
    )
    with tab_live:
        bagian_live()
    with tab_upload:
        bagian_upload()
    with tab_log:
        bagian_log()


if __name__ == "__main__":
    main()
