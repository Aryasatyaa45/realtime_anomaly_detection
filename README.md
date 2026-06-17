# 🛰️ Real-Time Anomaly Detection Pipeline

Deteksi **fraud transaksi** secara real-time dengan arsitektur streaming, membandingkan
dua model anomaly detection: **ECOD** (baseline parameter-free) vs **Half-Space Trees**
(online learning sejati).

> Status: 🚧 dalam pengembangan — ikuti [ROADMAP.md](ROADMAP.md).

## Arsitektur

```
CSV → producer → Iggy → Flink → ML scorer → Paimon → Dashboard
       (kirim)   (antri) (fitur)  (skor)     (simpan)  (tampil)
```

| Lapisan | Teknologi |
|---------|-----------|
| Sumber | Python producer (replay CSV) |
| Broker | Apache Iggy |
| Proses | Apache Flink (PyFlink) |
| Model | PyOD (ECOD) + River (Half-Space Trees) |
| Storage | Apache Paimon (+ Postgres untuk dashboard) |
| Dashboard | Streamlit (online di Streamlit Cloud) |
| Orkestrasi | Docker Compose |

## Dataset

Credit Card Fraud Detection (Kaggle `mlg-ulb/creditcardfraud`) — 284.807 transaksi,
fitur `Time`, `V1–V28`, `Amount`, `Class`. Fraud ~0.17% (sangat imbalanced).

> ⚠️ Dataset imbalanced → evaluasi pakai **Precision / Recall / F1 / PR-AUC**, BUKAN accuracy.

## Cara Menjalankan

```bash
# 1. Salin config
cp .env.example .env

# 2. Taruh dataset di data/creditcard.csv (download dari Kaggle)

# 3. Jalankan seluruh pipeline
docker compose up
```

*(Instruksi lengkap menyusul saat tiap komponen siap.)*

## Lisensi

MIT
