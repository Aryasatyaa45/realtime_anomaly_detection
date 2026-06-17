# 🛰️ Real-Time Anomaly Detection Pipeline — ROADMAP

> **Projek #12 AIDE** — deteksi fraud transaksi secara real-time pakai streaming.
> **Target deploy:** Level 2 (pipeline reproducible lokal + dashboard online).
> **Model perbandingan:** ECOD (baseline) vs Half-Space Trees (online streaming).

---

## 0. Gambaran Besar (baca ini dulu kalau bingung)

Tujuan akhir: orang buka **link dashboard online** → lihat transaksi mengalir + titik
anomali yang terdeteksi 2 model. Sumber dipakai recruiter buat menilai = **repo GitHub**
(arsitektur + kode rapi + README).

**Alur data (hafalkan ini):**

```
CSV → producer → Iggy → Flink → ML scorer → Paimon → Dashboard
       (kirim)   (antri) (fitur)  (skor)     (simpan)  (tampil)
```

**Aturan main biar nggak tersesat:**
1. Kerjakan **satu minggu = satu milestone**. Jangan loncat.
2. Tiap minggu ada **"Definition of Done" (DoD)** — kalau DoD ✅, baru lanjut.
3. Tiap komponen dites **sendiri-sendiri dulu** sebelum disambung.
4. Commit kecil & sering. Pesan commit jelas (`feat:`, `fix:`, `docs:`).

---

## 1. Prasyarat (kerjakan SEKALI di awal, ~1 jam)

| # | Tugas | Cara cek selesai |
|---|-------|------------------|
| 1 | Install **Docker Desktop** (Windows) | `docker --version` jalan |
| 2 | Install **Python 3.11** + bikin venv | `python --version` |
| 3 | Akun **Kaggle** (buat dataset) | bisa login |
| 4 | Akun **GitHub** + repo kosong `realtime-anomaly-detection` | repo ada |
| 5 | Akun **Streamlit Cloud** (login pakai GitHub) | bisa masuk |
| 6 | Editor: **VS Code** | — |

> 💡 Mak di Windows: kalau `git clone` rewel SSL, pakai `git config --global http.sslBackend schannel`.

---

## 2. Arsitektur Final

```
┌─────────────┐   ┌──────────┐   ┌───────────┐   ┌──────────────┐   ┌──────────┐
│  producer   │──▶│   Iggy   │──▶│   Flink   │──▶│  ML scorer   │──▶│  Paimon  │
│ replay CSV  │   │  broker  │   │ windowing │   │ ECOD +       │   │ storage  │
│ as stream   │   │          │   │ + fitur   │   │ HS-Trees     │   │ hasil    │
└─────────────┘   └──────────┘   └───────────┘   └──────┬───────┘   └────┬─────┘
                                                        │                │
                                                        ▼                ▼
                                                  ┌──────────┐   ┌──────────────┐
                                                  │  alert   │   │  Streamlit   │
                                                  │ log / TG │   │  dashboard   │──▶ ONLINE
                                                  └──────────┘   └──────────────┘
```

**Kunci Level 2:** semua kotak jalan lokal via `docker compose up`, **kecuali dashboard**
yang juga di-deploy ke Streamlit Cloud. Dashboard online baca hasil dari **Postgres/Supabase
gratis** supaya orang lain bisa lihat data anomali tanpa install apa pun.

---

## 3. Struktur Folder

```
realtime-anomaly-detection/
├── README.md                  # etalase projek (diagram, demo, cara jalanin)
├── ROADMAP.md                 # file ini
├── docker-compose.yml         # orkestrasi semua service
├── .env.example               # contoh config (commit ini)
├── .gitignore
├── data/
│   └── creditcard.csv         # dataset (gitignore — file besar)
├── producer/
│   ├── producer.py            # baca CSV → kirim ke Iggy seolah real-time
│   ├── requirements.txt
│   └── Dockerfile
├── flink_job/
│   ├── job.py                 # PyFlink: konsumsi stream + fitur window
│   └── Dockerfile
├── ml/
│   ├── train.py               # latih/siapkan model offline
│   ├── scorer.py              # service skoring real-time (ECOD + HS-Trees)
│   ├── evaluate.py            # bandingkan 2 model (Precision/Recall/F1/PR-AUC)
│   ├── models/                # artefak: ecod.pkl
│   ├── requirements.txt
│   └── Dockerfile
├── dashboard/
│   ├── app.py                 # Streamlit (yang di-deploy online)
│   └── requirements.txt
└── notebooks/
    └── eksplorasi.ipynb       # EDA + eksperimen model
```

---

## 4. Roadmap Mingguan (6 minggu)

### 🗓️ Minggu 1 — Fondasi & Pahami Data

**Tujuan:** repo siap + kamu paham datanya.

- [ ] Init repo + struktur folder + `.gitignore` + `README.md` kerangka
- [ ] Download **Credit Card Fraud Dataset** (Kaggle `mlg-ulb/creditcardfraud`) → `data/creditcard.csv`
- [ ] Bikin venv + `pip install pandas numpy matplotlib seaborn scikit-learn pyod river jupyter`
- [ ] EDA di `notebooks/eksplorasi.ipynb`:
  - [ ] Cek bentuk data: 284.807 baris, kolom `Time`, `V1–V28`, `Amount`, `Class`
  - [ ] Hitung rasio fraud (`Class==1`) → harusnya ~0.17% (sangat imbalanced)
  - [ ] Distribusi `Amount`, korelasi, perbedaan fraud vs normal
  - [ ] Catat insight di markdown notebook

**📦 DoD Minggu 1:** notebook EDA jalan tanpa error + kamu bisa jelaskan kenapa
accuracy ≠ metrik yang benar di sini.

---

### 🗓️ Minggu 2 — Model Offline & Perbandingan

**Tujuan:** dua model jalan + tabel perbandingan metrik.

- [ ] `ml/train.py`:
  - [ ] Split data: `fit` model pada data **normal saja** (unsupervised anomaly detection)
  - [ ] **ECOD** (PyOD): `from pyod.models.ecod import ECOD` → fit → simpan `models/ecod.pkl`
  - [ ] **Half-Space Trees** (River): `from river import anomaly` → `learn_one`/`score_one`
- [ ] `ml/evaluate.py`:
  - [ ] Hitung **Precision, Recall, F1, PR-AUC, ROC-AUC** untuk kedua model
  - [ ] Pilih ambang (threshold) dari PR-curve
  - [ ] Simpan tabel hasil + grafik PR-curve ke `notebooks/`
- [ ] Tulis ringkasan: model mana lebih unggul & kenapa

**📦 DoD Minggu 2:** ada **tabel perbandingan 2 model** + artefak `ecod.pkl` tersimpan.

---

### 🗓️ Minggu 3 — Streaming Backbone (Iggy + Producer)

**Tujuan:** data benar-benar mengalir di stream.

- [ ] Bikin `docker-compose.yml` awal → service **Iggy** saja dulu
- [ ] `docker compose up iggy` → pastikan broker nyala
- [ ] `producer/producer.py`:
  - [ ] Baca `creditcard.csv` baris per baris
  - [ ] Kirim tiap baris ke Iggy (format JSON) dengan jeda kecil (`time.sleep`) = simulasi real-time
  - [ ] Argumen kecepatan (mis. `--rps 50` transaksi/detik)
- [ ] Bikin consumer test sederhana → pastikan pesan sampai

**📦 DoD Minggu 3:** producer kirim data, consumer test nerima → data mengalir. ✅

> Skill `iggy` & `docker-compose` siap bantu di tahap ini.

---

### 🗓️ Minggu 4 — Flink + Skoring + Simpan

**Tujuan:** pipeline end-to-end jalan lokal.

- [ ] `flink_job/job.py` (PyFlink):
  - [ ] Source = konsumsi dari Iggy
  - [ ] Hitung fitur window: rata-rata `Amount` 10 detik, frekuensi transaksi/window
  - [ ] Panggil scorer untuk tiap event
- [ ] `ml/scorer.py`:
  - [ ] Load `ecod.pkl` → `score()` per event (baseline)
  - [ ] HS-Trees `learn_one` + `score_one` per event (online)
  - [ ] Output: `{transaksi, skor_ecod, skor_hst, is_anomali}`
- [ ] Sink ke **Paimon**: tulis hasil ke tabel (skill `flink` + `paimon` bantu)
- [ ] Tambah semua service ke `docker-compose.yml`

**📦 DoD Minggu 4:** `docker compose up` → data mengalir → skor keluar → tersimpan di Paimon. ✅

---

### 🗓️ Minggu 5 — Dashboard & Alert

**Tujuan:** visual cakep + notifikasi anomali.

- [ ] `dashboard/app.py` (Streamlit):
  - [ ] Grafik transaksi masuk (live refresh)
  - [ ] Titik merah saat anomali terdeteksi
  - [ ] Tabel transaksi terbaru + skor 2 model berdampingan
  - [ ] Metrik ringkas: total transaksi, total anomali, % anomali
- [ ] **Alert**: log ke file + (opsional) notif Telegram saat skor lewat ambang
- [ ] Dashboard baca dari **Postgres** (siapkan service `postgres` di compose) — ini yang
      nanti dipakai versi online

**📦 DoD Minggu 5:** dashboard lokal nampilin data live + alert nyala. ✅

---

### 🗓️ Minggu 6 — Deploy Level 2 & Polish

**Tujuan:** repo publik + dashboard online + siap dipamerkan.

- [ ] Tes bersih: hapus container, `docker compose up` dari nol → semua nyala
- [ ] Siapkan **Supabase/Postgres gratis** (online) → pipeline tulis hasil run terakhir ke sana
- [ ] **Deploy `dashboard/app.py` ke Streamlit Cloud** (connect GitHub) → dashboard baca dari Supabase
- [ ] **README.md** kuat:
  - [ ] Diagram arsitektur
  - [ ] GIF/screenshot demo
  - [ ] Tabel perbandingan ECOD vs HS-Trees
  - [ ] Cara jalanin (`docker compose up`)
  - [ ] Link dashboard online
- [ ] Rapikan kode (docstring, type hints), commit final, tag `v1.0`

**📦 DoD Minggu 6:** link dashboard online hidup + README lengkap + repo rapi. 🎉

---

## 5. Checklist "Apakah Aku Tersesat?"

Kalau bingung, jawab ini:
1. **Aku lagi di minggu berapa?** → kerjakan hanya tugas minggu itu.
2. **DoD minggu sebelumnya udah ✅?** → kalau belum, balik selesaikan dulu.
3. **Komponen yang kukerjain udah kutes sendiri?** → tes isolated dulu, baru sambung.
4. **Buntu di satu hal > 1 jam?** → tanya Cauw, jangan kebanyakan nyoba sendiri.

---

## 6. Tumpukan Teknologi

| Lapisan | Teknologi | Skill terpasang |
|---------|-----------|-----------------|
| Sumber | Python producer (replay CSV) | — |
| Broker | **Iggy** | ✅ `iggy` |
| Proses | **Apache Flink** (PyFlink) | ✅ `flink` |
| Model | **PyOD (ECOD)** + **River (HS-Trees)** | — |
| Storage | **Paimon** (+ Postgres untuk dashboard) | ✅ `paimon` |
| Dashboard | **Streamlit** (online di Streamlit Cloud) | — |
| Orkestrasi | **Docker Compose** | ✅ `docker-compose` |

---

## 7. Metrik Evaluasi (WAJIB diingat)

Dataset fraud sangat imbalanced (~0.17%) → **JANGAN pakai accuracy.**

| Metrik | Kenapa penting |
|--------|----------------|
| **Precision** | Dari yang ditandai anomali, berapa yang benar fraud |
| **Recall** | Dari semua fraud, berapa yang berhasil ketangkap |
| **F1** | Keseimbangan precision & recall |
| **PR-AUC** | Metrik utama untuk data imbalanced |

---

## 8. Langkah Paling Pertama (mulai SEKARANG)

1. Pastikan **Docker** & **Python 3.11** terinstall.
2. Minta Cauw **scaffold** semua folder + file kerangka (`.gitignore`, `README`, `requirements.txt`).
3. Download dataset Kaggle → `data/creditcard.csv`.
4. Mulai **Minggu 1**: EDA di notebook.

> Begitu siap, bilang ke Cauw: *"scaffold projeknya"* — nanti Cauw isi semua kerangka file.
