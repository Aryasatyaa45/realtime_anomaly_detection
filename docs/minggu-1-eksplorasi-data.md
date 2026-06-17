# 📘 Laporan Minggu 1 — Eksplorasi & Pemahaman Data

> Bagian dari projek **Real-Time Anomaly Detection Pipeline** (deteksi fraud transaksi).
> Dokumen ini menjelaskan **apa yang dikerjakan**, **konsep dasarnya**, dan **temuan** dari
> tahap eksplorasi data (EDA). Ditulis supaya bisa dipahami orang awam sekalipun.

---

## 🎯 Tujuan Minggu 1

Sebelum membuat model apa pun, kita **wajib paham datanya dulu**. Ibarat dokter: tidak
mungkin meresepkan obat sebelum memeriksa pasien. Maka tahap ini:

1. Memuat dan memeriksa data transaksi kartu kredit.
2. Memahami seberapa langka kasus fraud.
3. Membuktikan **kenapa metrik "accuracy" tidak boleh dipakai** di kasus ini.
4. Menemukan pola/fitur yang membedakan transaksi fraud dari yang normal.

---

## 🔑 Konsep Dasar (baca ini dulu)

### Apa itu *fraud*?

**Fraud** = transaksi penipuan. Dalam konteks kartu kredit, ini transaksi yang dilakukan
**bukan oleh pemilik sah** kartu — misalnya kartu dicuri, nomor kartu bocor, atau pembobolan.
Tujuan projek ini: **mendeteksi transaksi fraud secara otomatis dan real-time**, supaya bisa
diblokir sebelum merugikan korban.

### Apa itu *anomaly detection*?

**Anomaly detection** (deteksi anomali) = teknik menemukan data yang **"aneh" / menyimpang**
dari pola mayoritas. Fraud pada dasarnya adalah anomali: ia berperilaku beda dari transaksi
normal. Jadi alih-alih mengajari model "ini fraud, ini bukan" satu per satu (sulit, karena
contoh fraud sangat sedikit), kita ajari model mengenali **"seperti apa transaksi normal"**,
lalu apa pun yang menyimpang jauh ditandai sebagai kecurigaan.

### Apa itu data *imbalanced* (tidak seimbang)?

**Imbalanced** = jumlah antar-kategori sangat timpang. Di data kita:

- Transaksi **normal**: 284.315 (99,83%)
- Transaksi **fraud**: 492 (0,17%)

Artinya **1 fraud per ~577 transaksi normal**. Bayangkan mencari 492 jarum di tumpukan
284.807 jerami. Ketimpangan ekstrem ini adalah **tantangan utama** projek dan menentukan
seluruh strategi kita (pemilihan model & metrik).

### Kenapa imbalanced itu masalah?

Karena algoritma cenderung "malas": kalau 99,83% data normal, cara termudah mendapat skor
bagus adalah **menebak semuanya normal**. Model seperti ini terlihat hebat di atas kertas,
tapi **gagal total** di tugas sebenarnya (menangkap fraud). Inilah yang membawa kita ke poin
paling penting di bawah.

---

## ⚠️ Kenapa TIDAK pakai *accuracy*? (temuan terpenting)

**Accuracy** (akurasi) = persentase tebakan yang benar:

```
accuracy = jumlah tebakan benar / total tebakan
```

Terdengar masuk akal, tapi **menyesatkan** di data imbalanced. Kami membuktikannya secara
angka di notebook:

> Kami membuat model **paling bodoh** yang tidak belajar apa pun — ia **selalu menebak
> "normal"** untuk setiap transaksi. Hasilnya:

| Metrik | Nilai | Maknanya |
|--------|-------|----------|
| **Accuracy** | **99,83%** | Terlihat luar biasa 😍 |
| **Recall** | **0%** | Tidak menangkap **satu fraud pun** 😱 |
| **Precision** | **0%** | Tidak ada deteksi fraud yang benar |

Model yang **tidak berguna sama sekali** tetap mendapat akurasi 99,83%, hanya karena ia
menebak kelas mayoritas. **Akurasi tinggi di sini adalah ilusi.**

**Analogi:** alarm kebakaran yang tidak pernah berbunyi "benar" 99,9% waktu (karena kebakaran
langka), tapi sama sekali tidak berguna saat kebakaran benar terjadi.

---

## ✅ Metrik yang Kami Pakai (dan kenapa)

Kami fokus ke kelas minoritas (fraud), bukan ke mayoritas:

| Metrik | Menjawab pertanyaan | Kenapa penting |
|--------|---------------------|----------------|
| **Precision** | Dari yang ditandai fraud, berapa % benar fraud? | Hindari terlalu banyak alarm palsu |
| **Recall** | Dari semua fraud asli, berapa % berhasil ketangkap? | Hindari fraud lolos |
| **F1** | Keseimbangan precision & recall | Satu angka ringkas |
| **PR-AUC** ⭐ | Performa precision-recall di semua ambang | **Metrik utama** untuk data imbalanced |

> **Trade-off yang perlu disadari:** menaikkan recall (tangkap lebih banyak fraud) biasanya
> menurunkan precision (alarm palsu naik), dan sebaliknya. Tugas kita mencari keseimbangan
> yang pas — itulah gunanya F1 dan PR-AUC.

---

## 📊 Apa yang Dikerjakan & Temuannya

Seluruh analisis ada di `notebooks/eksplorasi.ipynb` dan sudah dijalankan tanpa error.

### 1. Struktur data
- **284.807 transaksi**, **31 kolom**.
- Kolom: `Time`, `V1`–`V28`, `Amount`, `Class`.
  - `Time` — detik sejak transaksi pertama (data mencakup ~2 hari).
  - `V1`–`V28` — fitur hasil **PCA** (sudah dianonimkan demi privasi; nama asli dirahasiakan).
  - `Amount` — nominal transaksi.
  - `Class` — label: `0` = normal, `1` = fraud.
- **Tidak ada nilai kosong (NaN)** → data bersih, tidak perlu imputasi.

### 2. Tingkat ketimpangan (imbalance)
- Fraud hanya **492 dari 284.807 (0,173%)**.
- Rasio **1 : 577** (fraud : normal).

### 3. Bukti accuracy menyesatkan
- Model "selalu normal" → akurasi **99,83%**, recall **0%** (lihat bagian di atas).

### 4. Analisis nominal transaksi (`Amount`)
- **Median Amount fraud (9,25) lebih kecil** dari normal (22,00).
- Pelaku fraud cenderung mulai dengan nominal kecil (kemungkinan menguji apakah kartu masih
  aktif sebelum transaksi besar).
- Namun `Amount` saja **bukan pembeda yang kuat** — banyak transaksi normal juga bernominal kecil.

### 5. Analisis waktu (`Time`)
- Transaksi normal mengikuti pola harian (ramai siang, sepi malam).
- Fraud tersebar lebih merata — tidak begitu mengikuti pola harian.

### 6. Fitur paling membedakan fraud
Berdasarkan kekuatan korelasi dengan `Class`, fitur paling diskriminatif:

| Peringkat | Fitur | Korelasi |
|-----------|-------|----------|
| 1 | **V17** | −0,326 |
| 2 | **V14** | −0,303 |
| 3 | **V12** | −0,261 |
| 4 | **V10** | −0,217 |
| 5 | **V16** | −0,197 |

> Korelasi **negatif** berarti: makin **rendah** nilai fitur tersebut, makin besar kemungkinan
> transaksi itu fraud. Fitur-fitur ini menjadi **sinyal kuat** untuk model nanti.

---

## 🧭 Kesimpulan Minggu 1

1. Data sangat **imbalanced** (fraud 0,17%) — ini menentukan strategi seluruh projek.
2. **Accuracy dilarang** sebagai metrik; kita pakai **Precision, Recall, F1, PR-AUC**.
3. Data **bersih** (tanpa NaN), siap untuk modeling.
4. Beberapa fitur V (V17, V14, V12, V10, V16) terbukti **diskriminatif** — modal bagus untuk model.
5. Pendekatan yang dipilih: **unsupervised anomaly detection** — model belajar pola "normal",
   lalu menandai penyimpangan sebagai dugaan fraud.

**✅ Definition of Done Minggu 1 tercapai:** notebook berjalan tanpa error, dan alasan
"kenapa bukan accuracy" sudah bisa dijelaskan dengan bukti angka.

---

## ➡️ Langkah Berikutnya (Minggu 2)

Melatih dan membandingkan **dua model**:

- **ECOD** (library PyOD) — baseline cepat, tanpa tuning, jago data berdimensi tinggi.
- **Half-Space Trees** (library River) — *online learning* sejati, belajar per-transaksi
  (cocok untuk skenario streaming/real-time).

Keduanya dievaluasi dengan metrik di atas, lalu disajikan dalam **tabel perbandingan**.
