"""Latih model offline: ECOD (PyOD) + Half-Space Trees (River).

Strategi: unsupervised anomaly detection. Model di-fit pada transaksi NORMAL saja
(fraud hanya untuk evaluasi di evaluate.py).

- ECOD   : model batch — sekali fit pada seluruh data normal, lalu disimpan ke ecod.pkl.
- HS-Tree: model online — belajar per-transaksi (learn_one). Di sini kita "warm-up" lalu
           simpan agar evaluate.py memakai model yang sama.

Jalankan:
    python ml/train.py
"""

from __future__ import annotations

import time
from pathlib import Path

import joblib
from pyod.models.ecod import ECOD
from river import anomaly

from data_utils import FEATURE_COLUMNS, fit_scaler, load_dataframe, split_raw

DATA_PATH = Path("data/creditcard.csv")
MODEL_DIR = Path("ml/models")
ECOD_PATH = MODEL_DIR / "ecod.pkl"
HST_PATH = MODEL_DIR / "hst.pkl"
# Scaler disimpan terpisah karena tiap model butuh scaler berbeda. Artefak ini
# nanti dipakai ulang oleh Flink scorer (Minggu 4) agar transformasi konsisten.
ECOD_SCALER_PATH = MODEL_DIR / "scaler_ecod.pkl"
HST_SCALER_PATH = MODEL_DIR / "scaler_hst.pkl"


def train_ecod(X_train) -> ECOD:
    """Fit model ECOD pada data normal.

    ECOD (Empirical-Cumulative-distribution-based Outlier Detection) bersifat
    parameter-free: tidak perlu tuning, cepat, dan cocok untuk data berdimensi tinggi.

    Args:
        X_train: Array fitur transaksi normal (sudah distandarkan).

    Returns:
        Objek ECOD yang sudah dilatih.
    """
    print(f"[ECOD] Melatih pada {X_train.shape[0]:,} transaksi normal ...")
    t0 = time.perf_counter()
    model = ECOD()
    model.fit(X_train)
    print(f"[ECOD] Selesai dalam {time.perf_counter() - t0:.1f} detik.")
    return model


def train_hst(X_train) -> anomaly.HalfSpaceTrees:
    """Warm-up model Half-Space Trees secara online pada data normal.

    HS-Trees belajar satu sampel demi satu sampel (learn_one) — meniru cara kerja
    streaming. Di sini kita latih pada data normal sebagai pemanasan.

    Args:
        X_train: Array fitur transaksi normal (sudah distandarkan).

    Returns:
        Model HalfSpaceTrees yang sudah dipanaskan.
    """
    print(f"[HST] Warm-up online pada {X_train.shape[0]:,} transaksi normal ...")
    t0 = time.perf_counter()
    # n_features wajib diset; limits default [0,1] → River menstandarkan internal.
    # PENTING: HST mengasumsikan tiap fitur di rentang [0, 1] (limits default River).
    # Input WAJIB sudah di-MinMaxScaler — kalau StandardScaler, skor jadi sampah.
    model = anomaly.HalfSpaceTrees(n_trees=25, height=15, window_size=250, seed=42)
    for row in X_train:
        sample = {name: float(val) for name, val in zip(FEATURE_COLUMNS, row)}
        model.learn_one(sample)
    print(f"[HST] Selesai dalam {time.perf_counter() - t0:.1f} detik.")
    return model


def main() -> None:
    """Pipeline pelatihan: load -> split -> scaling per model -> latih -> simpan artefak."""
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    df = load_dataframe(DATA_PATH)
    print(f"Data dimuat: {len(df):,} transaksi.")

    X_train_raw, _, _, _ = split_raw(df)

    # ECOD: StandardScaler (mean 0, varian 1).
    scaler_ecod = fit_scaler(X_train_raw, kind="standard")
    X_train_ecod = scaler_ecod.transform(X_train_raw)
    ecod = train_ecod(X_train_ecod)
    joblib.dump(ecod, ECOD_PATH)
    joblib.dump(scaler_ecod, ECOD_SCALER_PATH)
    print(f"[ECOD] Artefak disimpan -> {ECOD_PATH} (+ {ECOD_SCALER_PATH})")

    # HST: MinMaxScaler -> fitur di rentang [0, 1] sesuai asumsi Half-Space Trees.
    scaler_hst = fit_scaler(X_train_raw, kind="minmax")
    X_train_hst = scaler_hst.transform(X_train_raw)
    hst = train_hst(X_train_hst)
    joblib.dump(hst, HST_PATH)
    joblib.dump(scaler_hst, HST_SCALER_PATH)
    print(f"[HST] Artefak disimpan -> {HST_PATH} (+ {HST_SCALER_PATH})")

    print("\nSelesai. Lanjut: python ml/evaluate.py")


if __name__ == "__main__":
    main()
