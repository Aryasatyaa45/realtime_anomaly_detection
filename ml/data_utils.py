"""Util bersama untuk pemuatan & persiapan data.

Dipakai oleh train.py dan evaluate.py supaya logika split fitur/label konsisten.
Strategi: unsupervised anomaly detection — model di-fit pada transaksi NORMAL saja,
fraud hanya dipakai saat evaluasi.

Catatan desain scaling:
- ECOD (PyOD) bekerja baik dengan StandardScaler (mean 0, varian 1).
- Half-Space Trees (River) mengasumsikan fitur berada di rentang [0, 1], sehingga
  WAJIB pakai MinMaxScaler — bukan StandardScaler. Salah scaler -> skor HST jadi sampah.

Karena itu split (pembagian data) dipisah dari scaling: `split_raw` mengembalikan data
mentah, lalu `fit_scaler` membuat scaler sesuai model. Scaler disimpan sebagai artefak
agar Flink scorer nanti memakai transformasi yang sama persis.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler, StandardScaler

# Kolom yang dipakai sebagai fitur: V1..V28 + Amount (Time dibuang, bukan sinyal fraud).
FEATURE_COLUMNS: list[str] = [f"V{i}" for i in range(1, 29)] + ["Amount"]
LABEL_COLUMN = "Class"
RANDOM_STATE = 42


def load_dataframe(csv_path: str | Path) -> pd.DataFrame:
    """Muat dataset dari CSV.

    Args:
        csv_path: Lokasi file creditcard.csv.

    Returns:
        DataFrame mentah.

    Raises:
        FileNotFoundError: Jika file tidak ada.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Dataset tidak ditemukan di '{path}'. "
            "Download dari Kaggle (mlg-ulb/creditcardfraud) ke folder data/."
        )
    return pd.read_csv(path)


def split_raw(
    df: pd.DataFrame, test_size: float = 0.3
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Pisah data menjadi train (normal saja) & test (campuran), TANPA scaling.

    Penting: model anomaly detection unsupervised hanya boleh "melihat" data normal saat
    belajar. Maka data fraud dikeluarkan dari train, dan test berisi campuran normal + fraud
    agar evaluasi realistis. Scaling sengaja TIDAK dilakukan di sini — itu tugas
    `fit_scaler` karena tiap model butuh scaler berbeda.

    Args:
        df: DataFrame mentah hasil load_dataframe.
        test_size: Proporsi data untuk test set (default 0.3).

    Returns:
        Tuple (X_train, X_test, y_train, y_test) berupa array mentah (belum distandarkan).
        X_train hanya berisi transaksi normal; X_test campuran normal + fraud.
    """
    X = df[FEATURE_COLUMNS].to_numpy(dtype=float)
    y = df[LABEL_COLUMN].to_numpy(dtype=int)

    # Split stratified supaya proporsi fraud di test mewakili kondisi nyata.
    X_train_full, X_test, y_train_full, y_test = train_test_split(
        X, y, test_size=test_size, random_state=RANDOM_STATE, stratify=y
    )

    # Buang fraud dari train: model hanya belajar pola "normal".
    normal_mask = y_train_full == 0
    X_train = X_train_full[normal_mask]
    y_train = y_train_full[normal_mask]

    return X_train, X_test, y_train, y_test


def fit_scaler(X_train: np.ndarray, kind: str = "standard"):
    """Buat & fit scaler pada data train (HANYA train, cegah kebocoran info test).

    Args:
        X_train: Array fitur transaksi normal (mentah).
        kind: "standard" untuk StandardScaler (ECOD) atau "minmax" untuk
            MinMaxScaler (Half-Space Trees, butuh rentang [0, 1]).

    Returns:
        Scaler sklearn yang sudah di-fit (punya .transform).

    Raises:
        ValueError: Jika kind tidak dikenali.
    """
    if kind == "standard":
        scaler = StandardScaler()
    elif kind == "minmax":
        scaler = MinMaxScaler()
    else:
        raise ValueError(f"Jenis scaler tidak dikenal: '{kind}' (pakai 'standard'/'minmax').")
    scaler.fit(X_train)
    return scaler
