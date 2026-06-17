"""Evaluasi & bandingkan ECOD vs Half-Space Trees pada test set.

Metrik untuk data imbalanced: Precision, Recall, F1, PR-AUC, ROC-AUC.
JANGAN pakai accuracy (fraud hanya ~0.17%).

Threshold dipilih dari titik PR-curve dengan F1 terbaik (bukan ditebak manual).

Jalankan (setelah train.py):
    python ml/evaluate.py
"""

from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from river import anomaly
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)

from data_utils import FEATURE_COLUMNS, load_dataframe, split_raw

DATA_PATH = Path("data/creditcard.csv")
MODEL_DIR = Path("ml/models")
ECOD_PATH = MODEL_DIR / "ecod.pkl"
HST_PATH = MODEL_DIR / "hst.pkl"
ECOD_SCALER_PATH = MODEL_DIR / "scaler_ecod.pkl"
HST_SCALER_PATH = MODEL_DIR / "scaler_hst.pkl"
REPORT_DIR = Path("notebooks")
RESULT_CSV = REPORT_DIR / "hasil_perbandingan.csv"


def best_threshold_by_f1(y_true: np.ndarray, scores: np.ndarray) -> float:
    """Cari ambang skor yang memaksimalkan F1 dari PR-curve.

    Args:
        y_true: Label asli (0/1).
        scores: Skor anomali (makin tinggi makin anomali).

    Returns:
        Nilai ambang terbaik.
    """
    precision, recall, thresholds = precision_recall_curve(y_true, scores)
    # precision_recall_curve memberi N+1 titik; thresholds berukuran N.
    f1 = np.divide(
        2 * precision * recall,
        precision + recall,
        out=np.zeros_like(precision),
        where=(precision + recall) > 0,
    )
    best_idx = int(np.argmax(f1[:-1])) if len(thresholds) else 0
    return float(thresholds[best_idx]) if len(thresholds) else 0.5


def evaluate_scores(name: str, y_true: np.ndarray, scores: np.ndarray) -> dict[str, float]:
    """Hitung metrik lengkap untuk satu model berdasarkan skor anomalinya.

    Args:
        name: Nama model (untuk pelabelan).
        y_true: Label asli (0/1).
        scores: Skor anomali kontinu (tinggi = anomali).

    Returns:
        Dict metrik: precision, recall, f1, pr_auc, roc_auc, threshold.
    """
    pr_auc = average_precision_score(y_true, scores)  # = PR-AUC, metrik utama
    roc_auc = roc_auc_score(y_true, scores)

    thr = best_threshold_by_f1(y_true, scores)
    y_pred = (scores >= thr).astype(int)

    return {
        "model": name,
        "precision": precision_score(y_true, y_pred, zero_division=0),
        "recall": recall_score(y_true, y_pred, zero_division=0),
        "f1": f1_score(y_true, y_pred, zero_division=0),
        "pr_auc": pr_auc,
        "roc_auc": roc_auc,
        "threshold": thr,
    }


def score_ecod(model, X_test: np.ndarray) -> np.ndarray:
    """Skor anomali ECOD untuk test set (decision_function: tinggi = anomali)."""
    return model.decision_function(X_test)


def score_hst(model: anomaly.HalfSpaceTrees, X_test: np.ndarray) -> np.ndarray:
    """Skor anomali HS-Trees per-baris (score_one), meniru mode streaming.

    Catatan: kita TIDAK memanggil learn_one di sini agar evaluasi adil (model
    sudah dipanaskan saat training). score_one mengembalikan nilai 0..1.
    """
    scores = np.empty(X_test.shape[0], dtype=float)
    for i, row in enumerate(X_test):
        sample = {name: float(val) for name, val in zip(FEATURE_COLUMNS, row)}
        scores[i] = model.score_one(sample)
    return scores


def main() -> None:
    """Pipeline evaluasi: load model -> skor test -> hitung metrik -> tabel perbandingan."""
    df = load_dataframe(DATA_PATH)
    _, X_test_raw, _, y_test = split_raw(df)
    print(f"Test set: {len(y_test):,} transaksi | fraud: {int(y_test.sum())}")

    ecod = joblib.load(ECOD_PATH)
    hst = joblib.load(HST_PATH)
    scaler_ecod = joblib.load(ECOD_SCALER_PATH)
    scaler_hst = joblib.load(HST_SCALER_PATH)

    # Tiap model di-skor dengan scaler-nya sendiri (ECOD: standard, HST: minmax).
    X_test_ecod = scaler_ecod.transform(X_test_raw)
    X_test_hst = scaler_hst.transform(X_test_raw)

    rows = [
        evaluate_scores("ECOD", y_test, score_ecod(ecod, X_test_ecod)),
        evaluate_scores("Half-Space Trees", y_test, score_hst(hst, X_test_hst)),
    ]

    result = pd.DataFrame(rows).set_index("model")
    pd.set_option("display.float_format", lambda x: f"{x:.4f}")

    print("\n=== Tabel Perbandingan (test set) ===")
    print(result.to_string())

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    result.to_csv(RESULT_CSV)
    print(f"\nTabel disimpan -> {RESULT_CSV}")

    winner = result["pr_auc"].idxmax()
    print(f"\nPemenang berdasarkan PR-AUC (metrik utama): {winner}")


if __name__ == "__main__":
    main()
