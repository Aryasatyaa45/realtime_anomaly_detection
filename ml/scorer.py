"""Service skoring real-time: skor tiap transaksi dengan ECOD + Half-Space Trees.

Dijalankan sebagai **HTTP service terpisah** (bukan di dalam image PyFlink) supaya versi
dependency ML dipin persis sama dengan environment training — kalau model `.pkl` di-unpickle
di lingkungan dengan numpy/sklearn/pyod/river versi beda, load bisa gagal. Flink job memanggil
service ini per-event lewat HTTP (`POST /score`), jadi Flink cukup butuh `requests`.

Service ini juga jadi rumah natural untuk **state online Half-Space Trees**: tiap transaksi
di-`score_one` lalu di-`learn_one` (belajar berkelanjutan). Karena state HST berubah tiap call,
akses ke model HST dijaga dengan lock agar aman saat banyak request paralel.

Logika skoring identik dengan ml/evaluate.py (Minggu 2):
- ECOD : StandardScaler -> decision_function (tinggi = anomali), ambang 120.58.
- HST  : MinMaxScaler  -> score_one (0..1, tinggi = anomali), ambang 0.914, lalu learn_one.

Jalankan (dari root projek atau dari dalam container):
    python ml/scorer.py
Lalu tes:
    curl -X POST localhost:8000/score -H "Content-Type: application/json" \
         -d '{"V1": -1.36, ..., "V28": -0.02, "Amount": 149.62}'
"""

from __future__ import annotations

import json
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import joblib
import numpy as np
from scipy.stats import skew as skew_sp

# Urutan fitur WAJIB sama persis dengan ml/data_utils.FEATURE_COLUMNS (V1..V28 + Amount).
# Sengaja didefinisikan ulang di sini — bukan di-import dari data_utils — agar service tidak
# menyeret dependency pandas (data_utils meng-import pandas di level modul).
FEATURE_COLUMNS: list[str] = [f"V{i}" for i in range(1, 29)] + ["Amount"]

# Ambang optimal hasil Minggu 2 (titik F1 terbaik pada PR-curve). Bisa di-override via env.
ECOD_THRESHOLD = float(os.environ.get("ECOD_THRESHOLD", "120.58"))
HST_THRESHOLD = float(os.environ.get("HST_THRESHOLD", "0.914"))

# Lokasi artefak: default relatif ke file ini (ml/models), jadi konsisten baik saat
# dijalankan dari host (E:\...\ml\models) maupun di container (/app/models).
MODEL_DIR = Path(os.environ.get("MODEL_DIR", str(Path(__file__).resolve().parent / "models")))

HOST = os.environ.get("SCORER_HOST", "0.0.0.0")
PORT = int(os.environ.get("SCORER_PORT", "8000"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] scorer: %(message)s",
)
logger = logging.getLogger("scorer")


class FastECOD:
    """ECOD scoring O(log n) per titik — pengganti drop-in ECOD.decision_function asli.

    PyOD ECOD menyimpan seluruh X_train dan menghitung ulang ECDF atas (X_train + titik baru)
    SETIAP call decision_function — ~2.8 dtk/titik untuk 199k baris training, tak layak streaming.
    Karena untuk satu titik baru ECDF tiap kolom hanya butuh "berapa nilai training <= / >= x",
    kita precompute kolom training tersortir sekali, lalu pakai searchsorted (O(log n)) per call.

    Skornya identik secara matematis dengan ECOD.decision_function (terverifikasi: selisih ~1e-14,
    0 perbedaan flag pada ambang 120.58), jadi ambang Minggu 2 tetap berlaku tanpa perubahan.
    """

    def __init__(self, ecod: Any) -> None:
        """Precompute dari model ECOD terlatih.

        Args:
            ecod: Instance pyod ECOD hasil fit (punya atribut X_train, sudah ter-scale).
        """
        x_train = np.asarray(ecod.X_train, dtype=float)
        self._n = x_train.shape[0]
        self._sorted_cols = np.sort(x_train, axis=0)  # (n, d) tersortir per kolom
        # Tanda skewness tiap kolom dihitung sekali dari data training; penambahan satu titik
        # tidak mengubah tandanya secara praktis (sama seperti asumsi ECOD asli per-batch kecil).
        self._skew_sign = np.sign(np.nan_to_num(skew_sp(x_train, axis=0)))

    def decision_function_one(self, x_scaled: np.ndarray) -> float:
        """Skor satu titik (sudah di-scale StandardScaler), meniru decision_function asli.

        Args:
            x_scaled: Array bentuk (1, d) atau (d,) — fitur sudah ter-scale.

        Returns:
            Skor anomali ECOD (makin tinggi makin anomali), identik dgn ECOD.decision_function.
        """
        x = np.asarray(x_scaled, dtype=float).ravel()
        n1 = self._n + 1  # gabungan mencakup titik baru itu sendiri (seperti concat di PyOD)
        left_cnt = np.array(
            [np.searchsorted(self._sorted_cols[:, j], x[j], side="right") for j in range(x.size)]
        ) + 1  # P(X <= x): +1 karena x <= x
        right_cnt = self._n - np.array(
            [np.searchsorted(self._sorted_cols[:, j], x[j], side="left") for j in range(x.size)]
        ) + 1  # P(X >= x): +1 karena x >= x
        u_l = -np.log(left_cnt / n1)
        u_r = -np.log(right_cnt / n1)
        u_skew = u_l * -1 * np.sign(self._skew_sign - 1) + u_r * np.sign(self._skew_sign + 1)
        o = np.maximum(np.maximum(u_l, u_r), u_skew)
        return float(o.sum())


class Scorer:
    """Pembungkus kedua model + scaler, dengan skoring thread-safe.

    Model & scaler dimuat sekali saat inisialisasi (mahal: ecod.pkl ~234 MB). ECOD bersifat
    read-only sehingga aman dipanggil paralel; Half-Space Trees menyimpan state yang berubah
    tiap `learn_one`, jadi semua akses HST diserialisasi lewat satu lock.
    """

    def __init__(self, model_dir: Path) -> None:
        """Muat model & scaler dari direktori artefak.

        Args:
            model_dir: Folder berisi ecod.pkl, hst.pkl, scaler_ecod.pkl, scaler_hst.pkl.

        Raises:
            FileNotFoundError: Jika salah satu artefak tidak ada.
        """
        required = ["ecod.pkl", "hst.pkl", "scaler_ecod.pkl", "scaler_hst.pkl"]
        missing = [name for name in required if not (model_dir / name).exists()]
        if missing:
            raise FileNotFoundError(
                f"Artefak model tidak ditemukan di '{model_dir}': {missing}. "
                "Jalankan ml/train.py dulu (Minggu 2)."
            )

        logger.info("Memuat model dari %s ...", model_dir)
        ecod = joblib.load(model_dir / "ecod.pkl")
        # Bungkus dgn FastECOD: decision_function asli ~2.8 dtk/titik (recompute ECDF 199k baris
        # tiap call) -> tak layak streaming. FastECOD precompute kolom tersortir -> ~3 ms/titik,
        # skor identik (selisih ~1e-14). Ambang 120.58 tetap berlaku.
        self._ecod = FastECOD(ecod)
        self._hst = joblib.load(model_dir / "hst.pkl")
        self._scaler_ecod = joblib.load(model_dir / "scaler_ecod.pkl")
        self._scaler_hst = joblib.load(model_dir / "scaler_hst.pkl")
        self._hst_lock = threading.Lock()
        logger.info("Model siap. Ambang ECOD=%.2f, HST=%.3f.", ECOD_THRESHOLD, HST_THRESHOLD)

    def _extract_features(self, event: dict[str, Any]) -> np.ndarray:
        """Ambil 29 fitur dari event sesuai urutan FEATURE_COLUMNS.

        Args:
            event: Dict transaksi (boleh berisi kolom ekstra seperti Time/Class — diabaikan).

        Returns:
            Array bentuk (1, 29) bertipe float, siap untuk scaler.transform.

        Raises:
            KeyError: Jika ada fitur wajib yang hilang.
            ValueError: Jika nilai fitur tidak bisa dikonversi ke float.
        """
        missing = [c for c in FEATURE_COLUMNS if c not in event]
        if missing:
            raise KeyError(f"Fitur hilang: {missing}")
        try:
            values = [float(event[c]) for c in FEATURE_COLUMNS]
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Nilai fitur tidak numerik: {exc}") from exc
        return np.array([values], dtype=float)

    def score(self, event: dict[str, Any], learn: bool = True) -> dict[str, Any]:
        """Skor satu transaksi dengan kedua model.

        ECOD memakai StandardScaler + decision_function; HST memakai MinMaxScaler + score_one,
        lalu learn_one (pembelajaran online). HST dijaga lock agar state konsisten antar-thread.

        Args:
            event: Satu transaksi (dict fitur V1..V28, Amount, kolom lain diabaikan).
            learn: Bila True (default), HST memperbarui state via learn_one (dipakai pipeline
                streaming). Bila False, HST hanya skor TANPA belajar — dipakai untuk skoring
                ad-hoc (mis. upload CSV di dashboard) agar tidak mencemari state model live.

        Returns:
            Dict berisi skor_ecod, skor_hst, is_anomali_ecod, is_anomali_hst, is_anomali.
        """
        x_raw = self._extract_features(event)

        # ECOD — read-only, aman tanpa lock.
        x_ecod = self._scaler_ecod.transform(x_raw)
        skor_ecod = self._ecod.decision_function_one(x_ecod)

        # HST — score lalu (opsional) learn (online); serialisasi via lock karena state berubah.
        x_hst = self._scaler_hst.transform(x_raw)
        sample = {name: float(val) for name, val in zip(FEATURE_COLUMNS, x_hst[0])}
        with self._hst_lock:
            skor_hst = float(self._hst.score_one(sample))
            if learn:
                self._hst.learn_one(sample)

        is_anom_ecod = skor_ecod >= ECOD_THRESHOLD
        is_anom_hst = skor_hst >= HST_THRESHOLD
        return {
            "skor_ecod": skor_ecod,
            "skor_hst": skor_hst,
            "is_anomali_ecod": is_anom_ecod,
            "is_anomali_hst": is_anom_hst,
            # Half-Space Trees adalah model pemenang (Minggu 2) -> jadi keputusan utama.
            "is_anomali": is_anom_hst,
        }


def make_handler(scorer: Scorer) -> type[BaseHTTPRequestHandler]:
    """Bangun kelas handler HTTP yang terikat ke instance Scorer tertentu.

    Args:
        scorer: Scorer yang sudah memuat model.

    Returns:
        Subclass BaseHTTPRequestHandler siap dipakai ThreadingHTTPServer.
    """

    class ScoreHandler(BaseHTTPRequestHandler):
        """Handler HTTP: GET /health untuk cek, POST /score untuk skoring."""

        def _send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 (nama wajib dari BaseHTTPRequestHandler)
            if self.path == "/health":
                self._send_json(200, {"status": "ok", "models_loaded": True})
            else:
                self._send_json(404, {"error": f"path tidak dikenal: {self.path}"})

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path != "/score":
                self._send_json(404, {"error": f"path tidak dikenal: {self.path}"})
                return
            # ?learn=false -> skor TANPA update state HST (dipakai upload CSV ad-hoc di dashboard
            # agar state model live tak tercemar). Default (pipeline streaming) tetap belajar.
            learn = parse_qs(parsed.query).get("learn", ["true"])[0].lower() != "false"
            try:
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length) if length else b""
                event = json.loads(raw.decode("utf-8"))
                if not isinstance(event, dict):
                    raise ValueError("body harus berupa objek JSON (dict transaksi).")
            except (ValueError, json.JSONDecodeError) as exc:
                self._send_json(400, {"error": f"body tidak valid: {exc}"})
                return

            try:
                result = scorer.score(event, learn=learn)
            except KeyError as exc:
                self._send_json(400, {"error": str(exc)})
                return
            except Exception as exc:  # pragma: no cover - jaring pengaman
                logger.exception("Gagal skoring transaksi.")
                self._send_json(500, {"error": f"gagal skoring: {exc}"})
                return

            self._send_json(200, result)

        def log_message(self, *_args: Any) -> None:
            """Bungkam log akses bawaan (per-request) agar log tidak berisik."""

    return ScoreHandler


def main() -> None:
    """Muat model lalu jalankan HTTP server sampai dihentikan."""
    scorer = Scorer(MODEL_DIR)
    server = ThreadingHTTPServer((HOST, PORT), make_handler(scorer))
    logger.info("Scorer mendengarkan di http://%s:%d (POST /score, GET /health)", HOST, PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Dihentikan, menutup server.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
