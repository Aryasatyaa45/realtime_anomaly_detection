"""PyFlink job — Minggu 4 (versi 4d: skoring per-event via scorer service).

Lanjutan dari 4c (yang hanya socket -> print). Di 4d setiap baris JSON transaksi dari bridge
di-parse lalu dikirim ke **scorer service** (HTTP `POST /score`) untuk diberi skor anomali oleh
ECOD + Half-Space Trees. Hasilnya dicetak ke stdout TaskManager dengan prefiks `[SCORE]`.

Mengapa skoring lewat HTTP, bukan load model di dalam Flink: versi dependency ML (numpy/sklearn/
pyod/river) harus dipin persis sama dengan environment training agar unpickle model tidak gagal;
memisahkan scorer sebagai service menghindari konflik dependency dengan PyFlink. Flink job cukup
butuh `requests`.

Aliran 4d:
    bridge (TCP socket, 1 baris JSON/transaksi)
        -> socket_text_stream
        -> ScoreMap (parse JSON -> POST http://scorer:8000/score -> gabung skor)
        -> print (stdout TM, prefiks [SCORE])

Window 10s (enrichment demonstratif) ditambahkan di 4d-3; sink Paimon di 4e.

Submit (dari dalam container):
    flink run -d -py /opt/flink_job/job.py
Host/port bridge & URL scorer diambil dari env (default cocok dgn nama service docker-compose).
"""

from __future__ import annotations

import os

from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.data_stream import DataStream
from pyflink.datastream.functions import AllWindowFunction, MapFunction, RuntimeContext
from pyflink.datastream.window import TumblingProcessingTimeWindows


def socket_text_stream(env: StreamExecutionEnvironment, host: str, port: int) -> DataStream:
    """Sumber socket teks untuk PyFlink via gateway Java.

    PyFlink (1.20) TIDAK mengekspos `socket_text_stream` di StreamExecutionEnvironment
    (hanya tersedia di API Java). Kita panggil `socketTextStream` Java lewat gateway lalu
    bungkus DataStream-nya jadi objek PyFlink, sehingga operator Python (map/process) tetap
    bisa dipasang di hilir. Tiap baris teks (dipisah '\\n') = satu transaksi JSON dari bridge.

    Args:
        env: StreamExecutionEnvironment PyFlink.
        host: Host bridge (nama service docker, mis. "bridge").
        port: Port TCP socket bridge.

    Returns:
        DataStream berisi baris teks (Types.STRING) dari socket.
    """
    j_ds = env._j_stream_execution_environment.socketTextStream(host, port)
    return DataStream(j_ds)


class ScoreMap(MapFunction):
    """Operator map: parse baris JSON transaksi -> minta skor ke scorer service -> ringkas.

    Skoring dilakukan lewat HTTP `POST /score` ke scorer (lihat ml/scorer.py). Koneksi HTTP
    di-reuse via satu `requests.Session` per subtask (dibuat di `open`), jauh lebih hemat
    daripada membuka koneksi baru tiap transaksi.

    Error sengaja TIDAK dilempar agar satu transaksi rusak / scorer sesaat tak tersedia tidak
    menghentikan seluruh job: baris bermasalah dikembalikan dengan prefiks [SKIP]/[ERR] dan
    pipeline lanjut.
    """

    def __init__(self, scorer_url: str, timeout: float = 5.0) -> None:
        """Simpan konfigurasi; objek berat (Session) baru dibuat di `open` pada TaskManager.

        Args:
            scorer_url: URL endpoint skoring, mis. "http://scorer:8000/score".
            timeout: Batas waktu (detik) tiap permintaan HTTP ke scorer.
        """
        self._url = scorer_url
        self._timeout = timeout
        self._session = None  # diisi di open() — Session tak bisa diserialisasi antar-node

    def open(self, runtime_context: RuntimeContext) -> None:
        """Buat HTTP Session sekali per subtask (dipanggil Flink saat operator start)."""
        import requests

        self._session = requests.Session()

    def map(self, line: str) -> str:
        """Skor satu transaksi; kembalikan ringkasan string untuk dicetak.

        Args:
            line: Satu baris teks dari socket (diharapkan JSON transaksi).

        Returns:
            String ringkasan: [SCORE] bila sukses, [SKIP]/[ERR] bila gagal parse/koneksi.
        """
        import json

        line = line.strip()
        if not line:
            return "[SKIP] baris kosong"

        try:
            tx = json.loads(line)
        except (ValueError, json.JSONDecodeError) as exc:
            return f"[SKIP] JSON tak valid: {exc}"

        try:
            resp = self._session.post(self._url, json=tx, timeout=self._timeout)
            resp.raise_for_status()
            result = resp.json()
        except Exception as exc:  # noqa: BLE001 - jaring pengaman agar job tak mati
            return f"[ERR] scorer gagal ({self._url}): {exc}"

        flag = "ANOMALI" if result.get("is_anomali") else "normal"
        return (
            f"[SCORE] hst={result.get('skor_hst', float('nan')):.4f} "
            f"ecod={result.get('skor_ecod', float('nan')):.2f} -> {flag} "
            f"(Amount={tx.get('Amount')}, Class={tx.get('Class')})"
        )


def to_amount(line: str) -> float:
    """Ekstrak nilai Amount dari satu baris JSON transaksi (untuk agregasi window).

    Baris yang gagal di-parse dikembalikan sebagai -1.0 agar bisa difilter keluar — Amount
    asli selalu >= 0, jadi nilai negatif aman dipakai sebagai penanda "tidak valid".

    Args:
        line: Satu baris teks dari socket.

    Returns:
        Amount (float) bila valid, atau -1.0 bila baris kosong/JSON rusak/Amount hilang.
    """
    import json

    line = line.strip()
    if not line:
        return -1.0
    try:
        return float(json.loads(line).get("Amount", -1.0))
    except (ValueError, TypeError, json.JSONDecodeError):
        return -1.0


class WindowSummary(AllWindowFunction):
    """Ringkas satu window: jumlah transaksi + rata-rata Amount.

    Window ini bersifat DEMONSTRATIF (bukti Flink melakukan komputasi stateful ber-window);
    hasilnya BUKAN input model — skoring tetap memakai 29 fitur asli per-event di scorer.
    """

    def apply(self, window, inputs):  # noqa: ANN001 - signatur ditetapkan PyFlink
        """Hitung count & rata-rata Amount untuk semua transaksi dalam satu window.

        Catatan: di PyFlink, AllWindowFunction.apply MENGEMBALIKAN iterable hasil (berbeda
        dari API Java yang memakai Collector `out`).

        Args:
            window: Metadata window (tak dipakai di ringkasan ini).
            inputs: Iterable Amount (float) transaksi yang jatuh di window ini.

        Returns:
            List berisi satu baris ringkasan string.
        """
        amounts = list(inputs)
        n = len(amounts)
        avg = (sum(amounts) / n) if n else 0.0
        return [f"[WINDOW] {n} transaksi / 10s, rata-rata Amount={avg:.2f}"]


def main() -> None:
    """Bangun & jalankan pipeline 4d: socket -> (skoring per-event) + (window 10s)."""
    host = os.environ.get("BRIDGE_HOST", "bridge")
    port = int(os.environ.get("BRIDGE_SOCKET_PORT", "9999"))
    scorer_url = os.environ.get("SCORER_URL", "http://scorer:8000/score")

    env = StreamExecutionEnvironment.get_execution_environment()
    # Parallelism 1: bridge hanya melayani satu koneksi (socket single-connection),
    # dan urutan transaksi penting untuk Half-Space Trees (online) di scorer.
    env.set_parallelism(1)

    stream = socket_text_stream(env, host, port)

    # --- Cabang 1: skoring per-event lewat scorer service ---
    # output_type eksplisit agar serialisasi Java<->Python jelas.
    stream.map(ScoreMap(scorer_url), output_type=Types.STRING()).print()

    # --- Cabang 2: window tumbling 10s (count + rata-rata Amount), enrichment demonstratif ---
    # window_all (non-keyed) cukup karena ini agregat global per interval, bukan per-kelompok.
    (
        stream.map(to_amount, output_type=Types.FLOAT())
        .filter(lambda a: a >= 0.0)  # buang baris rusak (ditandai -1.0)
        .window_all(TumblingProcessingTimeWindows.of(Time.seconds(10)))
        .apply(WindowSummary(), output_type=Types.STRING())
        .print()
    )

    env.execute("anomaly-4d-score-window")


if __name__ == "__main__":
    main()
