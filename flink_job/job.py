"""PyFlink job — Minggu 4 (versi 4e: skoring per-event -> sink ke Paimon).

Lanjutan dari 4d. Di 4d setiap transaksi diberi skor anomali (ECOD + Half-Space Trees) lalu
HANYA dicetak ke stdout, plus cabang window 10s demonstratif. Di 4e fokusnya **persistensi**:
hasil skor ditulis ke tabel **Paimon** (append-only, filesystem catalog di volume `/opt/paimon`)
lewat Table API, sehingga dashboard Minggu 5 bisa membacanya.

Cabang window 10s (count + rata-rata Amount) SENGAJA tidak diikutkan di job 4e ini. Window itu
murni demonstratif (bukti Flink stateful) dan sudah divalidasi + di-commit di 4d (`0ef02fe`).
Menjalankannya bersama sink Paimon berarti dua operator Python + `attach_as_datastream` yang
rapuh (timer window gagal forward ke Print sink -> job failover, snapshot Paimon tak ter-commit),
tanpa nilai baru. Bila nanti window perlu ikut di pipeline final, tambahkan sebagai Flink SQL
`TUMBLE` (jalan di JVM, bukan operator Python) — lebih kokoh.
ponytail: window di-drop dari job 4e; kode-nya ada di commit 4d, re-add via SQL TUMBLE bila perlu.

Mengapa skoring lewat HTTP, bukan load model di dalam Flink: versi dependency ML (numpy/sklearn/
pyod/river) harus dipin persis sama dengan environment training agar unpickle model tidak gagal;
memisahkan scorer sebagai service menghindari konflik dependency dengan PyFlink. Flink job cukup
butuh `requests`.

Aliran 4e:
    bridge (TCP socket, 1 baris JSON/transaksi)
        -> socket_text_stream
        -> ScoreMap (parse JSON -> POST http://scorer:8000/score -> Row hasil skor)
        -> filter (buang baris gagal parse/skor)
        -> Table -> INSERT INTO paimon.default.scores

Paimon meng-commit data PER CHECKPOINT, jadi checkpointing WAJIB diaktifkan (lihat main()).

Submit (dari dalam container):
    flink run -d -py /opt/flink_job/job.py
Host/port bridge, URL scorer, & warehouse Paimon diambil dari env (default cocok dgn compose).
"""

from __future__ import annotations

import os

from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.data_stream import DataStream
from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.table import StreamTableEnvironment

# Skema baris hasil skor yang ditulis ke Paimon. Urutan kolom = urutan Row positional di
# ScoreMap.map dan urutan kolom DDL tabel scores (lihat ensure_paimon_table).
SCORE_ROW_TYPE = Types.ROW_NAMED(
    ["Time", "Amount", "Class", "skor_ecod", "skor_hst", "is_anomali"],
    [
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.INT(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.BOOLEAN(),
    ],
)


def socket_text_stream(env: StreamExecutionEnvironment, host: str, port: int) -> DataStream:
    """Sumber socket teks untuk PyFlink via gateway Java.

    PyFlink (1.20) TIDAK mengekspos `socket_text_stream` di StreamExecutionEnvironment
    (hanya tersedia di API Java). Kita panggil `socketTextStream` Java lewat gateway lalu
    bungkus DataStream-nya jadi objek PyFlink, sehingga operator Python (map) tetap bisa
    dipasang di hilir. Tiap baris teks (dipisah '\\n') = satu transaksi JSON dari bridge.

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
    """Operator map: parse baris JSON transaksi -> minta skor ke scorer service -> Row hasil.

    Skoring dilakukan lewat HTTP `POST /score` ke scorer (lihat ml/scorer.py). Koneksi HTTP
    di-reuse via satu `requests.Session` per subtask (dibuat di `open`), jauh lebih hemat
    daripada membuka koneksi baru tiap transaksi.

    Error sengaja TIDAK dilempar agar satu transaksi rusak / scorer sesaat tak tersedia tidak
    menghentikan seluruh job: baris bermasalah dikembalikan sebagai Row dengan `skor_ecod=-1.0`
    (sentinel; skor ECOD asli selalu >= 0) lalu disaring keluar sebelum sink (lihat main()).
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

    @staticmethod
    def _bad_row() -> Row:
        """Row sentinel untuk transaksi gagal parse/skor (disaring keluar sebelum sink)."""
        return Row(-1.0, -1.0, -1, -1.0, -1.0, False)

    def map(self, line: str) -> Row:
        """Skor satu transaksi; kembalikan Row hasil skor (atau Row sentinel bila gagal).

        Args:
            line: Satu baris teks dari socket (diharapkan JSON transaksi).

        Returns:
            Row(Time, Amount, Class, skor_ecod, skor_hst, is_anomali). `skor_ecod=-1.0`
            menandai baris gagal yang akan disaring keluar sebelum ditulis ke Paimon.
        """
        import json

        line = line.strip()
        if not line:
            return self._bad_row()

        try:
            tx = json.loads(line)
        except (ValueError, json.JSONDecodeError):
            return self._bad_row()

        try:
            resp = self._session.post(self._url, json=tx, timeout=self._timeout)
            resp.raise_for_status()
            result = resp.json()
        except Exception:  # noqa: BLE001 - jaring pengaman agar job tak mati
            return self._bad_row()

        return Row(
            float(tx.get("Time", -1.0)),
            float(tx.get("Amount", -1.0)),
            int(tx.get("Class", -1)),
            float(result.get("skor_ecod", -1.0)),
            float(result.get("skor_hst", -1.0)),
            bool(result.get("is_anomali", False)),
        )


def ensure_paimon_table(t_env: StreamTableEnvironment, warehouse: str) -> None:
    """Daftarkan catalog Paimon (filesystem) & buat tabel `scores` bila belum ada.

    Tabel `scores` bersifat append-only (tanpa primary key) -> Paimon memakai bucket-mode
    'unaware' default, cocok untuk aliran hasil skor yang murni menambah baris. `write-buffer-size`
    dikecilkan (32mb) agar muat di TaskManager dev yang memorinya terbatas.

    Args:
        t_env: StreamTableEnvironment (berbagi env dengan DataStream).
        warehouse: URI warehouse Paimon, mis. "file:///opt/paimon".
    """
    t_env.execute_sql(
        f"""
        CREATE CATALOG paimon WITH (
            'type' = 'paimon',
            'warehouse' = '{warehouse}'
        )
        """
    )
    t_env.execute_sql(
        """
        CREATE TABLE IF NOT EXISTS paimon.`default`.scores (
            `Time` DOUBLE,
            `Amount` DOUBLE,
            `Class` INT,
            `skor_ecod` DOUBLE,
            `skor_hst` DOUBLE,
            `is_anomali` BOOLEAN
        ) WITH (
            'write-buffer-size' = '32mb'
        )
        """
    )


def main() -> None:
    """Bangun & jalankan pipeline 4e: socket -> skor per-event -> sink Paimon."""
    host = os.environ.get("BRIDGE_HOST", "bridge")
    port = int(os.environ.get("BRIDGE_SOCKET_PORT", "9999"))
    scorer_url = os.environ.get("SCORER_URL", "http://scorer:8000/score")
    warehouse = os.environ.get("PAIMON_WAREHOUSE", "file:///opt/paimon")

    env = StreamExecutionEnvironment.get_execution_environment()
    # Parallelism 1: bridge hanya melayani satu koneksi (socket single-connection),
    # dan urutan transaksi penting untuk Half-Space Trees (online) di scorer.
    env.set_parallelism(1)
    # Paimon meng-commit hasil tulis PER CHECKPOINT. Tanpa ini, file ditulis tapi tak pernah
    # jadi snapshot -> tabel selalu kosong saat dibaca. 10s = kompromi latensi vs overhead.
    env.enable_checkpointing(10_000)

    t_env = StreamTableEnvironment.create(env)
    ensure_paimon_table(t_env, warehouse)

    scored = (
        socket_text_stream(env, host, port)
        .map(ScoreMap(scorer_url), output_type=SCORE_ROW_TYPE)
        .filter(lambda r: r[3] >= 0.0)  # buang Row sentinel (skor_ecod = -1.0)
    )

    # from_data_stream -> Table (skema dari SCORE_ROW_TYPE), lalu INSERT ke tabel Paimon.
    # execute_insert mengirim job sendiri (dipakai dgn `flink run -d` = detached), jadi tak
    # perlu env.execute().
    t_env.from_data_stream(scored).execute_insert("paimon.`default`.scores")


if __name__ == "__main__":
    main()
