"""PyFlink job — Minggu 4 (versi 4c: trivial socket -> print).

Tahap 4c sengaja minimal demi mengetes rantai paling rapuh lebih dulu (sesuai aturan ROADMAP
"tiap komponen dites isolated"): apakah Flink bisa membaca stream transaksi dari bridge
(`socket_text_stream`) dan operator Python berjalan di TaskManager. Window, pemanggilan scorer,
dan sink Paimon ditambahkan bertahap di 4d/4e.

Aliran 4c:
    bridge (TCP socket, 1 baris JSON/transaksi)  ->  socket_text_stream  ->  print (stdout TM)

Submit (dari dalam container, mis. lewat service job-submitter):
    flink run -py /opt/flink_job/job.py
Host & port bridge diambil dari env (default cocok dgn nama service docker-compose).
"""

from __future__ import annotations

import os

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.data_stream import DataStream


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


def main() -> None:
    """Bangun & jalankan pipeline 4c: baca socket bridge lalu cetak tiap baris."""
    host = os.environ.get("BRIDGE_HOST", "bridge")
    port = int(os.environ.get("BRIDGE_SOCKET_PORT", "9999"))

    env = StreamExecutionEnvironment.get_execution_environment()
    # Parallelism 1: bridge hanya melayani satu koneksi (socket single-connection),
    # dan urutan transaksi penting untuk Half-Space Trees (online) di tahap berikutnya.
    env.set_parallelism(1)

    stream = socket_text_stream(env, host, port)

    # 4c: cetak ke stdout TaskManager untuk membuktikan data mengalir & operator Python jalan.
    # Prefiks memudahkan grep di log TM. output_type eksplisit agar serialisasi Java->Python jelas.
    stream.map(lambda line: f"[TX] {line}", output_type=Types.STRING()).print()

    env.execute("anomaly-4c-socket-print")


if __name__ == "__main__":
    main()
