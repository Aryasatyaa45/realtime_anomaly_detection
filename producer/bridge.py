"""Bridge Iggy(HTTP) -> TCP socket: jembatan agar Flink bisa membaca stream Iggy.

Flink tidak punya konektor Iggy, tetapi PyFlink menyediakan source `socket_text_stream`
yang membaca baris teks dari sebuah TCP socket. Bridge ini menutup celah itu:

    Iggy (HTTP REST)  --poll progresif-->  bridge  --1 baris JSON per transaksi-->  TCP socket
                                                                                       ^
                                                                              Flink konek sbg client

Bridge berperan sebagai **TCP server** (Flink yang konek sebagai client). Untuk tiap koneksi,
bridge mem-`poll` Iggy memakai strategi "next" + auto-commit sehingga setiap transaksi hanya
dikirim sekali (offset consumer dilacak server Iggy). Tiap transaksi ditulis sebagai satu baris
JSON diakhiri "\n" — format yang langsung bisa di-`json.loads` di sisi Flink.

Keterbatasan (sesuai cakupan demo, dicatat di ROADMAP):
- Melayani SATU koneksi pada satu waktu (cukup untuk `socket_text_stream` yang single-connection).
  Jika Flink putus, bridge kembali menunggu koneksi berikutnya.
- Hanya membaca partisi tunggal (partition_id=0). Default topik memang 1 partisi.

Jalankan dari root projek (broker & data harus sudah ada):
    docker compose up -d iggy
    python producer/producer.py --rps 50 --limit 1000      # isi Iggy
    python producer/bridge.py --socket-port 9999            # lalu sambungkan Flink / nc localhost 9999
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import time

from dotenv import load_dotenv

from iggy_http import IggyHTTPClient, IggyHTTPError


def parse_args() -> argparse.Namespace:
    """Parsing argumen CLI; default diisi dari environment (.env) bila ada."""
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Bridge Iggy (HTTP) -> TCP socket untuk Flink socket_text_stream."
    )
    # Sisi Iggy (sumber)
    parser.add_argument("--host", default=os.getenv("IGGY_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("IGGY_HTTP_PORT", "3000")))
    parser.add_argument("--stream", default=os.getenv("IGGY_STREAM", "transactions"))
    parser.add_argument("--topic", default=os.getenv("IGGY_TOPIC", "creditcard"))
    parser.add_argument("--partitions", type=int, default=int(os.getenv("IGGY_PARTITIONS", "1")))
    parser.add_argument("--user", default=os.getenv("IGGY_ROOT_USERNAME", "iggy"))
    parser.add_argument("--password", default=os.getenv("IGGY_ROOT_PASSWORD", "iggy"))
    parser.add_argument("--consumer-id", type=int, default=int(os.getenv("BRIDGE_CONSUMER_ID", "10")))
    # Sisi socket (sink ke Flink)
    parser.add_argument(
        "--bind-host",
        default=os.getenv("BRIDGE_BIND_HOST", "0.0.0.0"),
        help="Alamat bind socket server (0.0.0.0 agar bisa diakses dari container Flink).",
    )
    parser.add_argument(
        "--socket-port",
        type=int,
        default=int(os.getenv("BRIDGE_SOCKET_PORT", "9999")),
        help="Port TCP tempat Flink akan konek.",
    )
    # Perilaku poll
    parser.add_argument(
        "--poll-count",
        type=int,
        default=int(os.getenv("BRIDGE_POLL_COUNT", "100")),
        help="Maksimum transaksi diambil per poll.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=float(os.getenv("BRIDGE_POLL_INTERVAL", "0.2")),
        help="Jeda (detik) saat poll kosong, agar tidak membanjiri server.",
    )
    parser.add_argument(
        "--start-offset",
        type=int,
        default=int(os.getenv("BRIDGE_START_OFFSET", "0")),
        help="Offset awal partisi untuk mulai meneruskan (0 = dari paling awal).",
    )
    return parser.parse_args()


def connect_iggy(args: argparse.Namespace) -> IggyHTTPClient:
    """Bangun klien Iggy, pastikan hidup & login, dan pastikan stream/topic ada.

    Args:
        args: Hasil parse_args (host/port/kredensial/stream/topic).

    Returns:
        IggyHTTPClient yang sudah login dan siap di-poll.

    Raises:
        SystemExit: Bila broker tidak merespons (pesan ramah, bukan traceback).
    """
    client = IggyHTTPClient(
        host=args.host, port=args.port, stream=args.stream, topic=args.topic
    )
    if not client.ping():
        raise SystemExit(
            f"Broker Iggy tidak merespons di {client.base_url}. "
            "Jalankan dulu: docker compose up -d iggy"
        )
    client.login(args.user, args.password)
    # Idempoten — aman walau producer sudah membuatnya; mencegah poll gagal bila bridge jalan duluan.
    client.ensure_stream_and_topic(partitions=args.partitions)
    return client


def poll_at_offset(client: IggyHTTPClient, args: argparse.Namespace, offset: int) -> list[dict]:
    """Poll Iggy mulai dari `offset` (strategy "offset"); login ulang sekali bila perlu.

    Bridge melacak offset SENDIRI alih-alih mengandalkan auto-commit server: pada server
    Iggy 0.8 (HTTP) auto-commit + strategy "next" terbukti tidak memajukan offset, sedangkan
    strategy "offset" + nilai eksplisit dihormati dengan benar. Token JWT hanya berlaku ~1 jam
    sehingga bila poll gagal, coba login ulang lalu ulangi sekali.

    Args:
        client: Klien Iggy yang sudah login.
        args: Hasil parse_args (poll-count, kredensial untuk re-login).
        offset: Offset partisi (berbasis-0, inklusif) tempat mulai mengambil.

    Returns:
        List transaksi (dict) mulai dari `offset`; mungkin kosong bila belum ada data baru.

    Raises:
        IggyHTTPError: Bila poll tetap gagal setelah login ulang.
    """
    try:
        return client.poll_json(
            count=args.poll_count, strategy="offset", strategy_value=offset,
            auto_commit=False, consumer_id=args.consumer_id,
        )
    except IggyHTTPError:
        client.login(args.user, args.password)  # token mungkin kedaluwarsa
        return client.poll_json(
            count=args.poll_count, strategy="offset", strategy_value=offset,
            auto_commit=False, consumer_id=args.consumer_id,
        )


def serve_client(
    conn: socket.socket,
    client: IggyHTTPClient,
    args: argparse.Namespace,
    offset_state: dict[str, int],
) -> None:
    """Layani satu koneksi Flink: poll Iggy progresif & kirim baris JSON sampai putus.

    Offset disimpan di `offset_state['next']` agar konsumsi tetap maju lintas koneksi —
    bila Flink putus lalu menyambung lagi, bridge melanjutkan dari offset terakhir
    (tidak mengirim ulang transaksi yang sudah diteruskan).

    Args:
        conn: Socket koneksi ke Flink (client).
        client: Klien Iggy untuk poll.
        args: Hasil parse_args (poll-interval, dll).
        offset_state: Dict berisi kunci 'next' = offset partisi berikutnya yang akan diambil.
    """
    forwarded = 0
    while True:
        try:
            batch = poll_at_offset(client, args, offset_state["next"])
        except IggyHTTPError as exc:
            print(f"  [bridge] poll gagal, coba lagi: {exc}")
            time.sleep(1.0)
            continue

        if not batch:
            time.sleep(args.poll_interval)  # belum ada data baru di offset ini
            continue

        try:
            for tx in batch:
                line = json.dumps(tx, separators=(",", ":")) + "\n"
                conn.sendall(line.encode("utf-8"))
                forwarded += 1
        except (BrokenPipeError, ConnectionResetError, OSError) as exc:
            print(f"  [bridge] koneksi Flink putus setelah {forwarded:,} transaksi: {exc}")
            return

        # Partisi append-only & offset kontigu → maju sebanyak pesan yang diterima.
        offset_state["next"] += len(batch)
        if forwarded % 500 == 0:
            print(
                f"  [bridge] {forwarded:,} transaksi diteruskan "
                f"(offset berikutnya={offset_state['next']})"
            )


def main() -> None:
    """Titik masuk bridge: connect Iggy, buka socket server, layani koneksi berulang."""
    args = parse_args()
    client = connect_iggy(args)
    offset_state = {"next": args.start_offset}
    print(
        f"[bridge] terhubung ke Iggy {client.base_url} "
        f"(stream='{args.stream}', topic='{args.topic}', mulai offset={args.start_offset})"
    )

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((args.bind_host, args.socket_port))
        server.listen(1)
        print(
            f"[bridge] menunggu koneksi Flink di {args.bind_host}:{args.socket_port} "
            "(Ctrl+C untuk berhenti) ..."
        )
        try:
            while True:
                conn, addr = server.accept()
                print(f"[bridge] Flink tersambung dari {addr[0]}:{addr[1]}")
                with conn:
                    serve_client(conn, client, args, offset_state)
                print("[bridge] menunggu koneksi berikutnya ...")
        except KeyboardInterrupt:
            print("\n[bridge] dihentikan.")


if __name__ == "__main__":
    main()
