"""Producer: baca creditcard.csv baris per baris dan kirim ke Iggy sebagai stream.

Mensimulasikan aliran transaksi real-time dengan jeda antar-pesan (kontrol via --rps).
Memakai HTTP REST API Iggy (lihat iggy_http.py).

Jalankan dari root projek (broker harus sudah hidup: `docker compose up -d iggy`):
    python producer/producer.py --csv data/creditcard.csv --rps 50 --limit 1000
"""

from __future__ import annotations

import argparse
import csv
import os
import time
from pathlib import Path

from dotenv import load_dotenv

from iggy_http import IggyHTTPClient, IggyHTTPError


def parse_args() -> argparse.Namespace:
    """Parsing argumen CLI; default diisi dari environment (.env) bila ada."""
    load_dotenv()
    parser = argparse.ArgumentParser(description="Replay CSV transaksi ke Iggy (HTTP).")
    parser.add_argument(
        "--csv",
        default=os.getenv("DATA_PATH", "data/creditcard.csv"),
        help="Path dataset CSV.",
    )
    parser.add_argument(
        "--rps",
        type=int,
        default=int(os.getenv("PRODUCER_RPS", "50")),
        help="Transaksi per detik (simulasi real-time).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Batas jumlah baris dikirim (0 = semua).",
    )
    parser.add_argument("--host", default=os.getenv("IGGY_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("IGGY_HTTP_PORT", "3000")))
    parser.add_argument("--stream", default=os.getenv("IGGY_STREAM", "transactions"))
    parser.add_argument("--topic", default=os.getenv("IGGY_TOPIC", "creditcard"))
    parser.add_argument("--partitions", type=int, default=int(os.getenv("IGGY_PARTITIONS", "1")))
    parser.add_argument("--user", default=os.getenv("IGGY_ROOT_USERNAME", "iggy"))
    parser.add_argument("--password", default=os.getenv("IGGY_ROOT_PASSWORD", "iggy"))
    return parser.parse_args()


def _coerce(value: str) -> float | int | str:
    """Konversi sel CSV ke tipe numerik bila memungkinkan (agar payload JSON rapi)."""
    try:
        f = float(value)
        return int(f) if f.is_integer() else f
    except ValueError:
        return value


def main() -> None:
    """Titik masuk producer: connect -> ensure stream/topic -> replay CSV."""
    args = parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV tidak ditemukan: {csv_path.resolve()}")

    client = IggyHTTPClient(
        host=args.host, port=args.port, stream=args.stream, topic=args.topic
    )

    if not client.ping():
        raise SystemExit(
            f"Broker Iggy tidak merespons di {client.base_url}. "
            "Jalankan dulu: docker compose up -d iggy"
        )
    client.login(args.user, args.password)
    client.ensure_stream_and_topic(partitions=args.partitions)
    print(f"Terhubung ke Iggy {client.base_url} | stream='{args.stream}' topic='{args.topic}'")

    delay = 1.0 / args.rps if args.rps > 0 else 0.0
    sent = 0
    t_start = time.perf_counter()

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            event = {key: _coerce(val) for key, val in row.items()}
            # Retry blip transien (mis. Iggy read-timeout sesaat di bawah beban) dgn backoff;
            # 1 hiccup tak boleh membunuh seeding 50k. ponytail: 3 percobaan cukup utk demo.
            for attempt in range(1, 4):
                try:
                    client.send_json(event)
                    break
                except IggyHTTPError as exc:
                    if attempt == 3:
                        raise SystemExit(f"Berhenti di transaksi ke-{sent}: {exc}")
                    time.sleep(attempt)  # 1s, 2s — beri Iggy waktu pulih

            sent += 1
            if sent % 500 == 0:
                rate = sent / (time.perf_counter() - t_start)
                print(f"  terkirim {sent:,} transaksi (~{rate:.0f}/dtk)")

            if args.limit and sent >= args.limit:
                break
            if delay:
                time.sleep(delay)

    elapsed = time.perf_counter() - t_start
    print(f"Selesai: {sent:,} transaksi terkirim dalam {elapsed:.1f} dtk.")


if __name__ == "__main__":
    main()
