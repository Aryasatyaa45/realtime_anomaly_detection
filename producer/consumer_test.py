"""Consumer uji sederhana: poll pesan dari Iggy untuk membuktikan data mengalir.

Bukan consumer produksi (itu tugas Flink di Minggu 4) — hanya pembuktian DoD Minggu 3:
producer kirim -> consumer terima.

Jalankan (setelah producer mengirim sebagian data):
    python producer/consumer_test.py --count 5
"""

from __future__ import annotations

import argparse
import json
import os

from dotenv import load_dotenv

from iggy_http import IggyHTTPClient, IggyHTTPError


def parse_args() -> argparse.Namespace:
    """Parsing argumen CLI; default dari environment (.env)."""
    load_dotenv()
    parser = argparse.ArgumentParser(description="Consumer uji: poll pesan dari Iggy.")
    parser.add_argument("--count", type=int, default=5, help="Jumlah pesan diambil.")
    parser.add_argument("--host", default=os.getenv("IGGY_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("IGGY_HTTP_PORT", "3000")))
    parser.add_argument("--stream", default=os.getenv("IGGY_STREAM", "transactions"))
    parser.add_argument("--topic", default=os.getenv("IGGY_TOPIC", "creditcard"))
    parser.add_argument("--user", default=os.getenv("IGGY_ROOT_USERNAME", "iggy"))
    parser.add_argument("--password", default=os.getenv("IGGY_ROOT_PASSWORD", "iggy"))
    return parser.parse_args()


def main() -> None:
    """Connect -> poll -> tampilkan beberapa pesan sebagai bukti aliran data."""
    args = parse_args()
    client = IggyHTTPClient(
        host=args.host, port=args.port, stream=args.stream, topic=args.topic
    )

    if not client.ping():
        raise SystemExit(f"Broker Iggy tidak merespons di {client.base_url}.")
    client.login(args.user, args.password)

    try:
        # auto_commit=False supaya pemanggilan ulang tetap membaca dari awal (mode uji).
        messages = client.poll_json(count=args.count, auto_commit=False)
    except IggyHTTPError as exc:
        raise SystemExit(f"Gagal poll: {exc}")

    if not messages:
        print("Belum ada pesan di topic. Jalankan producer.py dulu.")
        return

    print(f"Menerima {len(messages)} pesan dari '{args.stream}/{args.topic}':\n")
    for i, msg in enumerate(messages, start=1):
        amount = msg.get("Amount")
        label = msg.get("Class")
        print(f"  [{i}] Amount={amount} Class={label} | {json.dumps(msg)[:80]}...")

    print("\nDoD Minggu 3 terbukti: producer -> Iggy -> consumer. Data mengalir.")


if __name__ == "__main__":
    main()
