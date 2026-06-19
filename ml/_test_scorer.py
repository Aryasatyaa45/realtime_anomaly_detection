"""Tes sementara sub-step 4a: POST 1 transaksi normal + 1 fraud ke scorer service.

Hapus setelah verifikasi. Membaca langsung dari creditcard.csv (pakai csv bawaan, tanpa pandas)
agar nilai fitur 100% asli, lalu membandingkan flag anomali dengan ekspektasi label.
"""

from __future__ import annotations

import csv
import json
import urllib.request
from pathlib import Path

CSV = Path("data/creditcard.csv")
URL = "http://localhost:8000/score"
FEATURES = [f"V{i}" for i in range(1, 29)] + ["Amount"]


def post(event: dict) -> dict:
    req = urllib.request.Request(
        URL, data=json.dumps(event).encode(), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def find_rows() -> tuple[dict, dict]:
    """Ambil baris normal pertama dan baris fraud pertama dari CSV."""
    normal = fraud = None
    with CSV.open() as f:
        for row in csv.DictReader(f):
            event = {k: float(row[k]) for k in FEATURES}
            if row["Class"] == "0" and normal is None:
                normal = event
            elif row["Class"] == "1" and fraud is None:
                fraud = event
            if normal and fraud:
                break
    return normal, fraud


def main() -> None:
    normal, fraud = find_rows()
    for label, event in [("NORMAL (Class=0)", normal), ("FRAUD  (Class=1)", fraud)]:
        result = post(event)
        print(f"\n== {label} | Amount={event['Amount']:.2f} ==")
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
