"""Klien tipis untuk Apache Iggy lewat HTTP REST API (server 0.8.x).

Dipakai bersama oleh producer.py dan consumer_test.py. Memakai HTTP (bukan SDK
native apache-iggy) demi reproducibility di Windows — SDK Python hanya menyediakan
wheel sampai 0.6.0 sehingga tak wire-compatible dengan server 0.8.

Kontrak API di bawah sudah diverifikasi langsung terhadap server apache/iggy:0.8.0:
- Autentikasi  : POST /users/login -> {access_token:{token}}; header Bearer.
- Stream/topic : bisa dialamatkan memakai NAMA pada path (mis. /streams/transactions).
- partition_id : berbasis-0 (partisi pertama = 0).
- Kirim pesan  : partitioning.value WAJIB string base64 (kosong "" untuk balanced),
                 payload tiap pesan juga base64.
"""

from __future__ import annotations

import base64
import json
from typing import Any

import requests


class IggyHTTPError(RuntimeError):
    """Galat saat memanggil HTTP API Iggy (status non-2xx atau respons tak terduga)."""


class IggyHTTPClient:
    """Klien minimal Iggy HTTP: login, pastikan stream/topic, kirim & poll pesan.

    Attributes:
        base_url: Basis URL HTTP API, mis. "http://localhost:3000".
        stream: Nama stream.
        topic: Nama topic.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3000,
        stream: str = "transactions",
        topic: str = "creditcard",
        timeout: float = 10.0,
    ) -> None:
        self.base_url = f"http://{host}:{port}"
        self.stream = stream
        self.topic = topic
        self.timeout = timeout
        self._session = requests.Session()
        self._token: str | None = None

    # --- internal ---------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        """Header standar; sisipkan Bearer token bila sudah login."""
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        """Bungkus requests dengan header & timeout standar.

        Args:
            method: Metode HTTP (GET/POST/DELETE).
            path: Path relatif terhadap base_url (diawali "/").
            **kwargs: Argumen tambahan untuk requests (mis. json, params).

        Returns:
            Objek Response.

        Raises:
            IggyHTTPError: Bila koneksi gagal.
        """
        url = f"{self.base_url}{path}"
        try:
            return self._session.request(
                method, url, headers=self._headers(), timeout=self.timeout, **kwargs
            )
        except requests.RequestException as exc:  # koneksi putus / timeout
            raise IggyHTTPError(f"Gagal menghubungi Iggy di {url}: {exc}") from exc

    # --- API publik -------------------------------------------------------

    def ping(self) -> bool:
        """Cek server hidup lewat /ping (tanpa autentikasi).

        Returns:
            True bila server membalas 200.
        """
        return self._request("GET", "/ping").status_code == 200

    def login(self, username: str = "iggy", password: str = "iggy") -> None:
        """Login user root dan simpan access token.

        Args:
            username: Nama user (default root "iggy").
            password: Kata sandi (default "iggy").

        Raises:
            IggyHTTPError: Bila login gagal atau token tak ditemukan.
        """
        resp = self._request(
            "POST", "/users/login", json={"username": username, "password": password}
        )
        if resp.status_code != 200:
            raise IggyHTTPError(f"Login gagal ({resp.status_code}): {resp.text}")
        token = resp.json().get("access_token", {}).get("token")
        if not token:
            raise IggyHTTPError(f"Token tidak ada di respons login: {resp.text}")
        self._token = token

    def ensure_stream_and_topic(self, partitions: int = 1) -> None:
        """Buat stream & topic bila belum ada (idempoten).

        Iggy membalas galat bila resource sudah ada; status seperti itu diabaikan
        agar producer aman dijalankan berulang.

        Args:
            partitions: Jumlah partisi topic.

        Raises:
            IggyHTTPError: Bila pembuatan gagal karena alasan selain "sudah ada".
        """
        # Stream
        resp = self._request("POST", "/streams", json={"stream_id": 1, "name": self.stream})
        self._ensure_ok_or_exists(resp, f"buat stream '{self.stream}'")

        # Topic
        topic_body = {
            "topic_id": 1,
            "name": self.topic,
            "partitions_count": partitions,
            "compression_algorithm": "none",
            "message_expiry": 0,
            "max_topic_size": 0,
            "replication_factor": 1,
        }
        resp = self._request(
            "POST", f"/streams/{self.stream}/topics", json=topic_body
        )
        self._ensure_ok_or_exists(resp, f"buat topic '{self.topic}'")

    @staticmethod
    def _ensure_ok_or_exists(resp: requests.Response, action: str) -> None:
        """Terima status 2xx; abaikan galat 'sudah ada'; selain itu lempar error."""
        if 200 <= resp.status_code < 300:
            return
        text = resp.text.lower()
        if "already" in text or "exist" in text:
            return  # idempoten: resource memang sudah ada
        raise IggyHTTPError(f"Gagal {action} ({resp.status_code}): {resp.text}")

    def send_json(self, obj: dict[str, Any]) -> None:
        """Kirim satu objek (di-encode JSON lalu base64) sebagai satu pesan.

        Args:
            obj: Dict yang akan dikirim sebagai payload pesan.

        Raises:
            IggyHTTPError: Bila pengiriman tidak membalas 2xx.
        """
        payload_b64 = base64.b64encode(
            json.dumps(obj, separators=(",", ":")).encode("utf-8")
        ).decode("ascii")
        body = {
            "partitioning": {"kind": "balanced", "value": ""},
            "messages": [{"payload": payload_b64}],
        }
        resp = self._request(
            "POST",
            f"/streams/{self.stream}/topics/{self.topic}/messages",
            json=body,
        )
        if not (200 <= resp.status_code < 300):
            raise IggyHTTPError(f"Kirim pesan gagal ({resp.status_code}): {resp.text}")

    def poll_json(
        self,
        count: int = 10,
        partition_id: int = 0,
        consumer_id: int = 1,
        auto_commit: bool = True,
    ) -> list[dict[str, Any]]:
        """Ambil pesan dari awal partisi dan kembalikan payload yang sudah ter-decode.

        Args:
            count: Maksimum pesan yang diambil.
            partition_id: Partisi (berbasis-0).
            consumer_id: ID consumer.
            auto_commit: Bila True, offset di-commit otomatis di server.

        Returns:
            List dict hasil decode payload JSON tiap pesan.

        Raises:
            IggyHTTPError: Bila poll tidak membalas 2xx.
        """
        params = {
            "consumer_id": consumer_id,
            "partition_id": partition_id,
            "strategy.kind": "first",
            "strategy.value": 0,
            "count": count,
            "auto_commit": str(auto_commit).lower(),
        }
        resp = self._request(
            "GET",
            f"/streams/{self.stream}/topics/{self.topic}/messages",
            params=params,
        )
        if not (200 <= resp.status_code < 300):
            raise IggyHTTPError(f"Poll gagal ({resp.status_code}): {resp.text}")

        out: list[dict[str, Any]] = []
        for msg in resp.json().get("messages", []):
            raw = base64.b64decode(msg["payload"])
            out.append(json.loads(raw))
        return out
