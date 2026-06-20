# Producer: replay creditcard.csv -> Iggy (HTTP REST).
# Dipakai penuh di Minggu 4 (pipeline via docker compose). Di Minggu 3 producer
# divalidasi dari host, tapi image ini sudah disiapkan agar reproducible.
FROM python:3.13-slim

WORKDIR /app

# Install dependency dulu (manfaatkan layer cache) baru salin kode.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py consumer_test.py iggy_http.py bridge.py ./

# Override argumen via docker-compose / .env bila perlu.
ENTRYPOINT ["python", "producer.py"]
