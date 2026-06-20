# Image dashboard Streamlit (Minggu 5). Ringan: python:3.11-slim + requirements + app.py.
FROM python:3.11-slim

WORKDIR /app

# psycopg2-binary butuh libpq saat runtime; -binary sudah bundling, tapi pasang ca-certificates
# agar koneksi TLS ke Supabase (Minggu 6) aman. Bersihkan apt cache utk image kecil.
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8501

# --server.address=0.0.0.0 agar bisa diakses dari host (port 8501 di-publish compose).
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
