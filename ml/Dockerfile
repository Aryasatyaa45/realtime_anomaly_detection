# Image scorer service (terpisah dari Flink). Python 3.11 = sama dengan env training,
# memastikan unpickle ecod.pkl/hst.pkl konsisten.
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
# --trusted-host: network Mak melakukan TLS interception (proxy/AV) sehingga verifikasi
# sertifikat PyPI gagal di dalam container (pola sama dgn `curl --ssl-no-revoke` utk Maven).
RUN pip install --no-cache-dir \
    --trusted-host pypi.org --trusted-host files.pythonhosted.org \
    -r requirements.txt

# Salin kode scorer. Model (ecod.pkl/hst.pkl + scaler) TIDAK di-COPY ke image (besar, ~285 MB);
# di-mount read-only lewat docker-compose ke /app/models saat runtime.
COPY scorer.py .

EXPOSE 8000

# Service skoring real-time: load model lalu dengarkan POST /score & GET /health di :8000.
CMD ["python", "scorer.py"]
