FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Service scorer dipanggil dari pipeline; default jalankan train bila dijalankan langsung
CMD ["python", "train.py"]
