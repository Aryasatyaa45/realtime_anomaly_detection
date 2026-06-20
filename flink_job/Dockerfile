# Image Flink + PyFlink + jar Paimon/Hadoop untuk pipeline anomaly detection (Minggu 4).
#
# Versi dipin (diverifikasi ada di Maven Central & Docker Hub, 2026-06-19):
#   - Flink            : 1.20.4 (scala 2.12, Java 11)
#   - PyFlink          : apache-flink==1.20.4 (cocok patch dgn cluster -> JobGraph konsisten)
#   - Paimon (sink)    : paimon-flink-1.20-1.4.1.jar
#   - Hadoop (Paimon FS): flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
FROM flink:1.20.4-scala_2.12-java11

# Python 3 + venv. PyFlink butuh Python 3.8-3.11; image Flink tidak menyertakan Python.
# Pakai virtualenv agar lolos PEP 668 (externally-managed-environment) di Debian.
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends python3 python3-venv python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/pyflink-venv
# venv di depan PATH -> `python`/`python3`/`pip` resolve ke venv (dipakai Flink utk eksekusi
# operator Python pada client maupun TaskManager, default python.executable="python").
ENV PATH="/opt/pyflink-venv/bin:${PATH}"

# requests dipakai job utk memanggil scorer service per-event (Minggu 4d).
# Wheel apache-flink besar (~ratusan MB) -> timeout & retry longgar agar tahan koneksi lambat.
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --timeout 300 --retries 10 \
        apache-flink==1.20.4 requests==2.32.3

# Jar konektor diletakkan di /opt/flink/lib agar otomatis masuk classpath JM & TM.
COPY jars/paimon-flink-1.20-1.4.1.jar /opt/flink/lib/
COPY jars/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar /opt/flink/lib/

# Kode job (di-submit via `flink run -py /opt/flink_job/job.py`).
COPY job.py /opt/flink_job/job.py
