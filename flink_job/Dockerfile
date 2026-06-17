# Base image Flink resmi; PyFlink + dependensi ditambahkan saat Minggu 4.
FROM flink:1.20-scala_2.12-java11

# Install Python + PyFlink (versi difinalkan di Minggu 4)
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# RUN pip3 install --no-cache-dir apache-flink pyod river

COPY job.py /opt/flink_job/job.py

# Cara submit job difinalkan di Minggu 4 (flink run -py ...).
