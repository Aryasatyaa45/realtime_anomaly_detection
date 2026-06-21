#!/usr/bin/env bash
# Unduh jar Flink/Paimon/JDBC ke flink_job/jars/ (di-gitignore karena besar).
# WAJIB dijalankan sekali setelah fresh clone, SEBELUM `docker compose build`.
#
# --ssl-no-revoke: di Windows (schannel) cek revocation sertifikat ke Maven sering gagal
# di balik proxy/AV; flag ini melewatinya. Aman di Linux/Mac juga (curl mengabaikannya).
set -euo pipefail

DEST="$(cd "$(dirname "$0")/.." && pwd)/flink_job/jars"
MAVEN="https://repo1.maven.org/maven2"
mkdir -p "$DEST"

# nama_file -> path Maven (groupId/artifactId/version/file)
JARS=(
  "flink-connector-jdbc-3.3.0-1.20.jar|org/apache/flink/flink-connector-jdbc/3.3.0-1.20"
  "flink-shaded-hadoop-2-uber-2.8.3-10.0.jar|org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0"
  "paimon-flink-1.20-1.4.1.jar|org/apache/paimon/paimon-flink-1.20/1.4.1"
  "postgresql-42.7.4.jar|org/postgresql/postgresql/42.7.4"
)

for entry in "${JARS[@]}"; do
  file="${entry%%|*}"
  path="${entry##*|}"
  if [[ -f "$DEST/$file" ]]; then
    echo "[skip] $file sudah ada"
    continue
  fi
  echo "[unduh] $file"
  curl -fSL --ssl-no-revoke "$MAVEN/$path/$file" -o "$DEST/$file"
done

echo "Selesai. Jar di: $DEST"
ls -lh "$DEST"
