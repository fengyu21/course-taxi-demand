#!/usr/bin/env bash
# 上传原始数据到 HDFS（在 Ubuntu VM 的项目目录运行；需要 master 容器在运行）。
# 用 docker cp + 容器内 hdfs 客户端，不改原 Compose、不格式化、不删卷。
set -euo pipefail

CONTAINER="${CONTAINER:-master}"
HDFS_DIR="${HDFS_DIR:-/user/bigdata/course_taxi/input}"
RAW_DIR="${RAW_DIR:-data/raw}"

for f in "$RAW_DIR/yellow_tripdata_2024-01.parquet" "$RAW_DIR/taxi_zone_lookup.csv"; do
  [ -f "$f" ] || { echo "缺少文件: $f —— 先在 Windows 运行 scripts/download_data.py 并 scp 传入"; exit 1; }
done

# 操作记录自动存档（HDFS 操作证据）
EVID_DIR="$(cd "$(dirname "$0")/.." && pwd)/docs/evidence"
mkdir -p "$EVID_DIR"
EVID_FILE="$EVID_DIR/hdfs_upload_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee "$EVID_FILE") 2>&1
echo "== 本次 HDFS 操作记录将存档到 $EVID_FILE =="

docker ps --format '{{.Names}}' | grep -qx "$CONTAINER" || { echo "容器 $CONTAINER 未运行：先 cd ~/lab02-student && bash start-hadoop.sh"; exit 1; }

echo "== 复制文件进 $CONTAINER =="
docker cp "$RAW_DIR/yellow_tripdata_2024-01.parquet" "$CONTAINER":/tmp/
docker cp "$RAW_DIR/taxi_zone_lookup.csv" "$CONTAINER":/tmp/

echo "== HDFS: $HDFS_DIR =="
docker exec "$CONTAINER" hdfs dfs -mkdir -p "$HDFS_DIR"
docker exec "$CONTAINER" hdfs dfs -put -f /tmp/yellow_tripdata_2024-01.parquet "$HDFS_DIR"/
docker exec "$CONTAINER" hdfs dfs -put -f /tmp/taxi_zone_lookup.csv "$HDFS_DIR"/
docker exec "$CONTAINER" hdfs dfs -count "$HDFS_DIR"
echo "== 完成（只新增 /user/bigdata/course_taxi，未触碰原实验数据） =="
