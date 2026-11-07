#!/usr/bin/env bash
# 在 master 容器内用 Spark local 模式跑 pipeline（读 HDFS 数据，写容器内输出，再拷回）。
# 这是主路线：零改动宿主机/原实验配置；HDFS 分布式存储 + 单机 Spark 计算（报告中如实标注）。
# 用法：在 Ubuntu VM 的项目目录运行  bash scripts/run_pipeline_container.sh
set -euo pipefail

CONTAINER="${CONTAINER:-master}"
PROJECT_IN_CONTAINER="/opt/course-taxi-demand"
HDFS_INPUT="${HDFS_INPUT:-hdfs:///user/bigdata/course_taxi/input}"
SPARK_BIN="/opt/spark/bin/spark-submit"

# 1) 把项目代码、配置与数据文件同步进容器（不含 venv）
docker exec "$CONTAINER" mkdir -p "$PROJECT_IN_CONTAINER/outputs" "$PROJECT_IN_CONTAINER/data"
for f in pipeline.py common.py app.py config.json; do
  [ -f "$f" ] || { echo "缺少 $f，请在项目根目录运行本脚本"; exit 1; }
  docker cp "$f" "$CONTAINER":"$PROJECT_IN_CONTAINER"/
done
INGEST_ARG=""
if [ -f data/yellow_tripdata_2024-01.parquet ] && [ -f data/taxi_zone_lookup.csv ]; then
  docker cp data/yellow_tripdata_2024-01.parquet "$CONTAINER":"$PROJECT_IN_CONTAINER"/data/
  docker cp data/taxi_zone_lookup.csv "$CONTAINER":"$PROJECT_IN_CONTAINER"/data/
  INGEST_ARG="--ingest $PROJECT_IN_CONTAINER/data"   # Spark 原生写入 HDFS，入库也留在 Python 代码里
fi

# 2) 容器内运行（local[2]）
docker exec "$CONTAINER" bash -c "
  [ -x $SPARK_BIN ] || { echo '容器内未安装 Spark：先运行 scripts/setup_spark_vm.sh'; exit 1; }
  cd $PROJECT_IN_CONTAINER
  PYTHONPATH=$PROJECT_IN_CONTAINER PYTHONHASHSEED=0 \
  $SPARK_BIN --master 'local[2]' pipeline.py \
    --config config.json $INGEST_ARG --input $HDFS_INPUT --output $PROJECT_IN_CONTAINER/outputs
"

# 3) 拷回结果到 VM 项目目录的 outputs/（运行日志一并存档为证据）
mkdir -p ./docs/evidence
docker cp "$CONTAINER":"$PROJECT_IN_CONTAINER/outputs/." ./outputs/
docker cp "$CONTAINER":"$PROJECT_IN_CONTAINER/run.log" "./docs/evidence/spark_run_$(date +%Y%m%d_%H%M%S).log" 2>/dev/null || true
echo "== 完成，结果在 $(pwd)/outputs/ =="
ls -la outputs/
