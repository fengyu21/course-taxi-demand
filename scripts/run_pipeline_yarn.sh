#!/usr/bin/env bash
# 最小 YARN 尝试（可选）：从 master 容器以 yarn client 模式提交。
# 前提：原 Hadoop 集群在运行；worker 容器内有 python3（先手动验证，不满足就用 local 模式）。
# 失败不要恋战：run_pipeline_container.sh 的 local 模式即可交付。
set -euo pipefail

CONTAINER="${CONTAINER:-master}"
PROJECT_IN_CONTAINER="/opt/course-taxi-demand"
HDFS_INPUT="${HDFS_INPUT:-hdfs:///user/bigdata/course_taxi/input}"
SPARK_BIN="/opt/spark/bin/spark-submit"

docker exec "$CONTAINER" bash -c "
  [ -x $SPARK_BIN ] || { echo '先运行 scripts/setup_spark_vm.sh'; exit 1; }
  echo '== worker python3 检查 =='
  for w in worker1 worker2; do
    docker exec \$w python3 --version || { echo \"\$w 无 python3，YARN 模式需先解决（或改用 local 模式）\"; exit 1; }
  done
  cd $PROJECT_IN_CONTAINER
  HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop \
  PYSPARK_PYTHON=python3 \
  $SPARK_BIN --master yarn --deploy-mode client \
    --num-executors 2 --executor-memory 1g --driver-memory 1g \
    pipeline.py --config config.json --input $HDFS_INPUT --output $PROJECT_IN_CONTAINER/outputs
"
docker cp "$CONTAINER":"$PROJECT_IN_CONTAINER/outputs/." ./outputs/
mkdir -p ./docs/evidence
YARN_LOG="./docs/evidence/yarn_run_$(date +%Y%m%d_%H%M%S).log"
docker cp "$CONTAINER":"$PROJECT_IN_CONTAINER/run_yarn.log" "$YARN_LOG" 2>/dev/null || true
echo "== YARN 应用 ID：$(grep -oE 'application_[0-9]+_[0-9]+' "$YARN_LOG" 2>/dev/null | head -1) =="
echo "== 完成（YARN 模式）。运行日志已存档 $YARN_LOG；任务记录见 YARN Web UI: http://<VM_IP>:8088 =="
