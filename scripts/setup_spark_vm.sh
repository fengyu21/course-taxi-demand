#!/usr/bin/env bash
# VM 上安装 Spark 3.5.7（主路线：装进 master 容器 /opt/spark，零改动宿主机与原实验配置）。
# 容器重启不影响；若容器被重建（compose down 后 up），重新运行本脚本即可。
set -euo pipefail

SPARK_VERSION=3.5.7
PKG="spark-${SPARK_VERSION}-bin-hadoop3"
CONTAINER="${CONTAINER:-master}"
DEST="/opt"

for f in "$PKG.tgz"; do
  [ -f "$f" ] || {
    echo "下载 $PKG.tgz（如已离线下载，放到本目录即可）"
    (curl -fL -o "$PKG.tgz" "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${PKG}.tgz" \
      || curl -fL -o "$PKG.tgz" "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${PKG}.tgz")
  }
done

docker ps --format '{{.Names}}' | grep -qx "$CONTAINER" || { echo "容器 $CONTAINER 未运行"; exit 1; }

docker cp "$PKG.tgz" "$CONTAINER":/tmp/
docker exec "$CONTAINER" bash -c "
  [ -d $DEST/spark ] && { echo 'master 容器内已有 /opt/spark，跳过安装'; exit 0; }
  tar -xzf /tmp/$PKG.tgz -C $DEST && mv $DEST/$PKG $DEST/spark && rm /tmp/$PKG.tgz
  echo '== 验证 =='
  $DEST/spark/bin/spark-submit --version 2>&1 | head -3
"

echo "== 容器内 Python 依赖（Python 3.12 无 distutils，pyspark.ml 还需要 numpy） =="
for c in master worker1 worker2; do
  if docker exec "$c" python3 -c "import distutils, numpy" 2>/dev/null; then
    echo "  $c：依赖已就绪"
  else
    echo "  为 $c 安装 python3-setuptools / python3-numpy ..."
    docker exec "$c" bash -c "apt-get update -qq && apt-get install -y -qq python3-setuptools python3-numpy"
  fi
done
docker exec "$CONTAINER" python3 -c "import distutils, numpy; print('容器依赖检查通过')"

echo "== Spark ${SPARK_VERSION} 已装入 $CONTAINER:/opt/spark =="
echo "下一步：数据入 HDFS（scripts/upload_hdfs.sh）后运行 scripts/run_pipeline_container.sh"
