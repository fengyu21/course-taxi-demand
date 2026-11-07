# 基于 Hadoop 与 Spark 的出租车区域小时需求预测与可视化系统

以纽约 TLC 公开出租车数据为例（Yellow Taxi 2024 年 1 月，2,964,624 行真实行程）。

## 项目内容

1. **数据获取与校验**：从 TLC 官网下载 2024 年 1 月全量黄色出租车数据与区域维表，并生成 `data/provenance.json` 记录文件信息与 SHA-256（`scripts/download_data.py`）
2. **数据入库**：用 Spark DataFrame 将数据写入 HDFS（`pipeline.py --ingest`），或通过 hdfs CLI 入库（`scripts/upload_hdfs.sh`）；数据存放路径在 `config.json` 中配置
3. **Spark 分布式处理**：清洗漏斗 → 全量统计（每日/小时/星期×小时热力/区域/Borough/距离/时长/热门 OD）→ 训练期 Top-10 区域 → 连续小时面板（缺失小时补 0）→ 滚动一步特征（`pipeline.py`，PySpark + Window 窗口函数）
4. **机器学习建模**：周季节性基线（lag_168）＋ MLlib 随机森林 ＋ 残差修正随机森林，同一套特征与参数，输出验证/测试 MAE 与 RMSE、逐区域逐小时预测
5. **可视化看板**：四个页面——数据总览、出行规律分析、需求预测（选区域选时刻）、高需求区域提示；支持默认离线结果与上传小型 CSV（≤50MB，同一套清洗统计口径 `analysis.py`）两种数据来源
6. **工程化**：11 步从零复现表、24 项自动化测试

## 系统架构

数据流：

```text
① 获取数据  TLC 官网 → Windows data/raw/（scripts/download_data.py）
② 入库      scp → VM → docker cp → Spark 原生写入 / hdfs dfs -put
            → HDFS（路径见 config.json）
③ 集群计算  spark-submit --master yarn --deploy-mode client（2 executor 派到 worker1/worker2）
            清洗 → 全量统计 → 训练期 Top-10 面板 → 滚动一步特征 → 三模型
④ 结果落地  14 个聚合 CSV/JSON（原始 296 万行在集群内计算，看板仅读取聚合结果）
⑤ 展示      结果取回 Windows → Streamlit 四页面；上传 CSV 走 analysis.py 即时分析
```

| 组件 | 角色 | 运行位置 |
|---|---|---|
| HDFS | 分布式存储：原始 Parquet 存储，NameNode + 2×DataNode | VM 三容器（master / worker1 / worker2） |
| YARN | 集群资源调度：分配 Spark executor 到两台 worker | VM master 容器（ResourceManager） |
| Spark 3.5.7 | 计算引擎：清洗、统计、特征、MLlib 随机森林 | master 容器 /opt/spark，读 HDFS、写聚合结果 |
| Streamlit | 可视化看板：四个页面 + 上传模式 | Windows 本机，读取聚合结果 |
| analysis.py | 上传 CSV 的即时清洗与统计 | Windows 本机（看板内） |

**存储（HDFS）、调度（YARN）、计算（Spark）、展示（Streamlit）四层分离**——
原始 296 万行只进 HDFS、只在集群里算，前端和建模之间只传递聚合后的小结果。
运行证据：YARN 集群实测运行全程约 62 秒；三种运行模式（Windows local /
容器 local[2]+HDFS / YARN）结果逐位一致。

## 目录结构

```text
lab-final/
  README.md                          # 项目说明（架构、复现指引、结果）
  pipeline.py                        # Spark 流水线：清洗→统计→面板→特征→三模型→导出
  common.py                          # 共享纯 Python 逻辑（划分/指标/残差/补零）
  analysis.py                        # 上传 CSV 的验证/清洗/统计（与 Spark 离线同口径）
  app.py                             # Streamlit 看板（双数据来源，四个页面）
  config.json                        # 日期划分、HDFS 路径、RF 参数
  requirements.txt                   # 固定依赖清单
  scripts/                           # 下载 / 入库 / 装 Spark / 运行 / 取回
  tests/                             # 24 项自动化测试
  data/provenance.json               # 数据来源与 SHA-256 元数据
  .streamlit/config.toml             # 看板主题与上传限制
```

## 快速开始（Windows 本机）

前提：Python 3.10+ 与任意 JDK（8/11/17/21 均可）。

```bash
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt

# 1. 下载数据（~50MB）并记录元数据
.venv/Scripts/python scripts/download_data.py

# 2. 本地运行流水线
.venv/Scripts/spark-submit pipeline.py --input data/raw --output outputs

# 3. 运行看板
.venv/Scripts/streamlit run app.py

# 4. 自动化测试
.venv/Scripts/python -m pytest tests/ -v
```

## 从零复现（Windows 本机 + VM 正式运行）

前提：Ubuntu 虚拟机装有实验二的 Hadoop 三容器镜像（bigdata/hadoop:3.3.6）；Windows 有 Python 3.10+ 与任意 JDK。

| # | 步骤 | 在哪执行 | 命令 | 预期结果 |
|---|---|---|---|---|
| 0 | 装依赖 | Windows | `python -m venv .venv && .venv/Scripts/pip install -r requirements.txt` | 依赖就绪 |
| 1 | 下载数据 | Windows | `.venv/Scripts/python scripts/download_data.py` | `data/raw/` 两个文件 + `data/provenance.json` |
| 2 | 本地试跑（可选） | Windows | `.venv/Scripts/spark-submit pipeline.py --input data/raw --output outputs` | `outputs/` 生成聚合结果 |
| 3 | 传输到 VM | Windows | `scp -r` 项目；再 `scp` 两个数据文件到 `~/course-taxi-demand/data/` | VM 上 `~/course-taxi-demand/` 结构齐全 |
| 4 | 启动 Hadoop 集群 | VM | `cd ~/lab02-student && bash start-hadoop.sh` | 三容器 Up；`docker exec master jps` 可见 NameNode/ResourceManager |
| 5 | 装 Spark + 依赖 | VM | `cd ~/course-taxi-demand && bash scripts/setup_spark_vm.sh` | `spark-submit --version` 显示 3.5.7 |
| 6 | 数据入 HDFS | VM | 两种方式任选：`bash scripts/upload_hdfs.sh`（hdfs CLI）；或直接运行第 7 步——容器脚本自带 `--ingest`（Spark 原生写入） | `hdfs dfs -count` 显示数据已入库 |
| 7 | 跑流水线 | VM | `bash scripts/run_pipeline_container.sh`（local[2]+HDFS）或 `bash scripts/run_pipeline_yarn.sh`（YARN client） | 容器内聚合结果拷回；YARN UI 出现 `application_*` |
| 8 | 取回结果并开看板 | Windows | `bash scripts/fetch_outputs.sh` → `streamlit run app.py -- --outputs outputs_vm` | 看板四页面可交互 |
| 9 | 测试 | Windows | `.venv/Scripts/python -m pytest tests/ -v` | **24 passed** |

只跑本地版：完成 0–2 步 + `streamlit run app.py` 即可演示，全程不需要虚拟机。

## 数据

| 项 | 值 |
|---|---|
| 来源 | NYC TLC 官网（见 `data/provenance.json` 内 URL 与下载时间） |
| 文件 | `yellow_tripdata_2024-01.parquet`（2,964,624 行）＋ `taxi_zone_lookup.csv` |
| 校验 | SHA-256 记录于 `data/provenance.json` |

## 运行结果

| 模型 | 验证 MAE / RMSE | 测试 MAE / RMSE |
|---|---|---|
| 周季节性基线（lag_168） | 37.84 / 59.37 | **23.61 / 37.65** |
| 随机森林（30×5, seed=42） | **30.74 / 45.63** | 26.01 / 38.64 |
| 残差修正随机森林 | 32.25 / 48.56 | 24.57 / 38.03 |

分析说明：验证周（1/22–1/25，前一周含 MLK 假日扰动）随机森林误差更低；
测试周规律平稳，基线误差更低；分区域看，机场类区域（如 JFK）随机森林始终占优。

## 范围说明

- 单月（2024-01）数据；预测仅覆盖训练期 Top-10 区域
- 滚动一步离线历史回测，假设历史小时计数可得
- 上传 CSV 走前端即时分析（同一套清洗统计口径），不参与建模；完整数据链路为 HDFS + Spark
- 需求标签 = 区域小时上车订单数
