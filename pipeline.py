"""出租车区域小时需求预测：Spark 离线流水线（课程作业）。

链路：读取 TLC 2024-01 黄色出租车 parquet → 基础清洗 → 全量统计 →
训练期 Top-10 活跃区域 → 连续小时面板（补 0）→ 滚动一步特征 →
周季节性基线 / 随机森林 / 残差修正随机森林 → 小结果导出 outputs/。

预测口径：区域小时"上车订单数"的滚动一步离线历史回测（不是实时、不是多步）。

时间口径（防时区错位）：数据列是 timestamp_ntz（纽约本地墙上时间）。
所有过滤/划分/特征统一换算成 **UTC 基准的整数小时 hour_int**（见 common.epoch_hour，
不读系统时区），因此结果与驱动 JVM / 运行机器的本地时区无关；
2024-01 美国无夏令时切换，hour_int % 24 即本地小时。

用法：
  # 本地文件（Windows 试跑）
  .venv/Scripts/spark-submit pipeline.py --config config.json \
      --input data/raw --output outputs
  # VM 上读 HDFS
  spark-submit pipeline.py --config config.json \
      --input hdfs:///user/bigdata/course_taxi/input --output outputs

约束：仅对聚合后的小表 collect；原始百万级行不 collect / toPandas。
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession, Window

from common import (
    EPOCH,
    UNKNOWN_ZONE_IDS,
    compute_metrics,
    epoch_hour,
    final_from_residual,
    get_splits,
    hour_str,
    load_config,
    split_of,
    weekday_mon0,
    write_csv,
    write_json,
)

# 行程描述性统计的阈值：项目内自定规则，非官方标准
TRIP_MAX_DISTANCE_MILES = 100.0
TRIP_MAX_MINUTES = 180.0

DISTANCE_BUCKETS_MILES = [0, 1, 2, 3, 5, 8, 12, 20, 100]
DURATION_BUCKETS_MIN = [0, 5, 10, 15, 30, 60, 120, 180]


def parse_args():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--input", default=None, help="输入目录（本地或 hdfs://，覆盖 config）")
    ap.add_argument("--output", default=None, help="输出目录（覆盖 config）")
    ap.add_argument("--ingest", default=None, help="先用 Spark 把该本地目录的数据写入 --input 指向的存储（HDFS 入库也留在 Python 代码里）")
    return ap.parse_args()


def bucket_expr(col: str, edges: list) -> str:
    """生成 CASE WHEN 分桶表达式，标签如 '5-8'（左闭右开），最后一桶 '>=20'。"""
    parts = []
    for lo, hi in zip(edges[:-1], edges[1:]):
        parts.append(f"WHEN {col} >= {lo} AND {col} < {hi} THEN '{lo}-{hi}'")
    parts.append(f"ELSE '>={edges[-2]}'")
    return "CASE " + " ".join(parts) + " END"


def bucket_key(label: str, edges: list):
    """分桶标签 → 排序键（桶下界在 edges 中的下标）。"""
    first = label.split("-")[0].lstrip(">=")
    return edges.index(int(first))


def coalesce_false(col):
    """isin(None) 会得到 null，取反前先固定为 False，避免漏计。"""
    return F.coalesce(col, F.lit(False))


def main():
    args = parse_args()
    cfg = load_config(args.config)
    splits = get_splits(cfg)
    data_start, data_end = split_of(splits, "warmup")[0], split_of(splits, "test")[1]
    train_start, train_end = split_of(splits, "train")
    val_start, val_end = split_of(splits, "val")
    test_start, test_end = split_of(splits, "test")

    # 统一换成整数小时（UTC 墙钟基准，与时区无关）
    base_h = epoch_hour(data_start)
    end_h = epoch_hour(data_end)
    train_h = (epoch_hour(train_start), epoch_hour(train_end))
    val_h = (epoch_hour(val_start), epoch_hour(val_end))
    test_h = (epoch_hour(test_start), epoch_hour(test_end))
    n_hours = end_h - base_h  # 744

    # 数据起点 2024-01-01 必须是周一（星期公式自检，防止公式错误静默通过）
    if weekday_mon0(base_h) != 0:
        raise RuntimeError("weekday 公式自检失败：2024-01-01 应为周一(0)")

    out_dir = Path(args.output) if args.output else (Path(__file__).parent / cfg["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    input_dir = args.input.rstrip("/") if args.input else "hdfs://" + cfg["data"]["hdfs_input_dir"].lstrip("/")
    manifest = {
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "input_dir": str(input_dir),
        "output_dir": str(out_dir),
        "config": cfg,
        "steps": {},
        "time_semantics": (
            "数据为 timestamp_ntz（纽约本地墙上时间）；所有过滤/划分/特征使用 UTC 基准的整数小时"
            " hour_int（common.epoch_hour，calendar.timegm，不读系统时区），与驱动/机器时区无关"
        ),
    }

    spark = (
        SparkSession.builder.appName("taxi-demand-course")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")  # 聚合后数据很小，默认 200 分区无意义
        .getOrCreate()
    )
    manifest["spark_version"] = spark.version
    manifest["spark_master"] = spark.conf.get("spark.master", "unknown")

    def step(name):
        class _T:
            def __enter__(self):
                self.t0 = time.time()
                return self

            def __exit__(self, *a):
                manifest["steps"][name] = round(time.time() - self.t0, 1)

        return _T()

    # ---------- 0.5 可选：Spark 原生入库（本地文件 → HDFS），入库也留在 Python 代码里 ----------
    if args.ingest:
        with step("ingest"):
            src = args.ingest.rstrip("/")
            spark.read.parquet(f"{src}/{cfg['data']['yellow_file']}").write.mode("overwrite").parquet(
                f"{input_dir}/{cfg['data']['yellow_file']}"
            )
            spark.read.option("header", True).csv(f"{src}/{cfg['data']['zone_file']}").write.mode(
                "overwrite"
            ).option("header", True).csv(f"{input_dir}/{cfg['data']['zone_file']}")
            manifest["ingest"] = {"from": src, "to": input_dir, "engine": "Spark DataFrame write（无需 hdfs CLI）"}
            print(f"[ingest] {src} -> {input_dir}")

    # ---------- 1. 读取区域维表 ----------
    with step("read_lookup"):
        lookup = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(f"{input_dir}/{cfg['data']['zone_file']}")
        )
        lookup = lookup.select(
            F.col("LocationID").cast("int").alias("zone_id"),
            F.col("Zone").alias("zone"),
            F.col("Borough").alias("borough"),
        ).cache()
        valid_zone_ids = [int(r["zone_id"]) for r in lookup.select("zone_id").collect()]
        manifest["lookup_rows"] = len(valid_zone_ids)

    # ---------- 2. 读取行程数据并清洗（漏斗，一次扫描统计各阶段计数） ----------
    with step("read_and_clean"):
        raw = spark.read.parquet(f"{input_dir}/{cfg['data']['yellow_file']}")
        manifest["schema_fields"] = [(f.name, f.dataType.simpleString()) for f in raw.schema.fields]
        df = raw.select(
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "trip_distance",
            "passenger_count",
            "fare_amount",
            "total_amount",
        )

        pick_sec = F.unix_timestamp("tpep_pickup_datetime")  # NTZ 列按 session 时区(UTC)取秒，即墙钟
        in_range = (pick_sec >= F.lit(base_h * 3600)) & (pick_sec < F.lit(end_h * 3600))
        in_lookup = coalesce_false(F.col("PULocationID").isin(valid_zone_ids))
        is_unknown_zone = coalesce_false(F.col("PULocationID").isin(list(UNKNOWN_ZONE_IDS)))
        base_ok = F.col("tpep_pickup_datetime").isNotNull() & in_range
        zone_ok = in_lookup & ~is_unknown_zone

        quality = df.agg(
            F.count("*").alias("total_rows"),
            F.sum(F.when(F.col("tpep_pickup_datetime").isNull(), 1).otherwise(0)).alias("n_missing_pickup_time"),
            F.sum(F.when(F.col("tpep_pickup_datetime").isNotNull() & ~coalesce_false(in_range), 1).otherwise(0)).alias(
                "n_pickup_out_of_range"
            ),
            F.sum(F.when(base_ok & ~in_lookup, 1).otherwise(0)).alias("n_zone_not_in_lookup"),
            F.sum(F.when(base_ok & is_unknown_zone, 1).otherwise(0)).alias("n_zone_unknown_or_outside"),
        ).first()
        quality = {k: int(v) for k, v in quality.asDict().items()}
        quality["valid_core_rows"] = quality["total_rows"] - (
            quality["n_missing_pickup_time"]
            + quality["n_pickup_out_of_range"]
            + quality["n_zone_not_in_lookup"]
            + quality["n_zone_unknown_or_outside"]
        )

        # 核心有效需求：只按必要字段过滤；负费用/乘客缺失等不删，仅记录标志（可能重叠，不求和）
        valid = (
            df.filter(base_ok & zone_ok)
            .withColumn("hour_int", F.floor(pick_sec / 3600).cast("long"))
            .cache()
        )
        demand = valid.groupBy("PULocationID", "hour_int").agg(F.count("*").alias("pickups"))

        flags = valid.agg(
            F.sum(
                F.when(F.col("passenger_count").isNull() | (F.col("passenger_count") <= 0), 1).otherwise(0)
            ).alias("flag_passenger_missing_or_zero"),
            F.sum(F.when(F.col("fare_amount") < 0, 1).otherwise(0)).alias("flag_negative_fare"),
            F.sum(F.when(F.col("trip_distance") > TRIP_MAX_DISTANCE_MILES, 1).otherwise(0)).alias(
                "flag_distance_over_100mi"
            ),
        ).first()
        quality["n_active_zones"] = int(
            valid.select(F.countDistinct("PULocationID")).first()[0]
        )
        flags = {k: int(v) for k, v in flags.asDict().items()}
        print("[quality]", quality, flags)

    manifest["data_quality"] = {"funnel": quality, "flags_on_valid_rows": flags}
    write_json(out_dir / "data_quality.json", manifest["data_quality"])

    # ---------- 3. 全量统计（小结果导出；日期/星期由 hour_int 在 Python 侧渲染，时区无关） ----------
    with step("stats"):
        daily = demand.groupBy((F.col("hour_int") / 24).cast("long").alias("day_int")).agg(
            F.sum("pickups").alias("pickups")
        )
        daily_rows = sorted(
            ((EPOCH + timedelta(days=int(d))).strftime("%Y-%m-%d"), int(p))
            for d, p in [tuple(r) for r in daily.collect()]
        )
        write_csv(out_dir / "daily_stats.csv", ["date", "pickups"], daily_rows)

        hourly = demand.groupBy((F.col("hour_int") % 24).cast("int").alias("hour_of_day")).agg(
            F.sum("pickups").alias("pickups_total")
        )
        hourly_rows = sorted(
            (int(h), int(t), round(t / (n_hours / 24), 1)) for h, t in [tuple(r) for r in hourly.collect()]
        )
        write_csv(out_dir / "hourly_stats.csv", ["hour", "pickups_total", "pickups_avg_per_day"], hourly_rows)

        # 星期×小时：总数 ÷ 该(星期,小时)在当月出现次数（次数直接由连续小时序列推出）
        wd_tot_rows = demand.groupBy(
            ((F.col("hour_int") / 24).cast("long") + 3).__mod__(7).alias("weekday"),
            (F.col("hour_int") % 24).cast("int").alias("hour"),
        ).agg(F.sum("pickups").alias("pickups_total"))
        occurrences: dict = {}
        for h in range(base_h, end_h):
            key = (weekday_mon0(h), h % 24)
            occurrences[key] = occurrences.get(key, 0) + 1
        wd_rows = sorted(
            (
                int(r["weekday"]),
                int(r["hour"]),
                int(r["pickups_total"]),
                occurrences[(int(r["weekday"]), int(r["hour"]))],
                round(r["pickups_total"] / occurrences[(int(r["weekday"]), int(r["hour"]))], 2),
            )
            for r in wd_tot_rows.collect()
        )
        write_csv(
            out_dir / "weekday_hour.csv",
            ["weekday", "hour", "pickups_total", "occurrences", "pickups_avg"],
            wd_rows,
        )

        zone_month = (
            demand.groupBy("PULocationID")
            .agg(F.sum("pickups").alias("pickups"))
            .join(lookup.alias("lz"), F.col("PULocationID") == F.col("lz.zone_id"), "left")
            .select(
                F.col("PULocationID").alias("zone_id"),
                F.col("lz.zone").alias("zone"),
                F.col("lz.borough").alias("borough"),
                "pickups",
            )
        )
        zone_top = [
            (i + 1, int(r["zone_id"]), r["zone"], r["borough"], int(r["pickups"]))
            for i, r in enumerate(zone_month.orderBy(F.desc("pickups"), F.asc("zone_id")).limit(20).collect())
        ]
        write_csv(out_dir / "zone_stats.csv", ["rank", "zone_id", "zone", "borough", "pickups"], zone_top)

        borough_rows = sorted(
            [
                (r["borough"], int(r["pickups"]))
                for r in zone_month.groupBy("borough").agg(F.sum("pickups").alias("pickups")).collect()
            ],
            key=lambda x: -x[1],
        )
        write_csv(out_dir / "borough_stats.csv", ["borough", "pickups"], borough_rows)

        # 行程描述统计：有效行程子集（下车>上车、时长/距离在合理区间；阈值是项目规则）
        tv = valid.withColumn(
            "dur_min",
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0,
        ).filter(
            (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
            & (F.col("dur_min") > 0)
            & (F.col("dur_min") <= TRIP_MAX_MINUTES)
            & (F.col("trip_distance") > 0)
            & (F.col("trip_distance") <= TRIP_MAX_DISTANCE_MILES)
        )
        manifest["trip_valid_rows"] = int(tv.count())

        tv = tv.withColumn("d_bucket", F.expr(bucket_expr("trip_distance", DISTANCE_BUCKETS_MILES))).withColumn(
            "t_bucket", F.expr(bucket_expr("dur_min", DURATION_BUCKETS_MIN))
        )
        dist_rows = sorted(
            [(r["d_bucket"], int(r["n"])) for r in tv.groupBy("d_bucket").agg(F.count("*").alias("n")).collect()],
            key=lambda x: bucket_key(x[0], DISTANCE_BUCKETS_MILES),
        )
        write_csv(out_dir / "distance_dist.csv", ["bucket_miles", "rows"], dist_rows)

        dur_rows = sorted(
            [(r["t_bucket"], int(r["n"])) for r in tv.groupBy("t_bucket").agg(F.count("*").alias("n")).collect()],
            key=lambda x: bucket_key(x[0], DURATION_BUCKETS_MIN),
        )
        write_csv(out_dir / "duration_dist.csv", ["bucket_minutes", "rows"], dur_rows)

        # 热门 OD（含 Unknown/Outside of NYC，完整展示）
        pu2 = lookup.select(
            F.col("zone_id").alias("pu_id"), F.col("zone").alias("pu_zone"), F.col("borough").alias("pu_borough")
        )
        do2 = lookup.select(
            F.col("zone_id").alias("do_id"), F.col("zone").alias("do_zone"), F.col("borough").alias("do_borough")
        )
        od = (
            valid.groupBy("PULocationID", "DOLocationID")
            .agg(F.count("*").alias("trips"))
            .join(pu2, F.col("PULocationID") == F.col("pu_id"))
            .join(do2, F.col("DOLocationID") == F.col("do_id"))
            .select("pu_zone", "pu_borough", "do_zone", "do_borough", "trips")
            .orderBy(F.desc("trips"))
            .limit(20)
        )
        od_rows = [
            (i + 1, r["pu_zone"], r["pu_borough"], r["do_zone"], r["do_borough"], int(r["trips"]))
            for i, r in enumerate(od.collect())
        ]
        write_csv(
            out_dir / "od_stats.csv",
            ["rank", "pu_zone", "pu_borough", "do_zone", "do_borough", "trips"],
            od_rows,
        )

    # ---------- 4. 训练期 Top-10 区域（严格只用训练期上车量） ----------
    with step("select_zones"):
        top_df = (
            demand.filter((F.col("hour_int") >= F.lit(train_h[0])) & (F.col("hour_int") < F.lit(train_h[1])))
            .groupBy("PULocationID")
            .agg(F.sum("pickups").alias("train_pickups"))
            .orderBy(F.desc("train_pickups"), F.asc("PULocationID"))
            .limit(cfg["top_n_zones"])
        )
        top10 = [(int(r["PULocationID"]), int(r["train_pickups"])) for r in top_df.collect()]
        top_ids = [z for z, _ in top10]
        name_map = {
            int(r["zone_id"]): (r["zone"], r["borough"])
            for r in lookup.filter(F.col("zone_id").isin(top_ids)).select("zone_id", "zone", "borough").collect()
        }
        manifest["selected_zones"] = [
            {
                "zone_id": z,
                "train_pickups": n,
                "zone": name_map.get(z, ("未知区域", "Unknown"))[0],
                "borough": name_map.get(z, ("未知区域", "Unknown"))[1],
            }
            for z, n in top10
        ]
        manifest["zone_selection_window"] = "训练期 [2024-01-08, 2024-01-22)，不含验证/测试期"
        print("[top10]", top10)

    # ---------- 5. 连续小时面板：网格补 0（前提：源文件完整可读） ----------
    with step("panel"):
        zones_small = spark.sql(f"SELECT explode(array({','.join(map(str, top_ids))})) AS zone_id")
        dem10 = demand.filter(F.col("PULocationID").isin(top_ids)).select(
            F.col("PULocationID").alias("d_zone"),
            F.col("hour_int").alias("d_hour"),
            "pickups",
        )
        grid = spark.range(base_h, end_h).select(F.col("id").cast("long").alias("hour_int"))
        panel = (
            grid.crossJoin(zones_small)
            .join(dem10, (F.col("zone_id") == F.col("d_zone")) & (F.col("hour_int") == F.col("d_hour")), "left")
            .select("zone_id", "hour_int", F.coalesce("pickups", F.lit(0)).cast("int").alias("pickups"))
        )
        panel_named = panel.join(lookup, "zone_id").select("zone_id", "zone", "borough", "hour_int", "pickups")
        panel_rows = [
            (int(r["zone_id"]), r["zone"], r["borough"], hour_str(r["hour_int"]), int(r["pickups"]))
            for r in panel_named.orderBy("zone_id", "hour_int").coalesce(2).collect()
        ]
        assert len(panel_rows) == len(top_ids) * n_hours, (
            f"面板行数应等于 区域数×小时数: {len(panel_rows)} vs {len(top_ids) * n_hours}"
        )
        write_csv(
            out_dir / "panel_10zones.csv",
            ["zone_id", "zone", "borough", "hour_ts", "pickups"],
            panel_rows,
        )

    # ---------- 6. 滚动一步特征（lag 不含未来；rolling 不含当前小时） ----------
    with step("features"):
        w = Window.partitionBy("zone_id").orderBy("hour_int")
        feat = (
            panel.withColumn("lag_1", F.lag("pickups", 1).over(w))
            .withColumn("lag_24", F.lag("pickups", 24).over(w))
            .withColumn("lag_168", F.lag("pickups", 168).over(w))
            .withColumn("roll24_mean", F.avg("pickups").over(w.rowsBetween(-24, -1)))
            .withColumn("hour_of_day", (F.col("hour_int") % 24).cast("int"))
            .withColumn("day_of_week", ((F.col("hour_int") / 24).cast("long") + 3).__mod__(7).cast("int"))
            .withColumn("is_weekend", (F.col("day_of_week") >= 5).cast("int"))
        )
        for z in top_ids:
            feat = feat.withColumn(f"z{z}", (F.col("zone_id") == F.lit(z)).cast("int"))
        feat = feat.filter(F.col("lag_168").isNotNull())  # 首周（warmup）无周滞后，不参与建模

        feature_cols = ["lag_1", "lag_24", "lag_168", "roll24_mean", "hour_of_day", "day_of_week", "is_weekend"] + [
            f"z{z}" for z in top_ids
        ]
        train_df = feat.filter((F.col("hour_int") >= F.lit(train_h[0])) & (F.col("hour_int") < F.lit(train_h[1]))).cache()
        val_df = feat.filter((F.col("hour_int") >= F.lit(val_h[0])) & (F.col("hour_int") < F.lit(val_h[1]))).cache()
        test_df = feat.filter((F.col("hour_int") >= F.lit(test_h[0])) & (F.col("hour_int") < F.lit(test_h[1]))).cache()
        manifest["feature_rows"] = {"train": train_df.count(), "val": val_df.count(), "test": test_df.count()}
        manifest["expected_feature_rows"] = {
            "train": len(top_ids) * (train_h[1] - train_h[0]),
            "val": len(top_ids) * (val_h[1] - val_h[0]),
            "test": len(top_ids) * (test_h[1] - test_h[0]),
        }
        assert manifest["feature_rows"] == manifest["expected_feature_rows"], (
            f"划分行数与预期不符: {manifest['feature_rows']} vs {manifest['expected_feature_rows']}"
        )
        manifest["feature_cols"] = feature_cols

    # ---------- 7. 基线 + 直接随机森林 + 残差修正随机森林（滚动一步回测） ----------
    with step("models"):
        rf_params = cfg["rf"]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="pickups",
            numTrees=rf_params["num_trees"],
            maxDepth=rf_params["max_depth"],
            seed=rf_params["seed"],
        )
        model = Pipeline(stages=[assembler, rf]).fit(train_df)

        # 残差模型：同一特征、同一 RF 配置，学习 r = y - lag_168（残差本身可为负，不截断）
        train_r = train_df.withColumn("residual", F.col("pickups") - F.col("lag_168"))
        rf_res = RandomForestRegressor(
            featuresCol="features",
            labelCol="residual",
            numTrees=rf_params["num_trees"],
            maxDepth=rf_params["max_depth"],
            seed=rf_params["seed"],
        )
        model_res = Pipeline(stages=[assembler, rf_res]).fit(train_r)

        def collect_preds(df, model_base, model_residual):
            base = {
                (r["zone_id"], r["hour_int"]): (r["pickups"], r["lag_168"], r["prediction"])
                for r in model_base.transform(df)
                .select("zone_id", "hour_int", "pickups", "lag_168", "prediction")
                .collect()
            }
            res = {}
            if model_residual is not None:
                res = {
                    (r["zone_id"], r["hour_int"]): r["prediction"]
                    for r in model_residual.transform(df).select("zone_id", "hour_int", "prediction").collect()
                }
            out = []
            for (z, h), (y, s, rf_pred) in sorted(base.items()):
                r_hat = res.get((z, h))
                out.append(
                    (
                        int(z),
                        hour_str(h),
                        int(y),
                        float(s),
                        round(float(rf_pred), 2),
                        round(final_from_residual(s, r_hat), 2) if r_hat is not None else None,
                    )
                )
            return out

        val_rows = collect_preds(val_df, model, model_res)
        test_rows = collect_preds(test_df, model, model_res)

    # ---------- 8. 指标与小结果导出 ----------
    with step("metrics_and_export"):
        header = ["zone_id", "hour_ts", "actual", "baseline", "rf", "rf_residual"]
        write_csv(out_dir / "predictions_val.csv", header, val_rows)
        write_csv(out_dir / "predictions_test.csv", header, test_rows)

        models = ["baseline", "rf", "rf_residual"]
        metrics = {
            "params": rf_params,
            "selected_zones": manifest["selected_zones"],
            "overall": {},
            "per_zone_test": {},
        }
        for idx, name in enumerate(models):
            col = 3 + idx
            metrics["overall"][name] = {
                "val": compute_metrics([r[2] for r in val_rows], [r[col] for r in val_rows]),
                "test": compute_metrics([r[2] for r in test_rows], [r[col] for r in test_rows]),
            }
            metrics["per_zone_test"][name] = {
                str(z): compute_metrics(
                    [r[2] for r in test_rows if r[0] == z], [r[col] for r in test_rows if r[0] == z]
                )
                for z in top_ids
            }
        write_json(out_dir / "metrics.json", metrics)
        print("[metrics]", metrics["overall"])

    # ---------- 9. 运行记录 ----------
    manifest["finished_at"] = datetime.now().isoformat(timespec="seconds")
    manifest["outputs"] = sorted(p.name for p in out_dir.iterdir() if p.is_file())
    write_json(out_dir / "run_manifest.json", manifest)
    print("[done] 输出目录:", out_dir)
    spark.stop()


if __name__ == "__main__":
    main()
