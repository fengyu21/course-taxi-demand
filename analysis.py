"""上传 CSV 的验证、清洗与统计（纯 pandas，前端即时分析用）。

与 pipeline.py 的 Spark 离线流程保持同一套口径：
- 只按必要字段过滤需求标签（上车时间可解析 + 区域有效）；
- 负费用、乘客缺失等可疑记录不删；
- Unknown (264) / Outside of NYC (265) 不算有效区域；
- 时间一律按数据里的墙上时间处理，不做时区换算。

上传文件是几十 MB 级的小样本，pandas 足够；完整数据仍走 HDFS + Spark。
"""

from __future__ import annotations

import io

import pandas as pd

REQUIRED_FIELDS = ("tpep_pickup_datetime", "PULocationID")
TRIP_FIELDS = ("tpep_dropoff_datetime", "trip_distance", "fare_amount", "total_amount")
UNKNOWN_ZONE_IDS = {264, 265}
MAX_UPLOAD_MB = 50

DISTANCE_BUCKETS = [0, 1, 2, 3, 5, 8, 12, 20, 100]
DURATION_BUCKETS = [0, 5, 10, 15, 30, 60, 120, 180]


def validate_size(size_bytes: int) -> tuple[bool, str]:
    if size_bytes == 0:
        return False, "文件是空的"
    if size_bytes > MAX_UPLOAD_MB * 1024 * 1024:
        return False, f"文件超过 {MAX_UPLOAD_MB}MB 上限（{size_bytes / 1024 / 1024:.1f}MB），请裁剪后再传"
    return True, f"{size_bytes / 1024 / 1024:.2f}MB"


def read_csv_bytes(data: bytes) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(data))


def validate_fields(df: pd.DataFrame) -> tuple[bool, str]:
    cols = {c.strip() for c in df.columns}
    missing = [f for f in REQUIRED_FIELDS if f not in cols]
    if missing:
        return False, "缺少必要字段：" + "、".join(missing)
    if len(df) == 0:
        return False, "文件没有数据行"
    return True, "字段检查通过"


def clean_orders(df: pd.DataFrame, valid_zones: set[int]) -> tuple[pd.DataFrame, dict]:
    """与 Spark 离线清洗同口径：时间可解析 + 区域有效；可疑费用/乘客字段不删。"""
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    total = len(df)
    df["pickup_ts"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
    n_bad_time = int(df["pickup_ts"].isna().sum())
    df = df.dropna(subset=["pickup_ts"])
    df["zone_id"] = pd.to_numeric(df["PULocationID"], errors="coerce")
    df = df.dropna(subset=["zone_id"])
    df["zone_id"] = df["zone_id"].astype("int64")
    n_bad_zone = int((~df["zone_id"].isin(valid_zones)).sum())
    df = df[df["zone_id"].isin(valid_zones)]
    df["hour_ts"] = df["pickup_ts"].dt.floor("h")
    quality = {
        "total": total,
        "bad_time": n_bad_time,
        "bad_zone": n_bad_zone,
        "valid": len(df),
        "tmin": df["pickup_ts"].min(),
        "tmax": df["pickup_ts"].max(),
        "n_zones": int(df["zone_id"].nunique()),
    }
    return df, quality


def summarize(df: pd.DataFrame) -> dict:
    """聚合出与离线结果同构的小统计表：每日 / 小时 / 星期×小时 / 区域榜。"""
    n_days = max(1, (df["hour_ts"].max().normalize() - df["hour_ts"].min().normalize()).days + 1)

    daily = (
        df.groupby(df["hour_ts"].dt.normalize())["pickup_ts"]
        .count()
        .rename("pickups")
        .reset_index()
        .rename(columns={"hour_ts": "date"})
    )
    daily["date"] = daily["date"].dt.strftime("%Y-%m-%d")

    hourly = (
        df.groupby(df["pickup_ts"].dt.hour)["pickup_ts"]
        .count()
        .rename("pickups_total")
        .reset_index()
        .rename(columns={"pickup_ts": "hour"})
    )
    hourly["pickups_avg_per_day"] = (hourly["pickups_total"] / n_days).round(1)

    wk = df.assign(weekday=df["pickup_ts"].dt.weekday, hour=df["pickup_ts"].dt.hour)
    wd = wk.groupby(["weekday", "hour"])["pickup_ts"].count().rename("pickups_total").reset_index()
    hours = pd.date_range(df["hour_ts"].min(), df["hour_ts"].max() + pd.Timedelta(hours=1), freq="h")
    occ = pd.Series(1, index=pd.DatetimeIndex(hours)).groupby([hours.weekday, hours.hour]).size()
    occ.name = "occurrences"
    wd = wd.join(occ, on=["weekday", "hour"])
    wd["pickups_avg"] = (wd["pickups_total"] / wd["occurrences"]).round(2)

    zone_ranking = (
        df.groupby("zone_id")["pickup_ts"]
        .count()
        .rename("pickups")
        .reset_index()
        .sort_values(["pickups", "zone_id"], ascending=[False, True])
        .reset_index(drop=True)
    )

    out = {
        "total": len(df),
        "n_days": n_days,
        "n_zones": int(df["zone_id"].nunique()),
        "tmin": df["pickup_ts"].min(),
        "tmax": df["pickup_ts"].max(),
        "avg_per_hour": round(len(df) / (n_days * 24), 1),
        "daily": daily,
        "hourly": hourly,
        "weekday_hour": wd,
        "zone_ranking": zone_ranking,
    }
    if set(TRIP_FIELDS).issubset(df.columns):
        tv = df.copy()
        tv["dropoff_ts"] = pd.to_datetime(tv["tpep_dropoff_datetime"], errors="coerce")
        tv["dur_min"] = (tv["dropoff_ts"] - tv["pickup_ts"]).dt.total_seconds() / 60
        tv = tv[
            (tv["dur_min"] > 0) & (tv["dur_min"] <= 180) & (tv["trip_distance"] > 0) & (tv["trip_distance"] <= 100)
        ]
        d = tv.groupby(pd.cut(tv["trip_distance"], DISTANCE_BUCKETS, right=False), observed=True)["trip_distance"].count()
        t = tv.groupby(pd.cut(tv["dur_min"], DURATION_BUCKETS, right=False), observed=True)["dur_min"].count()
        out["distance_dist"] = pd.DataFrame({"bucket": [f"{i.left:g}-{i.right:g}" for i in d.index], "rows": d.values})
        out["duration_dist"] = pd.DataFrame({"bucket": [f"{i.left:g}-{i.right:g}" for i in t.index], "rows": t.values})
    return out


def zone_hourly_series(df: pd.DataFrame, zone_id: int) -> pd.DataFrame:
    sub = df[df["zone_id"] == zone_id]
    series = sub.groupby("hour_ts")["pickup_ts"].count().rename("pickups")
    full = pd.date_range(series.index.min(), series.index.max(), freq="h")
    series = series.reindex(full, fill_value=0).rename_axis("hour_ts").reset_index()
    return series
