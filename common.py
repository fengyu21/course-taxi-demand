"""共享常量与纯 Python 核心逻辑：pipeline（Spark 端）、app（看板端）、tests 共用。

刻意不依赖 pandas / numpy / pyspark，VM 上仅 python3 也能复用。
"""

from __future__ import annotations

import calendar
import csv
import json
import math
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
EPOCH = datetime(1970, 1, 1)

# TLC 区域维表中的特殊 ID：264=Unknown，265=Outside of NYC（不计入有效区域）
UNKNOWN_ZONE_IDS = {264, 265}


def epoch_hour(dt: datetime) -> int:
    """将墙上时间换算为 UTC 基准的整数小时。

    数据列为 timestamp_ntz（纽约本地墙上时间）；所有比较统一按 UTC 墙钟换算成整数，
    与运行机器或驱动 JVM 的本地时区无关（calendar.timegm 不读取系统时区）。
    """
    return int(calendar.timegm(dt.timetuple())) // 3600


def hour_str(h: int) -> str:
    """epoch 整数小时 → 墙上时间字符串（显示用，UTC 基准）。"""
    return (EPOCH + timedelta(hours=int(h))).strftime("%Y-%m-%d %H:%M:%S")


def weekday_mon0(h: int) -> int:
    """epoch 整数小时 → 星期几（0=周一..6=周日）。1970-01-01 是周四。"""
    return (int(h) // 24 + 3) % 7


def load_config(path: str | Path | None = None) -> dict:
    p = Path(path) if path else PROJECT_ROOT / "config.json"
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def parse_ts(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def get_splits(cfg: dict) -> list[dict]:
    """从 config 读取时间段（左闭右开 [start, end)），并校验递增不重叠。

    返回 warmup / train / val / test 四段，相邻段首尾相接。
    """
    t = cfg["time"]
    data_start, data_end = parse_ts(t["data_start"]), parse_ts(t["data_end"])
    warmup_end, train_end, val_end = (
        parse_ts(t["warmup_end"]),
        parse_ts(t["train_end"]),
        parse_ts(t["val_end"]),
    )
    if not (data_start < warmup_end < train_end < val_end < data_end):
        raise ValueError("时间划分必须递增且不重叠: data_start < warmup_end < train_end < val_end < data_end")
    return [
        {"name": "warmup", "start": data_start, "end": warmup_end},
        {"name": "train", "start": warmup_end, "end": train_end},
        {"name": "val", "start": train_end, "end": val_end},
        {"name": "test", "start": val_end, "end": data_end},
    ]


def split_of(splits: list[dict], name: str) -> tuple[datetime, datetime]:
    for s in splits:
        if s["name"] == name:
            return s["start"], s["end"]
    raise KeyError(name)


def hour_range(start: datetime, end: datetime):
    """生成 [start, end) 内的连续小时时间戳（补齐空小时用）。"""
    t = start
    while t < end:
        yield t
        t += timedelta(hours=1)


def fill_hour_grid(counts: dict, zones, start: datetime, end: datetime) -> dict:
    """参考实现（测试/对照用）：为 zones × 连续小时网格补 0。

    counts: {(zone_id, datetime): 数值}。仅当源数据完整可读时才应调用；
    文件缺失/读取失败时不得把 0 当作真实观测。
    """
    out = {}
    for z in zones:
        for ts in hour_range(start, end):
            out[(z, ts)] = counts.get((z, ts), 0)
    return out


def compute_lag_features(series: list) -> list[dict]:
    """参考实现（测试/对照用）：单个区域按小时序列的滚动特征。

    series: [(ts, count)]，允许有缺口（缺口处视为无观测）。
    返回按 ts 升序的 [{ts, count, lag_1, lag_24, lag_168, roll24_mean}]，
    roll24_mean 为过去 24 小时（不含当前小时）均值，历史不足时相应字段为 None。
    """
    by_ts = dict(series)
    rows = []
    for ts, c in sorted(series, key=lambda x: x[0]):
        prev24 = [by_ts.get(ts - timedelta(hours=k)) for k in range(1, 25)]
        prev24 = [v for v in prev24 if v is not None]
        rows.append(
            {
                "ts": ts,
                "count": c,
                "lag_1": by_ts.get(ts - timedelta(hours=1)),
                "lag_24": by_ts.get(ts - timedelta(hours=24)),
                "lag_168": by_ts.get(ts - timedelta(hours=168)),
                "roll24_mean": (sum(prev24) / len(prev24)) if prev24 else None,
            }
        )
    return rows


def compute_metrics(y_true, y_pred) -> dict:
    """MAE / RMSE（pipeline、app、tests 共用，口径唯一）。"""
    if len(y_true) != len(y_pred) or not y_true:
        raise ValueError("y_true 与 y_pred 长度一致且非空")
    diffs = [float(a) - float(b) for a, b in zip(y_true, y_pred)]
    mae = sum(abs(d) for d in diffs) / len(diffs)
    rmse = math.sqrt(sum(d * d for d in diffs) / len(diffs))
    return {"mae": round(mae, 4), "rmse": round(rmse, 4)}


def final_from_residual(s: float, r_hat: float) -> float:
    """残差修正的最终预测 = max(0, S + r_hat)，S=lag_168。

    注意：残差 r 本身可为负；只在重建最终预测时截断负值。
    """
    return max(0.0, float(s) + float(r_hat))


def top_zones_in_window(zone_counts: dict, n: int) -> list:
    """参考实现（测试用）：按窗口内上车量取前 n 个区域（并列时按区域 ID 升序）。"""
    ranked = sorted(zone_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return [z for z, _ in ranked[:n]]


def write_csv(path: str | Path, header: list, rows: list) -> None:
    """把 collect 后的小结果写成 CSV（pipeline 在 VM 上无需 pandas）。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def write_json(path: str | Path, obj) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
