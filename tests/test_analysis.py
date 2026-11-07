"""analysis.py 的单元测试：上传 CSV 的验证、清洗、统计口径与离线流程一致。"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import analysis  # noqa: E402


def make_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


BASE = {
    "tpep_pickup_datetime": "2024-03-05 08:30:00",
    "PULocationID": 132,
    "tpep_dropoff_datetime": "2024-03-05 09:00:00",
    "trip_distance": 3.5,
    "fare_amount": 20.0,
    "total_amount": 25.0,
}


def test_validate_fields_missing_required():
    df = make_df([{"tpep_pickup_datetime": "2024-03-05 08:00:00"}])
    ok, msg = analysis.validate_fields(df)
    assert not ok and "PULocationID" in msg


def test_validate_size_empty_and_too_large():
    ok, msg = analysis.validate_size(0)
    assert not ok and "空" in msg
    ok, msg = analysis.validate_size(60 * 1024 * 1024)
    assert not ok and "50MB" in msg
    ok, msg = analysis.validate_size(1024)
    assert ok


def test_clean_drops_bad_time_and_invalid_zone():
    df = make_df([
        BASE,
        {**BASE, "tpep_pickup_datetime": "not-a-time"},
        {**BASE, "PULocationID": 264},   # Unknown
        {**BASE, "PULocationID": 999},   # 不在维表
    ])
    valid = set(range(1, 266)) - analysis.UNKNOWN_ZONE_IDS
    clean, q = analysis.clean_orders(df, valid)
    assert len(clean) == 1
    assert q["total"] == 4 and q["bad_time"] == 1 and q["bad_zone"] == 2 and q["valid"] == 1
    assert q["n_zones"] == 1


def test_clean_keeps_negative_fare_rows():
    """与离线口径一致：负费用也是真实需求，不删。"""
    df = make_df([BASE, {**BASE, "fare_amount": -10.0}])
    valid = set(range(1, 266)) - analysis.UNKNOWN_ZONE_IDS
    clean, _ = analysis.clean_orders(df, valid)
    assert len(clean) == 2


def test_summarize_daily_hourly_weekday():
    rows = []
    for h, n in [(8, 3), (18, 5)]:
        for k in range(n):
            rows.append({**BASE, "tpep_pickup_datetime": f"2024-03-05 {h:02d}:{k:02d}:00"})
    df = make_df(rows)
    valid = set(range(1, 266)) - analysis.UNKNOWN_ZONE_IDS
    clean, _ = analysis.clean_orders(df, valid)
    s = analysis.summarize(clean)
    assert s["total"] == 8
    assert s["n_zones"] == 1
    assert set(s["daily"]["date"]).__len__() == 1
    assert s["hourly"].set_index("hour").loc[18, "pickups_total"] == 5
    assert (s["weekday_hour"]["occurrences"] == 1).all()  # 单日数据，每个小时只出现一次
    assert s["weekday_hour"].set_index("hour").loc[18, "pickups_avg"] == 5.0


def test_zone_hourly_series_fills_gaps():
    rows = [
        {**BASE, "tpep_pickup_datetime": "2024-03-05 08:00:00"},
        {**BASE, "tpep_pickup_datetime": "2024-03-05 10:00:00"},
    ]
    df = make_df(rows)
    valid = set(range(1, 266)) - analysis.UNKNOWN_ZONE_IDS
    clean, _ = analysis.clean_orders(df, valid)
    series = analysis.zone_hourly_series(clean, 132)
    assert len(series) == 3  # 08/09/10 点连续，09 点补 0
    assert series.set_index("hour_ts").loc[pd.Timestamp("2024-03-05 09:00:00"), "pickups"] == 0


def test_trip_histograms_only_when_fields_present():
    df = make_df([BASE])
    valid = set(range(1, 266)) - analysis.UNKNOWN_ZONE_IDS
    clean, _ = analysis.clean_orders(df, valid)
    s = analysis.summarize(clean)
    assert "distance_dist" in s and "duration_dist" in s
    df2 = make_df([{k: v for k, v in BASE.items() if k in analysis.REQUIRED_FIELDS}])
    clean2, _ = analysis.clean_orders(df2, valid)
    s2 = analysis.summarize(clean2)
    assert "distance_dist" not in s2
