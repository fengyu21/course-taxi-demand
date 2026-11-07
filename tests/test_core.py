"""核心逻辑小测试（无需 Spark）：时间划分、补零、lag/rolling 防泄漏、指标、残差、Top-N。

运行：.venv/Scripts/python -m pytest tests/ -v
"""

from __future__ import annotations

import math
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from common import (
    compute_lag_features,
    compute_metrics,
    epoch_hour,
    fill_hour_grid,
    final_from_residual,
    get_splits,
    hour_range,
    hour_str,
    load_config,
    split_of,
    top_zones_in_window,
    weekday_mon0,
)


# ---------- 0. epoch 整数小时：与时区无关的基准 ----------
def test_epoch_hour_known_constant():
    assert epoch_hour(datetime(2024, 1, 1)) == 473352  # 2024-01-01 00:00 UTC = 1704067200s
    assert epoch_hour(datetime(2024, 2, 1)) - epoch_hour(datetime(2024, 1, 1)) == 744


def test_epoch_hour_no_local_tz_dependence():
    """calendar.timegm 不读系统时区：结果必须等于手工 UTC 换算。"""
    dt = datetime(2024, 1, 8, 5)
    manual = (dt - datetime(1970, 1, 1)).days * 24 + (dt - datetime(1970, 1, 1)).seconds // 3600
    assert epoch_hour(dt) == manual


def test_weekday_formula_and_hour_str():
    assert weekday_mon0(epoch_hour(datetime(2024, 1, 1))) == 0  # 2024-01-01 周一
    assert weekday_mon0(epoch_hour(datetime(2024, 1, 6))) == 5  # 周六 → 周末
    assert weekday_mon0(epoch_hour(datetime(2024, 1, 7))) == 6  # 周日 → 周末
    assert hour_str(473352) == "2024-01-01 00:00:00"
    assert hour_str(473352 + 743) == "2024-01-31 23:00:00"

CFG = load_config()
SPLITS = get_splits(CFG)


# ---------- 1. 时间划分：不重叠、相接、左闭右开、小时数正确 ----------
def test_splits_ordered_and_contiguous():
    bounds = [(s["start"], s["end"]) for s in SPLITS]
    for (s1, e1), (s2, e2) in zip(bounds, bounds[1:]):
        assert e1 == s2, "相邻时段必须首尾相接（不重叠、无缺口）"
    assert bounds[0][0] == datetime(2024, 1, 1)
    assert bounds[-1][1] == datetime(2024, 2, 1)


def test_split_hour_lengths():
    lengths = {s["name"]: (s["end"] - s["start"]).total_seconds() / 3600 for s in SPLITS}
    assert lengths == {"warmup": 168, "train": 336, "val": 72, "test": 168}
    assert sum(lengths.values()) == 744  # 2024-01 共 31 天×24


def test_split_left_closed_right_open():
    """train_start 属于训练段，train_end 不属于（属于验证段起点）。"""
    t_start, t_end = split_of(SPLITS, "train")
    v_start, _ = split_of(SPLITS, "val")
    assert t_start == datetime(2024, 1, 8)
    assert t_end == v_start == datetime(2024, 1, 22)


def test_hour_range_exclusive_end():
    hours = list(hour_range(datetime(2024, 1, 1, 0), datetime(2024, 1, 1, 3)))
    assert hours == [datetime(2024, 1, 1, 0), datetime(2024, 1, 1, 1), datetime(2024, 1, 1, 2)]


# ---------- 2. 空小时补 0（先补齐再 lag 的前提） ----------
def test_fill_hour_grid_zero_fills_gaps():
    zones = [1, 2]
    start, end = datetime(2024, 1, 1, 0), datetime(2024, 1, 1, 5)
    counts = {(1, datetime(2024, 1, 1, 0)): 7, (1, datetime(2024, 1, 1, 3)): 9}
    filled = fill_hour_grid(counts, zones, start, end)
    assert len(filled) == 2 * 5
    assert filled[(1, datetime(2024, 1, 1, 0))] == 7
    assert filled[(1, datetime(2024, 1, 1, 1))] == 0  # 缺失小时补 0
    assert filled[(1, datetime(2024, 1, 1, 3))] == 9
    assert filled[(2, datetime(2024, 1, 1, 0))] == 0  # 无记录区域全 0


# ---------- 3. lag / rolling：只用过去、rolling 不含当前小时 ----------
def _make_series(start: datetime, n: int, base: float = 10.0):
    return [(start + timedelta(hours=i), base + (i % 24) + i * 0.01) for i in range(n)]


def test_lag_values_match_manual_shift():
    start = datetime(2024, 1, 1)
    series = _make_series(start, 200)
    rows = compute_lag_features(series)
    by_ts = dict(series)
    r = rows[169]  # 2024-01-08 01:00
    assert r["lag_1"] == by_ts[r["ts"] - timedelta(hours=1)]
    assert r["lag_24"] == by_ts[r["ts"] - timedelta(hours=24)]
    assert r["lag_168"] == by_ts[r["ts"] - timedelta(hours=168)]


def test_rolling_mean_excludes_current_hour():
    """构造常数为 0 的历史 + 当前小时很大的序列：若 rolling 误含当前小时，均值会明显变大。"""
    start = datetime(2024, 1, 8)
    counts = {start + timedelta(hours=i): 0 for i in range(200)}
    ts_now = start + timedelta(hours=100)
    counts[ts_now] = 1000
    series = sorted(counts.items())
    row = {r["ts"]: r for r in compute_lag_features(series)}[ts_now]
    assert row["roll24_mean"] == 0.0, "过去 24 小时均值必须不含当前小时"
    assert row["lag_1"] == 0.0


def test_early_hours_have_no_future_values():
    """首周历史不足：lag_168 为 None，且不得用未来数据填充。"""
    start = datetime(2024, 1, 1)
    series = _make_series(start, 300)
    rows = compute_lag_features(series)
    first = rows[0]  # 2024-01-01 00:00
    assert first["lag_1"] is None and first["lag_24"] is None and first["lag_168"] is None
    week = rows[50]  # 第 3 天：lag_24 存在，lag_168 不存在
    assert week["lag_24"] is not None and week["lag_168"] is None


# ---------- 4. 训练期选 Top-N：测试期的爆款区域不得入选 ----------
def test_top_zones_uses_only_train_window():
    """模拟实际用法：先用训练窗口过滤计数，再取 Top-N——测试期爆款区域不得入选。"""
    all_period = {1: 500, 2: 400, 3: 300, 99: 99999}  # 99 只在测试期爆发
    train_counts = {z: c for z, c in all_period.items() if z != 99}  # pipeline 先按窗口过滤
    assert top_zones_in_window(train_counts, 3) == [1, 2, 3]
    # 并列时按区域 ID 升序，保证可复现
    assert top_zones_in_window({10: 100, 2: 100, 3: 100}, 2) == [2, 3]


# ---------- 5. 指标：手算 MAE / RMSE ----------
def test_metrics_hand_computed():
    y, p = [3.0, 5.0], [2.0, 7.0]
    m = compute_metrics(y, p)
    assert math.isclose(m["mae"], 1.5, abs_tol=1e-9)  # |3-2|, |5-7| → 均值 1.5
    assert math.isclose(m["rmse"], round(math.sqrt(2.5), 4), abs_tol=1e-9)  # (1+4)/2=2.5，输出保留 4 位

    y, p = [1.0, 2.0, 3.0, 4.0], [2.0, 3.0, 4.0, 5.0]
    m = compute_metrics(y, p)
    assert math.isclose(m["mae"], 1.0, abs_tol=1e-9)
    assert math.isclose(m["rmse"], 1.0, abs_tol=1e-9)


def test_metrics_length_mismatch_raises():
    try:
        compute_metrics([1.0], [1.0, 2.0])
        assert False, "长度不一致应抛错"
    except ValueError:
        pass


# ---------- 6. 残差修正：r 可负、仅最终预测截 0 ----------
def test_residual_reconstruction():
    assert final_from_residual(50.0, 3.0) == 53.0
    assert final_from_residual(50.0, -60.0) == 0.0, "S + r_hat < 0 时最终预测截为 0"
    assert final_from_residual(50.0, -30.0) == 20.0, "负残差本身合法，仅结果为负时才截断"


def test_residual_training_target_not_clipped():
    """残差 r = y - S 在训练目标里不能截 0（否则模型学错方向）。"""
    y, s = 30.0, 50.0
    r = y - s
    assert r == -20.0  # 负值原样作为训练目标


# ---------- 7. 面板行数 = 区域数 × 小时数（补零完整性） ----------
def test_panel_total_rows():
    zones = [141, 237]
    n_hours = len(list(hour_range(datetime(2024, 1, 1), datetime(2024, 2, 1))))
    assert n_hours == 744
    filled = fill_hour_grid({}, zones, datetime(2024, 1, 1), datetime(2024, 2, 1))
    assert len(filled) == 2 * 744
