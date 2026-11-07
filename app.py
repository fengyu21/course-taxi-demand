"""纽约出租车区域需求分析与预测系统（Streamlit 前端）。

两种数据来源：
- 默认模式（正式演示）：直接读取 pipeline.py 在 Spark 上产出的聚合结果（outputs/）；
- 上传模式：上传小型 CSV（≤50MB，必填 tpep_pickup_datetime / PULocationID），
  用 analysis.py 的同一套清洗与统计口径即时分析。

需求预测与高需求提示基于默认数据的离线结果（滚动一步历史回测，非实时）。

运行：streamlit run app.py [-- --outputs outputs_vm]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))
from common import load_config  # noqa: E402
import analysis  # noqa: E402

CFG = load_config()
WEEKDAY_NAMES = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
COLOR_MAIN = "#E63946"
COLOR_ACCENT = "#457B9D"

st.set_page_config(page_title="纽约出租车区域需求分析与预测系统", page_icon="🚕", layout="wide")

CUSTOM_CSS = """
<style>
    div[data-testid="stMetric"] {
        background: #f8f9fa; border: 1px solid #e9ecef;
        border-radius: 10px; padding: 14px 16px;
    }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

PLOTLY_LAYOUT = {
    "template": "plotly_white",
    "margin": dict(l=40, r=20, t=40, b=40),
    "font": dict(family="Microsoft YaHei, SimHei, sans-serif", size=13),
}


def style_fig(fig, height=380):
    fig.update_layout(**PLOTLY_LAYOUT, height=height)
    return fig


# ---------------- 离线小结果（默认模式） ----------------
def parse_args_streamlit():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outputs", default=str(PROJECT_ROOT / CFG["output_dir"]))
    args, _ = ap.parse_known_args()
    return args


ARGS = parse_args_streamlit()
OUT = ARGS.outputs


@st.cache_data
def load_csv(name: str, outputs_dir: str) -> pd.DataFrame | None:
    p = Path(outputs_dir) / name
    return pd.read_csv(p) if p.exists() else None


@st.cache_data
def load_json(name: str, outputs_dir: str):
    p = Path(outputs_dir) / name
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def missing_banner(names: list[str]):
    if names:
        st.error(
            "缺少运行结果文件：**" + "、".join(names) + "**。请先运行 pipeline（`spark-submit pipeline.py --input data/raw`），完成后刷新。"
        )


def load_bucket_csv(name: str, old_col: str, outputs_dir: str) -> pd.DataFrame | None:
    df = load_csv(name, outputs_dir)
    if df is None:
        return None
    return df.rename(columns={old_col: "bucket"})


daily = load_csv("daily_stats.csv", OUT)
hourly = load_csv("hourly_stats.csv", OUT)
weekday_hour = load_csv("weekday_hour.csv", OUT)
zone_stats = load_csv("zone_stats.csv", OUT)
borough = load_csv("borough_stats.csv", OUT)
panel = load_csv("panel_10zones.csv", OUT)
preds = load_csv("predictions_test.csv", OUT)
metrics = load_json("metrics.json", OUT)
quality = load_json("data_quality.json", OUT)


@st.cache_data
def zone_lookup() -> tuple[set[int], dict[int, str]]:
    """有效区域集合与名称表（来自 TLC 维表；264/265 为 Unknown / Outside of NYC）。"""
    p = PROJECT_ROOT / "data" / "raw" / "taxi_zone_lookup.csv"
    valid: set[int] = set(range(1, 266)) - analysis.UNKNOWN_ZONE_IDS
    names: dict[int, str] = {}
    if p.exists():
        lk = pd.read_csv(p)
        valid = {int(i) for i in lk["LocationID"]} - analysis.UNKNOWN_ZONE_IDS
        names = {int(r.LocationID): str(r.Zone) for r in lk.itertuples()}
    return valid, names


VALID_ZONES, ZONE_NAMES = zone_lookup()


def zone_label(zid, names: dict | None = None) -> str:
    nm = (names or ZONE_NAMES).get(int(zid))
    return f"{zid} · {nm}" if nm else f"Zone {zid}"


# ---------------- 高需求判定 ----------------
@st.cache_data
def zone_p90(outputs_dir: str) -> dict[int, float]:
    p = Path(outputs_dir) / "panel_10zones.csv"
    if not p.exists():
        return {}
    df = pd.read_csv(p)
    return {int(z): float(v) for z, v in df.groupby("zone_id")["pickups"].quantile(0.9).items()}


P90 = zone_p90(OUT)


def demand_status(rf_pred: float, p90: float, baseline: float | None) -> str:
    """高需求 = 超过该区域历史 90% 分位，或比上周同一小时高出 30%。"""
    if p90 and rf_pred > p90:
        return "高需求"
    if baseline and rf_pred > baseline * 1.3:
        return "偏高"
    return "普通"


# ---------------- 两种数据来源 ----------------
@st.cache_data
def load_upload(data: bytes, name: str):
    ok_size, msg_size = analysis.validate_size(len(data))
    if not ok_size:
        return False, msg_size, None
    try:
        df = analysis.read_csv_bytes(data)
    except Exception as e:  # noqa: BLE001
        return False, f"CSV 解析失败：{e}", None
    ok, msg = analysis.validate_fields(df)
    if not ok:
        return False, msg, None
    clean, quality = analysis.clean_orders(df, VALID_ZONES)
    if len(clean) == 0:
        return False, "清洗后没有有效数据行（时间或区域都不对？）", None
    return True, "检查通过", {"clean": clean, "quality": quality, "name": name}


with st.sidebar:
    st.markdown("**数据来源**")
    source = st.radio("数据来源", ["使用系统默认数据", "上传 CSV 文件"], label_visibility="collapsed")
    upload_ctx = None
    if source == "上传 CSV 文件":
        up = st.file_uploader(f"上传小型 CSV（≤{analysis.MAX_UPLOAD_MB}MB）", type="csv")
        if up is not None:
            ok, msg, payload = load_upload(up.getvalue(), up.name)
            if ok:
                upload_ctx = analysis.summarize(payload["clean"])
                upload_ctx["clean_df"] = payload["clean"]
                upload_ctx["name"] = payload["name"]
                upload_ctx["quality"] = payload["quality"]
                st.sidebar.success(f"{up.name}：{msg}")
            else:
                st.sidebar.error(msg)
        else:
            st.sidebar.info("等待上传——必填列为 tpep_pickup_datetime、PULocationID")

    st.divider()
    view = st.radio(
        "页面",
        ["① 数据总览", "② 出行规律分析", "③ 需求预测", "④ 高需求区域提示"],
        label_visibility="collapsed",
    )

upload_mode = source == "上传 CSV 文件" and upload_ctx is not None
offline_ok = all(x is not None for x in (daily, hourly, weekday_hour, zone_stats, quality, panel, preds, metrics))

if upload_mode:
    q = upload_ctx["quality"]
    ctx = {
        "name": upload_ctx["name"],
        "total": q["total"],
        "valid": q["valid"],
        "date_range": (q["tmin"].strftime("%Y-%m-%d %H:%M"), q["tmax"].strftime("%Y-%m-%d %H:%M")),
        "n_zones": upload_ctx["n_zones"],
        "avg_per_hour": upload_ctx["avg_per_hour"],
        "daily": upload_ctx["daily"],
        "hourly": upload_ctx["hourly"],
        "weekday_hour": upload_ctx["weekday_hour"],
        "zone_top": upload_ctx["zone_ranking"].head(20).assign(
            zone=lambda d: d["zone_id"].map(lambda z: ZONE_NAMES.get(int(z), f"Zone {z}"))
        ),
        "get_zone_curve": lambda zid: analysis.zone_hourly_series(upload_ctx["clean_df"], zid),
        "distance_dist": upload_ctx.get("distance_dist"),
        "duration_dist": upload_ctx.get("duration_dist"),
    }
else:
    # 默认模式：把离线小结果装进与上传模式同构的上下文
    n_days_default = 31
    ctx = None
    if offline_ok:
        q = quality["funnel"]
        ctx = {
            "name": "yellow_tripdata_2024-01.parquet（TLC 官方）",
            "total": q["total_rows"],
            "valid": q["valid_core_rows"],
            "date_range": ("2024-01-01 00:00", "2024-01-31 23:59"),
            "n_zones": q.get("n_active_zones"),
            "avg_per_hour": q["valid_core_rows"] / (n_days_default * 24),
            "daily": daily,
            "hourly": hourly,
            "weekday_hour": weekday_hour,
            "zone_top": zone_stats,
            "get_zone_curve": None,  # 默认模式用 panel
            "distance_dist": load_bucket_csv("distance_dist.csv", "bucket_miles", OUT),
            "duration_dist": load_bucket_csv("duration_dist.csv", "bucket_minutes", OUT),
        }

st.title("纽约出租车区域需求分析与预测系统")
if upload_mode:
    st.caption(
        f"当前数据：**{ctx['name']}** · {ctx['valid']:,} 行有效订单 · {ctx['date_range'][0]} ~ {ctx['date_range'][1]}"
    )
else:
    st.caption("数据来源：NYC TLC Yellow Taxi · 当前数据：2024 年 1 月 · 历史回测，非实时")


# ---------------- ① 数据总览 ----------------
def view_overview():
    if upload_mode:
        rows = [
            ("文件名", ctx["name"]),
            ("原始行数", f"{ctx['total']:,}"),
            ("有效订单数", f"{ctx['valid']:,}"),
            ("时间无法解析", f"{ctx['quality']['bad_time']:,} 行（已剔除）"),
            ("区域无效/未知", f"{ctx['quality']['bad_zone']:,} 行（已剔除）"),
            ("字段检查", "通过"),
        ]
        st.markdown("**上传文件检查结果**")
    else:
        if quality is None:
            missing_banner(["data_quality.json"])
            return
        q = quality["funnel"]
        rows = [
            ("文件名", "yellow_tripdata_2024-01.parquet（TLC 官方）"),
            ("原始行数", f"{q['total_rows']:,}"),
            ("有效订单数", f"{q['valid_core_rows']:,}"),
            ("时间无法解析 / 越界", f"{q['n_missing_pickup_time'] + q['n_pickup_out_of_range']:,} 行（已剔除）"),
            ("区域无效/未知", f"{q['n_zone_not_in_lookup'] + q['n_zone_unknown_or_outside']:,} 行（已剔除）"),
            ("字段检查", "通过"),
        ]
        st.markdown("**默认数据检查结果**")
    st.dataframe(pd.DataFrame(rows, columns=["检查项", "结果"]), use_container_width=True, hide_index=True)

    if ctx is None:
        missing_banner(["daily_stats.csv"])
        return

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("总订单数", f"{ctx['total']:,}")
    c2.metric("有效订单数", f"{ctx['valid']:,}")
    try:
        t0, t1 = pd.to_datetime(ctx["date_range"][0]), pd.to_datetime(ctx["date_range"][1])
        span = f"{t0.month}/{t0.day} ~ {t1.month}/{t1.day}"
    except Exception:
        span = str(ctx["date_range"][0])[:10]
    c3.metric("覆盖日期", span)
    c4.metric("覆盖区域数", f"{ctx['n_zones']}" if ctx["n_zones"] else "—")
    c5.metric("平均每小时订单量", f"{ctx['avg_per_hour']:,.0f}")

    st.subheader("每日订单量趋势")
    d = ctx["daily"].copy()
    d["date"] = pd.to_datetime(d["date"])
    d["weekday"] = d["date"].dt.weekday
    d["类型"] = d["weekday"].ge(5).map({True: "周末", False: "工作日"})
    fig = px.bar(
        d, x="date", y="pickups", color="类型",
        color_discrete_map={"工作日": COLOR_ACCENT, "周末": COLOR_MAIN},
        labels={"date": "日期", "pickups": "订单数", "类型": ""},
    )
    st.plotly_chart(style_fig(fig), use_container_width=True)
    st.download_button(
        "下载每日趋势 CSV", ctx["daily"].to_csv(index=False).encode("utf-8-sig"), "daily_trend.csv", "text/csv"
    )


# ---------------- ② 出行规律分析 ----------------
def view_patterns():
    if ctx is None:
        return
    wh = ctx["weekday_hour"]
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("小时分布（日均）")
        fig = px.bar(ctx["hourly"], x="hour", y="pickups_avg_per_day", labels={"hour": "小时", "pickups_avg_per_day": "日均订单量"})
        st.plotly_chart(style_fig(fig), use_container_width=True)
    with c2:
        st.subheader("星期 × 小时 热力图（平均订单量）")
        hm = wh.pivot(index="weekday", columns="hour", values="pickups_avg")
        fig = go.Figure(
            go.Heatmap(
                z=hm.values, x=[f"{h}时" for h in hm.columns], y=[WEEKDAY_NAMES[i] for i in hm.index],
                colorscale="YlOrRd", xgap=2, ygap=2,
                hovertemplate="%{y} %{x}：平均 %{z:.0f} 单/小时<extra></extra>",
                colorbar=dict(title=dict(text="单/时", side="top"), thickness=12, len=0.6, outlinewidth=0),
            )
        )
        fig.update_xaxes(tickmode="array", tickvals=[f"{h}时" for h in range(0, 24, 2)], tickangle=0, tickfont=dict(size=11), showgrid=False)
        fig.update_yaxes(autorange="reversed", tickfont=dict(size=12), showgrid=False)
        st.plotly_chart(style_fig(fig, 400), use_container_width=True)

    st.subheader("工作日 vs 周末（小时平均曲线）")
    w = wh.copy()
    w["类型"] = w["weekday"].ge(5).map({True: "周末", False: "工作日"})
    agg = w.groupby(["类型", "hour"], as_index=False).agg(pickups_sum=("pickups_total", "sum"), occ_sum=("occurrences", "sum"))
    agg["平均订单量"] = agg["pickups_sum"] / agg["occ_sum"]
    fig = px.line(
        agg, x="hour", y="平均订单量", color="类型", markers=True,
        color_discrete_map={"工作日": COLOR_ACCENT, "周末": COLOR_MAIN}, labels={"hour": "小时"},
    )
    st.plotly_chart(style_fig(fig), use_container_width=True)

    zt = ctx["zone_top"]
    st.subheader("订单量最高的 Top-10 区域")
    fig = px.bar(
        zt.head(10).sort_values("pickups"), y="zone", x="pickups", orientation="h",
        labels={"zone": "区域", "pickups": "订单量"}, height=420,
    )
    st.plotly_chart(style_fig(fig, 420), use_container_width=True)

    st.subheader("区域订单曲线")
    if upload_mode:
        options = [(int(r.zone_id), r.zone) for r in zt.head(10).itertuples()]
        pick = st.multiselect("选择区域（默认前 3）", options,
                              default=options[:3], format_func=lambda t: f"{t[0]} · {t[1]}")
        if pick:
            curves = pd.concat([ctx["get_zone_curve"](z).assign(区域=nm) for z, nm in pick])
            fig = px.line(curves, x="hour_ts", y="pickups", color="区域", labels={"hour_ts": "小时", "pickups": "订单量"})
            st.plotly_chart(style_fig(fig, 420), use_container_width=True)
            st.download_button("下载当前筛选数据 CSV", curves.to_csv(index=False).encode("utf-8-sig"), "zone_curves.csv", "text/csv")
    else:
        if panel is None:
            missing_banner(["panel_10zones.csv"])
            return
        zones_avail = panel[["zone_id", "zone"]].drop_duplicates().sort_values("zone_id")
        default_ids = list(zones_avail["zone_id"])[:3]
        pick = st.multiselect(
            "选择区域（默认前 3）", zones_avail.itertuples(index=False),
            default=tuple(t for t in zones_avail.itertuples(index=False) if t.zone_id in default_ids),
            format_func=lambda t: f"{t.zone_id} · {t.zone}",
        )
        if pick:
            ids = [t.zone_id for t in pick]
            sub = panel[panel["zone_id"].isin(ids)].copy()
            sub["hour_ts"] = pd.to_datetime(sub["hour_ts"])
            fig = px.line(sub, x="hour_ts", y="pickups", color="zone", labels={"hour_ts": "小时", "pickups": "订单量", "zone": "区域"})
            st.plotly_chart(style_fig(fig, 420), use_container_width=True)
            st.download_button("下载当前筛选数据 CSV", sub.to_csv(index=False).encode("utf-8-sig"), "zone_curves.csv", "text/csv")

    dist_df = ctx.get("distance_dist")
    dur_df = ctx.get("duration_dist")
    if dist_df is not None and dur_df is not None:
        c3, c4 = st.columns(2)
        with c3:
            st.subheader("行程距离分布")
            fig = px.bar(dist_df, x="bucket", y="rows", labels={"bucket": "距离（英里，左闭右开）", "rows": "行程数"})
            st.plotly_chart(style_fig(fig), use_container_width=True)
        with c4:
            st.subheader("行程时长分布")
            fig = px.bar(dur_df, x="bucket", y="rows", labels={"bucket": "时长（分钟，左闭右开）", "rows": "行程数"})
            st.plotly_chart(style_fig(fig), use_container_width=True)


# ---------------- ③ 需求预测 ----------------
def view_prediction():
    if preds is None or metrics is None or panel is None:
        missing_banner([n for n, d in [("predictions_test.csv", preds), ("metrics.json", metrics), ("panel_10zones.csv", panel)] if d is None])
        return
    if upload_mode:
        st.info("预测基于系统默认数据（2024-01 离线结果）——上传的数据不参与建模。")

    st.caption("拿 1 月最后一周（1/25–2/1）当考卷——每次只预测下一个小时，且只准用这小时之前的数据。历史回测，非实时。")

    sel = metrics["selected_zones"]
    zone_opts = [(z["zone_id"], z["zone"]) for z in sel]
    zid, zname = st.selectbox("上车区域", zone_opts, format_func=lambda t: f"{t[0]} · {t[1]}")

    sub_all = preds[preds["zone_id"] == zid].copy()
    sub_all["hour_dt"] = pd.to_datetime(sub_all["hour_ts"])
    hour_opts = sorted(sub_all["hour_dt"].unique())
    tot = sub_all.groupby("hour_dt")["actual"].sum()
    peak_idx = int(list(tot.index).index(tot.idxmax()))
    h_pick = st.slider("选择时间（默认需求最高的一小时）", 0, len(hour_opts) - 1, peak_idx, format="")
    sel_ts = pd.Timestamp(hour_opts[h_pick])

    row = sub_all[sub_all["hour_dt"] == sel_ts].iloc[0]
    p90 = P90.get(int(zid))
    status = demand_status(float(row["rf"]), p90, float(row["baseline"]) if row["baseline"] else None)
    status_txt = {"高需求": "高需求时段", "偏高": "偏高时段", "普通": "普通时段"}[status]

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("上周同一小时预测", f"{row['baseline']:.0f} 单")
    c2.metric("随机森林预测", f"{row['rf']:.0f} 单")
    c3.metric("实际订单数", f"{row['actual']:.0f} 单", help="历史回测才有实际值；预测未来时只显示预测值和基线")
    c4.metric("预测误差", f"{abs(row['actual'] - row['rf']):.0f} 单")
    c5.metric("状态", status_txt, help="高需求：超过该区域历史 90% 分位，或比上周同一小时高 30%")

    st.subheader("最近 24 小时")
    win_start = sel_ts - pd.Timedelta(hours=23)
    pnl = panel[panel["zone_id"] == zid].copy()
    pnl["hour_dt"] = pd.to_datetime(pnl["hour_ts"])
    act = pnl[(pnl["hour_dt"] >= win_start) & (pnl["hour_dt"] <= sel_ts)][["hour_dt", "pickups"]].rename(columns={"pickups": "实际"})
    pr = sub_all[(sub_all["hour_dt"] >= win_start) & (sub_all["hour_dt"] <= sel_ts)][["hour_dt", "baseline", "rf"]]
    pr = pr.rename(columns={"baseline": "上周同一小时", "rf": "随机森林"})
    chart_df = pd.merge(act, pr, on="hour_dt", how="outer").sort_values("hour_dt")
    long = chart_df.melt(id_vars="hour_dt", var_name="系列", value_name="订单量")
    fig = px.line(long, x="hour_dt", y="订单量", color="系列", markers=False)
    fig.update_traces(line=dict(width=2))
    fig.for_each_trace(lambda t: t.update(line=dict(width=3.5)) if t.name == "实际" else None)
    st.plotly_chart(style_fig(fig, 440), use_container_width=True)
    st.caption("实际值来自全月数据；预测线只在测试期（1/25–2/1）有。")
    st.download_button(
        "下载该区域预测结果 CSV", sub_all.drop(columns=["hour_dt"]).to_csv(index=False).encode("utf-8-sig"),
        f"pred_zone_{zid}.csv", "text/csv",
    )


# ---------------- ④ 高需求区域提示 ----------------
def view_hotspots():
    if preds is None or panel is None:
        missing_banner([n for n, d in [("predictions_test.csv", preds), ("panel_10zones.csv", panel)] if d is None])
        return
    if upload_mode:
        st.info("提示基于系统默认数据（2024-01 离线结果）——上传的数据不参与建模。")

    st.subheader("下一小时高需求区域提示")
    st.caption("判定标准——随机森林预测量超过该区域历史 90% 分位，或比上周同一小时高出 30%，就标记为高需求。")

    preds_all = preds.copy()
    preds_all["hour_dt"] = pd.to_datetime(preds_all["hour_ts"])
    hour_opts = sorted(preds_all["hour_dt"].unique())
    tot = preds_all.groupby("hour_dt")["actual"].sum()
    peak_idx = int(list(tot.index).index(tot.idxmax()))
    h_pick = st.slider("选择目标小时（默认需求最高的一小时）", 0, len(hour_opts) - 1, peak_idx, format="")
    sel_ts = pd.Timestamp(hour_opts[h_pick])

    rows = []
    for zid in sorted(preds_all["zone_id"].unique()):
        r = preds_all[(preds_all["zone_id"] == zid) & (preds_all["hour_dt"] == sel_ts)].iloc[0]
        p90 = P90.get(int(zid))
        st_ = demand_status(float(r["rf"]), p90, float(r["baseline"]) if r["baseline"] else None)
        rows.append({
            "排名": 0, "区域": zone_label(zid, {int(k): v for k, v in (panel.drop_duplicates("zone_id").set_index("zone_id")["zone"].items())}),
            "预测订单数": int(round(r["rf"])), "上周同一小时": int(round(r["baseline"])), "实际订单数": int(r["actual"]), "状态": st_,
        })
    table = pd.DataFrame(rows).sort_values("预测订单数", ascending=False).reset_index(drop=True)
    table["排名"] = table.index + 1

    st.markdown(f"**{sel_ts:%Y-%m-%d %H:%M}（纽约当地时间）**")
    st.dataframe(table, use_container_width=True, hide_index=True)
    n_hot = int((table["状态"] == "高需求").sum())
    if n_hot:
        tops = table[table["状态"] == "高需求"]["区域"].head(3).tolist()
        st.success(f"高需求提醒——{ '、'.join(tops) } 下一小时预计订单较多，可重点关注。")
    else:
        st.info("该时段没有区域触发高需求条件。")
    st.download_button(
        "下载高需求提示结果 CSV", table.to_csv(index=False).encode("utf-8-sig"), "hotspot_ranking.csv", "text/csv"
    )


# ---------------- 路由 ----------------
if view == "① 数据总览":
    view_overview()
elif view == "② 出行规律分析":
    view_patterns()
elif view == "③ 需求预测":
    view_prediction()
else:
    view_hotspots()
