from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

import pandas as pd

from transform import CANONICAL_CHANNELS

logger = logging.getLogger(__name__)


def _week_start(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, errors="coerce")
    return (ts - pd.to_timedelta(ts.dt.weekday, unit="D")).dt.normalize()


def _safe_div(numerator: float, denominator: float, none_on_zero: bool = False) -> float | None:
    if denominator in (0, 0.0) or pd.isna(denominator):
        return None if none_on_zero else 0.0
    if pd.isna(numerator):
        return None if none_on_zero else 0.0
    return float(numerator) / float(denominator)


def _wow_change(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None:
        return None
    if previous == 0:
        return None
    return (current - previous) / previous


def _round(value: float | None, digits: int = 4) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _to_date_str(value: Any) -> str:
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _empty_weekly_entry(week_start: str) -> dict[str, Any]:
    channels = {c: 0.0 for c in CANONICAL_CHANNELS}
    return {
        "week_start": week_start,
        "revenue": 0.0,
        "orders": 0,
        "aov": 0.0,
        "revenue_split_by_customer_type": {"new": 0.0, "returning": 0.0, "unknown": 0.0},
        "returning_revenue_share": 0.0,
        "revenue_by_channel": channels.copy(),
        "wow": {},
    }


def _empty_marketing_entry(week_start: str) -> dict[str, Any]:
    channels = {c: 0.0 for c in CANONICAL_CHANNELS}
    return {
        "week_start": week_start,
        "spend": 0.0,
        "impressions": 0,
        "clicks": 0,
        "conversions": 0,
        "ctr": 0.0,
        "cvr": 0.0,
        "cpc": 0.0,
        "cac_proxy": None,
        "spend_by_channel": channels.copy(),
        "wow": {},
    }


def _empty_efficiency_entry(week_start: str) -> dict[str, Any]:
    return {
        "week_start": week_start,
        "mer": None,
        "roas_by_channel": {c: None for c in CANONICAL_CHANNELS},
        "wow": {},
    }


def _channel_sorted_top3(revenue_by_channel: dict[str, float], roas_by_channel: dict[str, float | None]) -> list[dict[str, Any]]:
    pairs = sorted(revenue_by_channel.items(), key=lambda kv: kv[1], reverse=True)
    top = []
    for channel, revenue in pairs[:3]:
        top.append(
            {
                "channel": channel,
                "revenue": round(float(revenue), 2),
                "roas": _round(roas_by_channel.get(channel), 4),
            }
        )
    return top


def detect_anomalies(metrics: dict) -> list[dict[str, Any]]:
    """Rule-based anomaly detection using computed weekly metrics."""
    anomalies: list[dict[str, Any]] = []

    sales_weekly = metrics.get("sales_weekly", [])
    marketing_weekly = metrics.get("marketing_weekly", [])
    efficiency_weekly = metrics.get("efficiency_weekly", [])

    for sales in sales_weekly:
        week = sales["week_start"]
        wow = sales.get("wow", {})

        rev_wow = wow.get("revenue")
        if rev_wow is not None and abs(rev_wow) >= 0.10:
            anomalies.append(
                {
                    "rule_id": "revenue_wow_10pct",
                    "week_start": week,
                    "scope": "overall",
                    "entity": "revenue",
                    "current": sales["revenue"],
                    "previous": wow.get("previous_revenue"),
                    "delta": _round(rev_wow, 4),
                    "why": f"Revenue changed {rev_wow:.1%} WoW ({sales['revenue']:.2f} vs {wow.get('previous_revenue', 0):.2f})",
                }
            )

        share_pp = wow.get("returning_revenue_share_pp")
        if share_pp is not None and abs(share_pp) >= 0.08:
            anomalies.append(
                {
                    "rule_id": "returning_share_pp_8pt",
                    "week_start": week,
                    "scope": "overall",
                    "entity": "returning_revenue_share",
                    "current": sales["returning_revenue_share"],
                    "previous": wow.get("previous_returning_revenue_share"),
                    "delta": _round(share_pp, 4),
                    "why": f"Returning revenue share moved {share_pp:+.1%} points ({sales['returning_revenue_share']:.1%} vs {wow.get('previous_returning_revenue_share', 0):.1%})",
                }
            )

        for channel, channel_wow in wow.get("revenue_by_channel", {}).items():
            if channel_wow is not None and abs(channel_wow) >= 0.15:
                prev_val = wow.get("previous_revenue_by_channel", {}).get(channel)
                curr_val = sales.get("revenue_by_channel", {}).get(channel)
                anomalies.append(
                    {
                        "rule_id": "channel_revenue_wow_15pct",
                        "week_start": week,
                        "scope": "channel",
                        "entity": channel,
                        "current": curr_val,
                        "previous": prev_val,
                        "delta": _round(channel_wow, 4),
                        "why": f"{channel} revenue changed {channel_wow:.1%} WoW ({(curr_val or 0):.2f} vs {(prev_val or 0):.2f})",
                    }
                )

    for mk in marketing_weekly:
        wow = mk.get("wow", {})
        spend_wow = wow.get("spend")
        if spend_wow is not None and abs(spend_wow) >= 0.15:
            anomalies.append(
                {
                    "rule_id": "spend_wow_15pct",
                    "week_start": mk["week_start"],
                    "scope": "overall",
                    "entity": "spend",
                    "current": mk["spend"],
                    "previous": wow.get("previous_spend"),
                    "delta": _round(spend_wow, 4),
                    "why": f"Spend changed {spend_wow:.1%} WoW ({mk['spend']:.2f} vs {wow.get('previous_spend', 0):.2f})",
                }
            )

    for eff in efficiency_weekly:
        roas_wow = eff.get("wow", {}).get("roas_by_channel", {})
        prev_roas = eff.get("wow", {}).get("previous_roas_by_channel", {})
        for channel, wow in roas_wow.items():
            if wow is not None and wow <= -0.20:
                curr = eff.get("roas_by_channel", {}).get(channel)
                anomalies.append(
                    {
                        "rule_id": "roas_drop_20pct",
                        "week_start": eff["week_start"],
                        "scope": "channel",
                        "entity": channel,
                        "current": curr,
                        "previous": prev_roas.get(channel),
                        "delta": _round(wow, 4),
                        "why": f"{channel} ROAS dropped {abs(wow):.1%} WoW ({(curr or 0):.2f} vs {(prev_roas.get(channel) or 0):.2f})",
                    }
                )

    return anomalies


def compute_weekly_metrics(orders: pd.DataFrame, ads: pd.DataFrame) -> dict[str, Any]:
    """Compute weekly KPIs and anomalies from cleaned orders and ads data."""
    logger.info("Computing weekly metrics")
    orders = orders.copy()
    ads = ads.copy()

    if not orders.empty:
        orders["week_start"] = _week_start(orders["order_date"])
    else:
        orders["week_start"] = pd.Series(dtype="datetime64[ns]")

    if not ads.empty:
        ads["week_start"] = _week_start(ads["date"])
    else:
        ads["week_start"] = pd.Series(dtype="datetime64[ns]")

    weeks = sorted(set(orders.get("week_start", pd.Series(dtype="datetime64[ns]")).dropna().tolist()) | set(ads.get("week_start", pd.Series(dtype="datetime64[ns]")).dropna().tolist()))
    week_strings = [_to_date_str(w) for w in weeks]

    sales_weekly: list[dict[str, Any]] = []
    marketing_weekly: list[dict[str, Any]] = []
    efficiency_weekly: list[dict[str, Any]] = []

    if not orders.empty:
        sales_totals = (
            orders.groupby("week_start", dropna=False)
            .agg(revenue=("revenue", "sum"), orders=("order_id", "count"))
            .sort_index()
        )
        sales_totals["aov"] = sales_totals.apply(lambda row: _safe_div(row["revenue"], row["orders"]) or 0.0, axis=1)

        customer_split = (
            orders.groupby(["week_start", "customer_type"], dropna=False)["revenue"]
            .sum()
            .unstack(fill_value=0.0)
        )
        revenue_by_channel = (
            orders.groupby(["week_start", "channel"], dropna=False)["revenue"]
            .sum()
            .unstack(fill_value=0.0)
        )
    else:
        sales_totals = pd.DataFrame(columns=["revenue", "orders", "aov"])
        customer_split = pd.DataFrame()
        revenue_by_channel = pd.DataFrame()

    if not ads.empty:
        ads_totals = (
            ads.groupby("week_start", dropna=False)
            .agg(
                spend=("spend", "sum"),
                impressions=("impressions", "sum"),
                clicks=("clicks", "sum"),
                conversions=("conversions", "sum"),
            )
            .sort_index()
        )
        ads_channel_spend = (
            ads.groupby(["week_start", "channel"], dropna=False)["spend"]
            .sum()
            .unstack(fill_value=0.0)
        )
    else:
        ads_totals = pd.DataFrame(columns=["spend", "impressions", "clicks", "conversions"])
        ads_channel_spend = pd.DataFrame()

    for week, week_str in zip(weeks, week_strings, strict=False):
        sales_entry = _empty_weekly_entry(week_str)
        if week in sales_totals.index:
            row = sales_totals.loc[week]
            sales_entry["revenue"] = round(float(row.get("revenue", 0.0)), 2)
            sales_entry["orders"] = int(row.get("orders", 0))
            sales_entry["aov"] = round(float(row.get("aov", 0.0)), 2)

        if week in customer_split.index:
            for ct in ["new", "returning", "unknown"]:
                sales_entry["revenue_split_by_customer_type"][ct] = round(float(customer_split.loc[week].get(ct, 0.0)), 2)

        if week in revenue_by_channel.index:
            for channel in CANONICAL_CHANNELS:
                sales_entry["revenue_by_channel"][channel] = round(float(revenue_by_channel.loc[week].get(channel, 0.0)), 2)

        sales_entry["returning_revenue_share"] = _round(
            _safe_div(
                sales_entry["revenue_split_by_customer_type"]["returning"],
                sales_entry["revenue"],
            )
            or 0.0,
            4,
        ) or 0.0
        sales_weekly.append(sales_entry)

        mk_entry = _empty_marketing_entry(week_str)
        if week in ads_totals.index:
            row = ads_totals.loc[week]
            mk_entry["spend"] = round(float(row.get("spend", 0.0)), 2)
            mk_entry["impressions"] = int(float(row.get("impressions", 0.0)))
            mk_entry["clicks"] = int(float(row.get("clicks", 0.0)))
            mk_entry["conversions"] = int(float(row.get("conversions", 0.0)))
        if week in ads_channel_spend.index:
            for channel in CANONICAL_CHANNELS:
                mk_entry["spend_by_channel"][channel] = round(float(ads_channel_spend.loc[week].get(channel, 0.0)), 2)

        mk_entry["ctr"] = _round(_safe_div(mk_entry["clicks"], mk_entry["impressions"]) or 0.0, 4) or 0.0
        mk_entry["cvr"] = _round(_safe_div(mk_entry["conversions"], mk_entry["clicks"]) or 0.0, 4) or 0.0
        mk_entry["cpc"] = _round(_safe_div(mk_entry["spend"], mk_entry["clicks"]) or 0.0, 4) or 0.0
        mk_entry["cac_proxy"] = _round(_safe_div(mk_entry["spend"], mk_entry["conversions"], none_on_zero=True), 4)
        marketing_weekly.append(mk_entry)

        eff_entry = _empty_efficiency_entry(week_str)
        eff_entry["mer"] = _round(_safe_div(sales_entry["revenue"], mk_entry["spend"], none_on_zero=True), 4)
        for channel in CANONICAL_CHANNELS:
            spend = mk_entry["spend_by_channel"].get(channel, 0.0)
            revenue = sales_entry["revenue_by_channel"].get(channel, 0.0)
            eff_entry["roas_by_channel"][channel] = _round(_safe_div(revenue, spend, none_on_zero=True), 4)
        efficiency_weekly.append(eff_entry)

    for idx in range(len(week_strings)):
        prev_sales = sales_weekly[idx - 1] if idx > 0 else None
        curr_sales = sales_weekly[idx]
        if prev_sales:
            curr_sales["wow"] = {
                "revenue": _round(_wow_change(curr_sales["revenue"], prev_sales["revenue"]), 4),
                "orders": _round(_wow_change(float(curr_sales["orders"]), float(prev_sales["orders"])), 4),
                "aov": _round(_wow_change(curr_sales["aov"], prev_sales["aov"]), 4),
                "returning_revenue_share": _round(
                    _wow_change(curr_sales["returning_revenue_share"], prev_sales["returning_revenue_share"]), 4
                ),
                "returning_revenue_share_pp": _round(
                    curr_sales["returning_revenue_share"] - prev_sales["returning_revenue_share"], 4
                ),
                "previous_revenue": prev_sales["revenue"],
                "previous_returning_revenue_share": prev_sales["returning_revenue_share"],
                "revenue_by_channel": {
                    c: _round(_wow_change(curr_sales["revenue_by_channel"][c], prev_sales["revenue_by_channel"][c]), 4)
                    for c in CANONICAL_CHANNELS
                },
                "previous_revenue_by_channel": {c: prev_sales["revenue_by_channel"][c] for c in CANONICAL_CHANNELS},
            }
        else:
            curr_sales["wow"] = {
                "revenue": None,
                "orders": None,
                "aov": None,
                "returning_revenue_share": None,
                "returning_revenue_share_pp": None,
                "previous_revenue": None,
                "previous_returning_revenue_share": None,
                "revenue_by_channel": {c: None for c in CANONICAL_CHANNELS},
                "previous_revenue_by_channel": {c: None for c in CANONICAL_CHANNELS},
            }

        prev_mk = marketing_weekly[idx - 1] if idx > 0 else None
        curr_mk = marketing_weekly[idx]
        if prev_mk:
            curr_mk["wow"] = {
                "spend": _round(_wow_change(curr_mk["spend"], prev_mk["spend"]), 4),
                "ctr": _round(_wow_change(curr_mk["ctr"], prev_mk["ctr"]), 4),
                "cvr": _round(_wow_change(curr_mk["cvr"], prev_mk["cvr"]), 4),
                "cac_proxy": _round(_wow_change(curr_mk["cac_proxy"], prev_mk["cac_proxy"]), 4),
                "previous_spend": prev_mk["spend"],
            }
        else:
            curr_mk["wow"] = {"spend": None, "ctr": None, "cvr": None, "cac_proxy": None, "previous_spend": None}

        prev_eff = efficiency_weekly[idx - 1] if idx > 0 else None
        curr_eff = efficiency_weekly[idx]
        if prev_eff:
            curr_eff["wow"] = {
                "mer": _round(_wow_change(curr_eff["mer"], prev_eff["mer"]), 4),
                "roas_by_channel": {
                    c: _round(_wow_change(curr_eff["roas_by_channel"][c], prev_eff["roas_by_channel"][c]), 4)
                    for c in CANONICAL_CHANNELS
                },
                "previous_roas_by_channel": {c: prev_eff["roas_by_channel"][c] for c in CANONICAL_CHANNELS},
            }
        else:
            curr_eff["wow"] = {
                "mer": None,
                "roas_by_channel": {c: None for c in CANONICAL_CHANNELS},
                "previous_roas_by_channel": {c: None for c in CANONICAL_CHANNELS},
            }

    latest_snapshot: dict[str, Any]
    if week_strings:
        latest_sales = sales_weekly[-1]
        latest_mk = marketing_weekly[-1]
        latest_eff = efficiency_weekly[-1]
        latest_snapshot = {
            "week_start": latest_sales["week_start"],
            "revenue": latest_sales["revenue"],
            "orders": latest_sales["orders"],
            "aov": latest_sales["aov"],
            "returning_revenue_share": latest_sales["returning_revenue_share"],
            "spend": latest_mk["spend"],
            "ctr": latest_mk["ctr"],
            "cvr": latest_mk["cvr"],
            "cpc": latest_mk["cpc"],
            "cac_proxy": latest_mk["cac_proxy"],
            "mer": latest_eff["mer"],
            "top_channels_by_revenue": _channel_sorted_top3(
                latest_sales["revenue_by_channel"], latest_eff["roas_by_channel"]
            ),
        }
    else:
        latest_snapshot = {
            "week_start": None,
            "revenue": 0.0,
            "orders": 0,
            "aov": 0.0,
            "returning_revenue_share": 0.0,
            "spend": 0.0,
            "ctr": 0.0,
            "cvr": 0.0,
            "cpc": 0.0,
            "cac_proxy": None,
            "mer": None,
            "top_channels_by_revenue": [],
        }

    metrics: dict[str, Any] = {
        "meta": {
            "run_date": date.today().isoformat(),
            "orders_rows_clean": int(len(orders)),
            "ads_rows_clean": int(len(ads)),
            "week_range": {
                "start": week_strings[0] if week_strings else None,
                "end": week_strings[-1] if week_strings else None,
                "weeks": len(week_strings),
            },
            "week_starts": week_strings,
        },
        "sales_weekly": sales_weekly,
        "marketing_weekly": marketing_weekly,
        "efficiency_weekly": efficiency_weekly,
        "latest_week_snapshot": latest_snapshot,
    }
    metrics["anomalies"] = detect_anomalies(metrics)
    logger.info("Computed %s weekly periods and %s anomalies", len(week_strings), len(metrics["anomalies"]))
    return metrics
