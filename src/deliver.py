from __future__ import annotations

import csv
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _weekly_metrics_rows(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    sales = {row["week_start"]: row for row in metrics.get("sales_weekly", [])}
    marketing = {row["week_start"]: row for row in metrics.get("marketing_weekly", [])}
    efficiency = {row["week_start"]: row for row in metrics.get("efficiency_weekly", [])}

    anomalies_by_week: dict[str, list[str]] = {}
    for item in metrics.get("anomalies", []):
        week = str(item.get("week_start"))
        anomalies_by_week.setdefault(week, []).append(str(item.get("rule_id", "unknown")))

    weeks = sorted(set(sales) | set(marketing) | set(efficiency))
    rows: list[dict[str, Any]] = []
    channel_order = ["paid_social", "search", "email", "organic", "direct", "unknown"]

    for week in weeks:
        s = sales.get(week, {})
        m = marketing.get(week, {})
        e = efficiency.get(week, {})
        row: dict[str, Any] = {
            "week_start": week,
            "revenue": s.get("revenue"),
            "orders": s.get("orders"),
            "aov": s.get("aov"),
            "returning_revenue_share": s.get("returning_revenue_share"),
            "revenue_wow": (s.get("wow") or {}).get("revenue"),
            "orders_wow": (s.get("wow") or {}).get("orders"),
            "aov_wow": (s.get("wow") or {}).get("aov"),
            "returning_share_wow": (s.get("wow") or {}).get("returning_revenue_share"),
            "spend": m.get("spend"),
            "ctr": m.get("ctr"),
            "cvr": m.get("cvr"),
            "cpc": m.get("cpc"),
            "cac_proxy": m.get("cac_proxy"),
            "spend_wow": (m.get("wow") or {}).get("spend"),
            "ctr_wow": (m.get("wow") or {}).get("ctr"),
            "cvr_wow": (m.get("wow") or {}).get("cvr"),
            "cac_proxy_wow": (m.get("wow") or {}).get("cac_proxy"),
            "mer": e.get("mer"),
            "mer_wow": (e.get("wow") or {}).get("mer"),
            "anomaly_count": len(anomalies_by_week.get(week, [])),
            "anomaly_rules": ";".join(anomalies_by_week.get(week, [])),
        }

        revenue_by_channel = s.get("revenue_by_channel") or {}
        spend_by_channel = m.get("spend_by_channel") or {}
        roas_by_channel = e.get("roas_by_channel") or {}
        for channel in channel_order:
            row[f"revenue_{channel}"] = revenue_by_channel.get(channel)
            row[f"spend_{channel}"] = spend_by_channel.get(channel)
            row[f"roas_{channel}"] = roas_by_channel.get(channel)
        rows.append(row)
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_reports(report_md: str, metrics: dict[str, Any], run_date: date) -> dict[str, str]:
    """Write the markdown and metrics artifacts to reports/."""
    reports_dir = _project_root() / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    date_str = run_date.isoformat()
    weekly_md = reports_dir / f"weekly_report_{date_str}.md"
    latest_md = reports_dir / "latest.md"
    metrics_json = reports_dir / f"metrics_{date_str}.json"
    weekly_csv = reports_dir / f"weekly_report_{date_str}.csv"
    latest_csv = reports_dir / "latest.csv"

    weekly_md.write_text(report_md.strip() + "\n", encoding="utf-8")
    latest_md.write_text(report_md.strip() + "\n", encoding="utf-8")
    metrics_json.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")
    rows = _weekly_metrics_rows(metrics)
    _write_csv(weekly_csv, rows)
    _write_csv(latest_csv, rows)

    logger.info("Wrote report artifacts to %s", reports_dir)
    return {
        "weekly_report": str(weekly_md),
        "latest_report": str(latest_md),
        "metrics_json": str(metrics_json),
        "weekly_csv": str(weekly_csv),
        "latest_csv": str(latest_csv),
    }
