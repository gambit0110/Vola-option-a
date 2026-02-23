from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def write_reports(report_md: str, metrics: dict[str, Any], run_date: date) -> dict[str, str]:
    """Write the markdown and metrics artifacts to reports/."""
    reports_dir = _project_root() / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    date_str = run_date.isoformat()
    weekly_md = reports_dir / f"weekly_report_{date_str}.md"
    latest_md = reports_dir / "latest.md"
    metrics_json = reports_dir / f"metrics_{date_str}.json"

    weekly_md.write_text(report_md.strip() + "\n", encoding="utf-8")
    latest_md.write_text(report_md.strip() + "\n", encoding="utf-8")
    metrics_json.write_text(json.dumps(metrics, indent=2, ensure_ascii=False), encoding="utf-8")

    logger.info("Wrote report artifacts to %s", reports_dir)
    return {
        "weekly_report": str(weekly_md),
        "latest_report": str(latest_md),
        "metrics_json": str(metrics_json),
    }
