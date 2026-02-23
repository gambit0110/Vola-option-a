from __future__ import annotations

import json
import logging
import os
from typing import Any

from openai import OpenAI

logger = logging.getLogger(__name__)

GROQ_BASE_URL = "https://api.groq.com/openai/v1"
GROQ_MODEL = "openai/gpt-oss-120b"
MAX_WEEK_HISTORY_FOR_LLM = 3
MAX_ANOMALIES_FOR_LLM = 12


def _fmt_currency(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"${value:,.2f}"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.1%}"


def _fmt_ratio(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value:.2f}x"


def _fallback_summary(metrics: dict[str, Any]) -> str:
    meta = metrics.get("meta", {})
    week_range = meta.get("week_range", {})
    latest = metrics.get("latest_week_snapshot", {})
    anomalies = metrics.get("anomalies", [])
    top_channels = latest.get("top_channels_by_revenue", [])[:3]

    title = f"# Weekly Performance Report ({week_range.get('start', 'N/A')} to {week_range.get('end', 'N/A')})"

    highlights = [
        f"- Revenue: {_fmt_currency(latest.get('revenue'))} across {latest.get('orders', 0)} orders (AOV {_fmt_currency(latest.get('aov'))})",
        f"- Spend: {_fmt_currency(latest.get('spend'))}; MER: {_fmt_ratio(latest.get('mer'))}",
        f"- Funnel: CTR {_fmt_pct(latest.get('ctr'))}, CVR {_fmt_pct(latest.get('cvr'))}, CAC proxy {_fmt_currency(latest.get('cac_proxy'))}",
        f"- Returning revenue share: {_fmt_pct(latest.get('returning_revenue_share'))}",
        f"- Rule-based anomalies flagged: {len(anomalies)}",
    ]

    channel_lines = []
    for item in top_channels:
        channel_lines.append(
            f"- {item['channel']}: revenue {_fmt_currency(item.get('revenue'))}; ROAS {_fmt_ratio(item.get('roas'))}"
        )
    if not channel_lines:
        channel_lines = ["- No channel revenue data available for the latest week."]

    anomaly_lines = []
    for a in anomalies[:8]:
        anomaly_lines.append(f"- [{a.get('rule_id')}] {a.get('why')}")
    if not anomaly_lines:
        anomaly_lines = ["- No anomaly rules triggered this week."]

    actions = [
        "- Validate tracking consistency for channels with the largest WoW swings in revenue or ROAS.",
        "- Review campaign-level spend and conversion quality for paid channels with rising CAC proxy.",
        "- Confirm returning customer promotions, CRM sends, and site changes if returning revenue share shifted materially.",
    ]

    return "\n\n".join(
        [
            title,
            "## Highlights\n" + "\n".join(highlights),
            "## Channel Performance\n" + "\n".join(channel_lines),
            "## Anomalies\n" + "\n".join(anomaly_lines),
            "## What To Check Next\n" + "\n".join(actions),
            "\n> Note: Generated via deterministic fallback summary because Groq LLM was unavailable during this run.",
        ]
    )


def _compact_metrics_for_llm(metrics: dict[str, Any]) -> dict[str, Any]:
    """Reduce payload size to fit Groq token limits while preserving key facts."""
    sales_weekly = metrics.get("sales_weekly", [])
    marketing_weekly = metrics.get("marketing_weekly", [])
    efficiency_weekly = metrics.get("efficiency_weekly", [])
    anomalies = metrics.get("anomalies", [])

    recent_sales = sales_weekly[-MAX_WEEK_HISTORY_FOR_LLM:]
    recent_marketing = marketing_weekly[-MAX_WEEK_HISTORY_FOR_LLM:]
    recent_efficiency = efficiency_weekly[-MAX_WEEK_HISTORY_FOR_LLM:]

    compact = {
        "meta": metrics.get("meta", {}),
        "latest_week_snapshot": metrics.get("latest_week_snapshot", {}),
        "recent_weeks": {
            "sales_weekly": recent_sales,
            "marketing_weekly": recent_marketing,
            "efficiency_weekly": recent_efficiency,
        },
        "anomalies": anomalies[:MAX_ANOMALIES_FOR_LLM],
        "anomalies_summary": {
            "count_total": len(anomalies),
            "count_included": min(len(anomalies), MAX_ANOMALIES_FOR_LLM),
            "rule_counts": {},
        },
    }

    rule_counts: dict[str, int] = {}
    for item in anomalies:
        rule_id = str(item.get("rule_id", "unknown"))
        rule_counts[rule_id] = rule_counts.get(rule_id, 0) + 1
    compact["anomalies_summary"]["rule_counts"] = rule_counts
    return compact


def _build_prompt(metrics: dict[str, Any]) -> list[dict[str, str]]:
    compact_metrics = _compact_metrics_for_llm(metrics)
    metrics_json = json.dumps(compact_metrics, indent=2)
    system = (
        "You are a senior ecommerce analyst writing an executive-ready weekly report. "
        "Use only numbers present in the provided JSON. Do not invent, estimate, or infer missing values. "
        "If a number is missing/null, say N/A."
    )
    user = (
        "Write a markdown report with EXACTLY these sections in this order:\n"
        "1) Title with week range\n"
        "2) Highlights (4-7 bullets with key numbers)\n"
        "3) Channel performance (top 3 channels by revenue + ROAS if available)\n"
        "4) Anomalies (bulleted, include which rule triggered)\n"
        "5) What to check next (3 concrete actions)\n\n"
        "Rules:\n"
        "- No hallucinated numbers.\n"
        "- Cite values directly from JSON.\n"
        "- Be concise and executive-ready.\n"
        "- Output markdown only.\n\n"
        "- `anomalies` may be truncated; use `anomalies_summary.count_total` and `rule_counts` if helpful.\n\n"
        f"Metrics JSON:\n```json\n{metrics_json}\n```"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def generate_exec_summary(metrics: dict[str, Any], model: str = GROQ_MODEL) -> str:
    """Generate an executive-ready markdown summary using Groq via OpenAI SDK.

    Falls back to a deterministic local summary if GROQ_API_KEY is missing or the API call fails.
    """
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        logger.warning("GROQ_API_KEY is not set; using deterministic fallback summary")
        return _fallback_summary(metrics)

    try:
        client = OpenAI(base_url=GROQ_BASE_URL, api_key=api_key)
        response = client.chat.completions.create(
            model=model,
            temperature=0.2,
            messages=_build_prompt(metrics),
        )
        content = response.choices[0].message.content if response.choices else None
        if not content or not str(content).strip():
            raise ValueError("Empty LLM response content")
        logger.info("Generated report summary via Groq model %s", model)
        return str(content).strip()
    except Exception as exc:  # noqa: BLE001 - fallback is required behavior
        logger.error("Groq summary generation failed; using fallback summary: %s", exc)
        return _fallback_summary(metrics)
