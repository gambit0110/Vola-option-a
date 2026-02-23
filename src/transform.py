from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

CANONICAL_CHANNELS = ["paid_social", "search", "email", "organic", "direct", "unknown"]


def normalize_channel(raw: str | None) -> str:
    """Map messy channel labels to a shared canonical set."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return "unknown"
    text = str(raw).strip().lower()
    if not text:
        return "unknown"

    compact = re.sub(r"[^a-z0-9]+", "", text)

    if compact in {"fb", "facebook", "facebooks", "facebok", "facebookads", "facebooksads", "facebokads", "ig", "instagram", "meta"}:
        return "paid_social"
    if "face" in text and "book" in text:
        return "paid_social"
    if "instagram" in text or compact == "ig":
        return "paid_social"
    if "tiktok" in text:
        return "paid_social"

    if "google" in text or "search" in text:
        return "search"
    if compact in {"newsletter", "email", "mail", "klaviyo"}:
        return "email"
    if "newsletter" in text or text == "email":
        return "email"
    if "organic" in text:
        return "organic"
    if "direct" in text:
        return "direct"
    return "unknown"


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    text = str(value).strip().lower()
    return text in {"", "na", "n/a", "null", "none", "nan"}


def parse_money_to_float(raw: Any) -> float:
    """Parse messy currency strings supporting common US/EU formats."""
    if _is_blank(raw):
        return 0.0

    text = str(raw).strip()
    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]
    if text.startswith("-"):
        negative = True

    cleaned = re.sub(r"[^0-9,.-]", "", text)
    cleaned = cleaned.replace(" ", "")
    if cleaned.count("-") > 1:
        cleaned = cleaned.replace("-", "")
        negative = True
    cleaned = cleaned.lstrip("-")

    if not cleaned:
        return 0.0

    if "," in cleaned and "." in cleaned:
        if cleaned.rfind(",") > cleaned.rfind("."):
            normalized = cleaned.replace(".", "").replace(",", ".")
        else:
            normalized = cleaned.replace(",", "")
    elif "," in cleaned:
        parts = cleaned.split(",")
        if len(parts) > 1 and len(parts[-1]) in {1, 2}:
            normalized = "".join(parts[:-1]).replace(".", "") + "." + parts[-1]
        else:
            normalized = cleaned.replace(",", "")
    elif cleaned.count(".") > 1:
        parts = cleaned.split(".")
        if len(parts[-1]) in {1, 2}:
            normalized = "".join(parts[:-1]) + "." + parts[-1]
        else:
            normalized = "".join(parts)
    else:
        normalized = cleaned

    try:
        value = float(normalized)
    except ValueError:
        logger.warning("Failed to parse money value %r; defaulting to 0", raw)
        return 0.0
    return -value if negative else value


def _parse_generic_number(raw: Any) -> float:
    if _is_blank(raw):
        return 0.0
    text = str(raw).strip()
    text = re.sub(r"[^0-9,.-]", "", text)
    if not text:
        return 0.0

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        parts = text.split(",")
        if len(parts) == 2 and len(parts[-1]) in {1, 2}:
            text = parts[0] + "." + parts[1]
        else:
            text = text.replace(",", "")
    elif text.count(".") > 1:
        parts = text.split(".")
        if len(parts[-1]) in {1, 2}:
            text = "".join(parts[:-1]) + "." + parts[-1]
        else:
            text = "".join(parts)

    try:
        return float(text)
    except ValueError:
        logger.warning("Failed to parse numeric value %r; defaulting to 0", raw)
        return 0.0


def _parse_mixed_date(value: Any) -> pd.Timestamp:
    if _is_blank(value):
        return pd.NaT
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%b %d %Y", "%Y/%m/%d", "%B %d %Y"):
        try:
            return pd.Timestamp(datetime.strptime(text, fmt)).normalize()
        except ValueError:
            continue
    parsed = pd.to_datetime(text, errors="coerce", dayfirst=False)
    if pd.isna(parsed):
        return pd.NaT
    return pd.Timestamp(parsed).normalize()


def _normalize_customer_type(raw: Any) -> str:
    if _is_blank(raw):
        return "unknown"
    text = str(raw).strip().lower()
    if text in {"new", "first", "1st", "first-time", "first_time"}:
        return "new"
    if text in {"returning", "repeat", "existing", "return"}:
        return "returning"
    return "unknown"


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Clean orders data and return standardized columns."""
    logger.info("Cleaning orders data (%s raw rows)", len(df))
    out = df.copy()

    for col in ["order_id", "order_date", "channel", "revenue", "customer_type", "country"]:
        if col not in out.columns:
            out[col] = pd.NA

    out["order_id"] = out["order_id"].astype("string").str.strip()
    out["order_date"] = out["order_date"].apply(_parse_mixed_date)
    invalid_dates = int(out["order_date"].isna().sum())
    if invalid_dates:
        logger.warning("Dropping %s orders with invalid/missing order_date", invalid_dates)
    out = out.dropna(subset=["order_date"])

    out["channel"] = out["channel"].apply(normalize_channel)
    out["revenue"] = out["revenue"].apply(parse_money_to_float).astype(float)
    out["customer_type"] = out["customer_type"].apply(_normalize_customer_type)
    out["country"] = out["country"].fillna("unknown").astype("string").str.strip().replace("", "unknown")

    before_dedupe = len(out)
    out = out.drop_duplicates(subset=["order_id"], keep="last")
    dropped_dupes = before_dedupe - len(out)
    if dropped_dupes:
        logger.warning("Removed %s duplicate order rows by order_id (kept last)", dropped_dupes)

    out = out.sort_values("order_date").reset_index(drop=True)
    logger.info("Cleaned orders rows: %s", len(out))
    return out[["order_id", "order_date", "channel", "revenue", "customer_type", "country"]]


def clean_ads(df: pd.DataFrame) -> pd.DataFrame:
    """Clean ads performance data and return standardized columns."""
    logger.info("Cleaning ads data (%s raw rows)", len(df))
    out = df.copy()

    for col in ["date", "channel", "campaign", "spend", "impressions", "clicks", "conversions"]:
        if col not in out.columns:
            out[col] = pd.NA

    out["date"] = out["date"].apply(_parse_mixed_date)
    invalid_dates = int(out["date"].isna().sum())
    if invalid_dates:
        logger.warning("Dropping %s ads rows with invalid/missing date", invalid_dates)
    out = out.dropna(subset=["date"])

    out["channel"] = out["channel"].apply(normalize_channel)
    out["campaign"] = out["campaign"].fillna("unknown").astype("string").str.strip().replace("", "unknown")
    out["spend"] = out["spend"].apply(parse_money_to_float).astype(float)
    out["impressions"] = out["impressions"].apply(_parse_generic_number).round(0).astype(float)
    out["clicks"] = out["clicks"].apply(_parse_generic_number).round(0).astype(float)
    out["conversions"] = out["conversions"].apply(_parse_generic_number).round(0).astype(float)

    out = out.sort_values("date").reset_index(drop=True)
    logger.info("Cleaned ads rows: %s", len(out))
    return out[["date", "channel", "campaign", "spend", "impressions", "clicks", "conversions"]]
