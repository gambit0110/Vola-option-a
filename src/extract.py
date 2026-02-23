from __future__ import annotations

import io
import logging
import os
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

EXPECTED_ADS_COLUMNS = [
    "date",
    "channel",
    "campaign",
    "spend",
    "impressions",
    "clicks",
    "conversions",
]


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _resolve_path(path: str) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return _project_root() / candidate


def load_orders_csv(path: str) -> pd.DataFrame:
    """Load the local orders CSV as raw data without transformations."""
    csv_path = _resolve_path(path)
    logger.info("Loading orders CSV from %s", csv_path)
    return pd.read_csv(csv_path)


def load_ads_data(ads_url_env: str = "ADS_CSV_URL", fallback_path: str = "data/ads_spend_messy.csv") -> pd.DataFrame:
    """Load ads CSV from URL if configured, else local fallback.

    Returns an empty DataFrame with expected columns if both sources fail.
    """
    ads_url = os.getenv(ads_url_env)
    if ads_url:
        logger.info("Attempting to fetch ads CSV from %s env var", ads_url_env)
        try:
            response = requests.get(ads_url, timeout=20)
            response.raise_for_status()
            df = pd.read_csv(io.StringIO(response.text))
            logger.info("Loaded ads CSV from URL (%s rows)", len(df))
            return df
        except Exception as exc:  # noqa: BLE001 - fallback behavior is intentional
            logger.warning("Failed to fetch/parse ads CSV from URL; falling back to local file: %s", exc)
    else:
        logger.info("%s not set; using local ads fallback", ads_url_env)

    fallback = _resolve_path(fallback_path)
    if fallback.exists():
        logger.info("Loading ads CSV from fallback path %s", fallback)
        return pd.read_csv(fallback)

    logger.warning("Ads fallback file missing at %s; returning empty ads dataset", fallback)
    return pd.DataFrame(columns=EXPECTED_ADS_COLUMNS)
