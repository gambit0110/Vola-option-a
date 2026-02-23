from __future__ import annotations

import logging
from datetime import date

from dotenv import load_dotenv

from deliver import write_reports
from extract import load_ads_data, load_orders_csv
from llm_summary import generate_exec_summary
from metrics import compute_weekly_metrics
from transform import clean_ads, clean_orders


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def main() -> None:
    _configure_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting weekly reporting pipeline run")

    load_dotenv()

    orders_raw = load_orders_csv("data/orders_messy.csv")
    ads_raw = load_ads_data()

    orders_clean = clean_orders(orders_raw)
    ads_clean = clean_ads(ads_raw)

    metrics = compute_weekly_metrics(orders_clean, ads_clean)
    report_md = generate_exec_summary(metrics)

    saved = write_reports(report_md=report_md, metrics=metrics, run_date=date.today())

    print(f"Saved weekly report: {saved['weekly_report']}")
    print(f"Saved latest report: {saved['latest_report']}")
    print(f"Saved metrics JSON: {saved['metrics_json']}")
    logger.info("Pipeline run complete")


if __name__ == "__main__":
    main()
