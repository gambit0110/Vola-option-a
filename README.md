# Automated Weekly Reporting Pipeline (Groq)

A Python reporting pipeline for an AI Automation Developer take-home. It pulls messy ecommerce orders + ads performance data, cleans and normalizes both sources, computes weekly KPIs/anomalies, generates an executive-ready markdown summary with Groq (via the OpenAI-compatible SDK), and writes scheduled report artifacts.

## Architecture Overview

```text
Orders CSV (local, messy) -----> extract.py -----> transform.py -----> metrics.py -----> llm_summary.py -----> deliver.py
Ads CSV (HTTP or local fallback) --^                 |                   |                (Groq or fallback)    |
                                                      +-------------------+----------------------------------------+
                                                                                             run.py (orchestrator)
```

## Project Structure

```text
reporting-pipeline/
  data/
    orders_messy.csv
    ads_spend_messy.csv
  src/
    extract.py
    transform.py
    metrics.py
    llm_summary.py
    deliver.py
    run.py
  reports/
    .gitkeep
  .github/workflows/weekly.yml
  requirements.txt
  .env.example
  README.md
```

## Setup

1. Create and activate a virtual environment.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies.

```powershell
pip install -r requirements.txt
```

3. Create `.env` from `.env.example` and add your secrets.

```env
GROQ_API_KEY=your_groq_key_here
ADS_CSV_URL=https://example.com/raw/ads.csv   # optional
```

Notes:
- If `ADS_CSV_URL` is missing or fetch fails, the pipeline uses `data/ads_spend_messy.csv`.
- If `GROQ_API_KEY` is missing or Groq fails, the pipeline writes a deterministic fallback summary and still delivers files.

## How To Run Locally

```powershell
python src/run.py
```

Expected outputs:
- `reports/weekly_report_YYYY-MM-DD.md`
- `reports/latest.md`
- `reports/metrics_YYYY-MM-DD.json`

The script logs each stage and prints the saved paths at the end.

## Scheduling (GitHub Actions)

Workflow file: `.github/workflows/weekly.yml`

Triggers:
- Every Monday at `09:00 UTC` via cron: `0 9 * * 1`
- Manual run via `workflow_dispatch`

Required GitHub secret:
- `GROQ_API_KEY`

Optional GitHub secret:
- `ADS_CSV_URL`

## Transformations (Before / After Examples)

### Orders date parsing (mixed formats)

Examples accepted:
- `2026-02-01`
- `01/02/2026`
- `Feb 1 2026`
- `2026/02/01`

Invalid / blank dates are dropped.

### Channel normalization (shared canonical set)

Canonical output set:
- `paid_social`
- `search`
- `email`
- `organic`
- `direct`
- `unknown`

Examples:
- `fb`, `Facebook`, `Face Book`, `IG`, `instagram`, `tiktok` -> `paid_social`
- `google ads`, `google_search` -> `search`
- `newsletter`, `email` -> `email`

### Revenue cleanup (US + EU formats)

Examples parsed to floats:
- `$1,234.50` -> `1234.50`
- `€980,30` -> `980.30`
- `1.500,00` -> `1500.00`
- `` (blank) / `null` -> `0.0`

### Orders de-duplication and defaults
- Duplicate `order_id` rows are removed with `keep='last'`
- Missing `customer_type` becomes `unknown`
- Invalid order date rows are dropped

### Ads cleanup
- Date parsing + invalid date row drops
- Shared channel normalization
- `spend`, `impressions`, `clicks`, `conversions` robustly coerced to numeric
- Missing numerics default to `0`

## Metrics Definitions

Weekly buckets are **Monday-start**.

### Sales (Orders)
- `revenue`: total revenue in week
- `orders`: order row count after cleanup/dedupe
- `AOV`: `revenue / orders`
- `revenue_split_by_customer_type`: `new`, `returning`, `unknown`
- `revenue_by_channel`
- WoW % change: revenue, orders, AOV, returning revenue share

### Marketing (Ads)
- `spend` (total and by channel)
- `CTR = clicks / impressions`
- `CVR = conversions / clicks`
- `CPC = spend / clicks`
- `CAC proxy = spend / conversions` (`N/A` if conversions = 0)
- WoW % change: spend, CTR, CVR, CAC proxy

### Combined Efficiency
- `ROAS by channel = revenue_by_channel / spend_by_channel` (when spend > 0)
- `MER = total_revenue / total_spend`
- ROAS WoW drop anomalies are flagged when drop >= 20%

## Anomaly Rules (Explainable / Rule-Based)

The pipeline emits anomalies with `rule_id`, scope, entity, and a short explanation string containing supporting numbers.

Rules:
- Revenue WoW absolute change >= `10%`
- Any channel revenue WoW absolute change >= `15%`
- Spend WoW absolute change >= `15%`
- ROAS WoW drop >= `20%` (relative)
- Returning revenue share change >= `8 percentage points`

## Groq LLM Summary

The report summary uses the OpenAI Python SDK configured for Groq:

```python
from openai import OpenAI
client = OpenAI(
    base_url="https://api.groq.com/openai/v1",
    api_key=os.getenv("GROQ_API_KEY"),
)
```

Model:
- `openai/gpt-oss-120b`

The prompt passes the computed metrics JSON and explicitly instructs the model to use only values from the JSON (no hallucinated numbers).

## Example Output (Excerpt)

```md
# Weekly Performance Report (2025-12-29 to 2026-02-16)

## Highlights
- Revenue: $1,299.00 across 6 orders (AOV $216.50)
- Spend: $1,605.00; MER: 0.81x
- Funnel: CTR 2.97%, CVR 4.87%, CAC proxy $18.24
- Returning revenue share: 49.1%
- Rule-based anomalies flagged: 5

## Channel Performance
- direct: revenue $460.00; ROAS N/A
- search: revenue $290.00; ROAS 0.71x
- paid_social: revenue $333.00; ROAS 0.29x
```

Actual values depend on cleaned data for the latest run date.

## Reliability / Defensive Handling

- Uses `logging` across all modules (`INFO`, `WARNING`, `ERROR`)
- Ads source gracefully falls back from URL -> local file -> empty dataset
- Division-by-zero safe metrics (returns `0` or `N/A` as appropriate)
- Missing ads spend for a week/channel does not crash ROAS or MER calculations
- Missing Groq key/API failure still produces and delivers a report (deterministic fallback)

## Extension Ideas

- Deliver the markdown summary to email automatically (SMTP / SendGrid)
- Push report content to Google Docs or Notion
- Persist weekly metrics in a small warehouse (SQLite/Postgres) for trend dashboards
- Add campaign-level anomaly detection (not just channel-level)
- Add a Streamlit dashboard that reads `reports/metrics_*.json`
