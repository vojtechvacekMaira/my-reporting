"""
Kovoinox daily Slack report
READ ONLY – only SELECT queries, no data is ever modified.

Cost + Revenue → profi-kovoinox-prod-228.l0_marko_kovoinox.vw_campaign_marketing_data
  The view has one row per (date, campaign, event_name). cost_czk and ga4_revenue_czk
  are denormalised across all event_name rows, so a plain SUM (no DISTINCT, no
  event_name filter) gives the correct totals matching the Looker dashboard.

Metrics : Cost (CZK), Revenue GA4 (CZK), PNO
Periods : Yesterday + MTD
Currency: CZK only
"""

import os
import requests
from datetime import date, timedelta

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

WEBHOOK_URL   = os.environ["SLACK_WEBHOOK_URL"]

BQ_COST_TABLE = "profi-kovoinox-prod-228.l0_marko_kovoinox.vw_campaign_marketing_data"
BQ_PROJ       = "profi-kovoinox-prod-228"

# ── BigQuery – cost ───────────────────────────────────────────────────────────

def get_cost(bq: bigquery.Client, date_from: date, date_to: date) -> float:
    q = f"""
        SELECT COALESCE(SUM(cost_czk), 0) AS cost
        FROM `{BQ_COST_TABLE}`
        WHERE date BETWEEN '{date_from}' AND '{date_to}'
          AND cost_czk > 0
    """
    for row in bq.query(q).result():
        return float(row.cost)
    return 0.0


# ── BigQuery – revenue (GA4) ──────────────────────────────────────────────────

def get_revenue(bq: bigquery.Client, date_from: date, date_to: date) -> float:
    q = f"""
        SELECT COALESCE(SUM(ga4_revenue_czk), 0) AS revenue_czk
        FROM `{BQ_COST_TABLE}`
        WHERE date BETWEEN '{date_from}' AND '{date_to}'
    """
    for row in bq.query(q).result():
        return float(row.revenue_czk)
    return 0.0


# ── Formatting ────────────────────────────────────────────────────────────────

def fmt(v: float, decimals: int = 0) -> str:
    """Format number with narrow no-break space as thousands separator."""
    return f"{v:,.{decimals}f}".replace(",", " ")


def build_table(
    yesterday: date,
    mtd_start: date,
    cost_yd:   float, cost_mtd: float,
    rev_yd:    float, rev_mtd:  float,
    pno_yd:    float, pno_mtd:  float,
) -> str:
    mtd_label = f"MTD {mtd_start.strftime('%-d.%-m.')}–{yesterday.strftime('%-d.%-m.')}"
    col_w = max(len(mtd_label), 14)

    header = f"{'':22}{'Yesterday':>{col_w}}    {mtd_label:>{col_w}}"
    sep    = "─" * len(header)

    rows = [
        ("Cost (CZK)",    fmt(cost_yd),        fmt(cost_mtd)),
        ("Revenue (CZK)", fmt(rev_yd),          fmt(rev_mtd)),
        ("PNO",           fmt(pno_yd, 1) + "%", fmt(pno_mtd, 1) + "%"),
    ]

    lines = [header, sep]
    for label, yd_val, mtd_val in rows:
        lines.append(f"{label:<22}{yd_val:>{col_w}}    {mtd_val:>{col_w}}")

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    bq = bigquery.Client(project=BQ_PROJ)

    today     = date.today()
    yesterday = today - timedelta(days=1)
    mtd_start = today.replace(day=1)

    print(f"Fetching data for yesterday ({yesterday}) and MTD ({mtd_start}–{yesterday})…")

    cost_yd  = get_cost(bq, yesterday, yesterday)
    cost_mtd = get_cost(bq, mtd_start, yesterday)
    print(f"Cost YD: {cost_yd:.0f} | MTD: {cost_mtd:.0f}")

    rev_yd  = get_revenue(bq, yesterday, yesterday)
    rev_mtd = get_revenue(bq, mtd_start, yesterday)
    print(f"Revenue YD: {rev_yd:.0f} | MTD: {rev_mtd:.0f}")

    pno_yd  = (cost_yd  / rev_yd  * 100) if rev_yd  > 0 else 0.0
    pno_mtd = (cost_mtd / rev_mtd * 100) if rev_mtd > 0 else 0.0

    table = build_table(
        yesterday, mtd_start,
        cost_yd,  cost_mtd,
        rev_yd,   rev_mtd,
        pno_yd,   pno_mtd,
    )

    message = (
        f"📊 *Kovoinox – daily report | {yesterday.strftime('%-d.%-m.%Y')}*\n\n"
        f"```{table}```"
    )

    print("\n" + message)

    resp = requests.post(WEBHOOK_URL, json={"text": message})
    if resp.status_code == 200:
        print("\n✅ Sent to Slack!")
    else:
        print(f"\n❌ Slack error: {resp.status_code} – {resp.text}")


if __name__ == "__main__":
    main()
