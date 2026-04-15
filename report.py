"""
Hellocomp daily Slack report
READ ONLY – only SELECT queries, no data is ever modified.

Source : profi-hellocomp-data-prod-0861.marco.out_marketing
Metrics: Cost (CZK), Revenue/GA4 (CZK), Conversions, PNO
Periods: Yesterday + MTD
"""

import os
import requests
from datetime import date, timedelta

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]
BQ_TABLE    = "profi-hellocomp-data-prod-0861.marco.out_marketing"
BQ_PROJECT  = "profi-hellocomp-data-prod-0861"


# ── BigQuery ──────────────────────────────────────────────────────────────────

def get_metrics(
    bq: bigquery.Client,
    date_from: date,
    date_to: date,
) -> tuple[float, float, float]:
    """
    Returns (cost_czk, ga4_revenue_czk, conversions) for paid campaigns.
    Partition filter on `date` is required — table is partitioned.
    READ ONLY.
    """
    q = f"""
        SELECT
            COALESCE(SUM(cost_czk),         0) AS cost,
            COALESCE(SUM(ga4_revenue_czk),  0) AS revenue,
            COALESCE(SUM(conversions),       0) AS conversions
        FROM `{BQ_TABLE}`
        WHERE date BETWEEN '{date_from}' AND '{date_to}'
          AND paid = 'true'
    """
    for row in bq.query(q).result():
        return float(row.cost), float(row.revenue), float(row.conversions)
    return 0.0, 0.0, 0.0


# ── Formatting ────────────────────────────────────────────────────────────────

def fmt(v: float, decimals: int = 0) -> str:
    """Format number with narrow no-break space as thousands separator."""
    return f"{v:,.{decimals}f}".replace(",", "\u00a0")


def build_table(
    yesterday: date,
    mtd_start: date,
    cost_yd:  float, cost_mtd:  float,
    rev_yd:   float, rev_mtd:   float,
    conv_yd:  float, conv_mtd:  float,
    pno_yd:   float, pno_mtd:   float,
) -> str:
    mtd_label = f"MTD {mtd_start.strftime('%-d.%-m.')}–{yesterday.strftime('%-d.%-m.')}"
    col_w = max(len(mtd_label), 14)

    header = f"{'':22}{'Yesterday':>{col_w}}    {mtd_label:>{col_w}}"
    sep    = "─" * len(header)

    rows = [
        ("Cost (CZK)",    fmt(cost_yd),           fmt(cost_mtd)),
        ("Revenue (CZK)", fmt(rev_yd),             fmt(rev_mtd)),
        ("Conversions",   fmt(conv_yd),            fmt(conv_mtd)),
        ("PNO",           fmt(pno_yd, 1) + "%",    fmt(pno_mtd, 1) + "%"),
    ]

    lines = [header, sep]
    for label, yd_val, mtd_val in rows:
        lines.append(f"{label:<22}{yd_val:>{col_w}}    {mtd_val:>{col_w}}")

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    bq = bigquery.Client(project=BQ_PROJECT)

    today     = date.today()
    yesterday = today - timedelta(days=1)
    mtd_start = today.replace(day=1)

    print(f"Fetching data for yesterday ({yesterday}) and MTD ({mtd_start}–{yesterday})…")

    cost_yd,  rev_yd,  conv_yd  = get_metrics(bq, yesterday,  yesterday)
    cost_mtd, rev_mtd, conv_mtd = get_metrics(bq, mtd_start,  yesterday)

    print(f"YD  → cost: {cost_yd:.0f} | revenue: {rev_yd:.0f} | conv: {conv_yd:.0f}")
    print(f"MTD → cost: {cost_mtd:.0f} | revenue: {rev_mtd:.0f} | conv: {conv_mtd:.0f}")

    pno_yd  = (cost_yd  / rev_yd  * 100) if rev_yd  > 0 else 0.0
    pno_mtd = (cost_mtd / rev_mtd * 100) if rev_mtd > 0 else 0.0

    table = build_table(
        yesterday, mtd_start,
        cost_yd,   cost_mtd,
        rev_yd,    rev_mtd,
        conv_yd,   conv_mtd,
        pno_yd,    pno_mtd,
    )

    message = (
        f"📊 *Hellocomp – daily report | {yesterday.strftime('%-d.%-m.%Y')}*\n\n"
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
