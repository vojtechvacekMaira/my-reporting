"""
Hellocomp daily Slack report
READ ONLY – only SELECT queries, no data is ever modified.

Cost    → profi-hellocomp-data-prod-0861.marco.out_marketing  (partitioned on date)
Orders  → shoptet-exports.shoptet_export.hellocomp_customers_overall

Metrics : Cost (CZK), Revenue excl. VAT (CZK), Orders, PNO
Periods : Yesterday + MTD
Currency: EUR × 25 | HUF × 0.063 | CZK × 1  (matches Looker Studio "Tržby CZK_recalc.")
"""

import os
import requests
from datetime import date, timedelta

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

WEBHOOK_URL    = os.environ["SLACK_WEBHOOK_URL"]

BQ_COST_TABLE  = "profi-hellocomp-data-prod-0861.marco.out_marketing"
BQ_COST_PROJ   = "profi-hellocomp-data-prod-0861"

BQ_SHOP_TABLE  = "shoptet-exports.shoptet_export.hellocomp_customers_overall"
BQ_SHOP_PROJ   = "shoptet-exports"

# Paid order statuses (shoptet)
PAID_STATUSES  = "('Odesláno', 'Osobní odběr', 'Vyřizuje se', 'Vyřízeno')"


# ── BigQuery – cost ───────────────────────────────────────────────────────────

def get_cost(bq_cost: bigquery.Client, date_from: date, date_to: date) -> float:
    """Sum cost_czk for all paid campaigns (GAds, Sklik, Meta). READ ONLY."""
    q = f"""
        SELECT COALESCE(SUM(cost_czk), 0) AS cost
        FROM `{BQ_COST_TABLE}`
        WHERE date BETWEEN '{date_from}' AND '{date_to}'
          AND cost_czk > 0
    """
    for row in bq_cost.query(q).result():
        return float(row.cost)
    return 0.0


# ── BigQuery – orders & revenue ───────────────────────────────────────────────

def get_orders_and_revenue(
    bq_shop: bigquery.Client,
    date_from: date,
    date_to: date,
) -> tuple[int, float]:
    """
    Count paid E-shop orders and sum revenue excl. VAT converted to CZK.
    Conversion matches Looker Studio field 'Tržby CZK_recalc.':
      CZK × 1 | EUR × 25 | HUF × 0.063
    READ ONLY.
    """
    q = f"""
        SELECT
            COUNT(DISTINCT code) AS orders,
            COALESCE(SUM(
                CASE
                    WHEN currencyCode = 'CZK' THEN totalPriceWithoutVat
                    WHEN currencyCode = 'EUR' THEN totalPriceWithoutVat * 25
                    WHEN currencyCode = 'HUF' THEN totalPriceWithoutVat * 0.063
                    ELSE NULL
                END
            ), 0) AS revenue_czk
        FROM `{BQ_SHOP_TABLE}`
        WHERE date_only BETWEEN '{date_from}' AND '{date_to}'
          AND sourceName = 'E-shop'
          AND statusName IN {PAID_STATUSES}
    """
    for row in bq_shop.query(q).result():
        return int(row.orders or 0), float(row.revenue_czk or 0.0)
    return 0, 0.0


# ── Formatting ────────────────────────────────────────────────────────────────

def fmt(v: float, decimals: int = 0) -> str:
    """Format number with narrow no-break space as thousands separator."""
    return f"{v:,.{decimals}f}".replace(",", "\u00a0")


def build_table(
    yesterday: date,
    mtd_start: date,
    cost_yd:   float, cost_mtd:   float,
    rev_yd:    float, rev_mtd:    float,
    orders_yd: int,   orders_mtd: int,
    pno_yd:    float, pno_mtd:    float,
) -> str:
    mtd_label = f"MTD {mtd_start.strftime('%-d.%-m.')}–{yesterday.strftime('%-d.%-m.')}"
    col_w = max(len(mtd_label), 14)

    header = f"{'':22}{'Yesterday':>{col_w}}    {mtd_label:>{col_w}}"
    sep    = "─" * len(header)

    rows = [
        ("Cost (CZK)",    fmt(cost_yd),          fmt(cost_mtd)),
        ("Revenue (CZK)", fmt(rev_yd),            fmt(rev_mtd)),
        ("Orders",        fmt(orders_yd),         fmt(orders_mtd)),
        ("PNO",           fmt(pno_yd, 1) + "%",   fmt(pno_mtd, 1) + "%"),
    ]

    lines = [header, sep]
    for label, yd_val, mtd_val in rows:
        lines.append(f"{label:<22}{yd_val:>{col_w}}    {mtd_val:>{col_w}}")

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    bq_cost = bigquery.Client(project=BQ_COST_PROJ)
    bq_shop = bigquery.Client(project=BQ_SHOP_PROJ)

    today     = date.today()
    yesterday = today - timedelta(days=1)
    mtd_start = today.replace(day=1)

    print(f"Fetching data for yesterday ({yesterday}) and MTD ({mtd_start}–{yesterday})…")

    cost_yd  = get_cost(bq_cost, yesterday,  yesterday)
    cost_mtd = get_cost(bq_cost, mtd_start,  yesterday)
    print(f"Cost YD: {cost_yd:.0f} | MTD: {cost_mtd:.0f}")

    orders_yd,  rev_yd  = get_orders_and_revenue(bq_shop, yesterday,  yesterday)
    orders_mtd, rev_mtd = get_orders_and_revenue(bq_shop, mtd_start,  yesterday)
    print(f"Revenue YD: {rev_yd:.0f} ({orders_yd} orders) | MTD: {rev_mtd:.0f} ({orders_mtd} orders)")

    pno_yd  = (cost_yd  / rev_yd  * 100) if rev_yd  > 0 else 0.0
    pno_mtd = (cost_mtd / rev_mtd * 100) if rev_mtd > 0 else 0.0

    table = build_table(
        yesterday, mtd_start,
        cost_yd,   cost_mtd,
        rev_yd,    rev_mtd,
        orders_yd, orders_mtd,
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
