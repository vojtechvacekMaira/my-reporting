"""
Hellocomp daily Slack report
⚠️  READ ONLY – only SELECT queries, no data is ever modified.

Sources:
  cost   → client-reporting-395213.out_marketing.hellocz_sro_8231
  orders → shoptet-exports.shoptet_export.hellocomp_customers_overall

Metrics : Cost (CZK), Revenue excl. VAT (CZK), Orders, PNO
Periods : Yesterday + MTD
Currency: All converted to CZK; live EUR/CZK rate from CNB API
"""

import os
import requests
from datetime import date, timedelta

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

WEBHOOK_URL     = os.environ["SLACK_WEBHOOK_URL"]
BQ_COST_TABLE   = "client-reporting-395213.out_marketing.hellocz_sro_8231"
BQ_ORDERS_TABLE = "shoptet-exports.shoptet_export.hellocomp_customers_overall"

PAID_STATUSES   = "('Odesláno', 'Osobní odběr', 'Vyřizuje se', 'Vyřízeno')"
EUR_CZK_FALLBACK = 25.0


# ── Exchange rate ─────────────────────────────────────────────────────────────

def get_eur_czk_rate() -> float:
    """Fetch today's EUR/CZK from Czech National Bank. Falls back to 25.0."""
    try:
        r = requests.get(
            "https://api.cnb.cz/cnbapi/exrates/daily?lang=EN",
            timeout=5,
        )
        for entry in r.json().get("rates", []):
            if entry["currencyCode"] == "EUR":
                return round(entry["rate"] / entry["amount"], 4)
    except Exception as e:
        print(f"⚠️  CNB rate fetch failed ({e}), using fallback {EUR_CZK_FALLBACK}")
    return EUR_CZK_FALLBACK


# ── BigQuery – cost ───────────────────────────────────────────────────────────

def get_cost(bq: bigquery.Client, date_from: date, date_to: date) -> float:
    """Sum cost_czk for paid campaigns (GAds, Sklik, Meta). READ ONLY."""
    q = f"""
        SELECT COALESCE(SUM(cost_czk), 0) AS cost
        FROM `{BQ_COST_TABLE}`
        WHERE date BETWEEN '{date_from}' AND '{date_to}'
          AND source_medium LIKE '%cpc%'
    """
    for row in bq.query(q).result():
        return float(row.cost)
    return 0.0


# ── BigQuery – orders & revenue ───────────────────────────────────────────────

def get_orders_and_revenue(
    bq: bigquery.Client,
    date_from: date,
    date_to: date,
    eur_rate: float,
) -> tuple[int, float]:
    """
    Count orders and sum revenue excl. VAT, converted to CZK.
    Only paid statuses: Odesláno, Osobní odběr, Vyřizuje se, Vyřízeno.
    READ ONLY.
    """
    q = f"""
        SELECT
            COUNT(DISTINCT code) AS orders,
            COALESCE(SUM(
                CASE
                    WHEN currencyCode = 'EUR' THEN totalPriceWithoutVat * {eur_rate}
                    ELSE totalPriceWithoutVat
                END
            ), 0) AS revenue_czk
        FROM `{BQ_ORDERS_TABLE}`
        WHERE date_only BETWEEN '{date_from}' AND '{date_to}'
          AND statusName IN {PAID_STATUSES}
    """
    for row in bq.query(q).result():
        return int(row.orders or 0), float(row.revenue_czk or 0.0)
    return 0, 0.0


# ── Formatting ────────────────────────────────────────────────────────────────

def fmt(v: float, decimals: int = 0) -> str:
    """Format number with narrow no-break space as thousands separator."""
    return f"{v:,.{decimals}f}".replace(",", "\u00a0")


def build_table(
    yesterday: date,
    mtd_start: date,
    cost_yd: float,  cost_mtd: float,
    rev_yd: float,   rev_mtd: float,
    orders_yd: int,  orders_mtd: int,
    pno_yd: float,   pno_mtd: float,
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
    bq       = bigquery.Client(project="client-reporting-395213")
    eur_rate = get_eur_czk_rate()

    today     = date.today()
    yesterday = today - timedelta(days=1)
    mtd_start = today.replace(day=1)

    print(f"EUR/CZK rate: {eur_rate}")
    print(f"Fetching data for yesterday ({yesterday}) and MTD ({mtd_start}–{yesterday})…")

    cost_yd  = get_cost(bq, yesterday,  yesterday)
    cost_mtd = get_cost(bq, mtd_start,  yesterday)
    print(f"Cost YD: {cost_yd:.2f} | MTD: {cost_mtd:.2f}")

    orders_yd,  rev_yd  = get_orders_and_revenue(bq, yesterday,  yesterday,  eur_rate)
    orders_mtd, rev_mtd = get_orders_and_revenue(bq, mtd_start,  yesterday,  eur_rate)
    print(f"Revenue YD: {rev_yd:.2f} ({orders_yd} orders) | MTD: {rev_mtd:.2f} ({orders_mtd} orders)")

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
        f"📊 *Hellocomp – daily report | {yesterday.strftime('%-d.%-m.%Y')}*\n"
        f"_EUR/CZK: {eur_rate}_\n\n"
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
