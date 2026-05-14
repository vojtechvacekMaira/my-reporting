# Hellocomp – denní report do Slacku

Skript každý den pošle do Slacku tabulku s náklady, tržbami, objednávkami a PNO za včera a MTD.

---

## Co budeš potřebovat

- Python 3.10 nebo novější
- Přístup k BigQuery projektu `profi-hellocomp-data-prod-0861` — viz níže
- Slack Webhook URL — viz níže

---

## Jednorázové nastavení

### 1. Získej přístup k BigQuery

Napiš **Jakubu Křížovi** a požádej ho o přístup k authorized datasetu:
```
profi-hellocomp-data-prod-0861.marco.out_marketing
```
> ⚠️ Pozor: existují i source tabulky v projektu `client-reporting-395213` — ty nepoužívej, jsou jen pro čtení interními nástroji. Vždy pracuj s authorized views výše.

### 2. Získej Slack Webhook URL

Napiš **Mariánovi Laššákovi** a požádej ho o vytvoření incoming webhooku pro kanál `#hellocomp`. Musíš ho nejdřív přidat do kanálu.

### 3. Nainstaluj závislosti

Otevři terminál, přejdi do složky s projektem a spusť:

```bash
pip install -r requirements.txt
```

### 4. Vytvoř soubor `.env`

Ve složce s projektem vytvoř soubor `.env` (bez přípony) s tímto obsahem:

```
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXXX/YYYY/ZZZZ
```

URL doplň tou, co ti dá Marián.

### 5. Přihlas se do Google Cloudu

```bash
gcloud auth application-default login
```

Otevře se prohlížeč — přihlas se Google účtem, který má přístup k BQ.

> Pokud nemáš `gcloud`, stáhni ho na: https://cloud.google.com/sdk/docs/install

---

## Spuštění reportu ručně

```bash
python report.py
```

V terminálu uvidíš načtená data a na konci `✅ Sent to Slack!`

---

## Automatické spouštění přes GitHub Actions

Doporučený způsob je spouštět skript automaticky přes **GitHub Actions** — pak nemusíš na nic myslet a report chodí každé ráno sám.

Co k tomu potřebuješ:
1. Přístup k GitHub repozitáři — popros **Vojtu** o přidání
2. V nastavení repozitáře (Settings → Secrets) přidej secret `SLACK_WEBHOOK_URL` s hodnotou od Mariána
3. Workflow soubor `.github/workflows/report.yml` se postará o spuštění každý den v nastavenou hodinu

> Pokud GitHub Actions ještě nemáš nastavené, popros Vojtu — ukáže ti jak.

---

## Možné problémy

| Problém | Co udělat |
|---|---|
| `403 Access Denied` v BigQuery | Nemáš přístup k projektu — napiš Jakubu Křížovi |
| `SLACK_WEBHOOK_URL` not set | Zkontroluj, že soubor `.env` existuje a obsahuje správnou URL |
| `❌ Slack error 403/404` | Webhook URL je špatná nebo expirovala — napiš Mariánovi Laššákovi o novou |
| `google.auth` error | Znovu spusť `gcloud auth application-default login` |
| Data v reportu jsou podivná / nafouklá | Zkontroluj, že čerpáš z `profi-hellocomp-data-prod-0861.marco.out_marketing`, ne ze source tabulek |
| GitHub Actions se nespustí | Zkontroluj, že secret `SLACK_WEBHOOK_URL` je správně nastaven v Settings → Secrets |

---

## Kontakty

| Co řešíš | Kdo |
|---|---|
| Přístup k BigQuery | Jakub Kříž |
| Slack webhook | Marián Laššák |
| GitHub repo / GitHub Actions | Vojta |
