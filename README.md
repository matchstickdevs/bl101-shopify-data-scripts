# project-scout-shopify-daily

Source of truth for all daily Shopify → BigQuery sync scripts running in Google Cloud Run. Each script is triggered early every morning by Google Cloud Scheduler and pulls the previous day's data into BigQuery (`flowing-code-484318-c1` / dataset: `shopify`).

## Scripts

| Script | Runtime | What it syncs |
|---|---|---|
| `main.py` | Python | Orders, line items, shipping, discounts, refunds (via Shopify Bulk Operations API) |
| `daily/products.js` | Node.js | Products and product variants |
| `daily/customers.js` | Node.js | Customers |
| `daily/collections.js` | Node.js | Collections and collection↔product associations (full sync each run) |
| `daily/discounts.js` | Node.js | Discount codes and automatic discounts with usage stats (full sync each run) |
| `products.js` | Node.js | Products and variants — root-level copy used by the `shopify-products-to-bigquery` Cloud Run service |

## Architecture

- **Trigger:** Google Cloud Scheduler fires each script once per day, early morning UTC
- **Pattern:** Each run fetches records with `updated_at >= yesterday` (or full sync for collections/discounts), loads to a `_staging` table, then MERGEs into the target table
- **Auth:** Shopify OAuth client credentials stored in GCP Secret Manager
- **Destination:** BigQuery dataset `shopify` in project `flowing-code-484318-c1`

## Deploying changes

> **Current process:** Changes are deployed manually by copy-pasting the updated script into the Cloud Run inline editor in the GCP console. There is no CI/CD pipeline — this repo is the source of truth, so always edit here first, then paste into Cloud Run.

Steps:
1. Edit the script in this repo
2. Go to the corresponding Cloud Run service in the [GCP Console](https://console.cloud.google.com/run?project=flowing-code-484318-c1)
3. Click **Edit & Deploy New Revision**
4. Paste the updated script into the inline editor
5. Deploy — the new revision will be picked up on the next scheduled trigger

## Local / CLI usage (Node.js scripts)

```bash
# Incremental sync (yesterday by default)
node daily/products.js

# Sync from a specific date
node daily/products.js --start 2025-01-15

# Backfill all historical data
node daily/products.js --init
node daily/products.js --backfill
node daily/products.js --status
```

Set `SHOPIFY_CLIENT_SECRET` in your environment or add `shopify_client_secret` to GCP Secret Manager before running locally.
