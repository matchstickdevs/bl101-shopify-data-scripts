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

### `main.py` (orders) — Cloud Run **Job**

`main.py` runs as a Cloud Run Job (not a Service) so it isn't subject to HTTP request timeouts. Scheduler triggers the Job via the Cloud Run Admin API and returns immediately; the Job runs to completion independently.

Deploy a new revision from source:

```bash
gcloud run jobs deploy shopify-to-bigquery \
  --source . \
  --region us-east5 \
  --task-timeout 3600 \
  --memory 2Gi \
  --project flowing-code-484318-c1
```

(First-time setup commands for the Job and Scheduler trigger are in the section below.)

### Node.js scripts — Cloud Run Services

> **Current process:** Changes are deployed manually by copy-pasting the updated script into the Cloud Run inline editor in the GCP console.

Steps:
1. Edit the script in this repo
2. Go to the corresponding Cloud Run service in the [GCP Console](https://console.cloud.google.com/run?project=flowing-code-484318-c1)
3. Click **Edit & Deploy New Revision**
4. Paste the updated script into the inline editor
5. Deploy — the new revision will be picked up on the next scheduled trigger

## First-time setup: `main.py` as a Cloud Run Job

Run these once to migrate from the old HTTP-triggered service to a Job:

```bash
PROJECT=flowing-code-484318-c1
REGION=us-east5
JOB=shopify-to-bigquery
SCHEDULER_REGION=us-east4
SCHEDULER_JOB=shopify-to-bigquery-daily-3am
# Service account that runs the Job (must have Secret Manager + BigQuery access).
# Reuse the one the old Cloud Run service used.
RUN_SA=<existing-run-service-account>@${PROJECT}.iam.gserviceaccount.com
# Service account that invokes the Job from Scheduler (can be the same one,
# but needs roles/run.invoker on the Job).
SCHED_SA=$RUN_SA

# 1. Deploy the Job
gcloud run jobs deploy $JOB \
  --source . \
  --region $REGION \
  --task-timeout 3600 \
  --memory 2Gi \
  --service-account $RUN_SA \
  --project $PROJECT

# 2. Grant Scheduler permission to invoke the Job
gcloud run jobs add-iam-policy-binding $JOB \
  --region $REGION \
  --member "serviceAccount:$SCHED_SA" \
  --role roles/run.invoker \
  --project $PROJECT

# 3. Repoint the existing Scheduler job at the Job's run endpoint
gcloud scheduler jobs update http $SCHEDULER_JOB \
  --location $SCHEDULER_REGION \
  --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB}:run" \
  --http-method POST \
  --oauth-service-account-email $SCHED_SA \
  --oauth-token-scope https://www.googleapis.com/auth/cloud-platform \
  --project $PROJECT

# 4. Test
gcloud scheduler jobs run $SCHEDULER_JOB --location $SCHEDULER_REGION --project $PROJECT
gcloud run jobs executions list --job $JOB --region $REGION --project $PROJECT
```

Once the Job is running cleanly, delete the old Cloud Run service (`shopify-to-bigquery` in `us-east5`) to stop paying for an idle service.

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
