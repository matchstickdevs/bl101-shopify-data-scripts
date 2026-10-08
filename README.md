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
| `openai-feed/openai-feed.js` | Node.js | Fetches products/variants **directly from the Shopify Admin API** and upserts them to OpenAI via `PATCH /product_feeds/{id}/products` (out-bound REST; runs intraday, e.g. hourly — not a BigQuery sync) |

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
  --task-timeout 7200 \
  --memory 2Gi \
  --project flowing-code-484318-c1
```

(First-time setup commands for the Job and Scheduler trigger are in the section below.)

### Product inventory refresh — Cloud Run **Service**

`daily/products.js` re-fetches the **entire** product catalog (no `updated_at` filter) and MERGEs it into BigQuery when its HTTP handler is triggered with `?mode=refresh-inventory`. This exists because Shopify inventory adjustments (e.g. restocking a sold-out variant) don't reliably bump `product.updated_at`, so restocked products are skipped by the daily incremental sync and BigQuery keeps the stale quantity. The refresh is a full upsert of the same Products/ProductVariants columns, so it also self-heals any other drift.

It runs as a Cloud Run **Service** (deployed via the inline editor, like the other Node syncs — no buildpack/lock-file build to break). The same `daily/products.js` powers both modes off the `main` entry point:

- **default trigger** → incremental sync (yesterday → today)
- **`?mode=refresh-inventory`** (query string or JSON body) → full-catalog inventory refresh (~60–90s for ~4,500 products / ~30k variants), followed by a **per-location inventory snapshot**

**Per-location inventory snapshots.** After the catalog MERGE, refresh mode pages through `inventoryLevels` for every *active* Shopify location and writes one row per inventory item × location (`available`, `on_hand`, `committed`, `incoming`) into `shopify.InventoryLevelSnapshots`, partitioned by `snapshot_date` (UTC). It also refreshes `shopify.Locations`, which keeps all locations, inactive ones included, with an `is_active` flag. Re-running on the same UTC day replaces that day's partition instead of duplicating it. The `shopify.InventoryLevels` view shows the latest snapshot joined to location names. Diff snapshots across dates, plus units sold from the orders tables, for sell-through. Create the tables and view once with `shopify_inventory_schema.sql` before deploying.

To deploy: copy-paste `daily/products.js` into the service's inline editor (entry point `main`), keep `package.json` to the four runtime deps (`@google-cloud/functions-framework`, `@google-cloud/bigquery`, `@google-cloud/secret-manager`, `dotenv`), and set the request timeout to ~900s for headroom. Trigger the full refresh daily with a Cloud Scheduler job whose URI carries the mode param:

```bash
gcloud scheduler jobs create http shopify-products-inventory-refresh-daily \
  --location us-east4 \
  --schedule "0 4 * * *" \
  --time-zone "Etc/UTC" \
  --uri "https://YOUR_SERVICE_URL/?mode=refresh-inventory" \
  --http-method GET \
  --oidc-service-account-email YOUR_INVOKER_SA@flowing-code-484318-c1.iam.gserviceaccount.com \
  --project flowing-code-484318-c1
```

### OpenAI product feed — Cloud Run **Job**

`openai-feed/openai-feed.js` is **out-bound**: it fetches products + variants
**directly from the Shopify Admin GraphQL API** (not from BigQuery), transforms
them into OpenAI's nested `api/products` Product/Variant schema, and upserts them
to OpenAI with `PATCH {OPENAI_API_BASE}/product_feeds/{feed_id}/products`
(body `{ target_country, products }`, upsert-by-`id`, batched).

Pulling from Shopify directly (rather than the once-a-day BigQuery sync) lets it
run **multiple times a day — e.g. hourly** — so OpenAI stays close to live
inventory/pricing. The PATCH upserts the products in each request and leaves
omitted products unchanged.

Notes on a few mappings:
- **Hidden products** (Shopify tag `hidden`) are still sent, but with
  `availability: { available: false, status: "out_of_stock" }` — this schema has
  no eligibility flag, so availability is the only lever to turn a product off.
- **Sales**: Shopify `compare_at_price` (when higher than `price`) becomes the
  variant's `list_price`; `price` is the current selling price.
- **Variant options** use Shopify's real option names (`selectedOptions`).
- **API base URL** defaults to `https://api.ads.openai.com/v1` and is overridable
  via the `OPENAI_API_BASE` env var / `openai_api_base` secret.

It lives in its own self-contained folder (`openai-feed/`, with its own
`package.json` + `Dockerfile`) so the Docker build context is just this script
and its two runtime deps. It runs as a Cloud Run **Job** built from
`openai-feed/Dockerfile` (ENTRYPOINT `node`, default `CMD` `openai-feed.js`).

Deploy a new revision from source:

```bash
gcloud run jobs deploy bl101-openai-product-feed \
  --source openai-feed \
  --region us-east5 \
  --task-timeout 1800 \
  --memory 1Gi \
  --project flowing-code-484318-c1
```

(First-time Job + Scheduler + Secret Manager setup is in the section below.)

**Local / dry-run usage** (`--dry-run` fetches from Shopify and writes the JSON
locally without sending — needs the Shopify client secret but **not** the OpenAI
API key):

```bash
cd openai-feed
npm install                                       # once, to install deps locally
node openai-feed.js --dry-run --limit 100 --out ./.feed-sample --keep
node openai-feed.js --dry-run                     # full catalog, build + write JSON locally
node openai-feed.js --no-send                     # fetch + build + validate, skip the PATCH
node openai-feed.js --since-hours 2               # only products updated in the last 2h
node openai-feed.js                                # full run: fetch → build → PATCH upsert
node --test                                        # transform unit tests
```

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

## First-time setup: OpenAI product feed as a Cloud Run Job

The feed needs the OpenAI commerce API key + feed id (plus the existing Shopify
client secret it shares with the other syncs). **Store them in Secret Manager**
(env vars also work for local runs). Required keys:

| Secret ID | Purpose |
|---|---|
| `openai_product_feed_key` | OpenAI API key, sent as `Authorization: Bearer …` (**required**) |
| `openai_product_feed_id` | the `{id}` in `/product_feeds/{id}/products` (optional; defaults in code to `baseball_lifestyle_101_bl101_shopify_feed`) |
| `openai_api_base` | API base URL (optional; default `https://api.ads.openai.com/v1`) |
| `shopify_client_secret` | Shopify OAuth client secret used to fetch products (already exists) |

`target_country` is the constant `"US"`. The feed id is created once via
`POST {base}/product_feeds` (body `{ "target_country": "US" }`); the current
feed (`baseball_lifestyle_101_bl101_shopify_feed`) is the in-code default, so
only `openai_product_feed_key` is strictly required to send. Until it's set, run
with `--dry-run` (Shopify creds only) to fetch + build + inspect the JSON locally.

```bash
PROJECT=flowing-code-484318-c1
REGION=us-east5
JOB=bl101-openai-product-feed
SCHEDULER_REGION=us-east4
SCHEDULER_JOB=bl101-openai-product-feed-hourly
RUN_SA=shopify-to-bigquery-814@${PROJECT}.iam.gserviceaccount.com
SCHED_SA=$RUN_SA

# 0. Create the secret (run once)
printf '%s' "<value>" | gcloud secrets create openai_product_feed_key --data-file=- --project $PROJECT
# openai_product_feed_id and openai_api_base have in-code defaults; only create
# them as secrets if you need to override. shopify_client_secret already exists.

# 1. Deploy the Job (build context openai-feed/; openai-feed/Dockerfile)
gcloud run jobs deploy $JOB \
  --source openai-feed \
  --region $REGION \
  --task-timeout 1800 \
  --memory 1Gi \
  --service-account $RUN_SA \
  --project $PROJECT

# 2. Grant Scheduler permission to invoke the Job
gcloud run jobs add-iam-policy-binding $JOB \
  --region $REGION \
  --member "serviceAccount:$SCHED_SA" \
  --role roles/run.invoker \
  --project $PROJECT

# 3. Create the hourly Scheduler trigger (feed pulls from Shopify directly, so it
#    is independent of the BigQuery syncs and can run as often as you like)
gcloud scheduler jobs create http $SCHEDULER_JOB \
  --location $SCHEDULER_REGION \
  --schedule "0 * * * *" \
  --time-zone "Etc/UTC" \
  --uri "https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB}:run" \
  --http-method POST \
  --oauth-service-account-email $SCHED_SA \
  --oauth-token-scope https://www.googleapis.com/auth/cloud-platform \
  --project $PROJECT

# 4. Test
gcloud scheduler jobs run $SCHEDULER_JOB --location $SCHEDULER_REGION --project $PROJECT
gcloud run jobs executions list --job $JOB --region $REGION --project $PROJECT
```

The service account also needs `roles/secretmanager.secretAccessor` on the
`openai_product_feed_key` secret (and `openai_product_feed_id` /
`openai_api_base` only if you created them to override the defaults). The
existing sync SA already has Secret Manager access for `shopify_client_secret`;
just grant it the new OpenAI secret.

## Local / CLI usage (Node.js scripts)

```bash
# Incremental sync (yesterday by default)
node daily/products.js

# Sync from a specific date
node daily/products.js --start 2025-01-15

# Full inventory refresh (entire catalog) — catches restocks that don't bump
# product.updated_at and so are missed by the incremental sync
node daily/products.js --refresh-inventory

# Backfill all historical data
node daily/products.js --init
node daily/products.js --backfill
node daily/products.js --status
```

Set `SHOPIFY_CLIENT_SECRET` in your environment or add `shopify_client_secret` to GCP Secret Manager before running locally.
