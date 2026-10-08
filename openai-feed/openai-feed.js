/**
 * BL101 → OpenAI Product Feed (Node.js) — Cloud Run Job + CLI
 *
 * Fetches products + variants DIRECTLY from the Shopify Admin GraphQL API,
 * transforms them into OpenAI's **file-upload/products** schema (a FLAT,
 * one-row-per-variant record set —
 * https://developers.openai.com/commerce/specs/file-upload/products), writes a
 * full-catalog snapshot as CSV, and pushes it to OpenAI over
 * **SFTP** (https://developers.openai.com/commerce/specs/file-upload/overview).
 *
 * Delivery model (per the file-upload spec):
 *   - FULL SNAPSHOT every run — the file is the complete source of truth. There
 *     is no incremental/delta upload; to drop a product, omit it or set
 *     is_eligible_search=false. So this job always exports the whole active
 *     catalog (no --since-hours mode).
 *   - STABLE FILENAME — we always write/overwrite the same object name
 *     (bl101_products.csv) so OpenAI ingests the latest snapshot in place.
 *   - Format CSV (UTF-8), header row + one row per variant. The OpenAI UI
 *     rejects jsonl.gz ("upload a CSV file instead") even though the overview
 *     lists it as supported. Well under the spec's ~500k-row / ~500MB limits.
 *
 * Matches the existing Cloud Run Job pattern (see main.py): self-contained
 * `openai-feed/` folder with its own package.json + Dockerfile (ENTRYPOINT
 * `node`, default CMD `openai-feed.js`), deployed with
 * `gcloud run jobs deploy --source openai-feed`. Runs to completion, exits
 * non-zero on failure; Cloud Scheduler triggers it (recommended cadence: daily).
 *
 * Usage:
 *   node openai-feed.js                  # full catalog: fetch → build → gzip → SFTP upload
 *   node openai-feed.js --dry-run        # fetch + build + write .csv locally, NO upload
 *   node openai-feed.js --no-send        # same as --dry-run (build only, skip upload)
 *   node openai-feed.js --limit 100      # only the first 100 products (sampling)
 *   node openai-feed.js --out ./feed --keep   # where to write the local snapshot file
 *
 * GCP Secrets (or env vars — env wins for local dev):
 *   shopify_client_secret     / SHOPIFY_CLIENT_SECRET     Shopify OAuth client secret (source)
 *   openai_sftp_host          / OPENAI_SFTP_HOST          SFTP host (required to upload)
 *   openai_sftp_port          / OPENAI_SFTP_PORT          SFTP port (optional; default 22)
 *   openai_sftp_username      / OPENAI_SFTP_USERNAME      SFTP username (required to upload)
 *   openai_sftp_password      / OPENAI_SFTP_PASSWORD      SFTP password (required to upload)
 *   openai_sftp_remote_path   / OPENAI_SFTP_REMOTE_PATH   remote directory (optional; default '.')
 *
 * Requirements:
 *   npm install @google-cloud/secret-manager dotenv ssh2-sftp-client
 *   (Node 18+ provides global fetch.)
 */

const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
const SftpClient = require('ssh2-sftp-client');
const fs = require('fs');
const os = require('os');
const path = require('path');

// ============================================
// CONFIGURATION
// ============================================

const GCP_PROJECT = 'flowing-code-484318-c1';

// Shopify source (mirrors daily/products.js).
const SHOP_NAME = 'bl101';
const SHOPIFY_CLIENT_ID = '5c23ff36589a5c4ec853e23b60ca305c';
const GRAPHQL_URL = `https://${SHOP_NAME}.myshopify.com/admin/api/2025-01/graphql.json`;
const PAGE_SIZE = 100;            // products per GraphQL page
const VARIANTS_PER_PRODUCT = 100; // variants fetched per product
const THROTTLE_DELAY_MS = 200;    // between product pages

// Production storefront (confirmed via Shopify shop info: www.bl101.com).
const STOREFRONT_DOMAIN = 'www.bl101.com';
const STOREFRONT_BASE = `https://${STOREFRONT_DOMAIN}`;

// Stable snapshot filename — OVERWRITTEN on every run so OpenAI ingests the
// latest full snapshot in place. Brand-prefixed per the file-upload overview.
// NOTE: the OpenAI UI rejects jsonl.gz ("Unsupported file type — upload a CSV
// file instead") despite the overview listing jsonl.gz as supported, so we
// emit an uncompressed CSV.
const FEED_FILENAME = 'bl101_products.csv';

// Feed-level constants (file-upload/products schema).
const CURRENCY = 'USD';                 // ISO 4217
const TARGET_COUNTRIES = ['US'];        // ISO 3166-1 alpha-2
const STORE_COUNTRY = 'US';             // ISO 3166-1 alpha-2
const BRAND = 'Baseball Lifestyle 101';
const SELLER_NAME = 'Baseball Lifestyle 101';
const SELLER_URL = STOREFRONT_BASE;

// Checkout eligibility: products are listed for Instant Checkout. When
// is_eligible_checkout=true the spec REQUIRES seller_privacy_policy, seller_tos
// and return_policy — always emitted below.
const IS_ELIGIBLE_CHECKOUT = true;
const SELLER_PRIVACY_POLICY = `${STOREFRONT_BASE}/policies/privacy-policy`;
const SELLER_TOS = `${STOREFRONT_BASE}/policies/terms-of-service`;
const RETURN_POLICY = `${STOREFRONT_BASE}/policies/refund-policy`;

// Ads eligibility: eligible products are also opted into ads (all three
// eligibility flags move together). Flip to false to keep products out of ads
// while still search/checkout eligible.
const IS_ELIGIBLE_ADS = true;

// availability enum (file-upload/products): in_stock | out_of_stock | pre_order
// | backorder | unknown. (Note underscore in pre_order; NOT the api/products
// spelling.) We only emit the three derivable from Shopify inventory.
const STATUS_IN_STOCK = 'in_stock';
const STATUS_BACKORDER = 'backorder';
const STATUS_OUT_OF_STOCK = 'out_of_stock';

// Field length caps from the spec (truncate rather than drop the record).
const MAX_TITLE = 150;
const MAX_DESCRIPTION = 5000;
const MAX_ITEM_ID = 100;

// Products carrying this tag (case-insensitive) are suppressed the spec's way:
// is_eligible_search=false + is_eligible_checkout=false (they stay in the
// snapshot but ChatGPT won't surface or sell them).
const HIDDEN_TAG = 'hidden';

// ============================================
// LOGGING (matches products.js)
// ============================================

const logger = {
  info: (...args) => console.log(new Date().toISOString(), 'INFO:', ...args),
  warn: (...args) => console.log(new Date().toISOString(), 'WARN:', ...args),
  error: (...args) => console.error(new Date().toISOString(), 'ERROR:', ...args),
};

// ============================================
// CONFIG RESOLUTION (env first, then Secret Manager)
// ============================================

const secretManagerClient = new SecretManagerServiceClient();

async function getSecret(secretId) {
  const name = `projects/${GCP_PROJECT}/secrets/${secretId}/versions/latest`;
  const [version] = await secretManagerClient.accessSecretVersion({ name });
  return version.payload.data.toString('utf8');
}

/** Resolve a config value: env var wins (local dev), then Secret Manager. */
async function getConfigValue(envName, secretId) {
  if (process.env[envName]) return process.env[envName];
  try {
    return await getSecret(secretId);
  } catch (e) {
    return null;
  }
}

// ============================================
// SHOPIFY: OAuth + GraphQL fetch (ported from daily/products.js)
// ============================================

function extractLegacyId(gid) {
  if (!gid) return null;
  if (typeof gid === 'string' && gid.startsWith('gid://')) return gid.split('/').pop();
  return String(gid);
}

async function getShopifyAccessToken() {
  const clientSecret = await getConfigValue('SHOPIFY_CLIENT_SECRET', 'shopify_client_secret');
  if (!clientSecret) {
    throw new Error('Missing Shopify client secret (set SHOPIFY_CLIENT_SECRET or shopify_client_secret).');
  }

  const response = await fetch(`https://${SHOP_NAME}.myshopify.com/admin/oauth/access_token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      client_id: SHOPIFY_CLIENT_ID,
      client_secret: clientSecret,
      grant_type: 'client_credentials',
    }),
  });

  if (!response.ok) {
    throw new Error(`Shopify OAuth failed: HTTP ${response.status} ${await response.text()}`);
  }
  const data = await response.json();
  if (!data.access_token) throw new Error('No access_token in Shopify OAuth response');
  logger.info('Got Shopify access token');
  return data.access_token;
}

const PRODUCTS_QUERY = `
query GetProducts($cursor: String, $query: String) {
  products(first: ${PAGE_SIZE}, after: $cursor, query: $query) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        onlineStoreUrl
        publishedAt
        title
        productType
        handle
        status
        tags
        description
        descriptionHtml
        featuredImage { url }
        soldOutOnline: metafield(namespace: "custom", key: "sold_out_online") { value }
        variants(first: ${VARIANTS_PER_PRODUCT}) {
          edges {
            node {
              id
              title
              price
              position
              sku
              inventoryQuantity
              inventoryPolicy
              compareAtPrice
              barcode
              selectedOptions { name value }
              onlineInventory: metafield(namespace: "custom", key: "online_inventory") { value }
            }
          }
        }
      }
    }
  }
}
`;

async function executeGraphQL(accessToken, query, variables) {
  const response = await fetch(GRAPHQL_URL, {
    method: 'POST',
    headers: { 'X-Shopify-Access-Token': accessToken, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, variables }),
  });
  if (!response.ok) return { data: null, error: `HTTP ${response.status}: ${await response.text()}` };
  const result = await response.json();
  if (result.errors) return { data: null, error: `GraphQL Errors: ${JSON.stringify(result.errors)}` };
  return { data: result.data, error: null };
}

/**
 * Normalize a Shopify product node into the flat "row" shape the transform
 * consumes (product columns + a `variants` array with real option names).
 */
function normalizeProduct(node) {
  const variants = (node.variants?.edges || []).map((e) => {
    const v = e.node;
    return {
      id: extractLegacyId(v.id),
      sku: v.sku,
      title: v.title,
      price: v.price,
      compare_at_price: v.compareAtPrice,
      barcode: v.barcode,
      inventory_quantity: v.inventoryQuantity,
      inventory_policy: (v.inventoryPolicy || '').toLowerCase(),
      options: v.selectedOptions || [],
      position: v.position,
      // custom.online_inventory (integer): the online-store sellable qty. null
      // when the metafield is unset — the eligibility gate only fires on a
      // present value <= 0.
      online_inventory: parseIntOrNull(v.onlineInventory?.value),
    };
  });
  return {
    id: extractLegacyId(node.id),
    title: node.title,
    handle: node.handle,
    store_url: node.onlineStoreUrl,
    product_type: node.productType,
    image: node.featuredImage?.url || null,
    description_plain: node.description || null,
    description_html: node.descriptionHtml || null,
    tags: node.tags || [],
    hidden: hasHiddenTag(node.tags),
    // Online Store sales-channel membership: publishedAt is null when the
    // product isn't published to the Online Store channel.
    published_online: !!node.publishedAt,
    // custom.sold_out_online (boolean metafield stores "true"/"false").
    sold_out_online: node.soldOutOnline?.value === 'true',
    variants,
  };
}

/** Fetch all active products, normalized. Full catalog (optionally limited). */
async function fetchShopifyRows(accessToken, { limit = null } = {}) {
  const queryFilter = 'status:active';
  logger.info(`Shopify query filter: ${queryFilter}`);

  const rows = [];
  let cursor = null;
  let hasNextPage = true;
  let pageNum = 0;
  const fetchStart = Date.now();

  while (hasNextPage) {
    pageNum++;
    const { data, error } = await executeGraphQL(accessToken, PRODUCTS_QUERY, { cursor, query: queryFilter });
    if (error) throw new Error(`Shopify API Error: ${error}`);

    const productsData = data?.products || {};
    const edges = productsData.edges || [];
    for (const edge of edges) rows.push(normalizeProduct(edge.node));

    logger.info(`Page ${pageNum}: fetched ${edges.length} products (${rows.length} total)`);

    if (limit && rows.length >= limit) {
      rows.length = limit;
      break;
    }

    hasNextPage = productsData.pageInfo?.hasNextPage || false;
    cursor = productsData.pageInfo?.endCursor;
    if (hasNextPage) await new Promise((r) => setTimeout(r, THROTTLE_DELAY_MS));
  }

  logger.info(`Fetched ${rows.length} products in ${((Date.now() - fetchStart) / 1000).toFixed(2)}s`);
  return rows;
}

// ============================================
// TRANSFORM HELPERS (pure, unit-testable)
// ============================================

/** Parse a value to an integer, or null if missing/non-numeric. */
function parseIntOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const num = typeof value === 'number' ? value : parseInt(String(value), 10);
  return Number.isFinite(num) ? Math.trunc(num) : null;
}

/** Parse a price to a finite non-negative Number, or null. */
function parsePrice(value) {
  if (value === null || value === undefined || value === '') return null;
  const num = typeof value === 'number' ? value : parseFloat(String(value));
  if (!Number.isFinite(num) || num < 0) return null;
  return num;
}

/** Format a numeric amount as the spec's "29.99 USD" price string, or null. */
function formatPrice(value) {
  const num = parsePrice(value);
  if (num === null) return null;
  return `${num.toFixed(2)} ${CURRENCY}`;
}

/**
 * Resolve regular price + optional sale price from Shopify's price /
 * compareAtPrice. In the file-upload schema `price` is the REGULAR price and
 * `sale_price` (<= price) is the current discounted price. Shopify's `price` is
 * what the customer pays now; `compareAtPrice` is the "was" price when on sale.
 *   - on sale (compareAt > price): price = compareAt, sale_price = price
 *   - not on sale:                 price = price,     no sale_price
 * Returns { price, sale_price } as formatted strings, or null if unsellable.
 */
function resolvePricing(variant) {
  const current = parsePrice(variant.price);
  if (current === null || current <= 0) return null;
  const compareAt = parsePrice(variant.compare_at_price);
  if (compareAt !== null && compareAt > current) {
    return { price: formatPrice(compareAt), sale_price: formatPrice(current) };
  }
  return { price: formatPrice(current), sale_price: null };
}

/**
 * Map Shopify inventory → availability enum string.
 *   qty > 0                    → in_stock
 *   qty <= 0 & policy=continue  → backorder (oversell allowed)
 *   qty <= 0 & policy=deny       → out_of_stock
 */
function mapAvailabilityStatus(inventoryQuantity, inventoryPolicy) {
  const qty = Number.isFinite(Number(inventoryQuantity)) ? Math.trunc(Number(inventoryQuantity)) : 0;
  const oversell = String(inventoryPolicy || '').toLowerCase() === 'continue';
  if (qty > 0) return STATUS_IN_STOCK;
  if (oversell) return STATUS_BACKORDER;
  return STATUS_OUT_OF_STOCK;
}

/** True if a product's tags (array or comma-joined string) contain `hidden`. */
function hasHiddenTag(tags) {
  if (!tags) return false;
  const list = Array.isArray(tags) ? tags : String(tags).split(',');
  return list.map((t) => String(t).trim().toLowerCase()).includes(HIDDEN_TAG);
}

// Fallback option-name inference for variants without real Shopify option names
// (kept for the old option1/2/3 shape). Apparel sizes dominate the catalog.
const SIZE_VALUES = new Set([
  'xxs', 'xs', 's', 'm', 'l', 'xl', 'xxl', 'xxxl',
  '2xl', '3xl', '4xl', '5xl', '6xl', 'os', 'o/s', 'one size',
]);

function inferOptionName(value) {
  const v = String(value).trim().toLowerCase();
  if (SIZE_VALUES.has(v)) return 'size';
  if (/^y(?:outh)?\s*(?:xs|s|m|l|xl|xxl)$/.test(v)) return 'size'; // youth sizes
  if (/^\d{1,2}(?:\.\d)?$/.test(v)) return 'size';                  // numeric / shoe sizes
  if (/^\d{2}\/\d{2}$/.test(v)) return 'size';                       // waist/length
  return 'style';
}

/**
 * Build normalized option pairs [{name, value}]. Prefers real Shopify option
 * names (`variant.options` = selectedOptions); falls back to option1/2/3 with
 * inferred names. Shopify's synthetic "Title / Default Title" option is dropped.
 */
function buildVariantOptions(variant) {
  const opts = [];
  if (Array.isArray(variant.options) && variant.options.length > 0) {
    for (const o of variant.options) {
      if (!o) continue;
      const value = o.value != null ? String(o.value).trim() : '';
      if (value === '' || value.toLowerCase() === 'default title') continue;
      const name = o.name && String(o.name).trim();
      opts.push({ name: name || inferOptionName(value), value });
    }
    if (opts.length > 0) return opts;
  }
  for (const raw of [variant.option1, variant.option2, variant.option3]) {
    if (raw === null || raw === undefined || String(raw).trim() === '') continue;
    const value = String(raw).trim();
    if (value.toLowerCase() === 'default title') continue;
    opts.push({ name: inferOptionName(value), value });
  }
  return opts;
}

/**
 * Derive the file-upload variant attributes from option pairs:
 *   - variant_dict: {name: value} object (string→string)
 *   - size / color: pulled out by option-name match (recommended for apparel)
 */
function variantAttributes(options) {
  const dict = {};
  let size = null;
  let color = null;
  for (const { name, value } of options) {
    dict[name] = value;
    const lname = String(name).toLowerCase();
    if (size === null && lname.includes('size')) size = value;
    if (color === null && (lname.includes('color') || lname.includes('colour'))) color = value;
  }
  return { dict, size, color };
}

/**
 * Eligibility gate for search / checkout / ads. A variant is eligible only when:
 *   - the product is NOT `hidden`-tagged, AND
 *   - the product IS published to the Online Store channel (published_online), AND
 *   - the product is NOT flagged custom.sold_out_online = true, AND
 *   - the variant's custom.online_inventory (when set) is > 0.
 * `published_online`/`sold_out_online` default to eligible when absent (undefined),
 * so real Shopify rows — which always carry them — gate correctly, while terse
 * test/synthetic rows stay eligible. Returns { eligible, reason }.
 */
function computeEligibility(ctx, variant) {
  if (ctx.hidden) return { eligible: false, reason: 'hidden tag' };
  if (ctx.published_online === false) return { eligible: false, reason: 'not published to Online Store' };
  if (ctx.sold_out_online === true) return { eligible: false, reason: 'sold_out_online=true' };
  const inv = variant.online_inventory;
  if (inv !== null && inv !== undefined && inv <= 0) return { eligible: false, reason: `online_inventory<=0 (${inv})` };
  return { eligible: true, reason: null };
}

/** Clamp a string to `max` chars (spec length caps); passthrough for falsy. */
function clamp(str, max) {
  if (str == null) return str;
  const s = String(str);
  return s.length > max ? s.slice(0, max) : s;
}

/** Valid GTIN (8-14 digits, no dashes/spaces) from a Shopify barcode, or null. */
function gtinFromBarcode(barcode) {
  if (barcode == null) return null;
  const digits = String(barcode).trim();
  return /^\d{8,14}$/.test(digits) ? digits : null;
}

// ============================================
// TRANSFORM: normalized row → flat product records (one per variant)
// ============================================

/**
 * Build one flat file-upload record for a single variant, or null if the
 * variant isn't sellable (no valid positive price, or no product image — both
 * required by the spec). `reasons` collects skip messages; `seen` enforces
 * feed-wide unique item_id. When `hidden`, search + checkout eligibility are
 * turned off (the spec's way to suppress a product).
 */
function buildVariantRecord(variant, ctx, reasons, seen) {
  const shopifyVariantId = variant.id != null ? String(variant.id) : null;

  // item_id: SKU preferred, else Shopify variant id. Must be unique across the
  // whole feed — SKUs aren't always unique (shared gift-card SKU), so on a
  // collision suffix the stable variant id.
  const sku = variant.sku != null ? String(variant.sku).trim() : '';
  let itemId = sku !== '' ? sku : shopifyVariantId;
  if (!itemId) {
    reasons.push(`variant ${shopifyVariantId || '(unknown)'}: no SKU or variant id`);
    return null;
  }
  if (seen && seen.has(itemId)) {
    const unique = `${itemId}-${shopifyVariantId}`;
    reasons.push(`dup item_id ${itemId} in product ${ctx.productId} → ${unique}`);
    itemId = unique;
  }
  itemId = clamp(itemId, MAX_ITEM_ID);
  if (seen && seen.has(itemId)) {
    reasons.push(`variant ${shopifyVariantId}: item_id ${itemId} still colliding — skipped`);
    return null;
  }

  const pricing = resolvePricing(variant);
  if (!pricing) {
    reasons.push(`variant ${itemId}: missing/invalid price (${variant.price})`);
    return null;
  }

  // image_url is required by the spec — no product image ⇒ can't emit the row.
  if (!ctx.image) {
    reasons.push(`variant ${itemId}: no product image (image_url required) — skipped`);
    return null;
  }

  if (seen) seen.add(itemId);

  const options = buildVariantOptions(variant);
  const { dict, size, color } = variantAttributes(options);
  const { eligible } = computeEligibility(ctx, variant);

  const url = ctx.url && shopifyVariantId ? `${ctx.url}?variant=${shopifyVariantId}` : ctx.url;

  const record = {
    // --- required ---
    is_eligible_search: eligible,
    is_eligible_checkout: eligible && IS_ELIGIBLE_CHECKOUT,
    is_eligible_ads: eligible && IS_ELIGIBLE_ADS,
    item_id: itemId,
    title: ctx.title,
    description: ctx.description,
    url,
    brand: BRAND,
    image_url: ctx.image,
    price: pricing.price,
    availability: mapAvailabilityStatus(variant.inventory_quantity, variant.inventory_policy),
    seller_name: SELLER_NAME,
    seller_url: SELLER_URL,
    target_countries: TARGET_COUNTRIES,
    store_country: STORE_COUNTRY,
    // --- checkout-eligibility requirements (always safe to include) ---
    seller_privacy_policy: SELLER_PRIVACY_POLICY,
    seller_tos: SELLER_TOS,
    return_policy: RETURN_POLICY,
    // --- item info ---
    condition: 'new',
    // --- variant grouping ---
    group_id: ctx.productId,
    listing_has_variations: ctx.hasVariations,
    item_group_title: ctx.title,
    offer_id: itemId,
  };

  if (pricing.sale_price) record.sale_price = pricing.sale_price;

  const gtin = gtinFromBarcode(variant.barcode);
  if (gtin) record.gtin = gtin;

  if (ctx.productType) record.product_category = ctx.productType;

  if (Object.keys(dict).length > 0) record.variant_dict = dict;
  if (size) { record.size = clamp(size, 20); record.size_system = STORE_COUNTRY; }
  if (color) record.color = clamp(color, 40);

  return record;
}

/**
 * Build all flat records for one normalized product row.
 * Returns { records, skipped, hidden }.
 */
function buildProductRecords(row, seen) {
  const skipped = [];
  const productId = row.id != null ? String(row.id) : null;
  if (!productId) return { records: [], skipped: ['product: missing id'], hidden: false };

  const title = clamp(row.title, MAX_TITLE);
  if (!title) return { records: [], skipped: [`product ${productId}: missing title`], hidden: false };

  // description is required (plain text). Fall back to the title if absent.
  const description = clamp((row.description_plain && String(row.description_plain).trim()) || title, MAX_DESCRIPTION);

  const url = row.store_url || (row.handle ? `${STOREFRONT_BASE}/products/${row.handle}` : null);
  const hidden = !!row.hidden;
  const variantRows = row.variants || [];

  const ctx = {
    productId,
    title,
    description,
    url,
    image: row.image || null,
    productType: row.product_type && String(row.product_type).trim() ? String(row.product_type).trim() : null,
    hidden,
    published_online: row.published_online,
    sold_out_online: row.sold_out_online,
    hasVariations: variantRows.length > 1,
  };

  const records = [];
  for (const v of variantRows) {
    const rec = buildVariantRecord(v, ctx, skipped, seen);
    if (rec) records.push(rec);
  }

  return { records, skipped, hidden };
}

// ============================================
// VALIDATION (final gate before write)
// ============================================

const VALID_STATUSES = new Set([
  'in_stock', 'out_of_stock', 'pre_order', 'backorder', 'unknown',
]);
const PRICE_RE = /^\d+\.\d{2} [A-Z]{3}$/;
const REQUIRED_STRING_FIELDS = [
  'item_id', 'title', 'description', 'url', 'brand', 'image_url',
  'price', 'availability', 'seller_name', 'seller_url', 'store_country',
];

function priceAmount(str) {
  return parseFloat(String(str).split(' ')[0]);
}

/** Returns an array of error strings; empty == valid. */
function validateRecord(rec) {
  const errors = [];
  if (!rec || typeof rec !== 'object') return ['record is not an object'];
  const tag = rec.item_id ? `item ${rec.item_id}` : '(no item_id)';

  for (const f of REQUIRED_STRING_FIELDS) {
    if (!rec[f] || typeof rec[f] !== 'string') errors.push(`${tag}: ${f} missing`);
  }
  if (typeof rec.is_eligible_search !== 'boolean') errors.push(`${tag}: is_eligible_search not boolean`);
  if (typeof rec.is_eligible_checkout !== 'boolean') errors.push(`${tag}: is_eligible_checkout not boolean`);
  if (typeof rec.is_eligible_ads !== 'boolean') errors.push(`${tag}: is_eligible_ads not boolean`);
  if (rec.is_eligible_checkout && !rec.is_eligible_search) errors.push(`${tag}: checkout=true requires search=true`);

  if (!Array.isArray(rec.target_countries) || rec.target_countries.length === 0) {
    errors.push(`${tag}: target_countries must be a non-empty array`);
  }

  for (const f of ['url', 'seller_url', 'image_url']) {
    if (rec[f] && !/^https:\/\//.test(rec[f])) errors.push(`${tag}: ${f} not https`);
  }

  if (rec.price && !PRICE_RE.test(rec.price)) errors.push(`${tag}: price bad format (${rec.price})`);
  if (rec.sale_price) {
    if (!PRICE_RE.test(rec.sale_price)) errors.push(`${tag}: sale_price bad format (${rec.sale_price})`);
    else if (priceAmount(rec.sale_price) > priceAmount(rec.price)) errors.push(`${tag}: sale_price > price`);
  }

  if (rec.availability && !VALID_STATUSES.has(rec.availability)) {
    errors.push(`${tag}: bad availability ${rec.availability}`);
  }

  // Checkout eligibility requires the seller policy URLs.
  if (rec.is_eligible_checkout) {
    for (const f of ['seller_privacy_policy', 'seller_tos', 'return_policy']) {
      if (!rec[f] || !/^https:\/\//.test(rec[f])) errors.push(`${tag}: ${f} required (https) when checkout-eligible`);
    }
  }

  // Length caps.
  if (rec.title && rec.title.length > MAX_TITLE) errors.push(`${tag}: title > ${MAX_TITLE}`);
  if (rec.description && rec.description.length > MAX_DESCRIPTION) errors.push(`${tag}: description > ${MAX_DESCRIPTION}`);
  if (rec.item_id && rec.item_id.length > MAX_ITEM_ID) errors.push(`${tag}: item_id > ${MAX_ITEM_ID}`);
  if (rec.gtin && !/^\d{8,14}$/.test(rec.gtin)) errors.push(`${tag}: gtin bad format`);

  return errors;
}

// ============================================
// BUILD: normalized rows → validated flat records
// ============================================

/**
 * Transform + validate all rows into flat records. Bad variants/records are
 * logged and skipped (never abort the run). Returns { records, stats }.
 */
function buildFeed(rows) {
  const records = [];
  const seen = new Set();
  const stats = {
    productsIn: rows.length,
    productsWithRecords: 0,
    productsHidden: 0,
    productsNotPublished: 0,
    productsSoldOut: 0,
    productsSkipped: 0,
    variantsIn: 0,
    recordsOut: 0,
    recordsEligible: 0,
    recordsIneligible: 0,
    recordsSkipped: 0,
    validationFailures: 0,
  };

  for (const row of rows) {
    stats.variantsIn += (row.variants || []).length;
    const { records: recs, skipped, hidden } = buildProductRecords(row, seen);
    stats.recordsSkipped += skipped.filter((s) => s.startsWith('variant') || s.startsWith('dup')).length;
    if (hidden) stats.productsHidden += 1;
    if (row.published_online === false) stats.productsNotPublished += 1;
    if (row.sold_out_online === true) stats.productsSoldOut += 1;
    for (const s of skipped) logger.warn(`skip: ${s}`);

    let kept = 0;
    for (const rec of recs) {
      const errors = validateRecord(rec);
      if (errors.length > 0) {
        stats.recordsSkipped += 1;
        stats.validationFailures += 1;
        logger.warn(`skip record ${rec.item_id} (validation): ${errors.join('; ')}`);
        continue;
      }
      records.push(rec);
      if (rec.is_eligible_search) stats.recordsEligible += 1;
      else stats.recordsIneligible += 1;
      kept += 1;
    }

    if (kept > 0) {
      stats.productsWithRecords += 1;
      stats.recordsOut += kept;
    } else {
      stats.productsSkipped += 1;
    }
  }

  return { records, stats };
}

// ============================================
// SERIALIZE: records → CSV snapshot file
// ============================================

// Fixed column order. Every record emits every column (empty when absent) so
// the CSV is rectangular. List/object fields are serialized into the cell:
// target_countries → comma-joined, variant_dict → JSON text.
const CSV_COLUMNS = [
  'is_eligible_search', 'is_eligible_checkout', 'is_eligible_ads',
  'item_id', 'title', 'description', 'url', 'brand', 'image_url',
  'price', 'sale_price', 'availability', 'condition', 'gtin', 'product_category',
  'seller_name', 'seller_url', 'seller_privacy_policy', 'seller_tos', 'return_policy',
  'target_countries', 'store_country',
  'group_id', 'listing_has_variations', 'item_group_title', 'offer_id',
  'variant_dict', 'size', 'size_system', 'color',
];

/** Render one value as a CSV cell (RFC 4180 quoting only when needed). */
function csvCell(value) {
  if (value === null || value === undefined) return '';
  let s;
  if (typeof value === 'boolean') s = value ? 'true' : 'false';
  else if (Array.isArray(value)) s = value.join(',');
  else if (typeof value === 'object') s = JSON.stringify(value);
  else s = String(value);
  if (/[",\n\r]/.test(s)) s = `"${s.replace(/"/g, '""')}"`;
  return s;
}

/** Serialize records to a CSV string (header row + one row per record). */
function toCsv(records) {
  const lines = [CSV_COLUMNS.join(',')];
  for (const r of records) lines.push(CSV_COLUMNS.map((c) => csvCell(r[c])).join(','));
  return `${lines.join('\n')}\n`;
}

/** Write records as a UTF-8 CSV file to `outPath`. Returns bytes written. */
function writeSnapshot(records, outPath) {
  const csv = toCsv(records);
  fs.writeFileSync(outPath, csv, 'utf8');
  return Buffer.byteLength(csv, 'utf8');
}

// ============================================
// DELIVERY: push snapshot to OpenAI over SFTP (stable filename, overwrite)
// ============================================

async function uploadSnapshot(localPath, { host, port, username, password, remotePath }) {
  const sftp = new SftpClient();
  const remoteFile = `${String(remotePath || '.').replace(/\/+$/, '')}/${FEED_FILENAME}`;
  try {
    await sftp.connect({ host, port: port ? parseInt(port, 10) : 22, username, password });
    await sftp.put(localPath, remoteFile);
    logger.info(`SFTP upload OK: ${host}:${remoteFile}`);
    return remoteFile;
  } finally {
    try { await sftp.end(); } catch (e) { /* ignore close errors */ }
  }
}

// ============================================
// MAIN RUN
// ============================================

async function run(opts = {}) {
  const startTime = Date.now();
  const dryRun = !!opts.dryRun;
  const noSend = !!opts.noSend || dryRun;

  logger.info('='.repeat(60));
  logger.info('BL101 → OPENAI PRODUCT FEED (file-upload/products, Shopify → SFTP)');
  logger.info(`mode: ${noSend ? 'BUILD ONLY (no upload)' : 'FULL (build + SFTP upload)'}`);
  logger.info('scope: FULL CATALOG SNAPSHOT (active products)');
  logger.info('='.repeat(60));

  // 1. Fetch from Shopify (always the full active catalog — snapshot feed).
  const accessToken = await getShopifyAccessToken();
  const rows = await fetchShopifyRows(accessToken, { limit: opts.limit });

  // 2. Transform + validate → flat records.
  const { records, stats } = buildFeed(rows);

  // 3. Write the gzipped JSON-lines snapshot locally.
  const outDir = opts.outDir || fs.mkdtempSync(path.join(os.tmpdir(), 'openai-feed-'));
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, FEED_FILENAME);
  const bytes = writeSnapshot(records, outPath);
  logger.info(`Wrote ${records.length} records → ${outPath} (${(bytes / 1024 / 1024).toFixed(2)} MB CSV)`);

  // 4. Upload via SFTP (unless dry-run / --no-send).
  let uploaded = null;
  if (!noSend) {
    if (records.length === 0) throw new Error('Refusing to upload an empty snapshot (0 records).');
    const [host, port, username, password, remotePath] = await Promise.all([
      getConfigValue('OPENAI_SFTP_HOST', 'openai_sftp_host'),
      getConfigValue('OPENAI_SFTP_PORT', 'openai_sftp_port'),
      getConfigValue('OPENAI_SFTP_USERNAME', 'openai_sftp_username'),
      getConfigValue('OPENAI_SFTP_PASSWORD', 'openai_sftp_password'),
      getConfigValue('OPENAI_SFTP_REMOTE_PATH', 'openai_sftp_remote_path'),
    ]);
    const missing = [];
    if (!host) missing.push('OPENAI_SFTP_HOST/openai_sftp_host');
    if (!username) missing.push('OPENAI_SFTP_USERNAME/openai_sftp_username');
    if (!password) missing.push('OPENAI_SFTP_PASSWORD/openai_sftp_password');
    if (missing.length) throw new Error(`Missing SFTP config: ${missing.join(', ')}`);

    logger.info(`Uploading snapshot to ${host} (remote path: ${remotePath || '.'})...`);
    uploaded = await uploadSnapshot(outPath, { host, port, username, password, remotePath });
  }

  // Clean up the temp file unless the caller asked to keep it / chose the dir.
  if (!opts.outDir && !opts.keep) {
    try { fs.rmSync(outDir, { recursive: true, force: true }); } catch (e) { /* ignore */ }
  }

  const totalTime = (Date.now() - startTime) / 1000;
  logger.info('='.repeat(60));
  logger.info('FEED COMPLETE');
  logger.info(`Products: ${stats.productsWithRecords} with records, ${stats.productsSkipped} skipped (of ${stats.productsIn})`);
  logger.info(`Records: ${stats.recordsOut} written (of ${stats.variantsIn} variants); ${stats.recordsSkipped} skipped`);
  logger.info(`Eligibility: ${stats.recordsEligible} eligible, ${stats.recordsIneligible} ineligible (search/checkout/ads off)`);
  logger.info(`  gated → ${stats.productsNotPublished} products not in Online Store, ${stats.productsSoldOut} sold_out_online, ${stats.productsHidden} hidden-tag, plus per-variant online_inventory<=0`);
  logger.info(`Validation failures: ${stats.validationFailures}`);
  if (!noSend) logger.info(`Uploaded: ${uploaded}`);
  logger.info(`Total time: ${totalTime.toFixed(2)}s`);
  logger.info('='.repeat(60));

  return { stats, outPath, uploaded };
}

// ============================================
// CLI
// ============================================

function parseArgs(argv) {
  const opts = {
    dryRun: argv.includes('--dry-run'),
    noSend: argv.includes('--no-send'),
    keep: argv.includes('--keep'),
    limit: null,
    outDir: null,
  };
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === '--limit' && argv[i + 1]) opts.limit = parseInt(argv[i + 1], 10);
    if (argv[i] === '--out' && argv[i + 1]) opts.outDir = argv[i + 1];
  }
  return opts;
}

if (require.main === module) {
  // Load .env from cwd first, then fill any gaps from ../daily/.env (where the
  // shared Shopify/OpenAI secrets live) so local runs work from either folder.
  try {
    const dotenv = require('dotenv');
    dotenv.config();
    dotenv.config({ path: path.join(__dirname, '..', 'daily', '.env') });
  } catch (e) { /* optional */ }

  run(parseArgs(process.argv))
    .then(() => process.exit(0))
    .catch((e) => {
      logger.error(e.message);
      logger.error(e.stack);
      process.exit(1);
    });
}

// Exported for unit tests.
module.exports = {
  parseIntOrNull,
  parsePrice,
  formatPrice,
  resolvePricing,
  mapAvailabilityStatus,
  hasHiddenTag,
  computeEligibility,
  inferOptionName,
  buildVariantOptions,
  variantAttributes,
  gtinFromBarcode,
  buildVariantRecord,
  buildProductRecords,
  validateRecord,
  buildFeed,
  csvCell,
  toCsv,
  writeSnapshot,
  normalizeProduct,
};
