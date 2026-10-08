/**
 * One-off: Set each product's custom.sold_out_online metafield based on whether
 * any of its variants are in stock online.
 *
 * What it does:
 *   1. Authenticates with Shopify via OAuth client-credentials (same as daily/products.js).
 *   2. Paginates through ALL products, and for each product reads every variant's
 *      custom.online_inventory metafield.
 *   3. If ANY variant has custom.online_inventory > 0, the product is considered in
 *      stock online  -> custom.sold_out_online = false.
 *      Otherwise (all variants 0 / empty)               -> custom.sold_out_online = true.
 *   4. Writes the boolean into custom.sold_out_online using the metafieldsSet
 *      mutation, batched 25 metafields per request.
 *
 * Products whose metafield already matches the computed value are skipped, so
 * re-runs are cheap and idempotent.
 *
 * Usage:
 *   cd one-off && npm install
 *   node product-sold-out-online.js                 # apply updates
 *   node product-sold-out-online.js --dry-run       # report only, write nothing
 *
 * Auth:
 *   Reads SHOPIFY_CLIENT_SECRET from env / .env (this dir or ../daily/.env),
 *   falling back to GCP Secret Manager secret "shopify_client_secret".
 *
 * Requirements:
 *   npm install @google-cloud/secret-manager dotenv
 */

const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
const fs = require('fs');
const path = require('path');
const os = require('os');

// ============================================
// CONFIGURATION
// ============================================

const GCP_PROJECT = 'flowing-code-484318-c1';

const SHOP_NAME = 'bl101';
const API_VERSION = '2025-01';
const GRAPHQL_URL = `https://${SHOP_NAME}.myshopify.com/admin/api/${API_VERSION}/graphql.json`;

// Source metafield (read, per variant) and target metafield (write, per product)
const VARIANT_MF_NAMESPACE = 'custom';
const VARIANT_MF_KEY = 'online_inventory';
const PRODUCT_MF_NAMESPACE = 'custom';
const PRODUCT_MF_KEY = 'sold_out_online';
const PRODUCT_MF_TYPE = 'boolean';

// Batching / throttling
const PRODUCT_PAGE_SIZE = 50;     // products fetched per page
const VARIANT_PAGE_SIZE = 100;    // variants fetched per product (Shopify max per product)
const METAFIELD_BATCH_SIZE = 25;  // max metafields per metafieldsSet call
const THROTTLE_DELAY_MS = 250;    // pause between Shopify calls
const MAX_THROTTLE_RETRIES = 6;   // retries when Shopify reports THROTTLED

// Shopify OAuth settings (client-credentials flow, mirrors daily/products.js)
const SHOPIFY_CLIENT_ID = '5c23ff36589a5c4ec853e23b60ca305c';
const SHOPIFY_TOKEN_CACHE_FILE = path.join(os.homedir(), '.shopify_token_cache.json');
const SHOPIFY_TOKEN_REFRESH_BUFFER_SECONDS = 300;

// ============================================
// LOGGING
// ============================================

const logger = {
  info: (...args) => console.log(new Date().toISOString(), 'INFO:', ...args),
  warn: (...args) => console.log(new Date().toISOString(), 'WARN:', ...args),
  error: (...args) => console.error(new Date().toISOString(), 'ERROR:', ...args)
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ============================================
// AUTH (mirrors daily/products.js)
// ============================================

const secretManagerClient = new SecretManagerServiceClient();

async function getSecret(secretId) {
  const name = `projects/${GCP_PROJECT}/secrets/${secretId}/versions/latest`;
  const [version] = await secretManagerClient.accessSecretVersion({ name });
  return version.payload.data.toString('utf8');
}

function loadCachedToken() {
  try {
    if (fs.existsSync(SHOPIFY_TOKEN_CACHE_FILE)) {
      const cache = JSON.parse(fs.readFileSync(SHOPIFY_TOKEN_CACHE_FILE, 'utf8'));
      const expiresAt = new Date(cache.expires_at);
      const secondsUntilExpiry = (expiresAt - new Date()) / 1000;

      if (secondsUntilExpiry > SHOPIFY_TOKEN_REFRESH_BUFFER_SECONDS) {
        logger.info(`  Token valid for ${Math.floor(secondsUntilExpiry / 60)} more minutes`);
        return { accessToken: cache.access_token, expiresAt };
      }
      logger.info(`  Cached token expires in ${Math.floor(secondsUntilExpiry)}s, refreshing...`);
    }
  } catch (e) {
    logger.info(`  Could not load cached token: ${e.message}`);
  }
  return { accessToken: null, expiresAt: null };
}

function saveCachedToken(accessToken, expiresIn) {
  try {
    const expiresAt = new Date(Date.now() + expiresIn * 1000);
    const cache = {
      access_token: accessToken,
      cached_at: new Date().toISOString(),
      expires_in: expiresIn,
      expires_at: expiresAt.toISOString()
    };
    fs.writeFileSync(SHOPIFY_TOKEN_CACHE_FILE, JSON.stringify(cache));
  } catch (e) {
    logger.info(`  Warning: Could not cache token: ${e.message}`);
  }
}

async function getShopifyClientSecret() {
  const secret = process.env.SHOPIFY_CLIENT_SECRET;
  if (secret) return secret;

  try {
    return await getSecret('shopify_client_secret');
  } catch (e) {
    logger.error('Error: Could not get Shopify client secret.');
    logger.error('  Set SHOPIFY_CLIENT_SECRET env var or add "shopify_client_secret" to GCP Secret Manager.');
    throw new Error('Missing Shopify client secret');
  }
}

async function fetchShopifyAccessTokenOAuth() {
  logger.info('Fetching fresh Shopify access token via OAuth...');
  const clientSecret = await getShopifyClientSecret();

  const response = await fetch(`https://${SHOP_NAME}.myshopify.com/admin/oauth/access_token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      client_id: SHOPIFY_CLIENT_ID,
      client_secret: clientSecret,
      grant_type: 'client_credentials'
    })
  });

  if (!response.ok) {
    const errorText = await response.text();
    logger.error(`Error fetching access token: ${response.status}`);
    logger.error(errorText);
    throw new Error(`OAuth failed: ${response.status}`);
  }

  const data = await response.json();
  const accessToken = data.access_token;
  const expiresIn = data.expires_in || 86400;

  if (!accessToken) {
    logger.error('Error: No access_token in response:', data);
    throw new Error('No access token in OAuth response');
  }

  saveCachedToken(accessToken, expiresIn);
  const expiresAt = new Date(Date.now() + expiresIn * 1000);
  logger.info(`  Got fresh access token (expires in ${Math.floor(expiresIn / 3600)}h)`);
  return { accessToken, expiresAt };
}

async function getShopifyAccessToken() {
  const { accessToken, expiresAt } = loadCachedToken();
  if (accessToken) {
    logger.info('Using cached Shopify access token');
    return { accessToken, expiresAt };
  }
  return fetchShopifyAccessTokenOAuth();
}

// ============================================
// GRAPHQL EXECUTION (with throttle-aware retry)
// ============================================

/**
 * A small token holder so we can transparently refresh mid-run.
 */
const token = { accessToken: null, expiresAt: null };

async function ensureFreshToken() {
  if (token.expiresAt) {
    const secondsUntilExpiry = (token.expiresAt - new Date()) / 1000;
    if (secondsUntilExpiry <= SHOPIFY_TOKEN_REFRESH_BUFFER_SECONDS) {
      logger.info('  Token expiring soon, refreshing...');
      const refreshed = await fetchShopifyAccessTokenOAuth();
      token.accessToken = refreshed.accessToken;
      token.expiresAt = refreshed.expiresAt;
    }
  }
}

async function executeGraphQL(query, variables) {
  for (let attempt = 1; attempt <= MAX_THROTTLE_RETRIES; attempt++) {
    await ensureFreshToken();

    const response = await fetch(GRAPHQL_URL, {
      method: 'POST',
      headers: {
        'X-Shopify-Access-Token': token.accessToken,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ query, variables })
    });

    // HTTP-level throttling
    if (response.status === 429) {
      const backoff = THROTTLE_DELAY_MS * Math.pow(2, attempt);
      logger.warn(`  HTTP 429, backing off ${backoff}ms (attempt ${attempt}/${MAX_THROTTLE_RETRIES})`);
      await sleep(backoff);
      continue;
    }

    if (!response.ok) {
      const errorText = await response.text();
      return { data: null, error: `HTTP ${response.status}: ${errorText}` };
    }

    const result = await response.json();

    // GraphQL-level throttling
    const isThrottled = result.errors?.some(
      (e) => e.extensions?.code === 'THROTTLED' || /throttled/i.test(e.message || '')
    );
    if (isThrottled) {
      const backoff = THROTTLE_DELAY_MS * Math.pow(2, attempt);
      logger.warn(`  GraphQL THROTTLED, backing off ${backoff}ms (attempt ${attempt}/${MAX_THROTTLE_RETRIES})`);
      await sleep(backoff);
      continue;
    }

    if (result.errors) {
      return { data: null, error: `GraphQL Errors: ${JSON.stringify(result.errors)}` };
    }

    return { data: result.data, error: null };
  }

  return { data: null, error: `Exceeded ${MAX_THROTTLE_RETRIES} throttle retries` };
}

// ============================================
// QUERIES / MUTATIONS
// ============================================

const PRODUCTS_QUERY = `
query GetProducts($cursor: String) {
  products(first: ${PRODUCT_PAGE_SIZE}, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        title
        soldOut: metafield(namespace: "${PRODUCT_MF_NAMESPACE}", key: "${PRODUCT_MF_KEY}") {
          value
        }
        variants(first: ${VARIANT_PAGE_SIZE}) {
          pageInfo { hasNextPage }
          edges {
            node {
              id
              onlineStore: metafield(namespace: "${VARIANT_MF_NAMESPACE}", key: "${VARIANT_MF_KEY}") {
                value
              }
            }
          }
        }
      }
    }
  }
}
`;

const METAFIELDS_SET_MUTATION = `
mutation SetMetafields($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id }
    userErrors { field message }
  }
}
`;

// ============================================
// CORE LOGIC
// ============================================

/**
 * Parse a metafield value as a number, returning 0 for null/empty/non-numeric.
 */
function toNumber(value) {
  if (value === null || value === undefined || value === '') return 0;
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

/**
 * Walk every product, computing whether it should be sold out online.
 * A product is in stock online (sold_out_online = false) when ANY variant has
 * custom.online_inventory > 0; otherwise it is sold out (sold_out_online = true).
 * Skips products whose metafield already equals the computed value.
 */
async function collectUpdates() {
  const updates = [];
  let cursor = null;
  let hasNextPage = true;
  let pageNum = 0;
  let totalProducts = 0;
  let unchanged = 0;
  let truncatedVariants = 0;

  const start = Date.now();

  while (hasNextPage) {
    pageNum++;
    const { data, error } = await executeGraphQL(PRODUCTS_QUERY, { cursor });
    if (error) throw new Error(`Failed to fetch products (page ${pageNum}): ${error}`);

    const conn = data?.products || {};
    const edges = conn.edges || [];

    for (const edge of edges) {
      const product = edge.node;
      totalProducts++;

      const variantEdges = product.variants?.edges || [];
      if (product.variants?.pageInfo?.hasNextPage) {
        truncatedVariants++;
        logger.warn(
          `  Product ${product.id} ("${product.title}") has more than ${VARIANT_PAGE_SIZE} variants; ` +
          `only the first ${VARIANT_PAGE_SIZE} were considered.`
        );
      }

      const inStockOnline = variantEdges.some(
        (ve) => toNumber(ve.node.onlineStore?.value) > 0
      );
      // sold_out_online is the inverse of "in stock online"
      const soldOut = !inStockOnline;
      const desiredValue = soldOut ? 'true' : 'false';

      const current = product.soldOut?.value;
      if (current !== null && current !== undefined && String(current) === desiredValue) {
        unchanged++;
        continue;
      }

      updates.push({
        ownerId: product.id,
        namespace: PRODUCT_MF_NAMESPACE,
        key: PRODUCT_MF_KEY,
        type: PRODUCT_MF_TYPE,
        value: desiredValue
      });
    }

    hasNextPage = conn.pageInfo?.hasNextPage || false;
    cursor = conn.pageInfo?.endCursor;

    if (pageNum % 10 === 0 || !hasNextPage) {
      const elapsed = (Date.now() - start) / 1000;
      logger.info(
        `  Page ${pageNum}: scanned ${totalProducts} products, ` +
        `${updates.length} need update (${elapsed.toFixed(1)}s)`
      );
    }

    if (hasNextPage) await sleep(THROTTLE_DELAY_MS);
  }

  logger.info(
    `Scan complete: ${totalProducts} products, ${updates.length} to update, ` +
    `${unchanged} already current` +
    (truncatedVariants ? `, ${truncatedVariants} with >${VARIANT_PAGE_SIZE} variants (truncated).` : '.')
  );

  return updates;
}

/**
 * Write metafields in batches of METAFIELD_BATCH_SIZE.
 */
async function applyUpdates(updates) {
  let written = 0;
  let errorCount = 0;
  const totalBatches = Math.ceil(updates.length / METAFIELD_BATCH_SIZE);

  for (let i = 0; i < updates.length; i += METAFIELD_BATCH_SIZE) {
    const batchNum = Math.floor(i / METAFIELD_BATCH_SIZE) + 1;
    const batch = updates.slice(i, i + METAFIELD_BATCH_SIZE);

    const { data, error } = await executeGraphQL(METAFIELDS_SET_MUTATION, { metafields: batch });

    if (error) {
      logger.error(`  Batch ${batchNum}/${totalBatches} failed: ${error}`);
      errorCount += batch.length;
    } else {
      const userErrors = data?.metafieldsSet?.userErrors || [];
      if (userErrors.length > 0) {
        for (const ue of userErrors) {
          logger.error(`  Batch ${batchNum} userError: [${(ue.field || []).join('.')}] ${ue.message}`);
        }
        errorCount += userErrors.length;
      }
      const ok = data?.metafieldsSet?.metafields?.length || 0;
      written += ok;
    }

    if (batchNum % 10 === 0 || batchNum === totalBatches) {
      logger.info(`  Wrote batch ${batchNum}/${totalBatches} (${written} metafields set so far)`);
    }

    if (i + METAFIELD_BATCH_SIZE < updates.length) await sleep(THROTTLE_DELAY_MS);
  }

  return { written, errorCount };
}

// ============================================
// MAIN
// ============================================

async function main() {
  const argv = process.argv.slice(2);
  const dryRun = argv.includes('--dry-run');

  const startTime = Date.now();

  console.log('='.repeat(60));
  console.log('PRODUCT IN-STOCK -> custom.sold_out_online METAFIELD');
  console.log('='.repeat(60));
  logger.info(`Source (per variant): ${VARIANT_MF_NAMESPACE}.${VARIANT_MF_KEY} > 0`);
  logger.info(`Target (per product): ${PRODUCT_MF_NAMESPACE}.${PRODUCT_MF_KEY} (${PRODUCT_MF_TYPE})`);
  logger.info(`Mode: ${dryRun ? 'DRY RUN (no writes)' : 'APPLY'}`);

  // Authenticate
  const { accessToken, expiresAt } = await getShopifyAccessToken();
  token.accessToken = accessToken;
  token.expiresAt = expiresAt;

  // Scan products
  logger.info('Scanning all products and their variants...');
  const updates = await collectUpdates();

  if (updates.length === 0) {
    logger.info('Nothing to update. All metafields already current.');
  } else if (dryRun) {
    logger.info(`DRY RUN: would write ${updates.length} metafields. Sample:`);
    for (const u of updates.slice(0, 10)) {
      logger.info(`  ${u.ownerId} -> ${u.value}`);
    }
  } else {
    logger.info(`Applying ${updates.length} metafield updates in batches of ${METAFIELD_BATCH_SIZE}...`);
    const { written, errorCount } = await applyUpdates(updates);
    logger.info(`Done: ${written} metafields set, ${errorCount} errors.`);
  }

  const totalTime = (Date.now() - startTime) / 1000;
  console.log('='.repeat(60));
  console.log(`COMPLETE in ${totalTime.toFixed(1)}s`);
  console.log('='.repeat(60));
}

// Load env: this dir first, then ../daily/.env as fallback.
try {
  require('dotenv').config();
  if (!process.env.SHOPIFY_CLIENT_SECRET) {
    require('dotenv').config({ path: path.join(__dirname, '..', 'daily', '.env') });
  }
} catch (e) {
  // dotenv optional
}

main().catch((e) => {
  logger.error(`Unexpected error: ${e.message}`);
  logger.error(e.stack);
  process.exit(1);
});
