/**
 * One-off: Sync BL101 Warehouse "available" inventory into each variant's
 * custom.online_inventory metafield.
 *
 * What it does:
 *   1. Authenticates with Shopify via OAuth client-credentials (same as daily/products.js).
 *   2. Resolves the location ID for the "BL101 Warehouse" location.
 *   3. Paginates through ALL product variants, reading the "available" quantity
 *      at that location and the current value of custom.online_inventory.
 *   4. Writes the available quantity into custom.online_inventory using the
 *      metafieldsSet mutation, batched 25 metafields per request.
 *
 * Variants whose metafield already matches the warehouse quantity are skipped,
 * so re-runs are cheap and idempotent.
 *
 * Usage:
 *   cd one-off && npm install
 *   node product-variant-inventory.js                 # apply updates
 *   node product-variant-inventory.js --dry-run       # report only, write nothing
 *   node product-variant-inventory.js --location "Some Other Location"
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

// Inventory source + metafield target
const LOCATION_NAME = 'BL101 Warehouse';
const METAFIELD_NAMESPACE = 'custom';
const METAFIELD_KEY = 'online_inventory';
const METAFIELD_TYPE = 'number_integer';

// Batching / throttling
const VARIANT_PAGE_SIZE = 100;   // variants fetched per page
const METAFIELD_BATCH_SIZE = 25; // max metafields per metafieldsSet call
const THROTTLE_DELAY_MS = 250;   // pause between Shopify calls
const MAX_THROTTLE_RETRIES = 6;  // retries when Shopify reports THROTTLED

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

const LOCATIONS_QUERY = `
query GetLocations($cursor: String) {
  locations(first: 100, after: $cursor, includeInactive: true) {
    pageInfo { hasNextPage endCursor }
    edges { node { id name } }
  }
}
`;

const VARIANTS_QUERY = `
query GetVariants($cursor: String, $locationId: ID!) {
  productVariants(first: ${VARIANT_PAGE_SIZE}, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id
        sku
        inventoryItem {
          inventoryLevel(locationId: $locationId) {
            quantities(names: ["available"]) {
              name
              quantity
            }
          }
        }
        metafield(namespace: "${METAFIELD_NAMESPACE}", key: "${METAFIELD_KEY}") {
          value
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

async function resolveLocationId(locationName) {
  let cursor = null;
  let hasNextPage = true;
  const seen = [];

  while (hasNextPage) {
    const { data, error } = await executeGraphQL(LOCATIONS_QUERY, { cursor });
    if (error) throw new Error(`Failed to fetch locations: ${error}`);

    const conn = data?.locations || {};
    for (const edge of conn.edges || []) {
      seen.push(edge.node.name);
      if (edge.node.name === locationName) {
        return edge.node.id;
      }
    }

    hasNextPage = conn.pageInfo?.hasNextPage || false;
    cursor = conn.pageInfo?.endCursor;
    if (hasNextPage) await sleep(THROTTLE_DELAY_MS);
  }

  throw new Error(
    `Location "${locationName}" not found. Available locations: ${seen.join(', ')}`
  );
}

/**
 * Walk every product variant, returning the list of metafield updates needed.
 * Skips variants whose metafield already equals the warehouse quantity.
 */
async function collectUpdates(locationId) {
  const updates = [];
  let cursor = null;
  let hasNextPage = true;
  let pageNum = 0;
  let totalVariants = 0;
  let unchanged = 0;
  let notStocked = 0;

  const start = Date.now();

  while (hasNextPage) {
    pageNum++;
    const { data, error } = await executeGraphQL(VARIANTS_QUERY, { cursor, locationId });
    if (error) throw new Error(`Failed to fetch variants (page ${pageNum}): ${error}`);

    const conn = data?.productVariants || {};
    const edges = conn.edges || [];

    for (const edge of edges) {
      const variant = edge.node;
      totalVariants++;

      const level = variant.inventoryItem?.inventoryLevel;
      const availEntry = level?.quantities?.find((q) => q.name === 'available');
      // No inventory level at this location => treat as 0 available.
      if (!level) notStocked++;
      const available = availEntry?.quantity ?? 0;

      const current = variant.metafield?.value;
      if (current !== null && current !== undefined && String(current) === String(available)) {
        unchanged++;
        continue;
      }

      updates.push({
        ownerId: variant.id,
        namespace: METAFIELD_NAMESPACE,
        key: METAFIELD_KEY,
        type: METAFIELD_TYPE,
        value: String(available)
      });
    }

    hasNextPage = conn.pageInfo?.hasNextPage || false;
    cursor = conn.pageInfo?.endCursor;

    if (pageNum % 10 === 0 || !hasNextPage) {
      const elapsed = (Date.now() - start) / 1000;
      logger.info(
        `  Page ${pageNum}: scanned ${totalVariants} variants, ` +
        `${updates.length} need update (${elapsed.toFixed(1)}s)`
      );
    }

    if (hasNextPage) await sleep(THROTTLE_DELAY_MS);
  }

  logger.info(
    `Scan complete: ${totalVariants} variants, ${updates.length} to update, ` +
    `${unchanged} already current, ${notStocked} not stocked at location (set to 0).`
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
  const locIdx = argv.indexOf('--location');
  const locationName = locIdx !== -1 && argv[locIdx + 1] ? argv[locIdx + 1] : LOCATION_NAME;

  const startTime = Date.now();

  console.log('='.repeat(60));
  console.log('VARIANT INVENTORY -> custom.online_inventory METAFIELD');
  console.log('='.repeat(60));
  logger.info(`Location: "${locationName}"`);
  logger.info(`Metafield: ${METAFIELD_NAMESPACE}.${METAFIELD_KEY} (${METAFIELD_TYPE})`);
  logger.info(`Mode: ${dryRun ? 'DRY RUN (no writes)' : 'APPLY'}`);

  // Authenticate
  const { accessToken, expiresAt } = await getShopifyAccessToken();
  token.accessToken = accessToken;
  token.expiresAt = expiresAt;

  // Resolve location
  const locationId = await resolveLocationId(locationName);
  logger.info(`Resolved location ID: ${locationId}`);

  // Scan variants
  logger.info('Scanning all product variants...');
  const updates = await collectUpdates(locationId);

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
