/**
 * Shopify Discounts Sync Script (Node.js) - Cloud Run + CLI
 *
 * Syncs all discounts (code-based and automatic) including usage statistics.
 * Unlike orders/customers/products, discounts don't have an updated_at filter
 * in the API, so we sync ALL discounts on each run and use MERGE to upsert.
 *
 * Cloud Run Usage:
 *   Deployed as Cloud Run function, triggered daily via Cloud Scheduler.
 *   HTTP GET/POST request triggers full sync of all discounts.
 *
 * CLI Usage:
 *   node discounts-sync.js                     # Full sync of all discounts
 *   node discounts-sync.js --status active     # Sync only active discounts
 *   node discounts-sync.js --status expired    # Sync only expired discounts
 *
 * GCP Secrets Required:
 *   - shopify-access-token: Shopify OAuth access token (for Cloud Run)
 *   - shopify_client_secret: Shopify client secret (for CLI with OAuth)
 *
 * Requirements:
 *   npm install @google-cloud/bigquery @google-cloud/secret-manager @google-cloud/functions-framework dotenv
 */

const functions = require('@google-cloud/functions-framework');
const { BigQuery } = require('@google-cloud/bigquery');
const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
const fs = require('fs');
const path = require('path');
const os = require('os');

// ============================================
// CONFIGURATION
// ============================================

const GCP_PROJECT = 'flowing-code-484318-c1';
const DATASET_ID = 'shopify';
const TABLE_DISCOUNTS = 'Discounts';
const TABLE_DISCOUNT_CODES = 'DiscountCodes';

const SHOP_NAME = 'bl101';
const GRAPHQL_URL = `https://${SHOP_NAME}.myshopify.com/admin/api/2025-01/graphql.json`;

// Sync settings
const THROTTLE_DELAY_MS = 100; // Delay between API pages
const PAGE_SIZE = 250; // Max allowed by Shopify GraphQL API

// Shopify OAuth settings (for CLI mode)
const SHOPIFY_CLIENT_ID = process.env.SHOPIFY_CLIENT_ID || '5c23ff36589a5c4ec853e23b60ca305c';
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

// ============================================
// GRAPHQL QUERY
// ============================================

const DISCOUNTS_QUERY = `
query GetDiscounts($cursor: String, $query: String) {
  discountNodes(first: ${PAGE_SIZE}, after: $cursor, query: $query) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
        discount {
          ... on DiscountCodeBasic {
            __typename
            title
            status
            summary
            shortSummary
            startsAt
            endsAt
            createdAt
            updatedAt
            asyncUsageCount
            usageLimit
            appliesOncePerCustomer
            recurringCycleLimit
            combinesWith {
              orderDiscounts
              productDiscounts
              shippingDiscounts
            }
            totalSales {
              amount
              currencyCode
            }
            customerGets {
              value {
                ... on DiscountPercentage {
                  __typename
                  percentage
                }
                ... on DiscountAmount {
                  __typename
                  amount {
                    amount
                    currencyCode
                  }
                }
              }
            }
            minimumRequirement {
              ... on DiscountMinimumSubtotal {
                __typename
                greaterThanOrEqualToSubtotal {
                  amount
                }
              }
              ... on DiscountMinimumQuantity {
                __typename
                greaterThanOrEqualToQuantity
              }
            }
            codes(first: 20) {
              edges {
                node {
                  id
                  code
                  asyncUsageCount
                }
              }
            }
            codesCount {
              count
            }
          }
          ... on DiscountCodeBxgy {
            __typename
            title
            status
            summary
            startsAt
            endsAt
            createdAt
            updatedAt
            asyncUsageCount
            usageLimit
            appliesOncePerCustomer
            usesPerOrderLimit
            combinesWith {
              orderDiscounts
              productDiscounts
              shippingDiscounts
            }
            totalSales {
              amount
              currencyCode
            }
            codes(first: 20) {
              edges {
                node {
                  id
                  code
                  asyncUsageCount
                }
              }
            }
            codesCount {
              count
            }
          }
          ... on DiscountCodeFreeShipping {
            __typename
            title
            status
            summary
            shortSummary
            startsAt
            endsAt
            createdAt
            updatedAt
            asyncUsageCount
            usageLimit
            appliesOncePerCustomer
            appliesOnOneTimePurchase
            appliesOnSubscription
            recurringCycleLimit
            combinesWith {
              orderDiscounts
              productDiscounts
              shippingDiscounts
            }
            totalSales {
              amount
              currencyCode
            }
            maximumShippingPrice {
              amount
            }
            minimumRequirement {
              ... on DiscountMinimumSubtotal {
                __typename
                greaterThanOrEqualToSubtotal {
                  amount
                }
              }
              ... on DiscountMinimumQuantity {
                __typename
                greaterThanOrEqualToQuantity
              }
            }
            codes(first: 20) {
              edges {
                node {
                  id
                  code
                  asyncUsageCount
                }
              }
            }
            codesCount {
              count
            }
          }
          ... on DiscountCodeApp {
            __typename
            title
            status
            startsAt
            endsAt
            createdAt
            updatedAt
            asyncUsageCount
            usageLimit
            appliesOncePerCustomer
            appliesOnOneTimePurchase
            appliesOnSubscription
            recurringCycleLimit
            combinesWith {
              orderDiscounts
              productDiscounts
              shippingDiscounts
            }
            totalSales {
              amount
              currencyCode
            }
            appDiscountType {
              appKey
              functionId
            }
            codes(first: 20) {
              edges {
                node {
                  id
                  code
                  asyncUsageCount
                }
              }
            }
            codesCount {
              count
            }
          }
          ... on DiscountAutomaticBasic {
            __typename
            title
            status
            summary
            shortSummary
            startsAt
            endsAt
            createdAt
            updatedAt
            asyncUsageCount
            recurringCycleLimit
            combinesWith {
              orderDiscounts
              productDiscounts
              shippingDiscounts
            }
            customerGets {
              value {
                ... on DiscountPercentage {
                  __typename
                  percentage
                }
                ... on DiscountAmount {
                  __typename
                  amount {
                    amount
                    currencyCode
                  }
                }
              }
            }
            minimumRequirement {
              ... on DiscountMinimumSubtotal {
                __typename
                greaterThanOrEqualToSubtotal {
                  amount
                }
              }
              ... on DiscountMinimumQuantity {
                __typename
                greaterThanOrEqualToQuantity
              }
            }
          }
          ... on DiscountAutomaticBxgy {
            __typename
            title
            status
            summary
            startsAt
            endsAt
            createdAt
            updatedAt
            asyncUsageCount
            usesPerOrderLimit
            combinesWith {
              orderDiscounts
              productDiscounts
              shippingDiscounts
            }
          }
          ... on DiscountAutomaticFreeShipping {
            __typename
            title
            status
            summary
            shortSummary
            startsAt
            endsAt
            createdAt
            updatedAt
            asyncUsageCount
            appliesOnOneTimePurchase
            appliesOnSubscription
            recurringCycleLimit
            combinesWith {
              orderDiscounts
              productDiscounts
              shippingDiscounts
            }
            totalSales {
              amount
              currencyCode
            }
            maximumShippingPrice {
              amount
            }
            minimumRequirement {
              ... on DiscountMinimumSubtotal {
                __typename
                greaterThanOrEqualToSubtotal {
                  amount
                }
              }
              ... on DiscountMinimumQuantity {
                __typename
                greaterThanOrEqualToQuantity
              }
            }
          }
          ... on DiscountAutomaticApp {
            __typename
            title
            status
            startsAt
            endsAt
            createdAt
            updatedAt
            asyncUsageCount
            appliesOnOneTimePurchase
            appliesOnSubscription
            recurringCycleLimit
            combinesWith {
              orderDiscounts
              productDiscounts
              shippingDiscounts
            }
            appDiscountType {
              appKey
              functionId
            }
          }
        }
      }
    }
  }
}
`;

// ============================================
// HELPER FUNCTIONS
// ============================================

const secretManagerClient = new SecretManagerServiceClient();
const bigquery = new BigQuery({ projectId: GCP_PROJECT });

async function getSecret(secretId) {
  const name = `projects/${GCP_PROJECT}/secrets/${secretId}/versions/latest`;
  const [version] = await secretManagerClient.accessSecretVersion({ name });
  return version.payload.data.toString('utf8');
}

async function getShopifyToken(_useOAuth = false) {
  return getShopifyAccessTokenOAuth();
}

function loadCachedToken() {
  try {
    if (fs.existsSync(SHOPIFY_TOKEN_CACHE_FILE)) {
      const cache = JSON.parse(fs.readFileSync(SHOPIFY_TOKEN_CACHE_FILE, 'utf8'));
      const expiresAt = new Date(cache.expires_at);
      const now = new Date();
      const secondsUntilExpiry = (expiresAt - now) / 1000;

      if (secondsUntilExpiry > SHOPIFY_TOKEN_REFRESH_BUFFER_SECONDS) {
        logger.info(`  Token valid for ${Math.floor(secondsUntilExpiry / 60)} more minutes`);
        return { accessToken: cache.access_token, expiresAt };
      }
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
  if (secret) {
    logger.info('Using SHOPIFY_CLIENT_SECRET from environment');
    return secret;
  }

  try {
    return await getSecret('shopify_client_secret');
  } catch (e) {
    logger.error('Error: Could not get Shopify client secret.');
    logger.error('Set SHOPIFY_CLIENT_SECRET environment variable or');
    logger.error('add "shopify_client_secret" to GCP Secret Manager.');
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
    throw new Error(`OAuth failed: ${response.status}`);
  }

  const data = await response.json();
  const accessToken = data.access_token;
  const expiresIn = data.expires_in || 86400;

  if (!accessToken) {
    throw new Error('No access token in OAuth response');
  }

  saveCachedToken(accessToken, expiresIn);

  const expiresAt = new Date(Date.now() + expiresIn * 1000);
  logger.info(`  Got fresh access token (expires in ${Math.floor(expiresIn / 3600)} hours)`);

  return { accessToken, expiresAt };
}

async function getShopifyAccessTokenOAuth() {
  const { accessToken, expiresAt } = loadCachedToken();
  if (accessToken) {
    logger.info('Using cached Shopify access token');
    return { accessToken, expiresAt };
  }
  return fetchShopifyAccessTokenOAuth();
}

function extractLegacyId(gid) {
  if (!gid) return null;
  if (typeof gid === 'string' && gid.startsWith('gid://')) {
    return gid.split('/').pop();
  }
  return String(gid);
}

function parseTimestamp(tsStr) {
  if (!tsStr) return null;
  try {
    return new Date(tsStr).toISOString();
  } catch (e) {
    return null;
  }
}

// ============================================
// MAPPING FUNCTIONS
// ============================================

function getDiscountMethod(typename) {
  if (!typename) return 'unknown';
  if (typename.includes('Code')) return 'code';
  if (typename.includes('Automatic')) return 'automatic';
  return 'unknown';
}

function getDiscountType(typename) {
  if (!typename) return 'unknown';
  if (typename.includes('Basic')) return 'basic';
  if (typename.includes('Bxgy')) return 'bxgy';
  if (typename.includes('FreeShipping')) return 'free_shipping';
  if (typename.includes('App')) return 'app';
  return 'unknown';
}

function getDiscountClass(typename) {
  if (!typename) return null;
  if (typename.includes('FreeShipping')) return 'SHIPPING';
  if (typename.includes('Bxgy')) return 'PRODUCT';
  if (typename.includes('Basic')) return 'PRODUCT';
  if (typename.includes('App')) return 'PRODUCT'; // Default, could be any
  return null;
}

function extractDiscountValue(discount) {
  const customerGets = discount.customerGets;
  if (!customerGets || !customerGets.value) {
    return { valueType: null, value: null, currency: null };
  }

  const value = customerGets.value;
  
  if (value.__typename === 'DiscountPercentage') {
    return {
      valueType: 'percentage',
      value: value.percentage,
      currency: null
    };
  }
  
  if (value.__typename === 'DiscountAmount') {
    return {
      valueType: 'fixed_amount',
      value: parseFloat(value.amount?.amount) || 0,
      currency: value.amount?.currencyCode
    };
  }

  return { valueType: null, value: null, currency: null };
}

function extractMinimumRequirement(discount) {
  const minReq = discount.minimumRequirement;
  if (!minReq) {
    return { type: null, value: null };
  }

  if (minReq.__typename === 'DiscountMinimumSubtotal') {
    return {
      type: 'subtotal',
      value: parseFloat(minReq.greaterThanOrEqualToSubtotal?.amount) || 0
    };
  }

  if (minReq.__typename === 'DiscountMinimumQuantity') {
    return {
      type: 'quantity',
      value: minReq.greaterThanOrEqualToQuantity || 0
    };
  }

  return { type: null, value: null };
}

function mapDiscountToSchema(node) {
  const discount = node.discount;
  if (!discount) return null;

  const typename = discount.__typename;
  const method = getDiscountMethod(typename);
  const discountType = getDiscountType(typename);
  const discountClass = getDiscountClass(typename);
  const { valueType, value, currency } = extractDiscountValue(discount);
  const minReq = extractMinimumRequirement(discount);

  return {
    id: extractLegacyId(node.id),
    admin_graphql_api_id: node.id,
    title: discount.title,
    method: method,
    discount_type: discountType,
    status: (discount.status || '').toLowerCase(),
    value_type: discountType === 'free_shipping' ? 'free_shipping' : valueType,
    value: discountType === 'free_shipping' ? null : value,
    currency: currency,
    starts_at: parseTimestamp(discount.startsAt),
    ends_at: parseTimestamp(discount.endsAt),
    created_at: parseTimestamp(discount.createdAt),
    updated_at: parseTimestamp(discount.updatedAt),
    async_usage_count: discount.asyncUsageCount || 0,
    usage_limit: discount.usageLimit || null,
    applies_once_per_customer: discount.appliesOncePerCustomer || false,
    discount_class: discountClass,
    applies_on_one_time_purchase: discount.appliesOnOneTimePurchase ?? null,
    applies_on_subscription: discount.appliesOnSubscription ?? null,
    recurring_cycle_limit: discount.recurringCycleLimit || 0,
    minimum_requirement_type: minReq.type,
    minimum_requirement_value: minReq.value,
    combines_with_order_discounts: discount.combinesWith?.orderDiscounts ?? null,
    combines_with_product_discounts: discount.combinesWith?.productDiscounts ?? null,
    combines_with_shipping_discounts: discount.combinesWith?.shippingDiscounts ?? null,
    codes_count: discount.codesCount?.count || 0,
    uses_per_order_limit: discount.usesPerOrderLimit || null,
    maximum_shipping_price: discount.maximumShippingPrice ? parseFloat(discount.maximumShippingPrice.amount) : null,
    destination_selection: null, // Would need additional query to get this
    app_discount_type_app_key: discount.appDiscountType?.appKey || null,
    app_discount_type_function_id: discount.appDiscountType?.functionId || null,
    summary: discount.summary || null,
    short_summary: discount.shortSummary || null,
    total_sales: discount.totalSales ? parseFloat(discount.totalSales.amount) : null,
    total_sales_currency: discount.totalSales?.currencyCode || null,
    synced_at: new Date().toISOString()
  };
}

function mapDiscountCodesToSchema(node) {
  const discount = node.discount;
  if (!discount || !discount.codes) return [];

  const discountId = extractLegacyId(node.id);
  const codes = [];

  const edges = discount.codes.edges || [];
  for (const edge of edges) {
    const code = edge.node;
    codes.push({
      id: extractLegacyId(code.id),
      discount_id: discountId,
      code: code.code,
      async_usage_count: code.asyncUsageCount || 0,
      created_at: null, // Not available in this query
      updated_at: null,
      synced_at: new Date().toISOString()
    });
  }

  return codes;
}

// ============================================
// TABLE CONFIGURATION
// ============================================

const TABLE_CONFIG = {
  [TABLE_DISCOUNTS]: {
    merge_key: 'id',
    columns: [
      'id', 'admin_graphql_api_id', 'title', 'method', 'discount_type', 'status',
      'value_type', 'value', 'currency', 'starts_at', 'ends_at', 'created_at',
      'updated_at', 'async_usage_count', 'usage_limit', 'applies_once_per_customer',
      'discount_class', 'applies_on_one_time_purchase', 'applies_on_subscription',
      'recurring_cycle_limit', 'minimum_requirement_type', 'minimum_requirement_value',
      'combines_with_order_discounts', 'combines_with_product_discounts',
      'combines_with_shipping_discounts', 'codes_count', 'uses_per_order_limit',
      'maximum_shipping_price', 'destination_selection', 'app_discount_type_app_key',
      'app_discount_type_function_id', 'summary', 'short_summary', 'total_sales',
      'total_sales_currency', 'synced_at'
    ]
  },
  [TABLE_DISCOUNT_CODES]: {
    merge_key: 'id',
    columns: [
      'id', 'discount_id', 'code', 'async_usage_count', 'created_at', 'updated_at', 'synced_at'
    ]
  }
};

// ============================================
// STAGING TABLE OPERATIONS
// ============================================

async function ensureStagingTable(tableName) {
  const targetTable = `${GCP_PROJECT}.${DATASET_ID}.${tableName}`;
  const stagingTable = `${GCP_PROJECT}.${DATASET_ID}.${tableName}_staging`;

  try {
    await bigquery.dataset(DATASET_ID).table(`${tableName}_staging`).get();
  } catch (e) {
    if (e.code === 404) {
      const createQuery = `CREATE TABLE \`${stagingTable}\` LIKE \`${targetTable}\``;
      await bigquery.query(createQuery);
      logger.info(`Created staging table: ${stagingTable}`);
    } else {
      throw e;
    }
  }

  return stagingTable;
}

// Batch size for BigQuery inserts (to avoid 413 Request Entity Too Large)
const BIGQUERY_BATCH_SIZE = 5000;

async function loadToStaging(tableName, rows) {
  if (!rows || rows.length === 0) {
    return { tableName, success: true, error: null };
  }

  const stagingTable = await ensureStagingTable(tableName);

  // Truncate staging table
  await bigquery.query(`TRUNCATE TABLE \`${stagingTable}\``);

  const table = bigquery.dataset(DATASET_ID).table(`${tableName}_staging`);
  let totalInserted = 0;
  let totalPartialFailures = 0;

  // Insert in batches to avoid 413 errors
  for (let i = 0; i < rows.length; i += BIGQUERY_BATCH_SIZE) {
    const batch = rows.slice(i, i + BIGQUERY_BATCH_SIZE);
    const batchNum = Math.floor(i / BIGQUERY_BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(rows.length / BIGQUERY_BATCH_SIZE);

    try {
      await table.insert(batch);
      totalInserted += batch.length;
      
      if (totalBatches > 1) {
        logger.info(`  Batch ${batchNum}/${totalBatches}: Inserted ${batch.length} rows`);
      }
    } catch (e) {
      if (e.name === 'PartialFailureError') {
        const successCount = batch.length - (e.errors?.length || 0);
        totalInserted += successCount;
        totalPartialFailures += e.errors?.length || 0;
        logger.warn(`  Batch ${batchNum}/${totalBatches}: ${e.errors?.length || 0} rows failed`);
        if (e.errors && e.errors.length > 0 && batchNum === 1) {
          logger.warn(`  Sample errors:`, JSON.stringify(e.errors.slice(0, 2), null, 2));
        }
      } else {
        return { tableName, success: false, error: `Batch ${batchNum} failed: ${e.message}` };
      }
    }
  }

  logger.info(`Loaded ${totalInserted} rows to ${stagingTable}${totalPartialFailures > 0 ? ` (${totalPartialFailures} failures)` : ''}`);
  return { tableName, success: true, error: null, partialFailures: totalPartialFailures || null };
}

function buildMergeStatement(tableName) {
  const config = TABLE_CONFIG[tableName];
  const mergeKey = config.merge_key;
  const columns = config.columns;

  const targetTable = `\`${GCP_PROJECT}.${DATASET_ID}.${tableName}\``;
  const stagingTable = `\`${GCP_PROJECT}.${DATASET_ID}.${tableName}_staging\``;

  const updateCols = columns
    .filter(col => col !== mergeKey)
    .map(col => `T.${col} = S.${col}`);

  const insertCols = columns.join(', ');
  const insertVals = columns.map(col => `S.${col}`).join(', ');

  return `
    MERGE ${targetTable} T
    USING ${stagingTable} S
    ON T.${mergeKey} = S.${mergeKey}
    WHEN MATCHED THEN
      UPDATE SET ${updateCols.join(', ')}
    WHEN NOT MATCHED THEN
      INSERT (${insertCols})
      VALUES (${insertVals});
  `;
}

function buildDeleteOrphanedCodesStatement() {
  const targetTable = `\`${GCP_PROJECT}.${DATASET_ID}.${TABLE_DISCOUNT_CODES}\``;
  const codesStagingTable = `\`${GCP_PROJECT}.${DATASET_ID}.${TABLE_DISCOUNT_CODES}_staging\``;
  const discountsStagingTable = `\`${GCP_PROJECT}.${DATASET_ID}.${TABLE_DISCOUNTS}_staging\``;

  return `
    DELETE FROM ${targetTable} T
    WHERE T.discount_id IN (SELECT id FROM ${discountsStagingTable})
    AND T.id NOT IN (SELECT id FROM ${codesStagingTable});
  `;
}

// ============================================
// DATA ACCUMULATOR
// ============================================

class DiscountDataAccumulator {
  constructor() {
    this.discounts = [];
    this.codes = [];
    this.discountIds = new Set();
  }

  addDiscount(node) {
    const discountData = mapDiscountToSchema(node);
    if (!discountData) return;

    this.discountIds.add(discountData.id);
    this.discounts.push(discountData);

    const codesData = mapDiscountCodesToSchema(node);
    this.codes.push(...codesData);
  }

  getStats() {
    return {
      [TABLE_DISCOUNTS]: { total: this.discounts.length },
      [TABLE_DISCOUNT_CODES]: { total: this.codes.length }
    };
  }

  getAllData() {
    return {
      [TABLE_DISCOUNTS]: this.discounts,
      [TABLE_DISCOUNT_CODES]: this.codes
    };
  }

  getDiscountIds() {
    return Array.from(this.discountIds);
  }

  clear() {
    this.discounts = [];
    this.codes = [];
    this.discountIds.clear();
  }
}

// ============================================
// BULK MERGE OPERATIONS
// ============================================

async function executeBulkMerge(accumulator) {
  const allData = accumulator.getAllData();

  if (allData[TABLE_DISCOUNTS].length === 0) {
    return { success: true, error: null };
  }

  // Ensure all staging tables exist and are clean
  const allTables = [TABLE_DISCOUNTS, TABLE_DISCOUNT_CODES];
  for (const tableName of allTables) {
    const stagingTable = await ensureStagingTable(tableName);
    await bigquery.query(`TRUNCATE TABLE \`${stagingTable}\``);
  }

  // Load to staging tables
  logger.info('Loading data to staging tables...');
  const stagingStart = Date.now();

  for (const [tableName, data] of Object.entries(allData)) {
    if (data && data.length > 0) {
      const result = await loadToStaging(tableName, data);
      if (!result.success) {
        return { success: false, error: `Failed to load ${tableName} to staging: ${result.error}` };
      }
    }
  }

  const stagingTime = (Date.now() - stagingStart) / 1000;
  logger.info(`Staging complete in ${stagingTime.toFixed(2)}s`);

  // Build and execute MERGE statements
  logger.info('Executing MERGE operations...');
  const mergeStart = Date.now();

  const statements = [];

  // Delete orphaned codes
  if (allData[TABLE_DISCOUNT_CODES].length > 0) {
    statements.push(buildDeleteOrphanedCodesStatement());
  }

  // MERGE statements for all tables with data
  for (const [tableName, data] of Object.entries(allData)) {
    if (data && data.length > 0) {
      statements.push(buildMergeStatement(tableName));
    }
  }

  if (statements.length > 0) {
    const transactionQuery = `BEGIN TRANSACTION;\n${statements.join('\n')}\nCOMMIT TRANSACTION;`;

    try {
      await bigquery.query(transactionQuery);
    } catch (e) {
      return { success: false, error: `MERGE transaction failed: ${e.message}` };
    }
  }

  const mergeTime = (Date.now() - mergeStart) / 1000;
  logger.info(`MERGE complete in ${mergeTime.toFixed(2)}s`);

  // Cleanup staging tables
  logger.info('Cleaning up staging tables...');
  for (const tableName of allTables) {
    const stagingTable = `${GCP_PROJECT}.${DATASET_ID}.${tableName}_staging`;
    try {
      await bigquery.query(`TRUNCATE TABLE \`${stagingTable}\``);
    } catch (e) {
      logger.warn(`Failed to truncate ${stagingTable}: ${e.message}`);
    }
  }

  return { success: true, error: null };
}

// ============================================
// GRAPHQL EXECUTION
// ============================================

async function executeGraphQL(accessToken, query, variables) {
  const response = await fetch(GRAPHQL_URL, {
    method: 'POST',
    headers: {
      'X-Shopify-Access-Token': accessToken,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ query, variables })
  });

  if (!response.ok) {
    const errorText = await response.text();
    return { data: null, error: `HTTP ${response.status}: ${errorText}` };
  }

  const result = await response.json();

  if (result.errors) {
    return { data: null, error: `GraphQL Errors: ${JSON.stringify(result.errors)}` };
  }

  return { data: result.data, error: null };
}

// ============================================
// CORE SYNC LOGIC
// ============================================

async function fetchAllDiscounts(accessToken, statusFilter = null) {
  const accumulator = new DiscountDataAccumulator();

  const queryFilter = statusFilter ? `status:${statusFilter}` : null;
  if (queryFilter) {
    logger.info(`Query filter: ${queryFilter}`);
  } else {
    logger.info('Fetching all discounts (no filter)');
  }

  let cursor = null;
  let hasNextPage = true;
  let pageNum = 0;

  const fetchStart = Date.now();

  while (hasNextPage) {
    pageNum++;

    const { data, error } = await executeGraphQL(accessToken, DISCOUNTS_QUERY, { cursor, query: queryFilter });

    if (error) {
      return { accumulator: null, error: `Shopify API Error: ${error}` };
    }

    const discountsData = data?.discountNodes || {};
    const pageInfo = discountsData.pageInfo || {};
    const edges = discountsData.edges || [];

    for (const edge of edges) {
      accumulator.addDiscount(edge.node);
    }

    logger.info(`Page ${pageNum}: Fetched ${edges.length} discounts (${accumulator.discounts.length} total)`);

    hasNextPage = pageInfo.hasNextPage || false;
    cursor = pageInfo.endCursor;

    if (hasNextPage) {
      await new Promise(resolve => setTimeout(resolve, THROTTLE_DELAY_MS));
    }
  }

  const fetchTime = (Date.now() - fetchStart) / 1000;
  logger.info(`Fetched ${accumulator.discounts.length} discounts in ${fetchTime.toFixed(2)}s`);

  return { accumulator, error: null };
}

async function syncDiscounts(accessToken, statusFilter = null) {
  // Step 1: Fetch all discounts
  logger.info('Step 1: Fetching all discounts from Shopify...');
  const { accumulator, error } = await fetchAllDiscounts(accessToken, statusFilter);

  if (error) {
    return { success: false, error, stats: null };
  }

  const totalStats = accumulator.getStats();

  if (totalStats[TABLE_DISCOUNTS].total === 0) {
    logger.info('No discounts found.');
    return { success: true, error: null, stats: totalStats };
  }

  // Step 2: Bulk merge to BigQuery
  logger.info('Step 2: Executing bulk merge to BigQuery...');
  const mergeStart = Date.now();

  const { success, error: mergeError } = await executeBulkMerge(accumulator);

  if (!success) {
    return { success: false, error: mergeError, stats: totalStats };
  }

  const mergeTotalTime = (Date.now() - mergeStart) / 1000;
  logger.info(`BigQuery operations complete in ${mergeTotalTime.toFixed(2)}s`);

  return { success: true, error: null, stats: totalStats };
}

// ============================================
// CLOUD RUN ENTRY POINT
// ============================================

functions.http('main', async (req, res) => {
  try {
    const startTime = Date.now();

    logger.info('='.repeat(60));
    logger.info('SHOPIFY DISCOUNTS SYNC (CLOUD RUN)');
    logger.info('='.repeat(60));

    // Get Shopify token (from Secret Manager for Cloud Run)
    const { accessToken } = await getShopifyToken(false);

    // Run sync (all discounts)
    const { success, error, stats } = await syncDiscounts(accessToken, null);

    const totalTime = (Date.now() - startTime) / 1000;

    if (!success) {
      logger.error(`Sync failed: ${error}`);
      res.status(500).send(`Sync failed: ${error}`);
      return;
    }

    if (!stats || stats[TABLE_DISCOUNTS].total === 0) {
      logger.info('No discounts found.');
      res.status(200).send(
        `Discounts Sync Complete!\n\n` +
        `No discounts found.\n` +
        `Total time: ${totalTime.toFixed(2)}s`
      );
      return;
    }

    const responseLines = [
      'Discounts Sync Complete!',
      '',
      `Total time: ${totalTime.toFixed(2)}s`,
      '',
      'Records synced:',
      `- Discounts: ${stats[TABLE_DISCOUNTS].total}`,
      `- Discount Codes: ${stats[TABLE_DISCOUNT_CODES].total}`
    ];

    const response = responseLines.join('\n');
    logger.info(response);

    res.status(200).send(response);

  } catch (e) {
    logger.error(`Unexpected error: ${e.message}`);
    logger.error(e.stack);
    res.status(500).send(`Script Error: ${e.message}`);
  }
});

// ============================================
// CLI COMMANDS
// ============================================

async function cmdSync(args) {
  logger.info('Starting discounts sync...');
  if (args.status) {
    logger.info(`  Filtering by status: ${args.status}`);
  }
  logger.info('');

  const startTime = Date.now();

  // Get token via OAuth for CLI mode
  const { accessToken } = await getShopifyToken(true);

  // Run sync
  const { success, error, stats } = await syncDiscounts(accessToken, args.status);

  const totalTime = (Date.now() - startTime) / 1000;

  if (!success) {
    throw new Error(error);
  }

  const discountsCount = stats?.[TABLE_DISCOUNTS]?.total || 0;
  const codesCount = stats?.[TABLE_DISCOUNT_CODES]?.total || 0;

  console.log('\n' + '='.repeat(50));
  console.log('DISCOUNTS SYNC COMPLETE');
  console.log('='.repeat(50));
  console.log(`  Discounts synced: ${discountsCount.toLocaleString()}`);
  console.log(`  Discount codes synced: ${codesCount.toLocaleString()}`);
  console.log(`  Duration: ${totalTime.toFixed(1)} seconds`);
  console.log('='.repeat(50) + '\n');
}

// ============================================
// CLI ENTRY POINT
// ============================================

async function runCLI() {
  const args = {
    status: null
  };

  // Parse arguments
  for (let i = 2; i < process.argv.length; i++) {
    if (process.argv[i] === '--status' && process.argv[i + 1]) {
      args.status = process.argv[i + 1];
    }
  }

  try {
    await cmdSync(args);
  } catch (e) {
    console.error('Error:', e.message);
    process.exit(1);
  }
}

// Only run CLI if this is the main module
if (require.main === module) {
  // Load dotenv first before anything else
  try {
    require('dotenv').config();
    logger.info('Loaded .env file');
  } catch (e) {
    logger.info('dotenv not available, using environment variables directly');
  }
  
  // Log which credentials we're using
  if (process.env.SHOPIFY_CLIENT_ID) {
    logger.info(`Using SHOPIFY_CLIENT_ID from environment: ${process.env.SHOPIFY_CLIENT_ID.substring(0, 8)}...`);
  }
  if (process.env.SHOPIFY_CLIENT_SECRET) {
    logger.info('SHOPIFY_CLIENT_SECRET found in environment');
  }
  
  runCLI();
}

module.exports = {
  syncDiscounts,
  fetchAllDiscounts,
  DiscountDataAccumulator
};