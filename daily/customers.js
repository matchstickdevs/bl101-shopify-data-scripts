/**
 * Shopify Customers Sync Script (Node.js) - Cloud Run + CLI
 *
 * Supports two deployment modes:
 * 1. Cloud Run: Triggered daily via Cloud Scheduler (syncs yesterday's updates)
 * 2. CLI: Manual execution for backfills and testing
 *
 * Cloud Run Usage:
 *   Deployed as Cloud Run function, triggered daily via Cloud Scheduler.
 *   HTTP GET/POST request triggers sync of yesterday's updated customers.
 *
 * CLI Usage:
 *   # Incremental sync (daily updates)
 *   node customers-sync.js                       # Sync customers updated since yesterday
 *   node customers-sync.js --start 2025-01-15    # Sync customers updated since specific date
 *
 *   # Historical backfill
 *   node customers-sync.js --init                # Initialize checkpoints for full backfill
 *   node customers-sync.js --backfill            # Process all pending backfill chunks
 *   node customers-sync.js --status              # Check backfill progress
 *   node customers-sync.js --retry-failed        # Reset failed chunks
 *
 * GCP Secrets Required:
 *   - shopify-access-token: Shopify OAuth access token (for Cloud Run)
 *   - shopify-client-secret: Shopify client secret (for CLI with OAuth)
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
const DATASET_CUSTOMERS = 'customers';
const TABLE_CUSTOMERS = 'Customers';
const TABLE_CUSTOMERS_DATA = 'Customers_data';
const TABLE_ADDRESSES = 'Addresses';
const TABLE_SYNC_CHECKPOINTS = 'CustomerSyncCheckpoints';

const SHOP_NAME = 'bl101';
const GRAPHQL_URL = `https://${SHOP_NAME}.myshopify.com/admin/api/2025-01/graphql.json`;

// Backfill settings
const BACKFILL_START_DATE = '2015-01-01';
const BACKFILL_CHUNK_DAYS = 180;
const THROTTLE_DELAY_MS = 200;
const PAGE_SIZE = 100;
const BATCH_SIZE = 500;

// Shopify OAuth settings (for CLI mode)
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

// ============================================
// GRAPHQL QUERY
// ============================================

const CUSTOMERS_QUERY = `
query GetCustomers($cursor: String, $query: String) {
  customers(first: ${PAGE_SIZE}, after: $cursor, query: $query) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
        createdAt
        updatedAt
        firstName
        lastName
        numberOfOrders
        state
        amountSpent {
          amount
          currencyCode
        }
        lastOrder {
          id
          name
        }
        note
        verifiedEmail
        multipassIdentifier
        taxExempt
        tags
        email
        phone
        emailMarketingConsent {
          marketingState
          marketingOptInLevel
          consentUpdatedAt
        }
        smsMarketingConsent {
          marketingState
          marketingOptInLevel
          consentUpdatedAt
        }
        defaultAddress {
          id
        }
        addressesV2(first: 20) {
          edges {
            node {
              id
              firstName
              lastName
              company
              address1
              address2
              city
              province
              country
              zip
              phone
              provinceCode
              countryCodeV2
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

/**
 * Get Shopify access token - supports both Cloud Run and CLI modes
 * Cloud Run: Uses pre-authorized token from Secret Manager
 * CLI: Uses OAuth client credentials flow with local caching
 */
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
      } else {
        logger.info(`  Cached token expires in ${Math.floor(secondsUntilExpiry)} seconds, refreshing...`);
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
  // First try environment variable (for local development)
  const secret = process.env.SHOPIFY_CLIENT_SECRET;
  if (secret) return secret;

  // Fall back to Secret Manager
  try {
    return await getSecret('shopify_client_secret');
  } catch (e) {
    logger.error('Error: Could not get Shopify client secret.');
    logger.error('  Set SHOPIFY_CLIENT_SECRET environment variable or');
    logger.error('  add "shopify_client_secret" to GCP Secret Manager.');
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
    logger.error(`Error: No access_token in response:`, data);
    throw new Error('No access token in OAuth response');
  }

  saveCachedToken(accessToken, expiresIn);

  const expiresAt = new Date(Date.now() + expiresIn * 1000);
  logger.info(`  ✓ Got fresh access token (expires in ${Math.floor(expiresIn / 3600)} hours)`);

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
    logger.warn(`  Warning: Could not parse timestamp '${tsStr}': ${e.message}`);
    return null;
  }
}

function getYesterdayDate() {
  const yesterday = new Date();
  yesterday.setUTCDate(yesterday.getUTCDate() - 1);
  return yesterday.toISOString().slice(0, 10);
}

function getTodayDate() {
  return new Date().toISOString().slice(0, 10);
}

// ============================================
// MAPPING FUNCTIONS
// ============================================

function mapCustomerToSchema(customer) {
  const tags = Array.isArray(customer.tags) ? customer.tags.join(', ') : customer.tags;

  let emailMarketingConsent = null;
  if (customer.emailMarketingConsent) {
    const emc = customer.emailMarketingConsent;
    emailMarketingConsent = JSON.stringify({
      state: emc.marketingState,
      opt_in_level: emc.marketingOptInLevel,
      consent_updated_at: emc.consentUpdatedAt
    });
  }

  let smsMarketingConsent = null;
  if (customer.smsMarketingConsent) {
    const smc = customer.smsMarketingConsent;
    smsMarketingConsent = JSON.stringify({
      state: smc.marketingState,
      opt_in_level: smc.marketingOptInLevel,
      consent_updated_at: smc.consentUpdatedAt
    });
  }

  return {
    id: extractLegacyId(customer.id),
    created_at: parseTimestamp(customer.createdAt),
    updated_at: parseTimestamp(customer.updatedAt),
    orders_count: parseInt(customer.numberOfOrders) || 0,
    state: (customer.state || '').toLowerCase(),
    total_spent: parseFloat(customer.amountSpent?.amount) || 0,
    verified_email: Boolean(customer.verifiedEmail),
    tags: tags,
    tax_exempt: Boolean(customer.taxExempt),
    currency: customer.amountSpent?.currencyCode || null,
    email_marketing_consent: emailMarketingConsent,
    sms_marketing_consent: smsMarketingConsent
  };
}

function mapCustomerDataToSchema(customer) {
  return {
    customer_id: extractLegacyId(customer.id),
    first_name: customer.firstName,
    last_name: customer.lastName,
    last_order_id: extractLegacyId(customer.lastOrder?.id),
    note: customer.note,
    multipass_identifier: customer.multipassIdentifier,
    last_order_name: customer.lastOrder?.name || null,
    email: customer.email,
    phone: customer.phone,
    admin_graphql_api_id: customer.id,
    default_address_id: extractLegacyId(customer.defaultAddress?.id)
  };
}

function mapAddressToSchema(address, customerId, defaultAddressId) {
  const addressId = extractLegacyId(address.id);

  return {
    id: addressId,
    customer_id: customerId,
    first_name: address.firstName,
    last_name: address.lastName,
    company: address.company,
    address1: address.address1,
    address2: address.address2,
    city: address.city,
    province: address.province,
    country: address.country,
    zip: address.zip,
    phone: address.phone,
    name: [address.firstName, address.lastName].filter(Boolean).join(' ') || null,
    province_code: address.provinceCode,
    country_code: address.countryCodeV2,
    country_name: address.country,
    is_default: addressId === defaultAddressId
  };
}

// ============================================
// TABLE CONFIGURATION (for MERGE operations)
// ============================================

const TABLE_CONFIG = {
  [TABLE_CUSTOMERS_DATA]: {
    dataset: DATASET_CUSTOMERS,
    merge_key: 'customer_id',
    columns: [
      'customer_id', 'first_name', 'last_name', 'last_order_id', 'note',
      'multipass_identifier', 'last_order_name', 'email', 'phone',
      'admin_graphql_api_id', 'default_address_id'
    ]
  },
  [TABLE_CUSTOMERS]: {
    dataset: DATASET_ID,
    merge_key: 'id',
    columns: [
      'id', 'created_at', 'updated_at', 'orders_count', 'state', 'total_spent',
      'verified_email', 'tags', 'tax_exempt', 'currency',
      'email_marketing_consent', 'sms_marketing_consent'
    ]
  },
  [TABLE_ADDRESSES]: {
    dataset: DATASET_CUSTOMERS,
    merge_key: 'id',
    columns: [
      'id', 'customer_id', 'first_name', 'last_name', 'company', 'address1',
      'address2', 'city', 'province', 'country', 'zip', 'phone', 'name',
      'province_code', 'country_code', 'country_name', 'is_default'
    ]
  }
};

function getTableDataset(tableName) {
  return TABLE_CONFIG[tableName]?.dataset || DATASET_ID;
}

// ============================================
// STAGING TABLE OPERATIONS
// ============================================

async function ensureStagingTable(tableName) {
  const dataset = getTableDataset(tableName);
  const targetTable = `${GCP_PROJECT}.${dataset}.${tableName}`;
  const stagingTable = `${GCP_PROJECT}.${dataset}.${tableName}_staging`;

  try {
    await bigquery.dataset(dataset).table(`${tableName}_staging`).get();
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

async function loadToStaging(tableName, rows) {
  if (!rows || rows.length === 0) {
    return { tableName, success: true, error: null };
  }

  const stagingTable = await ensureStagingTable(tableName);

  // Truncate staging table
  await bigquery.query(`TRUNCATE TABLE \`${stagingTable}\``);

  try {
    await bigquery
      .dataset(getTableDataset(tableName))
      .table(`${tableName}_staging`)
      .insert(rows);

    logger.info(`Loaded ${rows.length} rows to ${stagingTable}`);
    return { tableName, success: true, error: null };
  } catch (e) {
    if (e.name === 'PartialFailureError') {
      const successCount = rows.length - (e.errors?.length || 0);
      logger.warn(`  Warning: ${e.errors?.length || 0} rows failed in ${tableName}`);
      return { tableName, success: true, error: null, partialFailures: e.errors?.length };
    }
    return { tableName, success: false, error: e.message };
  }
}

function buildMergeStatement(tableName) {
  const config = TABLE_CONFIG[tableName];
  const dataset = config.dataset;
  const mergeKey = config.merge_key;
  const columns = config.columns;

  const targetTable = `\`${GCP_PROJECT}.${dataset}.${tableName}\``;
  const stagingTable = `\`${GCP_PROJECT}.${dataset}.${tableName}_staging\``;

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

function buildDeleteOrphanedAddressesStatement(customerIds) {
  if (!customerIds || customerIds.length === 0) return null;

  const dataset = getTableDataset(TABLE_ADDRESSES);
  const targetTable = `\`${GCP_PROJECT}.${dataset}.${TABLE_ADDRESSES}\``;
  const stagingTable = `\`${GCP_PROJECT}.${dataset}.${TABLE_ADDRESSES}_staging\``;

  // Validate IDs are numeric
  for (const cid of customerIds) {
    if (!/^\d+$/.test(String(cid))) {
      throw new Error(`Invalid customer ID: ${cid}`);
    }
  }

  const customerIdsStr = customerIds.map(id => `'${id}'`).join(', ');

  return `
    DELETE FROM ${targetTable} T
    WHERE T.customer_id IN (${customerIdsStr})
    AND T.id NOT IN (SELECT id FROM ${stagingTable});
  `;
}

// ============================================
// DATA ACCUMULATOR CLASS
// ============================================

class CustomerDataAccumulator {
  constructor() {
    this.customers = [];
    this.customersData = [];
    this.addresses = [];
    this.customerIds = new Set();
  }

  addCustomer(customer) {
    const customerId = extractLegacyId(customer.id);
    const defaultAddressId = extractLegacyId(customer.defaultAddress?.id);

    this.customerIds.add(customerId);
    this.customers.push(mapCustomerToSchema(customer));
    this.customersData.push(mapCustomerDataToSchema(customer));

    const addressEdges = customer.addressesV2?.edges || [];
    for (const edge of addressEdges) {
      const address = edge.node;
      this.addresses.push(mapAddressToSchema(address, customerId, defaultAddressId));
    }
  }

  getStats() {
    return {
      [TABLE_CUSTOMERS]: { total: this.customers.length },
      [TABLE_CUSTOMERS_DATA]: { total: this.customersData.length },
      [TABLE_ADDRESSES]: { total: this.addresses.length }
    };
  }

  getAllData() {
    return {
      [TABLE_CUSTOMERS]: this.customers,
      [TABLE_CUSTOMERS_DATA]: this.customersData,
      [TABLE_ADDRESSES]: this.addresses
    };
  }

  getCustomerIds() {
    return Array.from(this.customerIds);
  }

  clear() {
    this.customers = [];
    this.customersData = [];
    this.addresses = [];
    this.customerIds.clear();
  }
}

// ============================================
// BULK MERGE OPERATIONS
// ============================================

async function executeBulkMerge(accumulator) {
  const allData = accumulator.getAllData();
  const customerIds = accumulator.getCustomerIds();

  if (allData[TABLE_CUSTOMERS].length === 0) {
    return { success: true, error: null };
  }

  // Ensure all staging tables exist and are clean
  const allTables = [TABLE_CUSTOMERS, TABLE_CUSTOMERS_DATA, TABLE_ADDRESSES];
  for (const tableName of allTables) {
    await ensureStagingTable(tableName);
    const dataset = getTableDataset(tableName);
    await bigquery.query(`TRUNCATE TABLE \`${GCP_PROJECT}.${dataset}.${tableName}_staging\``);
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

  // Delete orphaned addresses
  const deleteStmt = buildDeleteOrphanedAddressesStatement(customerIds);
  if (deleteStmt) {
    statements.push(deleteStmt);
  }

  // MERGE statements for all tables with data
  for (const [tableName, data] of Object.entries(allData)) {
    if (data && data.length > 0) {
      statements.push(buildMergeStatement(tableName));
    }
  }

  // Execute all in a transaction
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
    const dataset = getTableDataset(tableName);
    const stagingTable = `${GCP_PROJECT}.${dataset}.${tableName}_staging`;
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

async function executeGraphQL(accessToken, query, variables, debug = false) {
  if (debug) {
    logger.info('\n--- DEBUG: GraphQL Request ---');
    logger.info('URL:', GRAPHQL_URL);
    logger.info('Variables:', JSON.stringify(variables, null, 2));
  }

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

async function fetchAllCustomers(accessToken, startDate, endDate, useOAuth = false) {
  const accumulator = new CustomerDataAccumulator();

  const queryFilter = `updated_at:>='${startDate}' AND updated_at:<'${endDate}'`;
  logger.info(`Query filter: ${queryFilter}`);

  let cursor = null;
  let hasNextPage = true;
  let pageNum = 0;
  let expiresAt = null;

  const fetchStart = Date.now();

  while (hasNextPage) {
    pageNum++;

    // For OAuth mode, check if token needs refresh
    if (useOAuth && expiresAt) {
      const secondsUntilExpiry = (expiresAt - new Date()) / 1000;
      if (secondsUntilExpiry <= SHOPIFY_TOKEN_REFRESH_BUFFER_SECONDS) {
        logger.info('  Token expiring soon, refreshing...');
        const refreshed = await fetchShopifyAccessTokenOAuth();
        accessToken = refreshed.accessToken;
        expiresAt = refreshed.expiresAt;
      }
    }

    const { data, error } = await executeGraphQL(accessToken, CUSTOMERS_QUERY, { cursor, query: queryFilter });

    if (error) {
      return { accumulator: null, error: `Shopify API Error: ${error}` };
    }

    const customersData = data?.customers || {};
    const pageInfo = customersData.pageInfo || {};
    const edges = customersData.edges || [];

    for (const edge of edges) {
      accumulator.addCustomer(edge.node);
    }

    logger.info(`Page ${pageNum}: Fetched ${edges.length} customers (${accumulator.customers.length} total)`);

    hasNextPage = pageInfo.hasNextPage || false;
    cursor = pageInfo.endCursor;

    if (hasNextPage) {
      await new Promise(resolve => setTimeout(resolve, THROTTLE_DELAY_MS));
    }
  }

  const fetchTime = (Date.now() - fetchStart) / 1000;
  logger.info(`Fetched ${accumulator.customers.length} customers in ${fetchTime.toFixed(2)}s`);

  return { accumulator, error: null };
}

async function syncCustomersForDateRange(accessToken, startDate, endDate, useOAuth = false) {
  // Step 1: Fetch all customers
  logger.info('Step 1: Fetching all customers from Shopify...');
  const { accumulator, error } = await fetchAllCustomers(accessToken, startDate, endDate, useOAuth);

  if (error) {
    return { success: false, error, stats: null };
  }

  const totalStats = accumulator.getStats();

  if (totalStats[TABLE_CUSTOMERS].total === 0) {
    logger.info('No customers found for the date range.');
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
    logger.info('SHOPIFY DAILY CUSTOMER SYNC (CLOUD RUN)');
    logger.info('='.repeat(60));

    // Calculate yesterday's date range (UTC)
    const today = getTodayDate();
    const yesterday = getYesterdayDate();
    const startDate = yesterday;
    const endDate = today;

    logger.info(`Syncing customers updated: ${startDate} to ${endDate}`);

    // Get Shopify token (from Secret Manager for Cloud Run)
    const { accessToken } = await getShopifyToken(false);

    // Run sync
    const { success, error, stats } = await syncCustomersForDateRange(
      accessToken, startDate, endDate, false
    );

    const totalTime = (Date.now() - startTime) / 1000;

    if (!success) {
      logger.error(`Sync failed: ${error}`);
      res.status(500).send(`Sync failed: ${error}`);
      return;
    }

    // Format response
    if (!stats || stats[TABLE_CUSTOMERS].total === 0) {
      logger.info('No customers found for the date range.');
      res.status(200).send(
        `Daily Customer Sync Complete!\n\n` +
        `Date range: ${startDate} to ${endDate}\n` +
        `No customers found.\n` +
        `Total time: ${totalTime.toFixed(2)}s`
      );
      return;
    }

    const responseLines = [
      'Daily Customer Sync Complete!',
      '',
      `Date range: ${startDate} to ${endDate}`,
      `Filter: updated_at`,
      `Total time: ${totalTime.toFixed(2)}s`,
      '',
      'Records synced:',
      `- Customers: ${stats[TABLE_CUSTOMERS].total}`,
      `- Customers Data: ${stats[TABLE_CUSTOMERS_DATA].total}`,
      `- Addresses: ${stats[TABLE_ADDRESSES].total}`
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
// CHECKPOINT MANAGEMENT (for backfill)
// ============================================

async function ensureCheckpointTable() {
  const tableId = `${GCP_PROJECT}.${DATASET_ID}.${TABLE_SYNC_CHECKPOINTS}`;
  const table = bigquery.dataset(DATASET_ID).table(TABLE_SYNC_CHECKPOINTS);

  try {
    await table.get();
    logger.info(`Checkpoint table exists: ${tableId}`);
  } catch (e) {
    if (e.code === 404) {
      const schema = [
        { name: 'checkpoint_id', type: 'STRING', mode: 'REQUIRED' },
        { name: 'chunk_start', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'chunk_end', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'status', type: 'STRING', mode: 'REQUIRED' },
        { name: 'cursor', type: 'STRING', mode: 'NULLABLE' },
        { name: 'customers_processed', type: 'INTEGER', mode: 'NULLABLE' },
        { name: 'started_at', type: 'TIMESTAMP', mode: 'NULLABLE' },
        { name: 'completed_at', type: 'TIMESTAMP', mode: 'NULLABLE' },
        { name: 'error_message', type: 'STRING', mode: 'NULLABLE' }
      ];
      await bigquery.dataset(DATASET_ID).createTable(TABLE_SYNC_CHECKPOINTS, { schema });
      logger.info(`Created checkpoint table: ${tableId}`);
    } else {
      throw e;
    }
  }
}

function generateDateChunks(startDateStr, endDateStr, chunkDays) {
  const start = new Date(startDateStr);
  const end = new Date(endDateStr);
  const chunks = [];

  let current = new Date(start);
  while (current < end) {
    const chunkEnd = new Date(current);
    chunkEnd.setDate(chunkEnd.getDate() + chunkDays);
    if (chunkEnd > end) chunkEnd.setTime(end.getTime());

    chunks.push({
      start: current.toISOString(),
      end: chunkEnd.toISOString()
    });

    current = new Date(chunkEnd);
  }

  return chunks;
}

async function initializeCheckpoints(chunks) {
  const tableId = `${GCP_PROJECT}.${DATASET_ID}.${TABLE_SYNC_CHECKPOINTS}`;

  try {
    await bigquery.query(`DELETE FROM \`${tableId}\` WHERE TRUE`);
    logger.info('Cleared existing checkpoints');
  } catch (e) {
    logger.info(`Note: Could not clear checkpoints: ${e.message}`);
  }

  if (chunks.length > 0) {
    const values = chunks.map(chunk => {
      const checkpointId = `customers_${chunk.start.slice(0, 10)}`;
      return `('${checkpointId}', TIMESTAMP('${chunk.start}'), TIMESTAMP('${chunk.end}'), 'pending', NULL, 0, NULL, NULL, NULL)`;
    });

    const batchSize = 100;
    for (let i = 0; i < values.length; i += batchSize) {
      const batchValues = values.slice(i, i + batchSize);
      const query = `
        INSERT INTO \`${tableId}\`
        (checkpoint_id, chunk_start, chunk_end, status, cursor, customers_processed, started_at, completed_at, error_message)
        VALUES ${batchValues.join(', ')}
      `;
      await bigquery.query(query);
    }

    logger.info(`Initialized ${chunks.length} checkpoints`);
  }
}

async function getNextPendingCheckpoint() {
  const tableId = `${GCP_PROJECT}.${DATASET_ID}.${TABLE_SYNC_CHECKPOINTS}`;

  let [rows] = await bigquery.query(`
    SELECT * FROM \`${tableId}\`
    WHERE status = 'in_progress'
    ORDER BY chunk_start ASC
    LIMIT 1
  `);

  if (rows.length > 0) return rows[0];

  [rows] = await bigquery.query(`
    SELECT * FROM \`${tableId}\`
    WHERE status = 'pending'
    ORDER BY chunk_start ASC
    LIMIT 1
  `);

  return rows.length > 0 ? rows[0] : null;
}

async function updateCheckpoint(checkpointId, updates) {
  const tableId = `${GCP_PROJECT}.${DATASET_ID}.${TABLE_SYNC_CHECKPOINTS}`;

  const setClauses = Object.entries(updates).map(([key, value]) => {
    if (value === null) return `${key} = NULL`;
    if (typeof value === 'string') return `${key} = '${value.replace(/'/g, "\\'")}'`;
    if (typeof value === 'number') return `${key} = ${value}`;
    return `${key} = '${String(value).replace(/'/g, "\\'")}'`;
  });

  const query = `
    UPDATE \`${tableId}\`
    SET ${setClauses.join(', ')}
    WHERE checkpoint_id = '${checkpointId}'
  `;

  try {
    await bigquery.query(query);
  } catch (e) {
    if (!e.message.toLowerCase().includes('streaming buffer')) {
      logger.warn(`  Warning: Could not update checkpoint: ${e.message}`);
    }
  }
}

async function getBackfillProgress() {
  const tableId = `${GCP_PROJECT}.${DATASET_ID}.${TABLE_SYNC_CHECKPOINTS}`;

  try {
    const [rows] = await bigquery.query(`
      SELECT
        status,
        COUNT(*) as count,
        SUM(customers_processed) as total_customers
      FROM \`${tableId}\`
      GROUP BY status
    `);

    const progress = {};
    for (const row of rows) {
      progress[row.status] = { count: row.count, customers: row.total_customers || 0 };
    }
    return progress;
  } catch (e) {
    return {};
  }
}

// ============================================
// CLI COMMANDS
// ============================================

async function cmdIncrementalSync(args) {
  const startDate = args.startDate || getYesterdayDate();
  const endDate = args.endDate || getTodayDate();

  logger.info('Starting incremental customer sync...');
  logger.info(`  Fetching customers updated from ${startDate} to ${endDate}`);
  logger.info(`  Using MERGE upsert pattern (optimized)`);
  logger.info('');

  const startTime = Date.now();

  // Get token via OAuth for CLI mode
  const { accessToken } = await getShopifyToken(true);

  // Run sync
  const { success, error, stats } = await syncCustomersForDateRange(
    accessToken, startDate, endDate, true
  );

  const totalTime = (Date.now() - startTime) / 1000;

  if (!success) {
    throw new Error(error);
  }

  const customersCount = stats?.[TABLE_CUSTOMERS]?.total || 0;

  console.log('\n' + '='.repeat(50));
  console.log('INCREMENTAL SYNC COMPLETE');
  console.log('='.repeat(50));
  console.log(`  Date range: ${startDate} to ${endDate}`);
  console.log(`  Customers synced: ${customersCount.toLocaleString()}`);
  console.log(`  Customers data synced: ${stats?.[TABLE_CUSTOMERS_DATA]?.total || 0}`);
  console.log(`  Addresses synced: ${stats?.[TABLE_ADDRESSES]?.total || 0}`);
  console.log(`  Duration: ${totalTime.toFixed(1)} seconds`);
  console.log('='.repeat(50) + '\n');
}

async function cmdInit(args) {
  logger.info('Initializing customers backfill...');

  await ensureCheckpointTable();

  const startDate = args.backfillStartDate || BACKFILL_START_DATE;
  const endDate = args.backfillEndDate || new Date().toISOString().slice(0, 10);
  const chunkDays = args.chunkDays || BACKFILL_CHUNK_DAYS;

  const chunks = generateDateChunks(startDate, endDate, chunkDays);

  console.log(`Date range: ${startDate} to ${endDate}`);
  console.log(`Chunk size: ${chunkDays} days`);
  console.log(`Total chunks: ${chunks.length}`);

  await initializeCheckpoints(chunks);
  console.log('\nRun with --backfill to start processing.');
}

async function cmdStatus() {
  const progress = await getBackfillProgress();

  const completed = progress.completed || { count: 0, customers: 0 };
  const pending = progress.pending || { count: 0, customers: 0 };
  const inProgress = progress.in_progress || { count: 0, customers: 0 };
  const failed = progress.failed || { count: 0, customers: 0 };

  const totalChunks = completed.count + pending.count + inProgress.count + failed.count;

  console.log('\n' + '='.repeat(50));
  console.log('CUSTOMERS BACKFILL STATUS');
  console.log('='.repeat(50));
  console.log(`Completed:   ${String(completed.count).padStart(4)} chunks  (${completed.customers.toLocaleString()} customers)`);
  console.log(`In Progress: ${String(inProgress.count).padStart(4)} chunks`);
  console.log(`Pending:     ${String(pending.count).padStart(4)} chunks`);
  console.log(`Failed:      ${String(failed.count).padStart(4)} chunks`);
  console.log('-'.repeat(50));
  console.log(`Total:       ${String(totalChunks).padStart(4)} chunks`);
  console.log('='.repeat(50) + '\n');
}

async function cmdRetryFailed() {
  const tableId = `${GCP_PROJECT}.${DATASET_ID}.${TABLE_SYNC_CHECKPOINTS}`;

  await bigquery.query(`
    UPDATE \`${tableId}\`
    SET status = 'pending', error_message = NULL, cursor = NULL
    WHERE status = 'failed'
  `);

  console.log('Failed chunks reset to pending.');
}

async function cmdBackfill(args) {
  logger.info('Starting customers backfill...');
  logger.info(`Batch size: ${BATCH_SIZE} customers`);
  logger.info('Press Ctrl+C to pause (progress is saved)\n');

  let { accessToken, expiresAt } = await getShopifyToken(true);

  // Handle Ctrl+C gracefully
  process.on('SIGINT', async () => {
    console.log('\n\nPaused. Progress has been saved.');
    console.log('Run again to resume from where you left off.');
    await cmdStatus();
    process.exit(0);
  });

  while (true) {
    const checkpoint = await getNextPendingCheckpoint();

    if (!checkpoint) {
      console.log('\n' + '='.repeat(50));
      console.log('BACKFILL COMPLETE!');
      console.log('='.repeat(50));
      await cmdStatus();
      break;
    }

    const checkpointId = checkpoint.checkpoint_id;
    const chunkStart = checkpoint.chunk_start.value || checkpoint.chunk_start;
    const chunkEnd = checkpoint.chunk_end.value || checkpoint.chunk_end;

    const chunkStartStr = new Date(chunkStart).toISOString().slice(0, 10);
    const chunkEndStr = new Date(chunkEnd).toISOString().slice(0, 10);

    console.log(`\n${'='.repeat(50)}`);
    console.log(`Processing: ${chunkStartStr} to ${chunkEndStr}`);
    console.log('='.repeat(50));

    // Check if token needs refresh
    if (expiresAt) {
      const secondsUntilExpiry = (expiresAt - new Date()) / 1000;
      if (secondsUntilExpiry <= SHOPIFY_TOKEN_REFRESH_BUFFER_SECONDS) {
        logger.info('  Token expiring soon, refreshing...');
        ({ accessToken, expiresAt } = await fetchShopifyAccessTokenOAuth());
      }
    }

    await updateCheckpoint(checkpointId, {
      status: 'in_progress',
      started_at: new Date().toISOString()
    });

    // Backfill uses created_at for initial load
    const accumulator = new CustomerDataAccumulator();
    const queryFilter = `created_at:>='${chunkStartStr}' AND created_at:<'${chunkEndStr}'`;

    let cursor = null;
    let hasNextPage = true;
    let pageNum = 0;
    const chunkStartTime = Date.now();

    try {
      while (hasNextPage) {
        pageNum++;
        const { data, error } = await executeGraphQL(accessToken, CUSTOMERS_QUERY, { cursor, query: queryFilter });

        if (error) throw new Error(error);

        const customersData = data?.customers || {};
        const pageInfo = customersData.pageInfo || {};
        const edges = customersData.edges || [];

        for (const edge of edges) {
          accumulator.addCustomer(edge.node);
        }

        hasNextPage = pageInfo.hasNextPage || false;
        cursor = pageInfo.endCursor;

        if (pageNum % 10 === 0) {
          const elapsed = (Date.now() - chunkStartTime) / 1000;
          const rate = elapsed > 0 ? Math.round(accumulator.customers.length / elapsed) : 0;
          console.log(`  Page ${pageNum}: ${accumulator.customers.length} customers (${rate} customers/sec)`);
        }

        if (hasNextPage) {
          await new Promise(resolve => setTimeout(resolve, THROTTLE_DELAY_MS));
        }
      }

      // Execute bulk merge for this chunk
      if (accumulator.customers.length > 0) {
        logger.info(`  Writing ${accumulator.customers.length} customers to BigQuery via MERGE...`);
        const { success, error: mergeError } = await executeBulkMerge(accumulator);
        if (!success) throw new Error(mergeError);
      }

      const elapsed = (Date.now() - chunkStartTime) / 1000;
      const rate = elapsed > 0 ? Math.round(accumulator.customers.length / elapsed) : 0;

      await updateCheckpoint(checkpointId, {
        status: 'completed',
        completed_at: new Date().toISOString(),
        customers_processed: accumulator.customers.length,
        cursor: null
      });

      console.log(`  ✓ Completed: ${accumulator.customers.length} customers in ${elapsed.toFixed(1)}s (${rate} customers/sec)`);

    } catch (e) {
      await updateCheckpoint(checkpointId, {
        status: 'failed',
        error_message: String(e.message).slice(0, 500)
      });

      console.log(`  ✗ Failed: ${e.message}`);

      if (!args.continueOnError) {
        throw e;
      }
    }
  }
}

// ============================================
// CLI ENTRY POINT
// ============================================

async function runCLI() {
  const args = {
    init: process.argv.includes('--init'),
    status: process.argv.includes('--status'),
    retryFailed: process.argv.includes('--retry-failed'),
    backfill: process.argv.includes('--backfill'),
    continueOnError: process.argv.includes('--continue-on-error'),
    startDate: null,
    endDate: null,
    backfillStartDate: null,
    backfillEndDate: null,
    chunkDays: null
  };

  // Parse arguments
  for (let i = 2; i < process.argv.length; i++) {
    if (process.argv[i] === '--start' && process.argv[i + 1]) {
      args.startDate = process.argv[i + 1];
    }
    if (process.argv[i] === '--end' && process.argv[i + 1]) {
      args.endDate = process.argv[i + 1];
    }
    if (process.argv[i] === '--backfill-start-date' && process.argv[i + 1]) {
      args.backfillStartDate = process.argv[i + 1];
    }
    if (process.argv[i] === '--backfill-end-date' && process.argv[i + 1]) {
      args.backfillEndDate = process.argv[i + 1];
    }
    if (process.argv[i] === '--chunk-days' && process.argv[i + 1]) {
      args.chunkDays = parseInt(process.argv[i + 1]);
    }
  }

  try {
    if (args.init) {
      await cmdInit(args);
    } else if (args.status) {
      await cmdStatus();
    } else if (args.retryFailed) {
      await cmdRetryFailed();
    } else if (args.backfill) {
      await cmdBackfill(args);
    } else {
      // Default: incremental sync
      await cmdIncrementalSync(args);
    }
  } catch (e) {
    console.error('Error:', e.message);
    process.exit(1);
  }
}

// Only run CLI if this is the main module (not imported for Cloud Run)
if (require.main === module) {
  // Load dotenv for CLI mode
  try {
    require('dotenv').config();
  } catch (e) {
    // dotenv is optional
  }
  runCLI();
}

// Export for testing
module.exports = {
  syncCustomersForDateRange,
  fetchAllCustomers,
  CustomerDataAccumulator
};