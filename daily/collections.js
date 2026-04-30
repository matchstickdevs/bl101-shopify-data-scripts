/**
 * Shopify Collections Sync Script (Node.js) - Cloud Run + CLI
 *
 * Syncs all collections and their product associations to BigQuery.
 * Collections don't change frequently, so we sync ALL collections on each run
 * and use MERGE to upsert.
 *
 * Cloud Run Usage:
 *   Deployed as Cloud Run function, triggered daily via Cloud Scheduler.
 *   HTTP GET/POST request triggers full sync of all collections.
 *
 * CLI Usage:
 *   node collections-sync.js                     # Full sync of all collections
 *   node collections-sync.js --type smart        # Sync only smart collections
 *   node collections-sync.js --type custom       # Sync only custom/manual collections
 *
 * Environment Variables (for CLI via .env):
 *   - SHOPIFY_CLIENT_ID: Shopify app client ID
 *   - SHOPIFY_CLIENT_SECRET: Shopify app client secret
 *
 * GCP Secrets (for Cloud Run):
 *   - shopify-access-token: Shopify OAuth access token
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
const TABLE_COLLECTIONS = 'Collections';
const TABLE_COLLECTION_PRODUCTS = 'CollectionProducts';

const SHOP_NAME = 'bl101';
const GRAPHQL_URL = `https://${SHOP_NAME}.myshopify.com/admin/api/2025-01/graphql.json`;

// Sync settings
const THROTTLE_DELAY_MS = 200;
const COLLECTIONS_PAGE_SIZE = 50;
const PRODUCTS_PAGE_SIZE = 250; // Max allowed by Shopify

// Shopify OAuth settings
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
// GRAPHQL QUERIES
// ============================================

const COLLECTIONS_QUERY = `
query GetCollections($cursor: String, $query: String) {
  collections(first: ${COLLECTIONS_PAGE_SIZE}, after: $cursor, query: $query) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        id
        title
        handle
        description
        descriptionHtml
        sortOrder
        templateSuffix
        updatedAt
        productsCount {
          count
        }
        image {
          url
          altText
        }
        seo {
          title
          description
        }
        ruleSet {
          appliedDisjunctively
        }
      }
    }
  }
}
`;

const COLLECTION_PRODUCTS_QUERY = `
query GetCollectionProducts($collectionId: ID!, $cursor: String) {
  collection(id: $collectionId) {
    products(first: ${PRODUCTS_PAGE_SIZE}, after: $cursor) {
      pageInfo {
        hasNextPage
        endCursor
      }
      edges {
        node {
          id
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

function mapCollectionToSchema(node) {
  if (!node) return null;

  // Determine collection type based on presence of ruleSet
  // Smart collections have rules, custom collections don't
  const collectionType = node.ruleSet ? 'smart' : 'custom';

  return {
    id: extractLegacyId(node.id),
    admin_graphql_api_id: node.id,
    title: node.title,
    handle: node.handle,
    description: node.description || null,
    description_html: node.descriptionHtml || null,
    collection_type: collectionType,
    sort_order: node.sortOrder || null,
    products_count: node.productsCount?.count || 0,
    published_at: null, // Would need additional query with read_product_listings scope
    image_url: node.image?.url || null,
    image_alt_text: node.image?.altText || null,
    seo_title: node.seo?.title || null,
    seo_description: node.seo?.description || null,
    template_suffix: node.templateSuffix || null,
    created_at: null, // Not directly available in the API
    updated_at: parseTimestamp(node.updatedAt),
    synced_at: new Date().toISOString()
  };
}

function mapCollectionProductToSchema(collectionId, productGid, position) {
  return {
    collection_id: extractLegacyId(collectionId),
    product_id: extractLegacyId(productGid),
    position: position,
    synced_at: new Date().toISOString()
  };
}

// ============================================
// TABLE CONFIGURATION
// ============================================

const TABLE_CONFIG = {
  [TABLE_COLLECTIONS]: {
    merge_key: 'id',
    columns: [
      'id', 'admin_graphql_api_id', 'title', 'handle', 'description', 'description_html',
      'collection_type', 'sort_order', 'products_count', 'published_at', 'image_url',
      'image_alt_text', 'seo_title', 'seo_description', 'template_suffix',
      'created_at', 'updated_at', 'synced_at'
    ]
  },
  [TABLE_COLLECTION_PRODUCTS]: {
    merge_key: ['collection_id', 'product_id'], // Composite key
    columns: ['collection_id', 'product_id', 'position', 'synced_at']
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

  // Handle composite key for CollectionProducts
  let onClause;
  let updateCols;
  
  if (Array.isArray(mergeKey)) {
    onClause = mergeKey.map(k => `T.${k} = S.${k}`).join(' AND ');
    updateCols = columns
      .filter(col => !mergeKey.includes(col))
      .map(col => `T.${col} = S.${col}`);
  } else {
    onClause = `T.${mergeKey} = S.${mergeKey}`;
    updateCols = columns
      .filter(col => col !== mergeKey)
      .map(col => `T.${col} = S.${col}`);
  }

  const insertCols = columns.join(', ');
  const insertVals = columns.map(col => `S.${col}`).join(', ');

  return `
    MERGE ${targetTable} T
    USING ${stagingTable} S
    ON ${onClause}
    WHEN MATCHED THEN
      UPDATE SET ${updateCols.join(', ')}
    WHEN NOT MATCHED THEN
      INSERT (${insertCols})
      VALUES (${insertVals});
  `;
}

function buildDeleteOrphanedProductsStatement(collectionIds) {
  if (!collectionIds || collectionIds.length === 0) return null;

  const targetTable = `\`${GCP_PROJECT}.${DATASET_ID}.${TABLE_COLLECTION_PRODUCTS}\``;
  const stagingTable = `\`${GCP_PROJECT}.${DATASET_ID}.${TABLE_COLLECTION_PRODUCTS}_staging\``;

  // Validate collection IDs to prevent SQL injection
  for (const cid of collectionIds) {
    if (!/^\d+$/.test(String(cid))) {
      throw new Error(`Invalid collection ID: ${cid}`);
    }
  }

  const collectionIdsStr = collectionIds.map(id => `'${id}'`).join(', ');

  // Delete products from collections that were synced but are no longer in staging
  return `
    DELETE FROM ${targetTable} T
    WHERE T.collection_id IN (${collectionIdsStr})
    AND NOT EXISTS (
      SELECT 1 FROM ${stagingTable} S 
      WHERE S.collection_id = T.collection_id 
      AND S.product_id = T.product_id
    );
  `;
}

// ============================================
// DATA ACCUMULATOR
// ============================================

class CollectionDataAccumulator {
  constructor() {
    this.collections = [];
    this.collectionProducts = [];
    this.collectionIds = new Set();
  }

  addCollection(collectionData) {
    if (!collectionData) return;
    this.collectionIds.add(collectionData.id);
    this.collections.push(collectionData);
  }

  addCollectionProducts(collectionGid, products) {
    const collectionId = extractLegacyId(collectionGid);
    for (let i = 0; i < products.length; i++) {
      this.collectionProducts.push(
        mapCollectionProductToSchema(collectionGid, products[i].id, i + 1)
      );
    }
  }

  getStats() {
    return {
      [TABLE_COLLECTIONS]: { total: this.collections.length },
      [TABLE_COLLECTION_PRODUCTS]: { total: this.collectionProducts.length }
    };
  }

  getAllData() {
    return {
      [TABLE_COLLECTIONS]: this.collections,
      [TABLE_COLLECTION_PRODUCTS]: this.collectionProducts
    };
  }

  getCollectionIds() {
    return Array.from(this.collectionIds);
  }

  clear() {
    this.collections = [];
    this.collectionProducts = [];
    this.collectionIds.clear();
  }
}

// ============================================
// BULK MERGE OPERATIONS
// ============================================

async function executeBulkMerge(accumulator) {
  const allData = accumulator.getAllData();
  const collectionIds = accumulator.getCollectionIds();

  if (allData[TABLE_COLLECTIONS].length === 0) {
    return { success: true, error: null };
  }

  // Ensure all staging tables exist and are clean
  const allTables = [TABLE_COLLECTIONS, TABLE_COLLECTION_PRODUCTS];
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

  // Delete orphaned collection products first
  if (allData[TABLE_COLLECTION_PRODUCTS].length > 0) {
    const deleteStmt = buildDeleteOrphanedProductsStatement(collectionIds);
    if (deleteStmt) {
      statements.push(deleteStmt);
    }
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

async function fetchCollectionProducts(accessToken, collectionGid) {
  const products = [];
  let cursor = null;
  let hasNextPage = true;

  while (hasNextPage) {
    const { data, error } = await executeGraphQL(accessToken, COLLECTION_PRODUCTS_QUERY, {
      collectionId: collectionGid,
      cursor
    });

    if (error) {
      logger.warn(`  Error fetching products for collection: ${error}`);
      break;
    }

    const productsData = data?.collection?.products || {};
    const pageInfo = productsData.pageInfo || {};
    const edges = productsData.edges || [];

    for (const edge of edges) {
      products.push(edge.node);
    }

    hasNextPage = pageInfo.hasNextPage || false;
    cursor = pageInfo.endCursor;

    if (hasNextPage) {
      await new Promise(resolve => setTimeout(resolve, THROTTLE_DELAY_MS));
    }
  }

  return products;
}

async function fetchAllCollections(accessToken, typeFilter = null) {
  const accumulator = new CollectionDataAccumulator();

  // Build query filter
  let queryFilter = null;
  if (typeFilter === 'smart') {
    queryFilter = 'collection_type:smart';
  } else if (typeFilter === 'custom') {
    queryFilter = 'collection_type:custom';
  }

  if (queryFilter) {
    logger.info(`Query filter: ${queryFilter}`);
  } else {
    logger.info('Fetching all collections (no filter)');
  }

  let cursor = null;
  let hasNextPage = true;
  let pageNum = 0;
  let totalProductLinks = 0;

  const fetchStart = Date.now();

  while (hasNextPage) {
    pageNum++;

    const { data, error } = await executeGraphQL(accessToken, COLLECTIONS_QUERY, { 
      cursor, 
      query: queryFilter 
    });

    if (error) {
      return { accumulator: null, error: `Shopify API Error: ${error}` };
    }

    const collectionsData = data?.collections || {};
    const pageInfo = collectionsData.pageInfo || {};
    const edges = collectionsData.edges || [];

    logger.info(`Page ${pageNum}: Fetched ${edges.length} collections`);

    // Process each collection
    for (const edge of edges) {
      const collectionNode = edge.node;
      const collectionData = mapCollectionToSchema(collectionNode);
      accumulator.addCollection(collectionData);

      // Fetch products for this collection
      const productsCount = collectionNode.productsCount?.count || 0;
      if (productsCount > 0) {
        logger.info(`  Fetching ${productsCount} products for "${collectionNode.title}"...`);
        const products = await fetchCollectionProducts(accessToken, collectionNode.id);
        accumulator.addCollectionProducts(collectionNode.id, products);
        totalProductLinks += products.length;
        logger.info(`    Fetched ${products.length} product links`);
      }
    }

    hasNextPage = pageInfo.hasNextPage || false;
    cursor = pageInfo.endCursor;

    if (hasNextPage) {
      await new Promise(resolve => setTimeout(resolve, THROTTLE_DELAY_MS));
    }
  }

  const fetchTime = (Date.now() - fetchStart) / 1000;
  logger.info(`Fetched ${accumulator.collections.length} collections with ${totalProductLinks} product links in ${fetchTime.toFixed(2)}s`);

  return { accumulator, error: null };
}

async function syncCollections(accessToken, typeFilter = null) {
  // Step 1: Fetch all collections and their products
  logger.info('Step 1: Fetching all collections from Shopify...');
  const { accumulator, error } = await fetchAllCollections(accessToken, typeFilter);

  if (error) {
    return { success: false, error, stats: null };
  }

  const totalStats = accumulator.getStats();

  if (totalStats[TABLE_COLLECTIONS].total === 0) {
    logger.info('No collections found.');
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
    logger.info('SHOPIFY COLLECTIONS SYNC (CLOUD RUN)');
    logger.info('='.repeat(60));

    // Get Shopify token (from Secret Manager for Cloud Run)
    const { accessToken } = await getShopifyToken(false);

    // Run sync (all collections)
    const { success, error, stats } = await syncCollections(accessToken, null);

    const totalTime = (Date.now() - startTime) / 1000;

    if (!success) {
      logger.error(`Sync failed: ${error}`);
      res.status(500).send(`Sync failed: ${error}`);
      return;
    }

    if (!stats || stats[TABLE_COLLECTIONS].total === 0) {
      logger.info('No collections found.');
      res.status(200).send(
        `Collections Sync Complete!\n\n` +
        `No collections found.\n` +
        `Total time: ${totalTime.toFixed(2)}s`
      );
      return;
    }

    const responseLines = [
      'Collections Sync Complete!',
      '',
      `Total time: ${totalTime.toFixed(2)}s`,
      '',
      'Records synced:',
      `- Collections: ${stats[TABLE_COLLECTIONS].total}`,
      `- Collection-Product Links: ${stats[TABLE_COLLECTION_PRODUCTS].total}`
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
  logger.info('Starting collections sync...');
  if (args.type) {
    logger.info(`  Filtering by type: ${args.type}`);
  }
  logger.info('');

  const startTime = Date.now();

  // Get token via OAuth for CLI mode
  const { accessToken } = await getShopifyToken(true);

  // Run sync
  const { success, error, stats } = await syncCollections(accessToken, args.type);

  const totalTime = (Date.now() - startTime) / 1000;

  if (!success) {
    throw new Error(error);
  }

  const collectionsCount = stats?.[TABLE_COLLECTIONS]?.total || 0;
  const productLinksCount = stats?.[TABLE_COLLECTION_PRODUCTS]?.total || 0;

  console.log('\n' + '='.repeat(50));
  console.log('COLLECTIONS SYNC COMPLETE');
  console.log('='.repeat(50));
  console.log(`  Collections synced: ${collectionsCount.toLocaleString()}`);
  console.log(`  Product links synced: ${productLinksCount.toLocaleString()}`);
  console.log(`  Duration: ${totalTime.toFixed(1)} seconds`);
  console.log('='.repeat(50) + '\n');
}

// ============================================
// CLI ENTRY POINT
// ============================================

async function runCLI() {
  const args = {
    type: null
  };

  // Parse arguments
  for (let i = 2; i < process.argv.length; i++) {
    if (process.argv[i] === '--type' && process.argv[i + 1]) {
      args.type = process.argv[i + 1];
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
  syncCollections,
  fetchAllCollections,
  CollectionDataAccumulator
};