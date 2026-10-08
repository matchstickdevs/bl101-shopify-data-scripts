-- =====================================================================
-- Shopify → BigQuery :: per-location inventory
-- Tables: Locations, InventoryLevelSnapshots
-- View:   InventoryLevels (latest snapshot only)
--
-- Written by daily/products.js in refresh-inventory mode (the daily
-- 04:00 UTC Scheduler run, or `node index.js --refresh-inventory`).
--
-- Design notes:
--   - InventoryLevelSnapshots is append-only history: one row per
--     inventory item x active location x snapshot_date. Diffing two
--     snapshot dates (plus units sold from the orders tables) gives
--     sell-through per variant / location / period.
--   - A run DELETEs the current snapshot_date partition before inserting,
--     so re-running on the same day replaces that day's snapshot rather
--     than duplicating it. The last run of the UTC day wins.
--   - variant_id / product_id / sku are denormalized onto each snapshot
--     row so history stays readable after a variant is deleted from
--     Shopify (and drops out of ProductVariants).
--   - Inactive locations are skipped for snapshots, but Locations keeps
--     every location with an accurate is_active flag, so a location that
--     gets deactivated doesn't linger as "active".
--   - IDs are Shopify legacy numeric IDs stored as STRING, matching
--     Products / ProductVariants (ProductVariants.inventory_item_id joins
--     to InventoryLevelSnapshots.inventory_item_id).
--
-- Run these once before deploying the products.js change; the sync
-- creates the *_staging tables itself (CREATE TABLE ... LIKE these).
-- =====================================================================

-- ---------------------------------------------------------------------
-- Locations  (current state, MERGEd on id each run)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `flowing-code-484318-c1.shopify.Locations` (
  id                      STRING NOT NULL,  -- Shopify legacy location ID
  name                    STRING,
  is_active               BOOL,
  fulfills_online_orders  BOOL,
  address1                STRING,
  city                    STRING,
  province_code           STRING,
  country_code            STRING,
  zip                     STRING,
  created_at              TIMESTAMP,
  updated_at              TIMESTAMP,
  admin_graphql_api_id    STRING,
  synced_at               TIMESTAMP          -- ETL metadata
)
OPTIONS (
  description = 'Shopify locations (all, including inactive). Synced by daily/products.js refresh-inventory mode.'
);

-- ---------------------------------------------------------------------
-- InventoryLevelSnapshots  (append-only daily history)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `flowing-code-484318-c1.shopify.InventoryLevelSnapshots` (
  snapshot_date      DATE NOT NULL,       -- UTC date the snapshot was taken
  snapshot_at        TIMESTAMP NOT NULL,  -- exact capture time of this run
  inventory_item_id  STRING NOT NULL,
  location_id        STRING NOT NULL,
  variant_id         STRING,              -- null if the item has no variant in the catalog fetch
  product_id         STRING,
  sku                STRING,
  available          INT64,               -- sellable now (on_hand - committed - reserved etc.)
  on_hand            INT64,               -- physically at the location
  committed          INT64,               -- allocated to unfulfilled orders
  incoming           INT64,               -- inbound on transfers / purchase orders
  level_updated_at   TIMESTAMP            -- Shopify InventoryLevel.updatedAt
)
PARTITION BY snapshot_date
CLUSTER BY location_id, product_id, variant_id
OPTIONS (
  description = 'Daily per-location inventory snapshots (one row per inventory item x active location x day). Written by daily/products.js refresh-inventory mode.'
);

-- ---------------------------------------------------------------------
-- InventoryLevels  (convenience view: latest snapshot only)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `flowing-code-484318-c1.shopify.InventoryLevels` AS
SELECT
  s.*,
  l.name AS location_name
FROM `flowing-code-484318-c1.shopify.InventoryLevelSnapshots` s
LEFT JOIN `flowing-code-484318-c1.shopify.Locations` l
  ON l.id = s.location_id
WHERE s.snapshot_date = (
  SELECT MAX(snapshot_date)
  FROM `flowing-code-484318-c1.shopify.InventoryLevelSnapshots`
);
