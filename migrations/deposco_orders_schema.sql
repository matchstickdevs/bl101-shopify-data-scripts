-- =====================================================================
-- Deposco → BigQuery :: deposco_orders dataset
-- Tables: customer_orders, customer_order_lines, sales_orders, sales_order_lines
--
-- Design notes:
--   - Every table keeps a `raw_payload` JSON column holding the full
--     API response for that record. Deposco's DTOs have many optional
--     fields (custom attributes, retail activities, duty/customs info,
--     notes, channels, etc.) that aren't worth a dedicated column yet.
--     Mapping the common/reportable fields explicitly + keeping raw
--     payload means you never lose data and can backfill new columns
--     later via a SQL extraction from raw_payload instead of re-calling
--     the API.
--   - `_synced_at` / `_source_updated_at` are ETL metadata, not Deposco
--     fields, used for incremental sync watermarking and debugging.
--   - Primary identity for all tables is the Deposco internal `id`
--     (from the `self.id` field in the API response), stored as STRING
--     even though Deposco returns it as a number, to avoid precision
--     issues and to keep IDs safely joinable/loggable.
--   - EntityRef fields (businessUnit, tradingPartner, item, pack, etc.)
--     expose `id` (int64) + `businessKey` (a map) — NOT a flat `code`.
--     So every `*_id` column comes from `<ref>.id` and every `*_code`
--     column is pulled out of `<ref>.businessKey`.
--   - Tables partition on `created_date` (immutable per row) rather than
--     placed/updated dates, so rows never hop partitions on update.
--   - Column set validated against Deposco OpenAPI: CustomerOrderDTO,
--     CustomerOrderLineDTO, SalesOrderDTO, SalesOrderLineDTO. Fields that
--     only exist on the SalesOrder DTOs were removed from the customer
--     order tables (they were previously mismapped).
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS `flowing-code-484318-c1.deposco_orders`
OPTIONS (
  description = 'Deposco order data (customer orders + derived sales orders) synced for reporting and analytics',
  location = 'US'  -- change to 'EU' if using the EU Deposco API region
);

-- ---------------------------------------------------------------------
-- customer_orders  (maps to CustomerOrderDTO)
-- The incoming order — this is the "why": what the customer/channel
-- actually ordered, before Deposco derives fulfillment-facing sales
-- orders from it.
--
-- NOTE: CustomerOrderDTO carries order-level dates in a `coDates[]`
-- array of {qualifier, value} pairs, NOT as scalar planned/actual date
-- fields. If you need those, pivot coDates by qualifier (kept in
-- raw_payload) into their own columns/table later.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `flowing-code-484318-c1.deposco_orders.customer_orders`
(
  co_id                     STRING      NOT NULL OPTIONS(description="Deposco internal ID (self.id)"),
  number                    STRING      OPTIONS(description="Deposco-assigned customer order number (number)"),
  external_order_number     STRING      OPTIONS(description="Order number/ID from the originating external system, e.g. Shopify order # (externalOrderNumber)"),
  status                    STRING      OPTIONS(description="status: Back Ordered, Complete, New, Canceled, Voided, Incomplete, Closed, Open, Sourced, Allocating, Partially Allocated, Allocated, Partially InFulfillment, InFulfillment, Partially Released, Released, Review, Accepted, Draft, Template, Rejected, Future"),
  order_type                STRING      OPTIONS(description="orderType — used in order management profile processing"),
  order_priority            STRING      OPTIONS(description="orderPriority (STRING on CustomerOrderDTO; note SalesOrderDTO returns this as an integer)"),

  business_unit_id          STRING      OPTIONS(description="businessUnit.id"),
  business_unit_code        STRING      OPTIONS(description="businessUnit.businessKey"),

  trading_partner_id        STRING      OPTIONS(description="tradingPartner.id"),
  trading_partner_code      STRING      OPTIONS(description="tradingPartner.businessKey"),

  primary_sales_channel     STRING      OPTIONS(description="primarySalesChannel (EntityRef flattened to its code/businessKey)"),
  order_source              STRING      OPTIONS(description="orderSource, e.g. Shopify"),
  secondary_order_source    STRING      OPTIONS(description="secondaryOrderSource, e.g. specific storefront name"),
  order_summary             STRING      OPTIONS(description="orderSummary"),

  account_number            STRING      OPTIONS(description="accountNumber — customer account number"),

  ship_to_name              STRING      OPTIONS(description="shipToContact.contactName"),
  ship_to_phone             STRING      OPTIONS(description="shipToContact.phone"),
  ship_to_email             STRING      OPTIONS(description="shipToContact.email"),
  ship_to_address           STRUCT<
    line1 STRING, line2 STRING, city STRING,
    state_province STRING, postal_code STRING, country STRING
  >                                     OPTIONS(description="shipToContact address (AddressDTO; line3/line4 dropped)"),

  bill_to_name              STRING      OPTIONS(description="billToContact.contactName"),
  bill_to_phone             STRING      OPTIONS(description="billToContact.phone"),
  bill_to_email             STRING      OPTIONS(description="billToContact.email"),
  bill_to_address           STRUCT<
    line1 STRING, line2 STRING, city STRING,
    state_province STRING, postal_code STRING, country STRING
  >                                     OPTIONS(description="billToContact address (AddressDTO; line3/line4 dropped)"),

  placed_date               TIMESTAMP   OPTIONS(description="placedDate — date the order was placed, regardless of when sent to Deposco"),

  order_total               NUMERIC     OPTIONS(description="total"),
  order_subtotal            NUMERIC     OPTIONS(description="subTotal"),
  order_discount_subtotal   NUMERIC     OPTIONS(description="discountSubtotal"),
  order_shipping_total      NUMERIC     OPTIONS(description="shippingTotal"),
  order_ship_total          NUMERIC     OPTIONS(description="shippingAmount"),
  taxable                   BOOL        OPTIONS(description="taxable"),
  order_tax_total           NUMERIC     OPTIONS(description="taxTotal"),
  order_taxable_total       NUMERIC     OPTIONS(description="taxableTotal"),
  order_untaxable_total     NUMERIC     OPTIONS(description="untaxableTotal"),
  shipping_tax_total        NUMERIC     OPTIONS(description="shipTaxTotal"),

  ship_via                  STRING      OPTIONS(description="shipVia"),
  freight_terms_type        STRING      OPTIONS(description="freightTermsType"),
  weight_value              NUMERIC     OPTIONS(description="weight.weight"),
  weight_units              STRING      OPTIONS(description="weight.units"),

  duty_paid_by              STRING      OPTIONS(description="dutyPaidBy — DDU or DDP"),
  import_type               STRING      OPTIONS(description="importType"),
  other_reference_number    STRING      OPTIONS(description="otherReferenceNumber"),
  other_reference_number_2  STRING      OPTIONS(description="otherReferenceNumber2"),

  created_date              TIMESTAMP   OPTIONS(description="createdDate — Deposco record creation timestamp"),
  updated_date              TIMESTAMP   OPTIONS(description="updatedDate — Deposco record last-updated timestamp; drives incremental sync"),
  created_by                STRING      OPTIONS(description="createdBy.id / businessKey"),
  updated_by                STRING      OPTIONS(description="updatedBy.id / businessKey"),

  raw_payload               JSON        OPTIONS(description="Full raw API response for this record"),
  _synced_at                TIMESTAMP   OPTIONS(description="ETL: when this row was written/merged by the sync job"),
  _source_updated_at        TIMESTAMP   OPTIONS(description="ETL: copy of updated_date at sync time, used to detect drift/reprocessing")
)
PARTITION BY DATE(created_date)
CLUSTER BY business_unit_code, status
OPTIONS (
  description = "Customer order headers synced from Deposco — the point of new-order creation"
);

-- ---------------------------------------------------------------------
-- customer_order_lines  (maps to CustomerOrderLineDTO)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `flowing-code-484318-c1.deposco_orders.customer_order_lines`
(
  line_id                 STRING    NOT NULL OPTIONS(description="Deposco internal ID (self.id) for the line"),
  co_id                   STRING    NOT NULL OPTIONS(description="FK -> customer_orders.co_id (coHeader.id)"),
  co_number               STRING    OPTIONS(description="Denormalized customer order number, for convenient joins/filtering"),

  number                  STRING    OPTIONS(description="number — Deposco line identification number"),
  external_line_number    STRING    OPTIONS(description="externalLineNumber — line identifier in the external/originating system"),

  item_id                 STRING    OPTIONS(description="item.id"),
  item_number             STRING    OPTIONS(description="itemNumber"),
  pack_id                 STRING    OPTIONS(description="pack.id"),
  pack_quantity           NUMERIC   OPTIONS(description="packQuantity — unit-of-measure conversion quantity for the pack"),
  order_quantity          NUMERIC   OPTIONS(description="orderQuantity — quantity ordered on this line"),
  canceled_quantity       NUMERIC   OPTIONS(description="canceledQuantity"),
  returned_quantity       NUMERIC   OPTIONS(description="returnedQuantity"),

  created_date            TIMESTAMP OPTIONS(description="createdDate"),
  updated_date            TIMESTAMP OPTIONS(description="updatedDate"),
  created_by              STRING    OPTIONS(description="createdBy.id / businessKey"),
  updated_by              STRING    OPTIONS(description="updatedBy.id / businessKey"),

  raw_payload             JSON,
  _synced_at              TIMESTAMP,
  _source_updated_at      TIMESTAMP
)
PARTITION BY DATE(created_date)
CLUSTER BY co_number, item_number
OPTIONS (
  description = "Customer order line items synced from Deposco"
);

-- ---------------------------------------------------------------------
-- sales_orders  (maps to SalesOrderDTO)
-- The derived fulfillment-facing order — the "how": what Deposco
-- actually allocates, picks, and ships against.
--
-- NOTE: SalesOrderDTO has no single `parentOrder` field. The link back
-- to the customer order(s) is `customerOrderHeaders[]` (an array of
-- EntityRef — a SO can derive from more than one CO) plus the scalar
-- `customerOrderNumber`. We store the header IDs as an array and join
-- on IDs; see v_order_funnel below.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `flowing-code-484318-c1.deposco_orders.sales_orders`
(
  so_id                     STRING      NOT NULL OPTIONS(description="Deposco internal ID (self.id)"),
  number                    STRING      OPTIONS(description="number — Deposco-assigned sales order number"),
  order_status              STRING      OPTIONS(description="orderStatus: New, Hold, Ready for Fulfillment, Wave Built, Back Ordered, Partially Released, Released, Picking, Pick Complete, Partially Staged, Staged, Partially Loaded, Loaded, Partially Shipped, Ship Complete, Canceled, etc."),

  -- Link back to originating customer order(s)
  customer_order_header_ids ARRAY<STRING> OPTIONS(description="customerOrderHeaders[].id — Deposco IDs of the source customer order header(s); array because a SO can derive from multiple COs. Join key to customer_orders.co_id."),
  customer_order_number     STRING      OPTIONS(description="customerOrderNumber — customer-facing order number as returned on the SO"),

  business_unit_id          STRING      OPTIONS(description="businessUnit.id"),
  business_unit_code        STRING      OPTIONS(description="businessUnit.businessKey"),

  ship_from_facility_id     STRING      OPTIONS(description="shipFromFacility.id"),
  ship_from_facility_number STRING      OPTIONS(description="shipFromFacility.businessKey"),

  order_source              STRING      OPTIONS(description="orderSource"),
  secondary_order_source    STRING      OPTIONS(description="secondaryOrderSource"),
  order_summary             STRING      OPTIONS(description="orderSummary"),
  order_priority            INT64       OPTIONS(description="orderPriority (integer on SalesOrderDTO)"),

  consignee_partner_code    STRING      OPTIONS(description="consigneePartner.businessKey"),

  ship_to_name              STRING      OPTIONS(description="shipToContact.contactName"),
  ship_to_phone             STRING      OPTIONS(description="shipToContact.phone"),
  ship_to_email             STRING      OPTIONS(description="shipToContact.email"),
  ship_to_address           STRUCT<
    line1 STRING, line2 STRING, city STRING,
    state_province STRING, postal_code STRING, country STRING
  >                                     OPTIONS(description="shipToAddress (AddressDTO; line3/line4 dropped)"),

  bill_to_name              STRING      OPTIONS(description="billToContact.contactName"),
  bill_to_phone             STRING      OPTIONS(description="billToContact.phone"),
  bill_to_email             STRING      OPTIONS(description="billToContact.email"),
  bill_to_account_number    STRING      OPTIONS(description="billToAccountNumber"),
  bill_to_address           STRUCT<
    line1 STRING, line2 STRING, city STRING,
    state_province STRING, postal_code STRING, country STRING
  >                                     OPTIONS(description="billToAddress (AddressDTO; line3/line4 dropped)"),

  placed_date               TIMESTAMP   OPTIONS(description="placedDate"),
  planned_arrival_date      TIMESTAMP   OPTIONS(description="plannedArrivalDate"),
  planned_release_date      TIMESTAMP   OPTIONS(description="plannedReleaseDate"),
  planned_ship_date         TIMESTAMP   OPTIONS(description="plannedShipDate"),
  earliest_ship_date        TIMESTAMP   OPTIONS(description="earliestShipDate"),
  earliest_delivery_date    TIMESTAMP   OPTIONS(description="earliestDeliveryDate"),
  cancel_by_arrival_date    TIMESTAMP   OPTIONS(description="cancelByArrivalDate"),
  cancel_by_release_date    TIMESTAMP   OPTIONS(description="cancelByReleaseDate"),
  cancel_by_ship_date       TIMESTAMP   OPTIONS(description="cancelByShipDate"),
  actual_arrival_date       TIMESTAMP   OPTIONS(description="actualArrivalDate"),
  actual_release_date       TIMESTAMP   OPTIONS(description="actualReleaseDate"),
  actual_ship_date          TIMESTAMP   OPTIONS(description="actualShipDate"),

  order_total               NUMERIC     OPTIONS(description="orderTotal"),
  order_subtotal            NUMERIC     OPTIONS(description="orderSubtotal"),
  order_discount_subtotal   NUMERIC     OPTIONS(description="orderDiscountSubtotal"),
  order_shipping_total      NUMERIC     OPTIONS(description="orderShippingTotal"),
  order_ship_total          NUMERIC     OPTIONS(description="orderShipTotal"),
  taxable                   BOOL        OPTIONS(description="taxable"),
  order_tax_total           NUMERIC     OPTIONS(description="orderTaxTotal"),
  order_taxable_total       NUMERIC     OPTIONS(description="orderTaxableTotal"),
  order_untaxable_total     NUMERIC     OPTIONS(description="orderUntaxableTotal"),
  shipping_tax_total        NUMERIC     OPTIONS(description="shippingTaxTotal"),

  ship_via                  STRING      OPTIONS(description="shipVia"),
  freight_terms_type        STRING      OPTIONS(description="freightTermsType"),
  weight_value              NUMERIC     OPTIONS(description="weight.weight"),
  weight_units              STRING      OPTIONS(description="weight.units"),

  duty_paid_by              STRING      OPTIONS(description="dutyPaidBy"),
  duty_paid_by_type         STRING      OPTIONS(description="dutyPaidByType — Seller, Receiver, Third party"),
  drop_ship                 BOOL        OPTIONS(description="dropShip"),

  post_release_state        STRING      OPTIONS(description="postReleaseState — async post-release processing state: cartonization, SSCC generation, label printing, shipment creation"),

  created_date              TIMESTAMP   OPTIONS(description="createdDate"),
  updated_date              TIMESTAMP   OPTIONS(description="updatedDate"),
  created_by                STRING      OPTIONS(description="createdBy.id / businessKey"),
  updated_by                STRING      OPTIONS(description="updatedBy.id / businessKey"),

  raw_payload               JSON,
  _synced_at                TIMESTAMP,
  _source_updated_at        TIMESTAMP
)
PARTITION BY DATE(created_date)
CLUSTER BY business_unit_code, order_status
OPTIONS (
  description = "Sales order headers synced from Deposco — the fulfillment-facing order derived from a customer order"
);

-- ---------------------------------------------------------------------
-- sales_order_lines  (maps to SalesOrderLineDTO)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `flowing-code-484318-c1.deposco_orders.sales_order_lines`
(
  line_id                  STRING    NOT NULL OPTIONS(description="Deposco internal ID (self.id) for the line"),
  so_id                    STRING    NOT NULL OPTIONS(description="FK -> sales_orders.so_id (orderHeader.id)"),
  so_number                STRING    OPTIONS(description="Denormalized sales order number, for convenient joins/filtering"),
  line_number              STRING    OPTIONS(description="lineNumber"),
  co_line_id               STRING    OPTIONS(description="coLine.id — FK to customer_order_lines.line_id; line-grain link back to the CO line"),

  item_id                  STRING    OPTIONS(description="item.id"),
  product_code             STRING    OPTIONS(description="productCode"),
  pack_id                  STRING    OPTIONS(description="pack.id"),

  order_pack_quantity      NUMERIC   OPTIONS(description="orderPackQuantity — quantity of the item/pack ordered"),
  shipped_pack_quantity    NUMERIC   OPTIONS(description="shippedPackQuantity — quantity of the item/pack shipped"),
  canceled_pack_quantity   NUMERIC   OPTIONS(description="canceledPackQuantity"),
  returned_pack_quantity   NUMERIC   OPTIONS(description="returnedPackQuantity"),

  export_reference         STRING    OPTIONS(description="exportReference"),

  created_date             TIMESTAMP OPTIONS(description="createdDate"),
  updated_date             TIMESTAMP OPTIONS(description="updatedDate"),
  created_by               STRING    OPTIONS(description="createdBy.id / businessKey"),
  updated_by               STRING    OPTIONS(description="updatedBy.id / businessKey"),

  raw_payload              JSON,
  _synced_at               TIMESTAMP,
  _source_updated_at       TIMESTAMP
)
PARTITION BY DATE(created_date)
CLUSTER BY so_number, item_id
OPTIONS (
  description = "Sales order line items synced from Deposco"
);

-- ---------------------------------------------------------------------
-- Example: funnel view joining CO -> SO for lifecycle reporting.
-- Joins on the id-based relationship (sales_orders.customer_order_header_ids)
-- rather than matching order numbers, since customerOrderNumber is the
-- customer-facing value and may not equal customer_orders.number.
--
-- CAUTION: one CO can spawn multiple SOs, so a CO row can fan out to
-- multiple rows here. Aggregate SOs first before SUM-ing any CO-level
-- monetary column to avoid double counting.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `flowing-code-484318-c1.deposco_orders.v_order_funnel` AS
SELECT
  co.co_id,
  co.number             AS co_number,
  co.status             AS co_status,
  co.placed_date        AS co_placed_date,
  co.order_total        AS co_order_total,
  so.so_id,
  so.number             AS so_number,
  so.order_status       AS so_status,
  so.actual_ship_date   AS so_shipped_date
FROM `flowing-code-484318-c1.deposco_orders.customer_orders` co
LEFT JOIN `flowing-code-484318-c1.deposco_orders.sales_orders` so
  ON co.co_id IN UNNEST(so.customer_order_header_ids);
