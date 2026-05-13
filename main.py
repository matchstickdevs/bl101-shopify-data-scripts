"""
Shopify Daily Order Sync Script (Cloud Run) - BULK OPERATIONS

Syncs yesterday's updated orders to BigQuery using Shopify's Bulk Operations API.
Instead of paginating through orders one page at a time, this submits bulk queries
that Shopify processes asynchronously, then downloads the complete JSONL result file.

Key advantages over paginated approach:
1. No pagination / throttle management — Shopify handles it server-side
2. No API rate limit consumption during data fetch (only the mutation call counts)
3. Single JSONL download instead of dozens/hundreds of paginated requests
4. Dramatically faster for large date ranges (backfills)

Architecture:
- Single bulk query fetches orders with connections (lineItems, shippingLines,
  discountApplications) flattened into JSONL lines via __parentId.
- Plain list fields (transactions, refunds and their children) stay inline
  on each order object in the JSONL and are extracted during parsing.

Usage:
    Deployed as a Cloud Run Job, triggered daily via Cloud Scheduler.
    Run with no arguments for the daily sync (2-day rolling window).
    Run with --start/--end or --year for backfills.

GCP Secrets Required:
    - shopify_client_id: Shopify app client ID
    - shopify_client_secret: Shopify app client secret
"""

import requests
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import secretmanager
from google.cloud import bigquery
from datetime import datetime, timedelta, timezone
import time
import io

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURATION
# ============================================

GCP_PROJECT = "flowing-code-484318-c1"
DATASET_ID = "shopify"

# Table names
TABLE_ORDERS = "Orders"
TABLE_ITEMS = "LineItems"
TABLE_REFUNDS = "Refunds"
TABLE_REFUND_LINE_ITEMS = "RefundLineItems"
TABLE_TRANSACTIONS = "Transactions"
TABLE_ORDER_ADJUSTMENTS = "OrderAdjustments"
TABLE_SHIPPING_LINES = "ShippingLines"

# Shopify settings
SHOP_NAME = "bl101"
API_VERSION = "2026-01"
GRAPHQL_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/graphql.json"

# Bulk operation polling settings
POLL_INTERVAL_SECONDS = 5       # How often to check bulk op status
POLL_MAX_WAIT_SECONDS = 3600    # Max wait time (1 hour) before giving up
POLL_BACKOFF_MULTIPLIER = 1.2   # Gradually increase poll interval
POLL_MAX_INTERVAL_SECONDS = 30  # Cap on poll interval

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2

# ============================================
# TABLE SCHEMAS - Define merge keys and columns
# ============================================

TABLE_CONFIG = {
    TABLE_ORDERS: {
        "merge_key": "id",
        "columns": [
            "id", "admin_graphql_api_id", "app_id", "token", "checkout_token",
            "order_number", "number", "name", "confirmation_number", "customer_id",
            "contact_email", "email", "phone", "customer_locale", "created_at",
            "updated_at", "processed_at", "closed_at", "cancelled_at", "currency",
            "presentment_currency", "subtotal_price", "total_line_items_price",
            "total_discounts", "total_tax", "total_price", "total_outstanding",
            "total_tip_received", "current_subtotal_price", "current_total_discounts",
            "current_total_tax", "current_total_price", "financial_status",
            "fulfillment_status", "confirmed", "test", "tax_exempt", "taxes_included",
            "estimated_taxes", "duties_included", "buyer_accepts_marketing",
            "source_name", "source_identifier", "source_url", "landing_site",
            "landing_site_ref", "referring_site", "location_id", "shipping_address_id",
            "shipping_address_state", "billing_address_id", "total_weight", "note",
            "tags", "order_status_url", "merchant_business_entity_id",
            "merchant_of_record_app_id", "browser_ip", "device_id", "checkout_id",
            "cart_token", "company", "po_number", "reference", "user_id",
            "discount_codes", "discount_applications", "exchange",
            "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"
        ]
    },
    TABLE_ITEMS: {
        "merge_key": "id",
        "columns": [
            "id", "admin_graphql_api_id", "order_id", "product_id", "variant_id",
            "product_exists", "sku", "title", "name", "variant_title", "vendor",
            "quantity", "current_quantity", "fulfillable_quantity", "price",
            "pre_tax_price", "total_discount", "fulfillment_status",
            "fulfillment_service", "requires_shipping", "taxable", "gift_card",
            "grams", "variant_inventory_management", "properties", "attributed_staffs"
        ]
    },
    TABLE_SHIPPING_LINES: {
        "merge_key": "id",
        "columns": [
            "id", "order_id", "carrier_identifier", "code", "discounted_price",
            "is_removed", "phone", "price", "requested_fulfillment_service_id",
            "source", "title"
        ]
    },
    TABLE_TRANSACTIONS: {
        "merge_key": "id",
        "columns": [
            "id", "admin_graphql_api_id", "amount", "authorization", "created_at",
            "currency", "device_id", "error_code", "gateway", "kind", "location_id",
            "message", "order_id", "parent_id", "payment_id", "processed_at",
            "source_name", "status", "test", "user_id"
        ]
    },
    TABLE_REFUNDS: {
        "merge_key": "id",
        "columns": [
            "id", "admin_graphql_api_id", "created_at", "note", "order_id",
            "processed_at", "restock", "user_id"
        ]
    },
    TABLE_REFUND_LINE_ITEMS: {
        "merge_key": ["refund_id", "line_item_id"],
        "columns": [
            "id", "refund_id", "line_item_id", "location_id", "quantity",
            "restock_type", "subtotal", "total_tax"
        ]
    },
    TABLE_ORDER_ADJUSTMENTS: {
        "merge_key": "id",
        "columns": [
            "id", "amount", "kind", "order_id", "reason", "refund_id", "tax_amount"
        ]
    }
}

# ============================================
# BULK OPERATION GRAPHQL QUERY
#
# Single query with:
#   - 3 connections: orders → lineItems, shippingLines, discountApplications
#     (these get flattened into separate JSONL lines with __parentId)
#   - Plain list fields (NOT connections, stay inline on order object):
#     transactions, refunds (and refunds' nested children)
# ============================================

BULK_QUERY_MUTATION = """
mutation BulkQueryOrders($query: String!) {
  bulkOperationRunQuery(
    query: $query
  ) {
    bulkOperation {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
"""

# The actual query string passed as the $query variable
BULK_INNER_QUERY = """
{
  orders(query: "QUERY_FILTER", sortKey: UPDATED_AT) {
    edges {
      node {
        id
        name
        confirmationNumber
        email
        phone
        createdAt
        updatedAt
        processedAt
        closedAt
        cancelledAt
        currencyCode
        presentmentCurrencyCode
        subtotalPriceSet { shopMoney { amount } }
        totalPriceSet { shopMoney { amount } }
        totalDiscountsSet { shopMoney { amount } }
        totalTaxSet { shopMoney { amount } }
        currentSubtotalPriceSet { shopMoney { amount } }
        currentTotalPriceSet { shopMoney { amount } }
        currentTotalDiscountsSet { shopMoney { amount } }
        currentTotalTaxSet { shopMoney { amount } }
        totalOutstandingSet { shopMoney { amount } }
        totalTipReceivedSet { shopMoney { amount } }
        displayFinancialStatus
        displayFulfillmentStatus
        confirmed
        test
        taxExempt
        taxesIncluded
        dutiesIncluded
        customerAcceptsMarketing
        sourceName
        note
        tags
        totalWeight
        poNumber
        customerLocale
        
        customerJourneySummary {
          firstVisit {
            landingPage
            referrerUrl
            source
            sourceType
            utmParameters {
              source
              medium
              campaign
              content
              term
            }
          }
        }
        
        customer {
          id
        }
        
        shippingAddress {
          provinceCode
        }
        
        discountCodes
        
        discountApplications(first: 50) {
          edges {
            node {
              allocationMethod
              targetSelection
              targetType
              value {
                ... on MoneyV2 {
                  amount
                  currencyCode
                }
                ... on PricingPercentageValue {
                  percentage
                }
              }
            }
          }
        }
        
        lineItems(first: 250) {
          edges {
            node {
              id
              name
              title
              variantTitle
              sku
              vendor
              quantity
              currentQuantity
              requiresShipping
              taxable
              isGiftCard
              originalUnitPriceSet { shopMoney { amount } }
              totalDiscountSet { shopMoney { amount } }
              fulfillmentStatus
              product {
                id
              }
              variant {
                id
              }
              customAttributes {
                key
                value
              }
            }
          }
        }
        
        shippingLines(first: 20) {
          edges {
            node {
              id
              title
              code
              source
              phone
              carrierIdentifier
              requestedFulfillmentService { id }
              originalPriceSet { shopMoney { amount } }
              discountedPriceSet { shopMoney { amount } }
            }
          }
        }
        
        transactions {
          id
          kind
          status
          gateway
          amountSet { shopMoney { amount currencyCode } }
          errorCode
          createdAt
          processedAt
          test
          parentTransaction {
            id
          }
        }
        
        refunds {
          id
          createdAt
          note
        }
      }
    }
  }
}
"""


# ============================================
# GCP SECRET MANAGER
# ============================================

def get_secret(secret_id):
    """Retrieve secret from GCP Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


# ============================================
# SHOPIFY OAUTH TOKEN
# ============================================

def get_shopify_token():
    """Exchange client credentials for a short-lived Shopify access token."""
    logger.info("Fetching Shopify client credentials from Secret Manager...")
    client_id = get_secret("shopify_client_id")
    client_secret = get_secret("shopify_client_secret")

    logger.info("Requesting Shopify access token via client_credentials grant...")
    response = requests.post(
        f"https://{SHOP_NAME}.myshopify.com/admin/oauth/access_token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
    )
    response.raise_for_status()
    token = response.json()["access_token"]
    logger.info("Successfully obtained Shopify access token")
    return token


# ============================================
# BIGQUERY CLIENT
# ============================================

def get_bq_client():
    """Get BigQuery client."""
    return bigquery.Client(project=GCP_PROJECT)


# ============================================
# HELPER FUNCTIONS
# ============================================

def to_json_str(obj):
    """Convert object to JSON string."""
    if obj is None:
        return None
    return json.dumps(obj)


def parse_timestamp(ts_str):
    """Parse ISO timestamp string."""
    if ts_str is None:
        return None
    try:
        normalized = ts_str.replace('Z', '+00:00')
        dt = datetime.fromisoformat(normalized)
        return dt.isoformat()
    except (ValueError, TypeError, AttributeError) as e:
        logger.warning(f"Could not parse timestamp '{ts_str}': {e}")
        return None


def extract_money(money_set):
    """Extract float amount from GraphQL MoneyV2 structure."""
    if money_set is None:
        return 0.0
    try:
        return float(money_set.get("shopMoney", {}).get("amount", 0))
    except (ValueError, TypeError):
        return 0.0


def extract_legacy_id(gid):
    """Extract numeric ID from GraphQL global ID (gid://shopify/Order/123 -> '123')."""
    if gid is None:
        return None
    if isinstance(gid, str) and gid.startswith("gid://"):
        return gid.split("/")[-1]
    return str(gid)


# ============================================
# GRAPHQL EXECUTION
# ============================================

def execute_graphql(token, query, variables=None):
    """Execute GraphQL query against Shopify with retry logic."""
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": token
    }

    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(GRAPHQL_URL, headers=headers, json=payload)

            if response.status_code == 429:
                try:
                    retry_after = int(response.headers.get("Retry-After", RETRY_DELAY_SECONDS))
                except (ValueError, TypeError):
                    retry_after = RETRY_DELAY_SECONDS
                logger.warning(f"Rate limited, waiting {retry_after}s (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                errors = data["errors"]
                if any("THROTTLED" in str(e) for e in errors):
                    logger.warning(f"GraphQL throttled, waiting {RETRY_DELAY_SECONDS}s (attempt {attempt + 1}/{MAX_RETRIES})")
                    time.sleep(RETRY_DELAY_SECONDS)
                    continue
                return None, f"GraphQL Errors: {json.dumps(errors)}"

            return data.get("data"), None

        except requests.exceptions.RequestException as e:
            last_error = str(e)
            if attempt < MAX_RETRIES - 1:
                logger.warning(f"Request failed: {e}, retrying in {RETRY_DELAY_SECONDS}s (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY_SECONDS)
            continue

    return None, f"Failed after {MAX_RETRIES} attempts: {last_error}"


# ============================================
# BULK OPERATION MANAGEMENT
# ============================================

def submit_bulk_operation(token, inner_query, query_filter):
    """
    Submit a bulk operation query to Shopify.
    Returns (bulk_operation_id, error).
    """
    # Inject the date filter into the inner query template
    query_with_filter = inner_query.replace("QUERY_FILTER", query_filter)

    data, error = execute_graphql(
        token,
        BULK_QUERY_MUTATION,
        variables={"query": query_with_filter}
    )

    if error:
        return None, error

    result = data.get("bulkOperationRunQuery", {})
    user_errors = result.get("userErrors", [])

    if user_errors:
        return None, f"Bulk operation user errors: {json.dumps(user_errors)}"

    bulk_op = result.get("bulkOperation", {})
    op_id = bulk_op.get("id")
    status = bulk_op.get("status")

    logger.info(f"Bulk operation submitted: {op_id} (status: {status})")
    return op_id, None


def poll_bulk_operation(token, operation_id):
    """
    Poll a bulk operation until it completes, fails, or times out.
    Returns (download_url, error).
    """
    # Use the new 2026-01 bulkOperation(id:) query
    poll_query = """
    query PollBulkOperation($id: ID!) {
      bulkOperation(id: $id) {
        id
        status
        errorCode
        objectCount
        fileSize
        url
        partialDataUrl
      }
    }
    """

    elapsed = 0
    interval = POLL_INTERVAL_SECONDS

    while elapsed < POLL_MAX_WAIT_SECONDS:
        data, error = execute_graphql(token, poll_query, variables={"id": operation_id})

        if error:
            logger.warning(f"Poll error (will retry): {error}")
            time.sleep(interval)
            elapsed += interval
            continue

        op = data.get("bulkOperation", {})
        status = op.get("status")
        object_count = op.get("objectCount", "0")

        logger.info(f"Bulk op {operation_id}: status={status}, objects={object_count}")

        if status == "COMPLETED":
            url = op.get("url")
            if url is None:
                logger.info("Bulk operation completed with no results (url is null)")
                return None, None  # No data, but not an error
            return url, None

        elif status in ("FAILED", "EXPIRED"):
            error_code = op.get("errorCode")
            partial_url = op.get("partialDataUrl")
            if partial_url:
                logger.warning(f"Bulk op failed ({error_code}) but partial data available")
                return partial_url, f"Partial result (error: {error_code})"
            return None, f"Bulk operation failed: {error_code}"

        elif status == "CANCELED":
            return None, "Bulk operation was canceled"

        elif status in ("CREATED", "RUNNING", "CANCELING"):
            time.sleep(interval)
            elapsed += interval
            interval = min(interval * POLL_BACKOFF_MULTIPLIER, POLL_MAX_INTERVAL_SECONDS)

        else:
            logger.warning(f"Unknown bulk operation status: {status}")
            time.sleep(interval)
            elapsed += interval

    return None, f"Bulk operation timed out after {POLL_MAX_WAIT_SECONDS}s"


def download_jsonl(url):
    """
    Download and parse a JSONL file from Shopify's bulk operation result URL.
    Streams line by line to avoid loading entire file into memory.
    Returns list of parsed JSON objects.
    """
    logger.info(f"Downloading JSONL results...")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    objects = []
    line_count = 0
    for line in response.iter_lines():
        if line:
            line_count += 1
            obj = json.loads(line)
            objects.append(obj)

    logger.info(f"Downloaded {line_count} JSONL lines ({len(objects)} objects)")
    return objects


# ============================================
# JSONL PARSING → ACCUMULATOR
#
# Bulk operation JSONL flattens nested connections.
# Each child object gets a __parentId field pointing to its parent's GID.
# We reconstruct the parent-child relationships using these IDs.
# ============================================

def classify_jsonl_object(obj):
    """
    Determine the type of a JSONL object based on its 'id' field prefix.
    Bulk operations flatten all nested objects; we identify type by GID prefix.
    """
    gid = obj.get("id", "")
    if "Order/" in gid and "LineItem/" not in gid and "Transaction/" not in gid and "Adjustment/" not in gid:
        return "Order"
    elif "LineItem/" in gid:
        return "LineItem"
    elif "ShippingLine/" in gid:
        return "ShippingLine"
    elif "OrderTransaction/" in gid:
        return "Transaction"
    elif "Refund/" in gid:
        return "Refund"
    elif "OrderAdjustment/" in gid:
        return "OrderAdjustment"

    # DiscountApplication nodes from bulk ops don't have standard GIDs.
    # They appear as children of orders with discount-related fields.
    if "allocationMethod" in obj:
        return "DiscountApplication"

    # RefundLineItem nodes from bulk ops may not have a GID with "RefundLineItem/"
    # They have lineItem, location, quantity, restockType fields
    if "restockType" in obj or "subtotalSet" in obj:
        if "__parentId" in obj and "Refund/" in obj.get("__parentId", ""):
            return "RefundLineItem"

    return "Unknown"


class OrderDataAccumulator:
    """Accumulates all order data in memory before BigQuery operations."""

    def __init__(self):
        self.orders = []
        self.line_items = []
        self.shipping_lines = []
        self.transactions = []
        self.refunds = []
        self.refund_line_items = []
        self.order_adjustments = []
        self.order_ids = set()
        self.refund_ids = set()
        # Map GID → legacy ID for parent lookups
        self._gid_to_order_id = {}
        self._gid_to_refund_id = {}

    def process_bulk_results(self, objects):
        """
        Process JSONL objects from the single bulk query.

        Bulk JSONL flattening rules:
        - Connection fields (lineItems, shippingLines, discountApplications) get extracted
          into separate JSONL lines with __parentId pointing to the parent Order GID.
        - Plain list fields (transactions, refunds) stay INLINE on the order object.
          Refund children that are connections (refundLineItems, transactions, orderAdjustments)
          remain nested inside each inline refund object with edges/node structure.

        So each JSONL line is either:
        1. An Order object (with inline transactions + refunds including their children)
        2. A LineItem (with __parentId -> Order GID)
        3. A ShippingLine (with __parentId -> Order GID)
        4. A DiscountApplication (with __parentId -> Order GID)
        """
        # Collect discount applications per order GID for later assembly
        discount_apps_by_order = {}

        for obj in objects:
            obj_type = classify_jsonl_object(obj)

            if obj_type == "Order":
                order_gid = obj.get("id")
                order_id = extract_legacy_id(order_gid)
                self._gid_to_order_id[order_gid] = order_id
                self.order_ids.add(order_id)

                # Extract inline transactions (plain list, not a connection)
                for txn in obj.get("transactions", []):
                    self.transactions.append(self._map_transaction_from_bulk(txn, order_id))

                # Extract inline refunds (plain list, scalar data only —
                # refund children like refundLineItems, transactions, orderAdjustments
                # cannot be queried in bulk ops due to connection-in-list limitation,
                # so they are fetched separately via paginated queries)
                for refund in obj.get("refunds", []):
                    refund_id = extract_legacy_id(refund.get("id"))
                    self.refund_ids.add(refund_id)
                    self.refunds.append(self._map_refund_from_bulk(refund, order_id))

                # Map the order itself (discount_applications patched later)
                self.orders.append(self._map_order_from_bulk(obj))

            elif obj_type == "LineItem":
                parent_gid = obj.get("__parentId", "")
                order_id = self._gid_to_order_id.get(parent_gid)
                if order_id:
                    self.line_items.append(self._map_line_item_from_bulk(obj, order_id))

            elif obj_type == "ShippingLine":
                parent_gid = obj.get("__parentId", "")
                order_id = self._gid_to_order_id.get(parent_gid)
                if order_id:
                    self.shipping_lines.append(self._map_shipping_line_from_bulk(obj, order_id))

            elif obj_type == "DiscountApplication":
                parent_gid = obj.get("__parentId", "")
                if parent_gid not in discount_apps_by_order:
                    discount_apps_by_order[parent_gid] = []
                discount_apps_by_order[parent_gid].append(obj)

        # Patch discount_applications onto orders
        for order_row in self.orders:
            order_gid = order_row.get("admin_graphql_api_id")
            if order_gid and order_gid in discount_apps_by_order:
                order_row["discount_applications"] = to_json_str(discount_apps_by_order[order_gid])


    # ------------------------------------------
    # Mapping functions: bulk JSONL → BQ schema
    # ------------------------------------------

    def _map_order_from_bulk(self, order):
        """Map a bulk JSONL order object to BigQuery Orders schema."""
        customer = order.get("customer")
        shipping_address = order.get("shippingAddress")

        name = order.get("name", "")
        order_number = name.lstrip("#").split("-")[0] if name else None

        # Customer journey / attribution
        journey = order.get("customerJourneySummary") or {}
        first_visit = journey.get("firstVisit") or {}
        utm_params = first_visit.get("utmParameters") or {}

        return {
            "id": extract_legacy_id(order.get("id")),
            "admin_graphql_api_id": order.get("id"),
            "app_id": None,
            "token": None,
            "checkout_token": None,
            "order_number": order_number,
            "number": int(order_number) if order_number and order_number.isdigit() else None,
            "name": name,
            "confirmation_number": order.get("confirmationNumber"),
            "customer_id": extract_legacy_id(customer.get("id")) if customer else None,
            "contact_email": order.get("email"),
            "email": order.get("email"),
            "phone": order.get("phone"),
            "customer_locale": order.get("customerLocale"),
            "created_at": parse_timestamp(order.get("createdAt")),
            "updated_at": parse_timestamp(order.get("updatedAt")),
            "processed_at": parse_timestamp(order.get("processedAt")),
            "closed_at": parse_timestamp(order.get("closedAt")),
            "cancelled_at": parse_timestamp(order.get("cancelledAt")),
            "currency": order.get("currencyCode"),
            "presentment_currency": order.get("presentmentCurrencyCode"),
            "subtotal_price": extract_money(order.get("subtotalPriceSet")),
            "total_line_items_price": extract_money(order.get("subtotalPriceSet")) + extract_money(order.get("totalDiscountsSet")),
            "total_discounts": extract_money(order.get("totalDiscountsSet")),
            "total_tax": extract_money(order.get("totalTaxSet")),
            "total_price": extract_money(order.get("totalPriceSet")),
            "total_outstanding": extract_money(order.get("totalOutstandingSet")),
            "total_tip_received": extract_money(order.get("totalTipReceivedSet")),
            "current_subtotal_price": extract_money(order.get("currentSubtotalPriceSet")),
            "current_total_discounts": extract_money(order.get("currentTotalDiscountsSet")),
            "current_total_tax": extract_money(order.get("currentTotalTaxSet")),
            "current_total_price": extract_money(order.get("currentTotalPriceSet")),
            "financial_status": (order.get("displayFinancialStatus") or "").lower().replace("_", " "),
            "fulfillment_status": (order.get("displayFulfillmentStatus") or "").lower().replace("_", " ") if order.get("displayFulfillmentStatus") else None,
            "confirmed": bool(order.get("confirmed", False)),
            "test": bool(order.get("test", False)),
            "tax_exempt": bool(order.get("taxExempt", False)),
            "taxes_included": bool(order.get("taxesIncluded", False)),
            "estimated_taxes": False,
            "duties_included": bool(order.get("dutiesIncluded", False)),
            "buyer_accepts_marketing": bool(order.get("customerAcceptsMarketing", False)),
            "source_name": order.get("sourceName"),
            "source_identifier": first_visit.get("source"),
            "source_url": first_visit.get("referrerUrl"),
            "landing_site": first_visit.get("landingPage"),
            "landing_site_ref": utm_params.get("source"),
            "referring_site": first_visit.get("referrerUrl"),
            "location_id": None,
            "shipping_address_id": None,
            "shipping_address_state": shipping_address.get("provinceCode") if shipping_address else None,
            "billing_address_id": None,
            "total_weight": order.get("totalWeight"),
            "note": order.get("note"),
            "tags": ", ".join(order.get("tags", [])) if isinstance(order.get("tags"), list) else order.get("tags"),
            "order_status_url": None,
            "merchant_business_entity_id": None,
            "merchant_of_record_app_id": None,
            "browser_ip": None,
            "device_id": None,
            "checkout_id": None,
            "cart_token": None,
            "company": None,
            "po_number": order.get("poNumber"),
            "reference": None,
            "user_id": None,
            "discount_codes": to_json_str(order.get("discountCodes")),
            "discount_applications": None,  # Patched in after processing all children
            "exchange": "-EXC" in name,
            "utm_source": utm_params.get("source"),
            "utm_medium": utm_params.get("medium"),
            "utm_campaign": utm_params.get("campaign"),
            "utm_content": utm_params.get("content"),
            "utm_term": utm_params.get("term"),
        }

    def _map_line_item_from_bulk(self, item, parent_order_id):
        """Map a bulk JSONL line item to BigQuery LineItems schema."""
        product = item.get("product")
        variant = item.get("variant")
        custom_attributes = item.get("customAttributes", [])

        return {
            "id": extract_legacy_id(item.get("id")),
            "admin_graphql_api_id": item.get("id"),
            "order_id": parent_order_id,
            "product_id": extract_legacy_id(product.get("id")) if product else None,
            "variant_id": extract_legacy_id(variant.get("id")) if variant else None,
            "product_exists": product is not None,
            "sku": item.get("sku"),
            "title": item.get("title"),
            "name": item.get("name"),
            "variant_title": item.get("variantTitle"),
            "vendor": item.get("vendor"),
            "quantity": item.get("quantity", 0),
            "current_quantity": item.get("currentQuantity") or item.get("quantity", 0),
            "fulfillable_quantity": 0,
            "price": extract_money(item.get("originalUnitPriceSet")),
            "pre_tax_price": extract_money(item.get("originalUnitPriceSet")),
            "total_discount": extract_money(item.get("totalDiscountSet")),
            "fulfillment_status": item.get("fulfillmentStatus"),
            "fulfillment_service": None,
            "requires_shipping": bool(item.get("requiresShipping", True)),
            "taxable": bool(item.get("taxable", False)),
            "gift_card": bool(item.get("isGiftCard", False)),
            "grams": 0,
            "variant_inventory_management": None,
            "properties": to_json_str(custom_attributes),
            "attributed_staffs": None,
        }

    def _map_shipping_line_from_bulk(self, sl, parent_order_id):
        """Map a bulk JSONL shipping line to BigQuery ShippingLines schema."""
        fulfillment_service = sl.get("requestedFulfillmentService")

        return {
            "id": extract_legacy_id(sl.get("id")),
            "order_id": parent_order_id,
            "carrier_identifier": sl.get("carrierIdentifier"),
            "code": sl.get("code"),
            "discounted_price": extract_money(sl.get("discountedPriceSet")),
            "is_removed": False,
            "phone": sl.get("phone"),
            "price": extract_money(sl.get("originalPriceSet")),
            "requested_fulfillment_service_id": fulfillment_service.get("id") if fulfillment_service else None,
            "source": sl.get("source"),
            "title": sl.get("title"),
        }

    def _map_transaction_from_bulk(self, txn, parent_order_id):
        """Map a bulk JSONL transaction to BigQuery Transactions schema."""
        amount_set = txn.get("amountSet", {}).get("shopMoney", {})
        parent_txn = txn.get("parentTransaction")

        return {
            "id": extract_legacy_id(txn.get("id")),
            "admin_graphql_api_id": txn.get("id"),
            "amount": float(amount_set.get("amount", 0)) if amount_set.get("amount") else 0.0,
            "authorization": None,
            "created_at": parse_timestamp(txn.get("createdAt")),
            "currency": amount_set.get("currencyCode"),
            "device_id": None,
            "error_code": txn.get("errorCode"),
            "gateway": txn.get("gateway"),
            "kind": (txn.get("kind") or "").lower(),
            "location_id": None,
            "message": None,
            "order_id": parent_order_id,
            "parent_id": extract_legacy_id(parent_txn.get("id")) if parent_txn else None,
            "payment_id": None,
            "processed_at": parse_timestamp(txn.get("processedAt")),
            "source_name": None,
            "status": (txn.get("status") or "").lower(),
            "test": bool(txn.get("test", False)),
            "user_id": None,
        }

    def _map_refund_from_bulk(self, refund, parent_order_id):
        """Map a bulk JSONL refund to BigQuery Refunds schema."""
        return {
            "id": extract_legacy_id(refund.get("id")),
            "admin_graphql_api_id": refund.get("id"),
            "created_at": parse_timestamp(refund.get("createdAt")),
            "note": refund.get("note"),
            "order_id": parent_order_id,
            "processed_at": None,
            "restock": False,
            "user_id": None,
        }

    def _map_refund_line_item_from_bulk(self, rli, parent_refund_id):
        """Map a bulk JSONL refund line item to BigQuery RefundLineItems schema."""
        line_item = rli.get("lineItem")
        location = rli.get("location")

        return {
            "id": None,
            "refund_id": parent_refund_id,
            "line_item_id": extract_legacy_id(line_item.get("id")) if line_item else None,
            "location_id": extract_legacy_id(location.get("id")) if location else None,
            "quantity": rli.get("quantity", 0),
            "restock_type": (rli.get("restockType") or "").lower() if rli.get("restockType") else None,
            "subtotal": extract_money(rli.get("subtotalSet")),
            "total_tax": extract_money(rli.get("totalTaxSet")),
        }

    def _map_order_adjustment_from_bulk(self, adj, parent_order_id, parent_refund_id):
        """Map a bulk JSONL order adjustment to BigQuery OrderAdjustments schema."""
        return {
            "id": extract_legacy_id(adj.get("id")),
            "amount": extract_money(adj.get("amountSet")),
            "kind": None,
            "order_id": parent_order_id,
            "reason": adj.get("reason"),
            "refund_id": parent_refund_id,
            "tax_amount": extract_money(adj.get("taxAmountSet")),
        }

    # ------------------------------------------
    # Deduplication and stats
    # ------------------------------------------

    def _deduplicate_by_key(self, records, key_field):
        """Deduplicate records by a key field, keeping the last occurrence."""
        if not records:
            return records
        seen = {}
        for record in records:
            key = record.get(key_field)
            if key is not None:
                seen[key] = record
            else:
                seen[id(record)] = record
        return list(seen.values())

    def _deduplicate_by_composite_key(self, records, key_fields):
        """Deduplicate records by composite key (multiple fields)."""
        if not records:
            return records
        seen = {}
        for record in records:
            key = tuple(record.get(f) for f in key_fields)
            if None not in key:
                seen[key] = record
            else:
                seen[(id(record),)] = record
        return list(seen.values())

    def get_all_data(self):
        """Return all accumulated data as a dict, deduplicated by merge keys."""
        return {
            TABLE_ORDERS: self._deduplicate_by_key(self.orders, "id"),
            TABLE_ITEMS: self._deduplicate_by_key(self.line_items, "id"),
            TABLE_SHIPPING_LINES: self._deduplicate_by_key(self.shipping_lines, "id"),
            TABLE_TRANSACTIONS: self._deduplicate_by_key(self.transactions, "id"),
            TABLE_REFUNDS: self._deduplicate_by_key(self.refunds, "id"),
            TABLE_REFUND_LINE_ITEMS: self._deduplicate_by_composite_key(
                self.refund_line_items, ["refund_id", "line_item_id"]
            ),
            TABLE_ORDER_ADJUSTMENTS: self._deduplicate_by_key(self.order_adjustments, "id"),
        }

    def get_stats(self):
        """Return counts for all accumulated data (after deduplication)."""
        deduplicated = self.get_all_data()
        return {
            table: {"total": len(data)}
            for table, data in deduplicated.items()
        }


# ============================================
# BIGQUERY OPERATIONS (unchanged from original)
# ============================================

def ensure_staging_table(client, table_name, staging_suffix=""):
    """Ensure staging table exists (creates if not)."""
    target_table = f"{GCP_PROJECT}.{DATASET_ID}.{table_name}"
    staging_table = f"{GCP_PROJECT}.{DATASET_ID}.{table_name}_staging{staging_suffix}"

    try:
        client.get_table(staging_table)
    except Exception:
        create_query = f"CREATE TABLE `{staging_table}` LIKE `{target_table}`"
        client.query(create_query).result()
        logger.info(f"Created staging table: {staging_table}")

    return staging_table


def load_to_staging_parallel(client, table_name, rows, staging_suffix=""):
    """Load rows to staging table. Returns (table_name, success, error)."""
    if not rows:
        return table_name, True, None

    staging_table = ensure_staging_table(client, table_name, staging_suffix)

    truncate_query = f"TRUNCATE TABLE `{staging_table}`"
    client.query(truncate_query).result()

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    try:
        load_job = client.load_table_from_json(rows, staging_table, job_config=job_config)
        load_job.result()

        if load_job.errors:
            return table_name, False, str(load_job.errors)

        logger.info(f"Loaded {len(rows)} rows to {staging_table}")
        return table_name, True, None
    except Exception as e:
        return table_name, False, str(e)


def build_merge_statement(table_name, staging_suffix=""):
    """Build a MERGE statement for upserting data from staging to target table."""
    config = TABLE_CONFIG[table_name]
    merge_key = config["merge_key"]
    columns = config["columns"]

    target_table = f"`{GCP_PROJECT}.{DATASET_ID}.{table_name}`"
    staging_table = f"`{GCP_PROJECT}.{DATASET_ID}.{table_name}_staging{staging_suffix}`"

    update_cols = [f"T.{col} = S.{col}" for col in columns if col != merge_key and col not in (merge_key if isinstance(merge_key, list) else [merge_key])]
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join([f"S.{col}" for col in columns])

    if isinstance(merge_key, list):
        merge_condition = " AND ".join([f"T.{key} = S.{key}" for key in merge_key])
    else:
        merge_condition = f"T.{merge_key} = S.{merge_key}"

    merge_sql = f"""
    MERGE {target_table} T
    USING {staging_table} S
    ON {merge_condition}
    WHEN MATCHED THEN
        UPDATE SET {", ".join(update_cols)}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols})
        VALUES ({insert_vals});
    """

    return merge_sql


def build_delete_orphaned_children_statement(table_name, staging_suffix=""):
    """Build DELETE statement for child records no longer present in the new data."""
    target_table = f"`{GCP_PROJECT}.{DATASET_ID}.{table_name}`"
    staging_table = f"`{GCP_PROJECT}.{DATASET_ID}.{table_name}_staging{staging_suffix}`"
    orders_staging = f"`{GCP_PROJECT}.{DATASET_ID}.{TABLE_ORDERS}_staging{staging_suffix}`"
    config = TABLE_CONFIG[table_name]
    merge_key = config["merge_key"]

    if isinstance(merge_key, list):
        key_match = " AND ".join([f"T.{key} = S.{key}" for key in merge_key])
        delete_sql = f"""
        DELETE FROM {target_table} T
        WHERE T.order_id IN (SELECT id FROM {orders_staging})
        AND NOT EXISTS (
            SELECT 1 FROM {staging_table} S
            WHERE {key_match}
        );
        """
    else:
        delete_sql = f"""
        DELETE FROM {target_table} T
        WHERE T.order_id IN (SELECT id FROM {orders_staging})
        AND T.{merge_key} NOT IN (SELECT {merge_key} FROM {staging_table});
        """

    return delete_sql


def build_delete_orphaned_refund_children_statement(staging_suffix=""):
    """Delete RefundLineItems for refunds being updated but no longer in staging."""
    table_name = TABLE_REFUND_LINE_ITEMS
    target_table = f"`{GCP_PROJECT}.{DATASET_ID}.{table_name}`"
    staging_table = f"`{GCP_PROJECT}.{DATASET_ID}.{table_name}_staging{staging_suffix}`"
    refunds_staging = f"`{GCP_PROJECT}.{DATASET_ID}.{TABLE_REFUNDS}_staging{staging_suffix}`"

    delete_sql = f"""
    DELETE FROM {target_table} T
    WHERE T.refund_id IN (SELECT id FROM {refunds_staging})
    AND NOT EXISTS (
        SELECT 1 FROM {staging_table} S
        WHERE T.refund_id = S.refund_id
        AND T.line_item_id = S.line_item_id
    );
    """

    return delete_sql


def execute_bulk_merge(client, accumulator, staging_suffix=""):
    """Execute all MERGE operations in a single transaction."""
    all_data = accumulator.get_all_data()

    order_child_tables = [TABLE_ITEMS, TABLE_SHIPPING_LINES, TABLE_TRANSACTIONS,
                          TABLE_REFUNDS, TABLE_ORDER_ADJUSTMENTS]
    all_tables = [TABLE_ORDERS] + order_child_tables + [TABLE_REFUND_LINE_ITEMS]

    for table_name in all_tables:
        staging_table = ensure_staging_table(client, table_name, staging_suffix)
        client.query(f"TRUNCATE TABLE `{staging_table}`").result()

    logger.info("Loading data to staging tables in parallel...")
    staging_start = time.time()

    with ThreadPoolExecutor(max_workers=7) as executor:
        futures = {
            executor.submit(load_to_staging_parallel, client, table_name, data, staging_suffix): table_name
            for table_name, data in all_data.items()
            if data
        }

        for future in as_completed(futures):
            table_name, success, error = future.result()
            if not success:
                return False, f"Failed to load {table_name} to staging: {error}"

    staging_time = time.time() - staging_start
    logger.info(f"Staging complete in {staging_time:.2f}s")

    logger.info("Executing MERGE operations...")
    merge_start = time.time()

    statements = []

    for table_name in order_child_tables:
        statements.append(build_delete_orphaned_children_statement(table_name, staging_suffix))

    statements.append(build_delete_orphaned_refund_children_statement(staging_suffix))

    for table_name, data in all_data.items():
        if data:
            merge_stmt = build_merge_statement(table_name, staging_suffix)
            statements.append(merge_stmt)

    if statements:
        transaction_query = "BEGIN TRANSACTION;\n"
        transaction_query += "\n".join(statements)
        transaction_query += "\nCOMMIT TRANSACTION;"

        try:
            query_job = client.query(transaction_query)
            query_job.result()
        except Exception as e:
            return False, f"MERGE transaction failed: {str(e)}"

    merge_time = time.time() - merge_start
    logger.info(f"MERGE complete in {merge_time:.2f}s")

    logger.info("Cleaning up staging tables...")
    for table_name in all_tables:
        staging_table = f"{GCP_PROJECT}.{DATASET_ID}.{table_name}_staging{staging_suffix}"
        try:
            if staging_suffix:
                client.query(f"DROP TABLE IF EXISTS `{staging_table}`").result()
            else:
                client.query(f"TRUNCATE TABLE `{staging_table}`").result()
        except Exception as e:
            logger.warning(f"Failed to cleanup {staging_table}: {e}")

    return True, None


# ============================================
# MAIN SYNC LOGIC - BULK OPERATIONS
# ============================================

def run_bulk_query(token, inner_query, query_filter, label):
    """
    Submit a bulk query, poll until done, download and return JSONL objects.
    Returns (objects_list, error).
    """
    logger.info(f"[{label}] Submitting bulk operation...")
    op_id, error = submit_bulk_operation(token, inner_query, query_filter)
    if error:
        return None, f"[{label}] Submit failed: {error}"

    logger.info(f"[{label}] Polling bulk operation {op_id}...")
    url, error = poll_bulk_operation(token, op_id)
    if error:
        # Partial data warning, not fatal if url is available
        if url:
            logger.warning(f"[{label}] {error} — using partial data")
        else:
            return None, f"[{label}] Poll failed: {error}"

    if url is None:
        logger.info(f"[{label}] No results returned (empty date range)")
        return [], None

    logger.info(f"[{label}] Downloading results...")
    objects = download_jsonl(url)
    return objects, None


# ============================================
# PAGINATED FETCH FOR REFUND CHILDREN
#
# Bulk operations can't query connections inside list fields (refunds is a
# list on Order, not a connection). So we fetch refund children (refundLineItems,
# refund transactions, orderAdjustments) via standard paginated GraphQL,
# querying orders in pages of 75 filtered to only those with refunds in the
# same date range.
# ============================================

REFUND_CHILDREN_PAGINATED_QUERY = """
query GetRefundChildren($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on Order {
      id
      refunds {
        id
        refundLineItems(first: 100) {
          edges {
            node {
              lineItem { id }
              location { id }
              quantity
              restockType
              subtotalSet { shopMoney { amount } }
              totalTaxSet { shopMoney { amount } }
            }
          }
        }
        transactions(first: 10) {
          edges {
            node {
              id
              kind
              status
              gateway
              amountSet { shopMoney { amount currencyCode } }
              errorCode
              createdAt
              processedAt
              test
              parentTransaction { id }
            }
          }
        }
        orderAdjustments(first: 20) {
          edges {
            node {
              id
              amountSet { shopMoney { amount } }
              taxAmountSet { shopMoney { amount } }
              reason
            }
          }
        }
      }
    }
  }
}
"""

# Shopify nodes() accepts up to 250 IDs per request
REFUND_CHILDREN_BATCH_SIZE = 250


def fetch_refund_children(token, accumulator, start_date, end_date):
    """
    Fetch refund children (refundLineItems, refund transactions, orderAdjustments)
    via the nodes() query, targeting only orders the bulk query identified as having
    refunds. Fetches in batches of 250 IDs at a time.
    """
    # Build GIDs for orders that have refunds (from the bulk query results)
    order_ids_with_refunds = set()
    for refund in accumulator.refunds:
        order_ids_with_refunds.add(refund["order_id"])

    order_gids = [f"gid://shopify/Order/{oid}" for oid in order_ids_with_refunds]

    if not order_gids:
        logger.info("No order GIDs with refunds to fetch children for")
        return None

    logger.info(f"Fetching refund children for {len(order_gids)} orders with refunds")

    total_orders = 0

    for batch_start in range(0, len(order_gids), REFUND_CHILDREN_BATCH_SIZE):
        batch = order_gids[batch_start:batch_start + REFUND_CHILDREN_BATCH_SIZE]
        batch_num = batch_start // REFUND_CHILDREN_BATCH_SIZE + 1
        total_batches = (len(order_gids) + REFUND_CHILDREN_BATCH_SIZE - 1) // REFUND_CHILDREN_BATCH_SIZE

        data, error = execute_graphql(
            token,
            REFUND_CHILDREN_PAGINATED_QUERY,
            variables={"ids": batch}
        )

        if error:
            logger.error(f"Refund children fetch error on batch {batch_num}: {error}")
            return error

        nodes = data.get("nodes", [])

        for order in nodes:
            if order is None:
                continue
            order_id = extract_legacy_id(order.get("id"))
            total_orders += 1

            for refund in order.get("refunds", []):
                refund_id = extract_legacy_id(refund.get("id"))

                # Refund line items
                for rli_edge in refund.get("refundLineItems", {}).get("edges", []):
                    rli = rli_edge.get("node", {})
                    accumulator.refund_line_items.append(
                        accumulator._map_refund_line_item_from_bulk(rli, refund_id)
                    )

                # Refund transactions
                for txn_edge in refund.get("transactions", {}).get("edges", []):
                    txn = txn_edge.get("node", {})
                    accumulator.transactions.append(
                        accumulator._map_transaction_from_bulk(txn, order_id)
                    )

                # Order adjustments
                for adj_edge in refund.get("orderAdjustments", {}).get("edges", []):
                    adj = adj_edge.get("node", {})
                    accumulator.order_adjustments.append(
                        accumulator._map_order_adjustment_from_bulk(adj, order_id, refund_id)
                    )

        logger.info(f"Refund children batch {batch_num}/{total_batches}: "
                    f"{len(nodes)} orders ({total_orders} total)")

        if batch_start + REFUND_CHILDREN_BATCH_SIZE < len(order_gids):
            time.sleep(0.2)

    logger.info(f"Refund children complete: {total_orders} orders, "
                f"{len(accumulator.refund_line_items)} refund line items, "
                f"{len(accumulator.order_adjustments)} order adjustments")
    return None


def fetch_all_orders_bulk(token, start_date, end_date):
    """
    Fetch ALL orders for the date range using a single bulk operation.
    Returns (accumulator, error).
    """
    accumulator = OrderDataAccumulator()

    query_filter = f"updated_at:>='{start_date}' AND updated_at:<'{end_date}'"
    logger.info(f"Query filter: {query_filter}")

    fetch_start = time.time()

    # Submit single bulk operation
    objects, error = run_bulk_query(token, BULK_INNER_QUERY, query_filter, "BulkOrders")

    if error:
        return None, error

    # Process results into the accumulator
    parse_start = time.time()

    if objects:
        logger.info(f"Processing {len(objects)} objects from bulk query...")
        accumulator.process_bulk_results(objects)

    parse_time = time.time() - parse_start

    # Step 2: Fetch refund children via paginated queries (can't be in bulk ops)
    if accumulator.refunds:
        refund_start = time.time()
        fetch_refund_children(token, accumulator, start_date, end_date)
        refund_time = time.time() - refund_start
        logger.info(f"Refund children fetch took {refund_time:.2f}s")

    fetch_time = time.time() - fetch_start

    logger.info(f"Fetched and parsed all orders in {fetch_time:.2f}s (parse: {parse_time:.2f}s)")
    logger.info(f"Orders: {len(accumulator.orders)}, LineItems: {len(accumulator.line_items)}, "
                f"Transactions: {len(accumulator.transactions)}, Refunds: {len(accumulator.refunds)}")

    return accumulator, None


def sync_orders_for_date_range(token, client, start_date, end_date, staging_suffix=""):
    """
    Sync orders updated within the given date range using bulk operations.

    1. Submit a single bulk query to Shopify
    2. Poll and download JSONL results
    3. Parse into accumulator
    4. Bulk merge to BigQuery
    """
    # Step 1: Fetch all orders via bulk operations
    logger.info("Step 1: Fetching orders via Shopify Bulk Operations...")
    accumulator, error = fetch_all_orders_bulk(token, start_date, end_date)

    if error:
        return False, error, None

    total_stats = accumulator.get_stats()

    if total_stats[TABLE_ORDERS]["total"] == 0:
        logger.info("No orders found for the date range.")
        return True, None, total_stats

    # Step 2: Bulk merge to BigQuery
    logger.info("Step 2: Executing bulk merge to BigQuery...")
    merge_start = time.time()

    success, error = execute_bulk_merge(client, accumulator, staging_suffix)

    if not success:
        return False, error, total_stats

    merge_total_time = time.time() - merge_start
    logger.info(f"BigQuery operations complete in {merge_total_time:.2f}s")

    return True, None, total_stats


# ============================================
# CLOUD RUN JOB ENTRY POINT
# ============================================

def run_daily_sync():
    """
    Daily sync entry point. Syncs the last 2 days of orders to BigQuery.
    The 2-day window catches orders whose customerJourneySummary attribution
    data populates after creation.
    """
    start_time = time.time()

    logger.info("=" * 60)
    logger.info("SHOPIFY DAILY ORDER SYNC (BULK OPERATIONS)")
    logger.info("=" * 60)

    today = datetime.now(timezone.utc).date()
    two_days_ago = today - timedelta(days=2)
    start_date = two_days_ago.isoformat()
    end_date = today.isoformat()

    logger.info(f"Syncing orders updated: {start_date} to {end_date}")

    token = get_shopify_token()
    client = get_bq_client()

    success, error, total_stats = sync_orders_for_date_range(
        token, client, start_date, end_date
    )

    total_time = time.time() - start_time

    if not success:
        logger.error(f"Sync failed: {error}")
        sys.exit(1)

    if total_stats[TABLE_ORDERS]["total"] == 0:
        logger.info(f"Daily Sync Complete! No orders found for {start_date} to {end_date}. Total time: {total_time:.2f}s")
        return

    summary_lines = [
        "Daily Sync Complete! (BULK OPERATIONS)",
        f"Date range: {start_date} to {end_date}",
        f"Total time: {total_time:.2f}s",
        f"Orders: {total_stats[TABLE_ORDERS]['total']}",
        f"Line Items: {total_stats[TABLE_ITEMS]['total']}",
        f"Shipping Lines: {total_stats[TABLE_SHIPPING_LINES]['total']}",
        f"Transactions: {total_stats[TABLE_TRANSACTIONS]['total']}",
        f"Refunds: {total_stats[TABLE_REFUNDS]['total']}",
        f"Refund Line Items: {total_stats[TABLE_REFUND_LINE_ITEMS]['total']}",
        f"Order Adjustments: {total_stats[TABLE_ORDER_ADJUSTMENTS]['total']}",
    ]
    logger.info("\n".join(summary_lines))


# ============================================
# LOCAL BACKFILL
# ============================================

def generate_chunks(start, end, chunk_size="day"):
    """Generate (chunk_start, chunk_end) date pairs."""
    from calendar import monthrange

    current = start
    while current < end:
        if chunk_size == "month":
            last_day = monthrange(current.year, current.month)[1]
            month_end = current.replace(day=last_day) + timedelta(days=1)
            chunk_end = min(month_end, end)
        elif chunk_size == "week":
            chunk_end = min(current + timedelta(days=7), end)
        else:
            chunk_end = min(current + timedelta(days=1), end)

        yield current, chunk_end
        current = chunk_end


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Shopify → BigQuery sync (Bulk Operations). No args = daily sync (Cloud Run Job entrypoint).",
        epilog=(
            "Examples:\n"
            "  # Daily sync (2-day rolling window) — what Cloud Run Job runs:\n"
            "  python main.py\n\n"
            "  # Backfill all of 2024 month-by-month:\n"
            "  python main.py --year 2024 --chunk month\n\n"
            "  # Backfill a specific range week-by-week:\n"
            "  python main.py --start 2024-03-01 --end 2024-04-01 --chunk week\n\n"
            "  # Backfill a specific range day-by-day (default):\n"
            "  python main.py --start 2024-03-01 --end 2024-04-01\n\n"
            "  # Run multiple years in parallel:\n"
            "  python main.py --year 2022 --chunk month &\n"
            "  python main.py --year 2023 --chunk month &\n"
            "  python main.py --year 2024 --chunk month &\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--start", help="Start date inclusive (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date exclusive (YYYY-MM-DD)")
    parser.add_argument("--year", type=int, help="Backfill an entire year (sets --start to Jan 1 and --end to Jan 1 of next year)")
    parser.add_argument("--chunk", choices=["day", "week", "month"], default="day", help="Chunk size for processing (default: day)")
    args = parser.parse_args()

    # No args → daily sync (Cloud Run Job entrypoint)
    if not args.start and not args.end and not args.year:
        run_daily_sync()
        sys.exit(0)

    if args.year and (args.start or args.end):
        print("Error: --year cannot be combined with --start/--end")
        sys.exit(1)

    today = datetime.now(timezone.utc).date()

    if args.year:
        start = datetime(args.year, 1, 1).date()
        end = datetime(args.year + 1, 1, 1).date()
        if end > today:
            end = today
    else:
        start = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else today - timedelta(days=1)
        end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else today

    if start >= end:
        print(f"Error: start ({start}) must be before end ({end})")
        sys.exit(1)

    token = get_shopify_token()
    client = get_bq_client()

    if args.year:
        staging_suffix = f"_{args.year}"
    else:
        staging_suffix = f"_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}"
    logger.info(f"Staging table suffix: {staging_suffix}")

    chunks = list(generate_chunks(start, end, chunk_size=args.chunk))
    chunk_label = args.chunk
    total_orders = 0
    failed_chunks = []

    logger.info(f"Backfilling {len(chunks)} {chunk_label}(s): {start} to {end}")

    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {chunk_label} {i}/{len(chunks)}: {chunk_start} to {chunk_end}")
        logger.info(f"{'='*60}")

        success, error, stats = sync_orders_for_date_range(
            token, client, chunk_start.isoformat(), chunk_end.isoformat(),
            staging_suffix=staging_suffix
        )

        if not success:
            logger.error(f"FAILED {chunk_start}: {error}")
            failed_chunks.append((chunk_start, chunk_end, error))
        else:
            chunk_count = stats[TABLE_ORDERS]["total"] if stats else 0
            total_orders += chunk_count
            logger.info(f"Done {chunk_start} to {chunk_end}: {chunk_count} orders")

    print(f"\nBackfill complete: {total_orders} orders across {len(chunks)} {chunk_label}(s)")
    if failed_chunks:
        print(f"\nFailed {chunk_label}s ({len(failed_chunks)}):")
        for cs, ce, err in failed_chunks:
            print(f"  {cs} to {ce}: {err}")
