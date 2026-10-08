/**
 * Unit tests for the OpenAI feed transform (file-upload/products schema).
 * Pure functions only — no network. Run: node --test openai-feed.test.js
 */

const { test } = require('node:test');
const assert = require('node:assert');

const {
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
} = require('./openai-feed');

test('formatPrice: emits "N.NN USD" strings; rejects invalid/negative', () => {
  assert.strictEqual(formatPrice('29.99'), '29.99 USD');
  assert.strictEqual(formatPrice(29.9), '29.90 USD');
  assert.strictEqual(formatPrice('34.5'), '34.50 USD');
  assert.strictEqual(formatPrice('0'), '0.00 USD');
  assert.strictEqual(formatPrice('-5'), null);
  assert.strictEqual(formatPrice(null), null);
  assert.strictEqual(formatPrice('abc'), null);
});

test('resolvePricing: regular=compareAt & sale=price when on sale, else price only', () => {
  assert.deepStrictEqual(resolvePricing({ price: '39.99', compare_at_price: '49.99' }),
    { price: '49.99 USD', sale_price: '39.99 USD' });
  assert.deepStrictEqual(resolvePricing({ price: '39.99', compare_at_price: null }),
    { price: '39.99 USD', sale_price: null });
  // compareAt <= price ⇒ not a sale
  assert.deepStrictEqual(resolvePricing({ price: '39.99', compare_at_price: '39.99' }),
    { price: '39.99 USD', sale_price: null });
  assert.strictEqual(resolvePricing({ price: '0' }), null);
  assert.strictEqual(resolvePricing({ price: null }), null);
});

test('mapAvailabilityStatus: inventory + policy → enum string', () => {
  assert.strictEqual(mapAvailabilityStatus(10, 'deny'), 'in_stock');
  assert.strictEqual(mapAvailabilityStatus(0, 'deny'), 'out_of_stock');
  assert.strictEqual(mapAvailabilityStatus(-1, 'deny'), 'out_of_stock');
  assert.strictEqual(mapAvailabilityStatus(0, 'continue'), 'backorder');
  assert.strictEqual(mapAvailabilityStatus(-3, 'continue'), 'backorder');
});

test('hasHiddenTag: detects hidden in arrays and comma strings', () => {
  assert.strictEqual(hasHiddenTag(['Sale', 'Hidden']), true);
  assert.strictEqual(hasHiddenTag('sale, hidden, new'), true);
  assert.strictEqual(hasHiddenTag(['sale', 'new']), false);
  assert.strictEqual(hasHiddenTag(null), false);
});

test('inferOptionName: detects sizes, falls back to style', () => {
  assert.strictEqual(inferOptionName('S'), 'size');
  assert.strictEqual(inferOptionName('2XL'), 'size');
  assert.strictEqual(inferOptionName('10.5'), 'size');
  assert.strictEqual(inferOptionName('Black'), 'style');
});

test('buildVariantOptions: prefers real names, drops Default Title, falls back to option1/2/3', () => {
  assert.deepStrictEqual(
    buildVariantOptions({ options: [{ name: 'Color', value: 'Blue' }, { name: 'Size', value: '10' }] }),
    [{ name: 'Color', value: 'Blue' }, { name: 'Size', value: '10' }],
  );
  assert.deepStrictEqual(buildVariantOptions({ options: [{ name: 'Title', value: 'Default Title' }] }), []);
  assert.deepStrictEqual(buildVariantOptions({ option1: 'L' }), [{ name: 'size', value: 'L' }]);
});

test('variantAttributes: builds variant_dict + pulls out size/color', () => {
  const { dict, size, color } = variantAttributes([{ name: 'Color', value: 'Blue' }, { name: 'Size', value: '10' }]);
  assert.deepStrictEqual(dict, { Color: 'Blue', Size: '10' });
  assert.strictEqual(size, '10');
  assert.strictEqual(color, 'Blue');
  const empty = variantAttributes([]);
  assert.deepStrictEqual(empty, { dict: {}, size: null, color: null });
});

test('gtinFromBarcode: accepts 8-14 digits, rejects otherwise', () => {
  assert.strictEqual(gtinFromBarcode('012345678905'), '012345678905'); // 12 digits
  assert.strictEqual(gtinFromBarcode('  84123456 '), '84123456');       // trims to 8 digits
  assert.strictEqual(gtinFromBarcode('1234567'), null);                 // only 7 digits
  assert.strictEqual(gtinFromBarcode('12-345'), null);
  assert.strictEqual(gtinFromBarcode('abc'), null);
  assert.strictEqual(gtinFromBarcode(null), null);
});

const CTX = (over = {}) => ({
  productId: '100',
  title: 'USA Logo Tee',
  description: 'A comfy tee.',
  url: 'https://www.bl101.com/products/usa-logo-tee',
  image: 'https://cdn.shopify.com/x.jpg',
  productType: 'T-Shirt',
  hidden: false,
  published_online: true,
  sold_out_online: false,
  hasVariations: true,
  ...over,
});

test('buildVariantRecord: builds a full flat record with required + variant fields', () => {
  const reasons = [];
  const seen = new Set();
  const rec = buildVariantRecord(
    { id: '111', sku: 'TEE-BLK-L', title: 'L', price: '39.99', compare_at_price: '49.99', inventory_quantity: 5, inventory_policy: 'deny', barcode: '012345678905', options: [{ name: 'Size', value: 'L' }] },
    CTX(), reasons, seen,
  );
  assert.strictEqual(rec.item_id, 'TEE-BLK-L');
  assert.strictEqual(rec.offer_id, 'TEE-BLK-L');
  assert.strictEqual(rec.group_id, '100');
  assert.strictEqual(rec.title, 'USA Logo Tee');
  assert.strictEqual(rec.item_group_title, 'USA Logo Tee');
  assert.strictEqual(rec.brand, 'Baseball Lifestyle 101');
  assert.strictEqual(rec.price, '49.99 USD');       // regular = compareAt
  assert.strictEqual(rec.sale_price, '39.99 USD');   // sale = current
  assert.strictEqual(rec.availability, 'in_stock');
  assert.strictEqual(rec.is_eligible_search, true);
  assert.strictEqual(rec.is_eligible_checkout, true);
  assert.strictEqual(rec.is_eligible_ads, true);
  assert.strictEqual(rec.seller_privacy_policy, 'https://www.bl101.com/policies/privacy-policy');
  assert.strictEqual(rec.return_policy, 'https://www.bl101.com/policies/refund-policy');
  assert.strictEqual(rec.gtin, '012345678905');
  assert.strictEqual(rec.condition, 'new');
  assert.strictEqual(rec.product_category, 'T-Shirt');
  assert.deepStrictEqual(rec.variant_dict, { Size: 'L' });
  assert.strictEqual(rec.size, 'L');
  assert.strictEqual(rec.size_system, 'US');
  assert.deepStrictEqual(rec.target_countries, ['US']);
  assert.strictEqual(rec.url, 'https://www.bl101.com/products/usa-logo-tee?variant=111');
  assert.strictEqual(validateRecord(rec).length, 0);
});

test('buildVariantRecord: hidden product turns off search + checkout + ads eligibility', () => {
  const rec = buildVariantRecord(
    { id: '1', sku: 'X', title: 'L', price: '39.99', inventory_quantity: 50, inventory_policy: 'deny' },
    CTX({ hidden: true }), [], new Set(),
  );
  assert.strictEqual(rec.is_eligible_search, false);
  assert.strictEqual(rec.is_eligible_checkout, false);
  assert.strictEqual(rec.is_eligible_ads, false);
  assert.strictEqual(rec.availability, 'in_stock'); // availability still reflects real stock
  assert.strictEqual(validateRecord(rec).length, 0);
});

test('computeEligibility: gates on channel, sold_out_online, and variant online_inventory', () => {
  const okVariant = { online_inventory: 12 };
  assert.strictEqual(computeEligibility(CTX(), okVariant).eligible, true);
  assert.strictEqual(computeEligibility(CTX({ hidden: true }), okVariant).eligible, false);
  assert.strictEqual(computeEligibility(CTX({ published_online: false }), okVariant).eligible, false);
  assert.strictEqual(computeEligibility(CTX({ sold_out_online: true }), okVariant).eligible, false);
  // variant online_inventory 0 or negative → ineligible; positive/absent → fine
  assert.strictEqual(computeEligibility(CTX(), { online_inventory: 0 }).eligible, false);
  assert.strictEqual(computeEligibility(CTX(), { online_inventory: -3 }).eligible, false);
  assert.strictEqual(computeEligibility(CTX(), { online_inventory: null }).eligible, true);
  assert.strictEqual(computeEligibility(CTX(), {}).eligible, true);
});

test('buildVariantRecord: all three eligibility flags move together when gated', () => {
  // not published to Online Store
  const unpublished = buildVariantRecord(
    { id: '1', sku: 'A', title: 'S', price: '10.00', inventory_quantity: 5, inventory_policy: 'deny', online_inventory: 5 },
    CTX({ published_online: false }), [], new Set(),
  );
  assert.deepStrictEqual(
    [unpublished.is_eligible_search, unpublished.is_eligible_checkout, unpublished.is_eligible_ads],
    [false, false, false],
  );

  // sold_out_online = true
  const soldOut = buildVariantRecord(
    { id: '2', sku: 'B', title: 'S', price: '10.00', inventory_quantity: 5, inventory_policy: 'deny', online_inventory: 5 },
    CTX({ sold_out_online: true }), [], new Set(),
  );
  assert.deepStrictEqual(
    [soldOut.is_eligible_search, soldOut.is_eligible_checkout, soldOut.is_eligible_ads],
    [false, false, false],
  );

  // variant online_inventory <= 0 gates only that variant
  const zeroInv = buildVariantRecord(
    { id: '3', sku: 'C', title: 'S', price: '10.00', inventory_quantity: 5, inventory_policy: 'deny', online_inventory: 0 },
    CTX(), [], new Set(),
  );
  assert.deepStrictEqual(
    [zeroInv.is_eligible_search, zeroInv.is_eligible_checkout, zeroInv.is_eligible_ads],
    [false, false, false],
  );
  assert.strictEqual(validateRecord(zeroInv).length, 0); // still a valid record, just ineligible
});

test('buildProductRecords: mixed variant eligibility within one product', () => {
  const { records } = buildProductRecords({
    id: '500', title: 'Mixed Tee', handle: 'mixed', image: 'https://cdn/x.jpg', published_online: true, sold_out_online: false,
    variants: [
      { id: '1', sku: 'M-S', title: 'S', price: '20.00', inventory_quantity: 3, inventory_policy: 'deny', online_inventory: 3 },
      { id: '2', sku: 'M-M', title: 'M', price: '20.00', inventory_quantity: 0, inventory_policy: 'deny', online_inventory: 0 },
    ],
  }, new Set());
  const byId = Object.fromEntries(records.map((r) => [r.item_id, r]));
  assert.strictEqual(byId['M-S'].is_eligible_search, true);
  assert.strictEqual(byId['M-M'].is_eligible_search, false); // online_inventory 0
});

test('buildVariantRecord: skips missing price and missing image', () => {
  const r1 = [];
  assert.strictEqual(buildVariantRecord({ id: '1', sku: 'A', price: null }, CTX(), r1, new Set()), null);
  const r2 = [];
  assert.strictEqual(buildVariantRecord({ id: '2', sku: 'B', price: '10.00' }, CTX({ image: null }), r2, new Set()), null);
  assert.ok(r1[0].includes('price') && r2[0].includes('image'));
});

test('buildProductRecords: one record per variant, feed-wide unique item_id', () => {
  const seen = new Set();
  const { records } = buildProductRecords({
    id: '100', title: 'Gift Card', handle: 'gift-card', image: 'https://cdn/x.jpg',
    variants: [
      { id: '111', sku: 'EGC', title: '$25', price: '25.00', inventory_quantity: 1, inventory_policy: 'continue' },
      { id: '222', sku: 'EGC', title: '$50', price: '50.00', inventory_quantity: 1, inventory_policy: 'continue' },
    ],
  }, seen);
  const ids = records.map((r) => r.item_id);
  assert.deepStrictEqual(ids, ['EGC', 'EGC-222']); // collision suffixed with variant id
  assert.strictEqual(new Set(ids).size, ids.length);
  assert.ok(records.every((r) => r.group_id === '100'));
});

test('buildProductRecords: skips product with no image (image_url required)', () => {
  const { records, skipped } = buildProductRecords({
    id: '9', title: 'No Image', handle: 'x', image: null,
    variants: [{ id: '1', sku: 'A', title: 'S', price: '10.00', inventory_quantity: 1, inventory_policy: 'deny' }],
  }, new Set());
  assert.strictEqual(records.length, 0);
  assert.ok(skipped.some((s) => s.includes('image')));
});

test('buildProductRecords: falls back to title when description missing', () => {
  const { records } = buildProductRecords({
    id: '1', title: 'Snapback Hat', handle: 'h', image: 'https://cdn/x.jpg', description_plain: null,
    variants: [{ id: '1', sku: 'A', title: 'OS', price: '10.00', inventory_quantity: 1, inventory_policy: 'deny' }],
  }, new Set());
  assert.strictEqual(records[0].description, 'Snapback Hat');
});

test('validateRecord: flags bad https, price format, availability, and checkout requirements', () => {
  const base = buildVariantRecord(
    { id: '1', sku: 'A', title: 'S', price: '10.00', inventory_quantity: 1, inventory_policy: 'deny' },
    CTX(), [], new Set(),
  );
  assert.deepStrictEqual(validateRecord(base), []);

  assert.ok(validateRecord({ ...base, url: 'http://insecure.com' }).some((e) => e.includes('url not https')));
  assert.ok(validateRecord({ ...base, price: '10 USD' }).some((e) => e.includes('price bad format')));
  assert.ok(validateRecord({ ...base, availability: 'sold' }).some((e) => e.includes('bad availability')));
  assert.ok(validateRecord({ ...base, sale_price: '99.99 USD' }).some((e) => e.includes('sale_price > price')));
  // checkout-eligible but missing policy URL
  assert.ok(validateRecord({ ...base, seller_tos: '' }).some((e) => e.includes('seller_tos required')));
});

test('buildFeed: aggregates stats and emits only valid records', () => {
  const rows = [
    { id: '1', title: 'A', handle: 'a', image: 'https://cdn/a.jpg', variants: [{ id: '11', sku: 'A1', title: 'S', price: '10.00', inventory_quantity: 1, inventory_policy: 'deny' }] },
    { id: '2', title: 'B', handle: 'b', image: 'https://cdn/b.jpg', variants: [{ id: '22', sku: 'B1', title: 'S', price: null }] }, // no sellable variant
    { id: '3', title: 'C', handle: 'c', image: 'https://cdn/c.jpg', hidden: true, variants: [{ id: '33', sku: 'C1', title: 'S', price: '20.00', inventory_quantity: 5, inventory_policy: 'deny' }] },
  ];
  const { records, stats } = buildFeed(rows);
  assert.strictEqual(records.length, 2);            // A + hidden C (still in snapshot, search off)
  assert.strictEqual(stats.productsWithRecords, 2);
  assert.strictEqual(stats.productsSkipped, 1);
  assert.strictEqual(stats.productsHidden, 1);
  assert.strictEqual(stats.recordsOut, 2);
  assert.strictEqual(records.find((r) => r.group_id === '3').is_eligible_search, false);
});

test('csvCell: quotes/escapes commas, quotes, newlines; serializes arrays + objects', () => {
  assert.strictEqual(csvCell('plain'), 'plain');
  assert.strictEqual(csvCell(true), 'true');
  assert.strictEqual(csvCell(false), 'false');
  assert.strictEqual(csvCell(null), '');
  assert.strictEqual(csvCell(undefined), '');
  assert.strictEqual(csvCell(['US', 'CA']), '"US,CA"');           // comma → quoted
  assert.strictEqual(csvCell({ Size: 'L' }), '"{""Size"":""L""}"'); // JSON + escaped quotes
  assert.strictEqual(csvCell('a,b'), '"a,b"');
  assert.strictEqual(csvCell('say "hi"'), '"say ""hi"""');
  assert.strictEqual(csvCell('line1\nline2'), '"line1\nline2"');
});

test('toCsv: header row + one rectangular row per record; header order stable', () => {
  const csv = toCsv([
    { is_eligible_search: true, item_id: 'A', title: 'Comfy, Soft Tee', price: '10.00 USD', target_countries: ['US'], variant_dict: { Size: 'L' } },
  ]);
  const rows = csv.trim().split('\n');
  const header = rows[0].split(',');
  assert.strictEqual(header[0], 'is_eligible_search');
  assert.ok(header.includes('variant_dict'));
  // description/title with a comma must be quoted so the row stays rectangular
  assert.ok(rows[1].includes('"Comfy, Soft Tee"'));
  assert.ok(rows[1].includes('"{""Size"":""L""}"'));
  assert.strictEqual(rows.length, 2);
});

test('writeSnapshot: writes a UTF-8 CSV file with header + rows', () => {
  const os = require('os');
  const path = require('path');
  const fs = require('fs');
  const recs = [{ is_eligible_search: true, item_id: 'A', price: '10.00 USD' }, { is_eligible_search: false, item_id: 'B', price: '20.00 USD' }];
  const p = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'feed-test-')), 'out.csv');
  writeSnapshot(recs, p);
  const lines = fs.readFileSync(p, 'utf8').trim().split('\n');
  assert.strictEqual(lines.length, 3); // header + 2 rows
  assert.strictEqual(lines[0].split(',')[0], 'is_eligible_search');
});

test('normalizeProduct → buildProductRecords: end-to-end flat record with real option name', () => {
  const row = normalizeProduct({
    id: 'gid://shopify/Product/8802344140982',
    title: 'Drip Tee', handle: 'drip-tee', onlineStoreUrl: 'https://www.bl101.com/products/drip-tee',
    publishedAt: '2025-12-22T14:43:50Z',
    productType: 'T-Shirt', tags: ['New'], description: 'Soft tee.', descriptionHtml: '<p>Soft tee.</p>',
    featuredImage: { url: 'https://cdn.shopify.com/x.jpg' },
    soldOutOnline: { value: 'false' },
    variants: { edges: [
      { node: { id: 'gid://shopify/ProductVariant/45137666310326', sku: 'TS-S', title: 'S', price: '39.99', compareAtPrice: '49.99', barcode: '840460579292', inventoryQuantity: 10, inventoryPolicy: 'DENY', selectedOptions: [{ name: 'Size', value: 'S' }], onlineInventory: { value: '8' } } },
      { node: { id: 'gid://shopify/ProductVariant/999', sku: 'TS-M', title: 'M', price: '39.99', inventoryQuantity: 0, inventoryPolicy: 'DENY', selectedOptions: [{ name: 'Size', value: 'M' }], onlineInventory: { value: '0' } } },
    ] },
  });
  assert.strictEqual(row.id, '8802344140982');
  assert.strictEqual(row.hidden, false);
  assert.strictEqual(row.published_online, true);
  assert.strictEqual(row.sold_out_online, false);
  assert.strictEqual(row.variants[0].inventory_policy, 'deny');
  assert.strictEqual(row.variants[0].online_inventory, 8);
  assert.strictEqual(row.variants[1].online_inventory, 0);

  const { records } = buildProductRecords(row, new Set());
  const bySize = Object.fromEntries(records.map((r) => [r.item_id, r]));
  assert.deepStrictEqual(bySize['TS-S'].variant_dict, { Size: 'S' });
  assert.strictEqual(bySize['TS-S'].size, 'S');
  assert.strictEqual(bySize['TS-S'].availability, 'in_stock');
  assert.strictEqual(bySize['TS-S'].is_eligible_search, true);   // published, in online inventory
  assert.strictEqual(bySize['TS-S'].price, '49.99 USD');
  assert.strictEqual(bySize['TS-S'].sale_price, '39.99 USD');
  assert.strictEqual(bySize['TS-M'].is_eligible_search, false);  // online_inventory 0
  assert.strictEqual(validateRecord(bySize['TS-S']).length, 0);
});
