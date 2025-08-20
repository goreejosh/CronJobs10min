import cron from 'node-cron';
import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';
import { buildSkuMaps, resolveSku } from './utils/skuResolver.js';

// Load env
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const POLL_CRON = process.env.POLL_CRON || '*/10 * * * *';

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  console.error('[Cron] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { autoRefreshToken: false, persistSession: false }
});

// Minimal SKU resolver (delegates to DB for now) – for more advanced matching,
// we can import local maps or call an API; placeholder to keep structure.
const normalizeSku = (s) => String(s || '').toLowerCase().replace(/\s+/g, '');

async function runOnce() {
  const startedAt = new Date().toISOString();
  console.log(`[Cron] Inventory queue alert run started @ ${startedAt}`);

  // 1) Aggregate queued demand per baseSku from awaiting_shipment orders
  const { data: orderItems, error: ordersErr } = await supabase
    .from('order_items')
    .select('sku, quantity, orders!inner(order_status)')
    .eq('orders.order_status', 'awaiting_shipment');
  if (ordersErr) {
    console.error('[Cron] Orders fetch error:', ordersErr);
    return;
  }

  // Build catalog maps for robust SKU resolution
  const { data: products } = await supabase.from('products').select('id, Sku');
  const { data: bundles } = await supabase.from('bundle').select('id, name');
  const { productMap, bundleMap } = buildSkuMaps(products || [], bundles || []);

  const demand = new Map();
  for (const it of orderItems || []) {
    const resolved = resolveSku(it.sku, productMap, bundleMap);
    const base = normalizeSku(resolved.baseSku);
    if (!base) continue;
    demand.set(base, (demand.get(base) || 0) + (it.quantity || 0));
  }

  if (demand.size === 0) {
    console.log('[Cron] No queued demand found.');
    return;
  }

  // 2) Fetch client_inventory mapping first
  const { data: ciRows, error: ciErr } = await supabase
    .from('client_inventory')
    .select('id, sku, client_id');
  if (ciErr) {
    console.error('[Cron] client_inventory error:', ciErr);
    return;
  }
  const idToSku = new Map((ciRows || []).map(r => [String(r.id), normalizeSku(r.sku)]));
  const idToClientId = new Map((ciRows || []).map(r => [String(r.id), r.client_id]));
  
  // 3) Fetch pickable availability from stock levels excluding BackStock & Production
  const { data: pickableRows, error: pickErr } = await supabase
    .from('inventory_stock_levels')
    .select('item_type, item_id, available, location_id, inventory_locations!inner(type)')
    .eq('item_type', 'client_product')
    .neq('inventory_locations.type', 'BackStock')
    .neq('inventory_locations.type', 'Production');
  if (pickErr) {
    console.error('[Cron] pickable availability error:', pickErr);
    return;
  }
  const pickable = new Map();
  for (const r of pickableRows || []) {
    const sku = idToSku.get(String(r.item_id));
    if (!sku) continue;
    const client_id = idToClientId.get(String(r.item_id));
    const prev = pickable.get(sku) || { available: 0, client_id };
    prev.available += (r.available || 0);
    pickable.set(sku, prev);
  }

  // 4) Backstock and total supply by SKU
  const { data: stockRows, error: stockErr } = await supabase
    .from('inventory_stock_levels')
    .select('item_type, item_id, on_hand, location_id, inventory_locations!inner(code, type)')
    .eq('item_type', 'client_product');
  if (stockErr) {
    console.error('[Cron] stock levels error:', stockErr);
    return;
  }

  const backstockBySku = new Map();
  const totalBySku = new Map();

  for (const r of stockRows || []) {
    const sku = idToSku.get(String(r.item_id));
    if (!sku) continue;
    totalBySku.set(sku, (totalBySku.get(sku) || 0) + (r.on_hand || 0));
    if (r.inventory_locations?.type === 'BackStock') {
      backstockBySku.set(sku, (backstockBySku.get(sku) || 0) + (r.on_hand || 0));
    }
  }

  // 5) Upsert alerts
  for (const [sku, qtyNeeded] of demand.entries()) {
    const pick = pickable.get(sku) || { available: 0, client_id: null };
    const back = backstockBySku.get(sku) || 0;
    const total = totalBySku.get(sku) || 0;

    const needsRestock = qtyNeeded > pick.available && back > 0;
    const needsPurchase = qtyNeeded > total;

    const alertType = needsPurchase ? 'purchase' : (needsRestock ? 'restock' : null);
    if (!alertType) {
      // clear existing active alerts for sku
      await supabase
        .from('inventory_alerts')
        .update({ is_active: false, updated_at: new Date().toISOString() })
        .eq('item_type', 'client_product')
        .eq('message', sku)
        .eq('client_id', pick.client_id || null)
        .eq('is_active', true);
      continue;
    }

    const message = sku;
    const severity = needsPurchase ? 'high' : 'medium';
    const client_id = pick.client_id || null;

    // upsert by (client_id, item_type, message, alert_type, is_active=true)
    const payload = {
      item_type: 'client_product',
      item_id: null,
      alert_type: alertType,
      message,
      severity,
      is_active: true,
      client_id,
      updated_at: new Date().toISOString()
    };

    // First try native upsert with a stable composite key
    const { error: upsertErr } = await supabase
      .from('inventory_alerts')
      .upsert(payload, { onConflict: 'client_id,item_type,alert_type,message' });

    if (upsertErr) {
      console.error('[Cron] inventory_alerts upsert error; attempting fallback:', upsertErr);

      // Fallback: emulate upsert without requiring DB unique index
      const { data: existing, error: selErr } = await supabase
        .from('inventory_alerts')
        .select('id')
        .eq('client_id', client_id)
        .eq('item_type', 'client_product')
        .eq('alert_type', alertType)
        .eq('message', message)
        .eq('is_active', true)
        .maybeSingle();

      if (selErr) {
        console.error('[Cron] inventory_alerts select error:', selErr);
      } else if (existing?.id) {
        const { error: updErr } = await supabase
          .from('inventory_alerts')
          .update({ severity, updated_at: new Date().toISOString() })
          .eq('id', existing.id);
        if (updErr) console.error('[Cron] inventory_alerts update error:', updErr);
      } else {
        const { error: insErr } = await supabase
          .from('inventory_alerts')
          .insert([payload]);
        if (insErr) console.error('[Cron] inventory_alerts insert error:', insErr);
      }
    }
  }

  console.log('[Cron] Completed alert evaluation');
}

// Schedule
cron.schedule(POLL_CRON, () => {
  runOnce().catch(err => console.error('[Cron] runOnce error', err));
});

// Kick immediately
runOnce().catch(err => console.error('[Cron] startup error', err));

