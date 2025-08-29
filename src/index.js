import cron from 'node-cron';
import 'dotenv/config';
import { createClient } from '@supabase/supabase-js';
import { runBackfillShipmentsFromEvents } from './jobs/backfillShipmentsFromEvents.js';
import { runFixShippedOrdersMissingTracking } from './jobs/fixShippedOrdersMissingTracking.js';
import { buildSkuMaps, buildClientSkuMap, resolveSku, normalizeSku } from './utils/skuResolver.js';

// Load env
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const POLL_CRON = process.env.POLL_CRON || '*/10 * * * *';
const BACKFILL_CRON = process.env.BACKFILL_CRON || '*/10 * * * *';
const FIX_ORDERS_CRON = process.env.FIX_ORDERS_CRON || '*/10 * * * *';

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  console.error('[Cron] Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { autoRefreshToken: false, persistSession: false }
});

// Runtime controls for shipment reconciliation
const RECON_LOOKBACK_HOURS = parseInt(process.env.RECON_LOOKBACK_HOURS || '24', 10);
const RECON_PAGE_SIZE = parseInt(process.env.RECON_PAGE_SIZE || '500', 10);
const RECON_MAX_PAGES = parseInt(process.env.RECON_MAX_PAGES || '20', 10);

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
  const clientSkuMap = buildClientSkuMap(ciRows || []);
  
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

  // 6) Reconcile shipments → reduce inventory at Batch/Production
  try {
    const sinceIso = new Date(Date.now() - 1000 * 60 * 60 * RECON_LOOKBACK_HOURS).toISOString();

    let page = 0;
    let processed = 0;
    while (page < RECON_MAX_PAGES) {
      const from = page * RECON_PAGE_SIZE;
      const to = from + RECON_PAGE_SIZE - 1;
      const { data: shipments, error: shipErr } = await supabase
        .from('shipments')
        .select('id, shipment_items, order_id, order_number, ship_date, created_at, voided')
        .gte('created_at', sinceIso)
        .is('voided', false)
        .order('created_at', { ascending: true })
        .range(from, to);
      if (shipErr) {
        console.error('[Cron] shipments fetch error:', shipErr);
        break;
      }
      if (!shipments || shipments.length === 0) break;

      for (const s of shipments) {
        const raw = s.shipment_items;
        const items = Array.isArray(raw) ? raw : (raw?.items || raw?.ShipmentItems || []);

        // Group by canonical SKU to avoid duplicate movements per shipment
        const qtyBySku = new Map();
        for (const itm of items) {
          const skuRaw = itm?.sku || itm?.SKU || itm?.product?.sku || null;
          const qty = Number(itm?.quantity ?? itm?.Quantity ?? itm?.qty ?? 1);
          if (!skuRaw || !qty) continue;
          const skuCanon = normalizeSku(skuRaw);
          qtyBySku.set(skuCanon, (qtyBySku.get(skuCanon) || 0) + qty);
        }

        for (const [skuCanon, qty] of qtyBySku.entries()) {
          if (!skuCanon || !qty) continue;

          // Locate an existing movement to use as idempotency marker and update target
          // 1) Prefer explicit shipment reference
          let movementForRecon = null;
          let movementLookupError = null;
          try {
            const byShipment = await supabase
              .from('inventory_movements')
              .select('id, sku, notes')
              .eq('reference_type', 'shipment')
              .eq('reference_id', String(s.id))
              .limit(1)
              .maybeSingle();
            movementForRecon = byShipment.data;
            movementLookupError = byShipment.error;
            // 2) Fallback: find by order_shipment note containing order number
            if (!movementForRecon && s.order_number) {
              const byOrderNote = await supabase
                .from('inventory_movements')
                .select('id, sku, notes')
                .eq('reason', 'order_shipment')
                .ilike('notes', `%${s.order_number}%`)
                .order('created_at', { ascending: false })
                .limit(1)
                .maybeSingle();
              movementForRecon = byOrderNote.data;
              movementLookupError = movementLookupError || byOrderNote.error;
            }
          } catch (e) {
            movementLookupError = e;
          }
          if (movementLookupError) {
            console.error('[Cron] movement lookup error:', movementLookupError);
          }
          // If no related movement exists, skip to avoid double-deducting without a durable marker
          if (!movementForRecon) {
            console.warn('[Cron] No existing movement found for shipment', s.id, s.order_number || '');
            continue;
          }
          // Skip if already reconciled previously (notes marker)
          const alreadyReconciled = typeof movementForRecon.notes === 'string' && movementForRecon.notes.includes('Reconciled via cron');
          if (alreadyReconciled) continue;

          // Resolve to client_product or product
          const resolved = resolveSku(skuCanon, productMap, bundleMap, clientSkuMap);
          const itemType = resolved.matchType === 'client_product' ? 'client_product'
            : (resolved.matchType === 'product' ? 'product' : null);
          let itemId = null;
          if (itemType === 'client_product') itemId = String(resolved.client?.id || '');
          if (itemType === 'product') itemId = String(resolved.product?.id || '');
          if (!itemType || !itemId) {
            console.warn('[Cron] unresolved SKU for shipment', s.id, skuCanon);
            continue;
          }

          // Find a Batch/Production location with sufficient availability
          const { data: stockRows, error: stockFindErr } = await supabase
            .from('inventory_stock_levels')
            .select('id, location_id, on_hand, available, inventory_locations!inner(type, code)')
            .eq('item_type', itemType)
            .eq('item_id', itemId)
            .in('inventory_locations.type', ['Batch', 'Production'])
            .order('available', { ascending: false })
            .limit(1);
          if (stockFindErr) {
            console.error('[Cron] stock lookup error:', stockFindErr);
            continue;
          }
          const stock = (stockRows || [])[0];
          const available = (stock?.available ?? stock?.on_hand ?? 0);
          if (!stock || available < qty) {
            console.warn('[Cron] insufficient stock at Batch/Production for', skuCanon, 'shipment', s.id);
            continue;
          }

          // Reduce on_hand by qty
          const newOnHand = (stock.on_hand || 0) - qty;
          const { error: updErr } = await supabase
            .from('inventory_stock_levels')
            .update({ on_hand: newOnHand, updated_at: new Date().toISOString() })
            .eq('id', stock.id);
          if (updErr) {
            console.error('[Cron] stock deduction error:', updErr);
            continue;
          }

          // Update existing movement as the reconciliation marker (no new rows)
          const updatedNotes = `${movementForRecon.notes ? movementForRecon.notes + ' | ' : ''}Reconciled via cron`;
          const movementUpdate = { notes: updatedNotes };
          if (!movementForRecon.sku) {
            movementUpdate.sku = skuCanon; // may fail if column absent in env; error is logged below
          }
          const { error: movUpdErr } = await supabase
            .from('inventory_movements')
            .update(movementUpdate)
            .eq('id', movementForRecon.id);
          if (movUpdErr) console.error('[Cron] movement update error:', movUpdErr);
        }
      }

      processed += shipments.length;
      page += 1;
      if (shipments.length < RECON_PAGE_SIZE) break; // last page
    }
    console.log(`[Cron] Shipment reconciliation processed ${processed} shipments in lookback window`);
  } catch (reconErr) {
    console.error('[Cron] shipment reconciliation fatal error:', reconErr);
  }

  console.log('[Cron] Completed alert evaluation and shipment reconciliation');
}

// Schedule
cron.schedule(POLL_CRON, () => {
  runOnce().catch(err => console.error('[Cron] runOnce error', err));
});

// Kick immediately
runOnce().catch(err => console.error('[Cron] startup error', err));

// New: Backfill shipments from events
cron.schedule(BACKFILL_CRON, () => {
  runBackfillShipmentsFromEvents().catch(err => console.error('[Backfill] job error', err));
});

// New: Fix shipped orders missing tracking/date
cron.schedule(FIX_ORDERS_CRON, () => {
  runFixShippedOrdersMissingTracking().catch(err => console.error('[FixOrders] job error', err));
});

