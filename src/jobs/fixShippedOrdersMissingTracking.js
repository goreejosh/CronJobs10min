import { supabase } from '../lib/supabase.js';

const PAGE_SIZE = parseInt(process.env.FIX_ORDERS_PAGE_SIZE || '500', 10);
const MAX_PAGES = parseInt(process.env.FIX_ORDERS_MAX_PAGES || '40', 10);
const ORDERS_LOOKBACK_DAYS = parseInt(process.env.FIX_ORDERS_LOOKBACK_DAYS || '60', 10);

function toIsoOrNull(value) {
  if (!value) return null;
  try {
    if (/^\d{4}-\d{2}-\d{2}$/.test(String(value))) {
      return new Date(`${value}T00:00:00Z`).toISOString();
    }
    const d = new Date(value);
    return Number.isNaN(d.getTime()) ? null : d.toISOString();
  } catch (_) {
    return null;
  }
}

async function findFromShipStation(orderId, orderNumber) {
  // Prefer order_id match; fallback to order_number
  const orClauses = [];
  if (orderId != null) orClauses.push({ column: 'order_id', value: String(orderId) });
  if (orderNumber) orClauses.push({ column: 'order_number', value: orderNumber });
  for (const c of orClauses) {
    const { data, error } = await supabase
      .from('shipstation_events')
      .select('tracking_number, ship_date, create_date, voided, is_return_label')
      .eq(c.column, c.value)
      .neq('voided', true)
      .neq('is_return_label', true)
      .not('tracking_number', 'is', null)
      .order('create_date', { ascending: false })
      .limit(1)
      .maybeSingle();
    if (error) {
      console.error('[FixOrders] ShipStation query error:', error);
    } else if (data) {
      const tracking = (data.tracking_number || '').trim();
      const shipDateIso = toIsoOrNull(data.ship_date) || toIsoOrNull(data.create_date);
      if (tracking && shipDateIso) return { tracking, shipDateIso };
      if (tracking) return { tracking, shipDateIso: toIsoOrNull(data.create_date) };
    }
  }
  return null;
}

async function findFromShipEngine(orderNumber) {
  if (!orderNumber) return null;
  const { data, error } = await supabase
    .from('shipengine_events')
    .select('tracking_number, ship_date, create_date, voided, is_return_label')
    .eq('order_number', orderNumber)
    .neq('voided', true)
    .neq('is_return_label', true)
    .not('tracking_number', 'is', null)
    .order('create_date', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) {
    console.error('[FixOrders] ShipEngine query error:', error);
    return null;
  }
  if (!data) return null;
  const tracking = (data.tracking_number || '').trim();
  const shipDateIso = toIsoOrNull(data.ship_date) || toIsoOrNull(data.create_date);
  if (!tracking) return null;
  return { tracking, shipDateIso };
}

function extractShipDateFromLabelLedgerRow(row) {
  // Try raw_response first, then raw_payload; accept ISO or date-only
  try {
    const payload = row.raw_response || row.raw_payload || null;
    if (!payload) return null;
    const shipDate = payload.ship_date || payload.shipDate || payload.label_date || null;
    return toIsoOrNull(shipDate) || toIsoOrNull(row.created_at);
  } catch (_) {
    return toIsoOrNull(row.created_at);
  }
}

async function findFromLabelLedger(orderNumber) {
  if (!orderNumber) return null;
  const { data, error } = await supabase
    .from('label_ledger')
    .select('tracking_number, created_at, raw_payload, raw_response')
    .eq('order_ref', orderNumber)
    .not('tracking_number', 'is', null)
    .order('created_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) {
    console.error('[FixOrders] Label ledger query error:', error);
    return null;
  }
  if (!data) return null;
  const tracking = (data.tracking_number || '').trim();
  if (!tracking) return null;
  const shipDateIso = extractShipDateFromLabelLedgerRow(data) || toIsoOrNull(data.created_at);
  return { tracking, shipDateIso };
}

async function ensureShipmentExists(orderNumber, tracking) {
  if (!tracking) return;
  const { data: existing, error: lookupErr } = await supabase
    .from('shipments')
    .select('id')
    .eq('tracking_number', tracking)
    .limit(1)
    .maybeSingle();
  if (lookupErr) {
    console.error('[FixOrders] shipment lookup error:', lookupErr);
    return;
  }
  if (existing) return;

  // Minimal insert – the backfill job will enrich on next run
  const { data: orderRow } = await supabase
    .from('orders')
    .select('order_id')
    .eq('order_number', orderNumber)
    .limit(1)
    .maybeSingle();
  const insert = {
    source: 'orders_fix_cron',
    order_number: orderNumber || null,
    order_id: orderRow?.order_id != null ? String(orderRow.order_id) : null,
    tracking_number: tracking,
    status: 'active'
  };
  const { error: insErr } = await supabase.from('shipments').insert([insert], { returning: 'minimal' });
  if (insErr) console.error('[FixOrders] insert minimal shipment error:', insErr);
}

export async function runFixShippedOrdersMissingTracking() {
  const sinceIso = new Date(Date.now() - ORDERS_LOOKBACK_DAYS * 24 * 3600 * 1000).toISOString();
  let page = 0;
  let fixed = 0;
  console.log(`[FixOrders] Scanning shipped orders missing tracking/date since ${sinceIso}`);

  while (page < MAX_PAGES) {
    const from = page * PAGE_SIZE;
    const to = from + PAGE_SIZE - 1;
    const { data: orders, error } = await supabase
      .from('orders')
      .select('order_id, order_number, order_status, tracking_number, actual_ship_date, order_date')
      .ilike('order_status', 'shipped')
      .is('tracking_number', null)
      .is('actual_ship_date', null)
      .gte('order_date', sinceIso)
      .order('order_date', { ascending: false })
      .range(from, to);
    if (error) {
      console.error('[FixOrders] orders fetch error:', error);
      break;
    }
    if (!orders || orders.length === 0) break;

    for (const o of orders) {
      try {
        const ss = await findFromShipStation(o.order_id, o.order_number);
        const se = ss ? null : await findFromShipEngine(o.order_number);
        const ll = (ss || se) ? null : await findFromLabelLedger(o.order_number);
        const found = ss || se || ll;
        if (!found) continue;

        const { tracking, shipDateIso } = found;
        const update = {};
        if (tracking) update.tracking_number = tracking;
        if (shipDateIso) update.actual_ship_date = shipDateIso;
        if (Object.keys(update).length === 0) continue;

        const { error: updErr } = await supabase
          .from('orders')
          .update(update)
          .eq('order_id', o.order_id);
        if (updErr) {
          console.error('[FixOrders] orders update error:', updErr);
          continue;
        }
        fixed += 1;

        // Ensure shipments table reflects this tracking
        await ensureShipmentExists(o.order_number, tracking);
      } catch (e) {
        console.error('[FixOrders] order processing error:', e);
      }
    }

    if (orders.length < PAGE_SIZE) break;
    page += 1;
  }

  console.log(`[FixOrders] Completed scan. Fixed: ${fixed}`);
}


