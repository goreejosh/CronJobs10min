import { supabase } from '../lib/supabase.js';

const LOOKBACK_DAYS = parseInt(process.env.BACKFILL_LOOKBACK_DAYS || '14', 10);
const PAGE_SIZE = parseInt(process.env.BACKFILL_PAGE_SIZE || '500', 10);
const MAX_PAGES = parseInt(process.env.BACKFILL_MAX_PAGES || '40', 10);

function toIsoOrNull(value) {
  if (!value) return null;
  try {
    // Accept date-only (ShipStation) or ISO timestamps
    if (/^\d{4}-\d{2}-\d{2}$/.test(String(value))) {
      return new Date(`${value}T00:00:00Z`).toISOString();
    }
    const d = new Date(value);
    return Number.isNaN(d.getTime()) ? null : d.toISOString();
  } catch (_) {
    return null;
  }
}

async function findShipmentByTracking(trackingNumber) {
  if (!trackingNumber) return null;
  const { data, error } = await supabase
    .from('shipments')
    .select('id, order_number, order_id, tracking_number, create_date, ship_date, carrier_code, service_code, package_code, source, source_api_shipment_id, shipengine_label_id, shipstation_actual_shipment_id')
    .eq('tracking_number', trackingNumber)
    .limit(1)
    .maybeSingle();
  if (error) {
    console.error('[Backfill] shipments lookup error:', error);
    return null;
  }
  return data || null;
}

async function findOrderRow(orderNumber, storeId) {
  if (!orderNumber) return null;
  try {
    let query = supabase
      .from('orders')
      .select('order_id, store_id')
      .eq('order_number', orderNumber)
      .limit(1);
    if (storeId != null) query = query.eq('store_id', storeId);
    const { data, error } = await query.maybeSingle();
    if (error) {
      console.error('[Backfill] orders lookup error:', error);
      return null;
    }
    return data || null;
  } catch (e) {
    console.error('[Backfill] orders lookup exception:', e);
    return null;
  }
}

function makeShipmentRowFromShipStationEvent(evt, orderRow) {
  return {
    source: 'shipstation',
    type: null,
    shipstation_numeric_shipment_id: null,
    fulfillment_id: null,
    order_id: (orderRow?.order_id != null ? String(orderRow.order_id) : (evt.order_id != null ? String(evt.order_id) : null)),
    order_number: evt.order_number || null,
    user_id: null,
    customer_email: null,
    tracking_number: evt.tracking_number || null,
    create_date: toIsoOrNull(evt.create_date),
    ship_date: toIsoOrNull(evt.ship_date) || toIsoOrNull(evt.create_date),
    void_date: null,
    delivery_date: null,
    carrier_code: evt.carrier_code || null,
    service_code: evt.service_code || null,
    package_code: evt.package_code || null,
    confirmation: evt.confirmation || null,
    warehouse_id: evt.warehouse_id || null,
    shipment_cost: evt.shipment_cost || null,
    insurance_cost: evt.insurance_cost || null,
    fulfillment_fee: evt.fulfillment_fee || null,
    void_requested: null,
    voided: evt.voided || null,
    marketplace_notified: evt.marketplace_notified || null,
    notify_error_message: evt.notify_error_message || null,
    ship_to_name: evt.ship_to_name || null,
    ship_to_company: evt.ship_to_company || null,
    ship_to_street1: evt.ship_to_street1 || null,
    ship_to_street2: evt.ship_to_street2 || null,
    ship_to_street3: evt.ship_to_street3 || null,
    ship_to_city: evt.ship_to_city || null,
    ship_to_state: evt.ship_to_state || null,
    ship_to_postal_code: evt.ship_to_postal_code || null,
    ship_to_country: evt.ship_to_country || null,
    ship_to_phone: evt.ship_to_phone || null,
    ship_to_residential: evt.ship_to_residential || null,
    shipment_items: null,
    created_at: null,
    is_fulfillment: (evt.source_type && String(evt.source_type).toLowerCase().includes('fulfillment')) ? true : false,
    source_api_shipment_id: evt.shipstation_id != null ? String(evt.shipstation_id) : null,
    label_pdf_url: null,
    status: 'active',
    shipengine_label_id: null,
    receipt_pdf_url: null,
    usps_transaction_id: null,
    shipstation_actual_shipment_id: evt.shipstation_id != null ? String(evt.shipstation_id) : null,
    create_date_shipstation: toIsoOrNull(evt.create_date)
  };
}

function makeShipmentRowFromShipEngineEvent(evt, orderRow) {
  return {
    source: 'shipengine',
    type: null,
    shipstation_numeric_shipment_id: null,
    fulfillment_id: null,
    order_id: (orderRow?.order_id != null ? String(orderRow.order_id) : null),
    order_number: evt.order_number || null,
    user_id: null,
    customer_email: null,
    tracking_number: evt.tracking_number || null,
    create_date: toIsoOrNull(evt.create_date),
    ship_date: toIsoOrNull(evt.ship_date) || toIsoOrNull(evt.create_date),
    void_date: toIsoOrNull(evt.voided_at),
    delivery_date: null,
    carrier_code: evt.carrier_code || null,
    service_code: evt.service_code || null,
    package_code: evt.package_code || null,
    confirmation: null,
    warehouse_id: null,
    shipment_cost: evt.shipping_amount || null,
    insurance_cost: evt.insurance_amount || null,
    fulfillment_fee: null,
    void_requested: null,
    voided: evt.voided || null,
    marketplace_notified: null,
    notify_error_message: null,
    ship_to_name: evt.ship_to_name || null,
    ship_to_company: evt.ship_to_company || null,
    ship_to_street1: evt.ship_to_street1 || null,
    ship_to_street2: evt.ship_to_street2 || null,
    ship_to_street3: evt.ship_to_street3 || null,
    ship_to_city: evt.ship_to_city || null,
    ship_to_state: evt.ship_to_state || null,
    ship_to_postal_code: evt.ship_to_postal_code || null,
    ship_to_country: evt.ship_to_country || null,
    ship_to_phone: evt.ship_to_phone || null,
    ship_to_residential: evt.ship_to_residential || null,
    shipment_items: null,
    created_at: null,
    is_fulfillment: false,
    source_api_shipment_id: evt.shipengine_id || null,
    label_pdf_url: null,
    status: 'active',
    shipengine_label_id: evt.shipengine_id || null,
    receipt_pdf_url: null,
    usps_transaction_id: null,
    shipstation_actual_shipment_id: null,
    create_date_shipstation: null
  };
}

function buildUpdateForMissingFields(existing, candidate) {
  const update = {};
  for (const [key, value] of Object.entries(candidate)) {
    if (value === null || value === undefined) continue;
    if (existing[key] === null || existing[key] === undefined) {
      update[key] = value;
    }
  }
  return update;
}

function buildOrderMaps(ordersList) {
  const byComposite = new Map(); // key: order_number|store_id
  const byNumber = new Map();    // key: order_number
  for (const r of ordersList || []) {
    const on = r.order_number;
    const sid = r.store_id;
    const key = `${on}|${sid ?? 'null'}`;
    byComposite.set(key, r);
    if (!byNumber.has(on)) byNumber.set(on, r);
  }
  return { byComposite, byNumber };
}

function buildExistingShipmentMap(rows) {
  const map = new Map();
  for (const r of rows || []) {
    if (r.tracking_number) map.set(r.tracking_number, r);
  }
  return map;
}

async function processShipStationEvents(sinceIso) {
  let page = 0;
  while (page < MAX_PAGES) {
    const from = page * PAGE_SIZE;
    const to = from + PAGE_SIZE - 1;
    const { data, error } = await supabase
      .from('shipstation_events')
      .select('shipstation_id, order_id, order_number, store_id, tracking_number, carrier_code, service_code, package_code, confirmation, warehouse_id, shipment_cost, insurance_cost, fulfillment_fee, create_date, ship_date, voided, is_return_label, marketplace_notified, notify_error_message, source_type')
      .gte('create_date', sinceIso)
      .neq('voided', true)
      .neq('is_return_label', true)
      .not('tracking_number', 'is', null)
      .order('create_date', { ascending: true })
      .range(from, to);
    if (error) {
      console.error('[Backfill] ShipStation fetch error:', error);
      break;
    }
    if (!data || data.length === 0) break;
    // Batch prep
    const trackings = Array.from(new Set((data.map(e => (e.tracking_number || '').trim())).filter(Boolean)));
    const orderNumbers = Array.from(new Set((data.map(e => e.order_number).filter(Boolean))));

    // Batch fetch existing shipments
    let existingMap = new Map();
    if (trackings.length) {
      const { data: existingRows, error: existErr } = await supabase
        .from('shipments')
        .select('id, tracking_number, order_number, order_id, create_date, ship_date, carrier_code, service_code, package_code, source, source_api_shipment_id, shipengine_label_id, shipstation_actual_shipment_id')
        .in('tracking_number', trackings);
      if (existErr) console.error('[Backfill] existing shipments batch error:', existErr);
      existingMap = buildExistingShipmentMap(existingRows);
    }

    // Batch fetch orders
    let orderMaps = { byComposite: new Map(), byNumber: new Map() };
    if (orderNumbers.length) {
      const { data: orderRows, error: ordErr } = await supabase
        .from('orders')
        .select('order_id, order_number, store_id')
        .in('order_number', orderNumbers);
      if (ordErr) console.error('[Backfill] orders batch error:', ordErr);
      orderMaps = buildOrderMaps(orderRows);
    }

    const inserts = [];
    const updates = [];
    for (const evt of data) {
      try {
        const tracking = (evt?.tracking_number || '').trim();
        if (!tracking) continue;
        const existing = existingMap.get(tracking) || null;

        const ordKey = `${evt.order_number}|${evt.store_id ?? 'null'}`;
        const orderRow = orderMaps.byComposite.get(ordKey) || orderMaps.byNumber.get(evt.order_number) || null;
        const candidate = makeShipmentRowFromShipStationEvent(evt, orderRow);

        if (!existing) {
          inserts.push(candidate);
          continue;
        }
        const update = buildUpdateForMissingFields(existing, candidate);
        if (Object.keys(update).length > 0) updates.push({ id: existing.id, update });
      } catch (e) {
        console.error('[Backfill] ShipStation upsert error:', e);
      }
    }

    if (inserts.length) {
      const { error: insErr } = await supabase.from('shipments').insert(inserts, { returning: 'minimal' });
      if (insErr) console.error('[Backfill] batch insert shipments (ShipStation) error:', insErr);
    }
    for (const u of updates) {
      const { error: updErr } = await supabase.from('shipments').update(u.update).eq('id', u.id);
      if (updErr) console.error('[Backfill] batch update shipments (ShipStation) error:', updErr);
    }
    if (data.length < PAGE_SIZE) break;
    page += 1;
  }
}

async function processShipEngineEvents(sinceIso) {
  let page = 0;
  while (page < MAX_PAGES) {
    const from = page * PAGE_SIZE;
    const to = from + PAGE_SIZE - 1;
    const { data, error } = await supabase
      .from('shipengine_events')
      .select('shipengine_id, order_number, tracking_number, carrier_code, service_code, package_code, shipment_status, ship_date, create_date, voided, voided_at, is_return_label, ship_to_name, ship_to_company, ship_to_street1, ship_to_street2, ship_to_street3, ship_to_city, ship_to_state, ship_to_postal_code, ship_to_country, ship_to_phone, ship_to_residential, shipping_amount, insurance_amount')
      .gte('create_date', sinceIso)
      .neq('voided', true)
      .neq('is_return_label', true)
      .not('tracking_number', 'is', null)
      .order('create_date', { ascending: true })
      .range(from, to);
    if (error) {
      console.error('[Backfill] ShipEngine fetch error:', error);
      break;
    }
    if (!data || data.length === 0) break;
    // Batch prep
    const trackings = Array.from(new Set((data.map(e => (e.tracking_number || '').trim())).filter(Boolean)));
    const orderNumbers = Array.from(new Set((data.map(e => e.order_number).filter(Boolean))));

    let existingMap = new Map();
    if (trackings.length) {
      const { data: existingRows, error: existErr } = await supabase
        .from('shipments')
        .select('id, tracking_number, order_number, order_id, create_date, ship_date, carrier_code, service_code, package_code, source, source_api_shipment_id, shipengine_label_id, shipstation_actual_shipment_id')
        .in('tracking_number', trackings);
      if (existErr) console.error('[Backfill] existing shipments batch error:', existErr);
      existingMap = buildExistingShipmentMap(existingRows);
    }

    let orderMaps = { byComposite: new Map(), byNumber: new Map() };
    if (orderNumbers.length) {
      const { data: orderRows, error: ordErr } = await supabase
        .from('orders')
        .select('order_id, order_number, store_id')
        .in('order_number', orderNumbers);
      if (ordErr) console.error('[Backfill] orders batch error:', ordErr);
      orderMaps = buildOrderMaps(orderRows);
    }

    const inserts = [];
    const updates = [];
    for (const evt of data) {
      try {
        const tracking = (evt?.tracking_number || '').trim();
        if (!tracking) continue;
        const existing = existingMap.get(tracking) || null;
        const orderRow = orderMaps.byNumber.get(evt.order_number) || null;
        const candidate = makeShipmentRowFromShipEngineEvent(evt, orderRow);
        if (!existing) {
          inserts.push(candidate);
          continue;
        }
        const update = buildUpdateForMissingFields(existing, candidate);
        if (Object.keys(update).length > 0) updates.push({ id: existing.id, update });
      } catch (e) {
        console.error('[Backfill] ShipEngine upsert error:', e);
      }
    }

    if (inserts.length) {
      const { error: insErr } = await supabase.from('shipments').insert(inserts, { returning: 'minimal' });
      if (insErr) console.error('[Backfill] batch insert shipments (ShipEngine) error:', insErr);
    }
    for (const u of updates) {
      const { error: updErr } = await supabase.from('shipments').update(u.update).eq('id', u.id);
      if (updErr) console.error('[Backfill] batch update shipments (ShipEngine) error:', updErr);
    }
    if (data.length < PAGE_SIZE) break;
    page += 1;
  }
}

export async function runBackfillShipmentsFromEvents() {
  const sinceIso = new Date(Date.now() - LOOKBACK_DAYS * 24 * 3600 * 1000).toISOString();
  console.log(`[Backfill] Starting backfill from events since ${sinceIso}`);
  await processShipStationEvents(sinceIso);
  await processShipEngineEvents(sinceIso);
  console.log('[Backfill] Completed backfill run');
}


