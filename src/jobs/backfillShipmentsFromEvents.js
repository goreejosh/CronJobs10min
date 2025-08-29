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

async function findOrderByOrderNumber(orderNumber) {
  if (!orderNumber) return null;
  const { data, error } = await supabase
    .from('orders')
    .select('order_id, store_id')
    .eq('order_number', orderNumber)
    .limit(1)
    .maybeSingle();
  if (error) {
    console.error('[Backfill] orders lookup error:', error);
    return null;
  }
  return data || null;
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

async function upsertShipmentFromShipStationEvent(evt) {
  const tracking = (evt?.tracking_number || '').trim();
  if (!tracking) return;

  const existing = await findShipmentByTracking(tracking);
  const orderRow = await findOrderByOrderNumber(evt.order_number);
  const candidate = makeShipmentRowFromShipStationEvent(evt, orderRow);

  if (!existing) {
    const { error } = await supabase
      .from('shipments')
      .insert([candidate], { returning: 'minimal' });
    if (error) console.error('[Backfill] insert shipments (ShipStation) error:', error);
    return;
  }

  const update = buildUpdateForMissingFields(existing, candidate);
  if (Object.keys(update).length > 0) {
    const { error } = await supabase
      .from('shipments')
      .update(update)
      .eq('id', existing.id);
    if (error) console.error('[Backfill] update shipments (ShipStation) error:', error);
  }
}

async function upsertShipmentFromShipEngineEvent(evt) {
  const tracking = (evt?.tracking_number || '').trim();
  if (!tracking) return;

  const existing = await findShipmentByTracking(tracking);
  const orderRow = await findOrderByOrderNumber(evt.order_number);
  const candidate = makeShipmentRowFromShipEngineEvent(evt, orderRow);

  if (!existing) {
    const { error } = await supabase
      .from('shipments')
      .insert([candidate], { returning: 'minimal' });
    if (error) console.error('[Backfill] insert shipments (ShipEngine) error:', error);
    return;
  }

  const update = buildUpdateForMissingFields(existing, candidate);
  if (Object.keys(update).length > 0) {
    const { error } = await supabase
      .from('shipments')
      .update(update)
      .eq('id', existing.id);
    if (error) console.error('[Backfill] update shipments (ShipEngine) error:', error);
  }
}

async function processShipStationEvents(sinceIso) {
  let page = 0;
  while (page < MAX_PAGES) {
    const from = page * PAGE_SIZE;
    const to = from + PAGE_SIZE - 1;
    const { data, error } = await supabase
      .from('shipstation_events')
      .select('shipstation_id, order_id, order_number, tracking_number, carrier_code, service_code, package_code, confirmation, warehouse_id, shipment_cost, insurance_cost, fulfillment_fee, create_date, ship_date, voided, is_return_label, marketplace_notified, notify_error_message, source_type')
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
    for (const evt of data) {
      try { await upsertShipmentFromShipStationEvent(evt); } catch (e) { console.error('[Backfill] ShipStation upsert error:', e); }
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
    for (const evt of data) {
      try { await upsertShipmentFromShipEngineEvent(evt); } catch (e) { console.error('[Backfill] ShipEngine upsert error:', e); }
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


