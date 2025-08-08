// ESM SKU resolver for cron job usage

export function normalizeSku(raw) {
  return String(raw || '').toLowerCase().replace(/\s+/g, '');
}

export function buildSkuMaps(products = [], bundles = []) {
  const productMap = {};
  products.forEach((p) => {
    if (p && p.Sku) {
      productMap[normalizeSku(p.Sku)] = p;
    }
  });

  const bundleMap = {};
  bundles.forEach((b) => {
    if (b && b.name) {
      bundleMap[normalizeSku(b.name)] = b;
    }
  });
  return { productMap, bundleMap };
}

export function resolveSku(rawSku, productMap = {}, bundleMap = {}) {
  if (!rawSku) return { baseSku: '', matchType: null };

  const norm = normalizeSku(rawSku);

  if (bundleMap[norm]) {
    return { baseSku: norm, matchType: 'bundle', bundle: bundleMap[norm] };
  }
  if (productMap[norm]) {
    return { baseSku: norm, matchType: 'product', product: productMap[norm] };
  }

  let bestBundleKey = '';
  let bestProductKey = '';

  const tryPrefix = (key, isBundle) => {
    if (norm.startsWith(key) && key.length > (isBundle ? bestBundleKey.length : bestProductKey.length)) {
      if (isBundle) bestBundleKey = key; else bestProductKey = key;
    }
  };

  Object.keys(bundleMap).forEach((k) => tryPrefix(k, true));
  Object.keys(productMap).forEach((k) => tryPrefix(k, false));

  if (bestBundleKey) {
    return { baseSku: bestBundleKey, matchType: 'bundle', bundle: bundleMap[bestBundleKey] };
  }
  if (bestProductKey) {
    return { baseSku: bestProductKey, matchType: 'product', product: productMap[bestProductKey] };
  }

  const fallback = norm.split(/[-_.]/)[0];
  return { baseSku: fallback, matchType: null };
}

