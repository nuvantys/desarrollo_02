import {
  balanceBarOption,
  donutOption,
  formatCompact,
  formatCurrency,
  formatDate,
  formatNumber,
  formatPreciseNumber,
  horizontalBarOption,
  lineComboOption,
  paretoOption,
  resizeCharts,
  setOption,
  stackedBarOption,
} from "./charts.js";

const fileNames = [
  "manifest.json",
  "overview.json",
  "commercial.json",
  "customers.json",
  "products.json",
  "inventory.json",
  "accounting.json",
  "quality.json",
  "tables.json",
];

const state = {
  global: {
    dateFrom: "",
    dateTo: "",
    documentType: "",
    documentState: "",
    bodega: "",
    category: "",
  },
  local: {
    vendor: "",
    inventoryType: "",
    account: "",
    center: "",
  },
};

const tableExports = new Map();

const elements = {
  heroText: document.getElementById("hero-text"),
  heroGeneratedAt: document.getElementById("hero-generated-at"),
  heroCoverage: document.getElementById("hero-coverage"),
  heroAlerts: document.getElementById("hero-alerts"),
  overviewMetrics: document.getElementById("overview-metrics"),
  storyCards: document.getElementById("story-cards"),
  qualityMetrics: document.getElementById("quality-metrics"),
  activeFilters: document.getElementById("active-filters"),
  filters: {
    dateFrom: document.getElementById("filter-date-from"),
    dateTo: document.getElementById("filter-date-to"),
    documentType: document.getElementById("filter-document-type"),
    documentState: document.getElementById("filter-document-state"),
    bodega: document.getElementById("filter-bodega"),
    category: document.getElementById("filter-category"),
    vendor: document.getElementById("commercial-vendor-filter"),
    inventoryType: document.getElementById("inventory-type-filter"),
    account: document.getElementById("accounting-account-filter"),
    center: document.getElementById("accounting-center-filter"),
  },
};

let snapshot = null;

function toNumber(value) {
  return Number(value || 0);
}

function monthBucket(value) {
  return `${String(value).slice(0, 7)}-01`;
}

function sortByValueDescending(rows, key = "value") {
  return [...rows].sort((a, b) => toNumber(b[key]) - toNumber(a[key]));
}

function uniqueCount(values) {
  return new Set(values.filter(Boolean)).size;
}

function makeOptionList(select, items, valueKey = "value", labelKey = "label", includeAll = true, allLabel = "Todos") {
  const options = [];
  if (includeAll) {
    options.push(`<option value="">${allLabel}</option>`);
  }
  for (const item of items) {
    options.push(`<option value="${item[valueKey] ?? ""}">${item[labelKey] ?? ""}</option>`);
  }
  select.innerHTML = options.join("");
}

function inDateRange(dateValue, from, to) {
  if (!dateValue) return false;
  if (from && dateValue < from) return false;
  if (to && dateValue > to) return false;
  return true;
}

function aggregateBy(rows, keyBuilder, initializer, reducer) {
  const map = new Map();
  for (const row of rows) {
    const key = keyBuilder(row);
    if (!map.has(key)) {
      map.set(key, initializer(row));
    } else {
      reducer(map.get(key), row);
    }
  }
  return [...map.values()];
}

function renderMetricCards(target, metrics) {
  target.innerHTML = metrics
    .map(
      (metric) => `
        <article class="metric-card">
          <h3>${metric.label}</h3>
          <strong>${metric.value}</strong>
          <span>${metric.caption}</span>
        </article>
      `,
    )
    .join("");
}

function renderStoryCards(target, cards) {
  target.innerHTML = cards
    .map(
      (card) => `
        <article class="story-card">
          <h3>${card.title}</h3>
          <p>${card.body}</p>
        </article>
      `,
    )
    .join("");
}

function renderAlerts(alerts) {
  elements.heroAlerts.innerHTML = alerts
    .map(
      (alert) => `
        <article class="alert-card ${alert.level}">
          <h3>${alert.title}</h3>
          <p>${alert.message}</p>
          <p><strong>${formatCompact(alert.metric)}</strong></p>
        </article>
      `,
    )
    .join("");
}

function renderActiveFilters() {
  const chips = [];
  if (state.global.dateFrom) chips.push(`Desde ${state.global.dateFrom}`);
  if (state.global.dateTo) chips.push(`Hasta ${state.global.dateTo}`);
  if (state.global.documentType) chips.push(`Tipo ${state.global.documentType}`);
  if (state.global.documentState) chips.push(`Estado ${state.global.documentState}`);
  if (state.global.bodega) chips.push(`Bodega ${elements.filters.bodega.selectedOptions[0]?.textContent}`);
  if (state.global.category) chips.push(`Categoría ${elements.filters.category.selectedOptions[0]?.textContent}`);
  if (state.local.vendor) chips.push(`Vendedor ${elements.filters.vendor.selectedOptions[0]?.textContent}`);
  if (state.local.inventoryType) chips.push(`Movimiento ${state.local.inventoryType}`);
  if (state.local.account) chips.push(`Cuenta ${elements.filters.account.selectedOptions[0]?.textContent}`);
  if (state.local.center) chips.push(`Centro ${elements.filters.center.selectedOptions[0]?.textContent}`);
  elements.activeFilters.innerHTML = chips.length
    ? chips.map((chip) => `<span class="filter-chip">${chip}</span>`).join("")
    : `<span class="filter-chip">Sin filtros activos</span>`;
}

function renderTable(targetId, rows, columns) {
  const target = document.getElementById(targetId);
  if (!rows.length) {
    target.innerHTML = `<div class="table-empty">No hay filas disponibles para esta combinación de filtros.</div>`;
    return;
  }
  const header = columns.map((column) => `<th>${column.label}</th>`).join("");
  const body = rows
    .map(
      (row) => `
        <tr>
          ${columns
            .map((column) => `<td>${column.formatter ? column.formatter(row[column.key], row) : row[column.key] ?? ""}</td>`)
            .join("")}
        </tr>
      `,
    )
    .join("");
  target.innerHTML = `<table><thead><tr>${header}</tr></thead><tbody>${body}</tbody></table>`;
}

function registerTableExport(key, rows, columns) {
  tableExports.set(key, { rows, columns });
}

function exportTable(key) {
  const table = tableExports.get(key);
  if (!table) return;
  const csv = [
    table.columns.map((column) => column.label).join(","),
    ...table.rows.map((row) =>
      table.columns
        .map((column) => {
          const escaped = String(row[column.key] ?? "").replaceAll('"', '""');
          return `"${escaped}"`;
        })
        .join(","),
    ),
  ].join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `${key}.csv`;
  link.click();
  URL.revokeObjectURL(url);
}

function currentDateBounds() {
  return {
    from: state.global.dateFrom || snapshot.manifest.coverage_min,
    to: state.global.dateTo || snapshot.manifest.coverage_max,
  };
}

function filteredCommercialDocs() {
  const { from, to } = currentDateBounds();
  return snapshot.commercial.document_facts.filter((row) => {
    if (!inDateRange(row.date, from, to)) return false;
    if (state.global.documentType && row.tipo_documento !== state.global.documentType) return false;
    if (state.global.documentState && row.estado !== state.global.documentState) return false;
    if (state.local.vendor && row.vendedor_id !== state.local.vendor) return false;
    return true;
  });
}

function filteredProductLines() {
  const { from, to } = currentDateBounds();
  const categoryLabel = elements.filters.category.selectedOptions[0]?.textContent;
  return snapshot.products.line_facts.filter((row) => {
    if (!inDateRange(row.date, from, to)) return false;
    if (state.global.documentType && row.tipo_documento !== state.global.documentType) return false;
    if (state.global.documentState && row.estado !== state.global.documentState) return false;
    if (state.global.category && row.categoria_nombre !== categoryLabel) return false;
    return true;
  });
}

function filteredMovements() {
  const { from, to } = currentDateBounds();
  return snapshot.inventory.movement_facts.filter((row) => {
    if (!inDateRange(row.date, from, to)) return false;
    if (state.global.bodega && row.bodega_id !== state.global.bodega) return false;
    if (state.local.inventoryType && row.tipo !== state.local.inventoryType) return false;
    return true;
  });
}

function filteredMovementLines() {
  const { from, to } = currentDateBounds();
  const bodegaLabel = elements.filters.bodega.selectedOptions[0]?.textContent;
  const categoryLabel = elements.filters.category.selectedOptions[0]?.textContent;
  return snapshot.inventory.movement_line_facts.filter((row) => {
    if (!inDateRange(row.date, from, to)) return false;
    if (state.global.bodega && row.bodega_nombre !== bodegaLabel) return false;
    if (state.local.inventoryType && row.tipo !== state.local.inventoryType) return false;
    if (state.global.category && row.categoria_nombre !== categoryLabel) return false;
    return true;
  });
}

function filteredAccountingFacts() {
  const { from, to } = currentDateBounds();
  return snapshot.accounting.monthly_facts.filter((row) => {
    if (!inDateRange(row.period, from, to)) return false;
    if (state.local.account && row.cuenta_id !== state.local.account) return false;
    if (state.local.center && row.centro_costo_id !== state.local.center) return false;
    return true;
  });
}

function filteredAccountingSummary() {
  const { from, to } = currentDateBounds();
  return snapshot.accounting.monthly_summary.filter((row) => inDateRange(row.period, from, to));
}

function renderOverview() {
  const docs = filteredCommercialDocs();
  const lines = filteredProductLines();
  const movements = filteredMovements();
  const accountingSummary = filteredAccountingSummary();

  const revenue = docs.reduce((sum, row) => sum + toNumber(row.total), 0);
  const activeCustomers = uniqueCount(docs.map((row) => row.cliente_id));
  const activeProducts = uniqueCount(lines.map((row) => row.producto_id || row.producto_nombre));
  const totalAsientos = accountingSummary.reduce((sum, row) => sum + toNumber(row.asientos), 0);

  renderMetricCards(elements.overviewMetrics, [
    { label: "Documentos filtrados", value: formatPreciseNumber(docs.length), caption: "Volumen documental dentro de la vista activa." },
    { label: "Monto comercial", value: formatCurrency(revenue), caption: "Total acumulado de documentos en el filtro actual." },
    { label: "Clientes activos", value: formatPreciseNumber(activeCustomers), caption: "Clientes con movimiento en documentos filtrados." },
    { label: "Productos activos", value: formatPreciseNumber(activeProducts), caption: "Productos detectados en líneas comerciales filtradas." },
    { label: "Movimientos únicos", value: formatPreciseNumber(movements.length), caption: "Movimientos de inventario dentro de la ventana activa." },
    { label: "Asientos del periodo", value: formatPreciseNumber(totalAsientos), caption: "Asientos contables consolidados por rango temporal." },
  ]);

  const docsByMonth = aggregateBy(
    docs,
    (row) => monthBucket(row.date),
    (row) => ({ period: monthBucket(row.date), documentos: 1, monto_total: toNumber(row.total) }),
    (acc, row) => {
      acc.documentos += 1;
      acc.monto_total += toNumber(row.total);
    },
  ).sort((a, b) => a.period.localeCompare(b.period));

  const accountingMap = new Map(accountingSummary.map((row) => [row.period, row]));
  const timelineRows = docsByMonth.map((row) => ({
    period: row.period,
    documentos: row.documentos,
    monto_total: row.monto_total,
    asientos: toNumber(accountingMap.get(row.period)?.asientos || 0),
  }));

  setOption(
    "overview-timeline-chart",
    lineComboOption({
      categories: timelineRows.map((row) => formatDate(row.period)),
      bars: timelineRows.map((row) => row.documentos),
      line: timelineRows.map((row) => row.monto_total),
      barName: "Documentos",
      lineName: "Monto",
    }),
  );

  const anomaly = snapshot.quality.source_vs_core.find((row) => row.resource === "movimiento-inventario");
  renderStoryCards(elements.storyCards, [
    {
      title: "Cobertura extensa y estable",
      body: `El snapshot cubre desde ${snapshot.manifest.coverage_min} hasta ${snapshot.manifest.coverage_max}, con continuidad suficiente para análisis histórico de tendencias.`,
    },
    {
      title: "Peso operativo del negocio",
      body: `Documentos y asientos dominan el volumen analítico; en esta vista se observan ${formatCompact(docs.length)} documentos y ${formatCompact(totalAsientos)} asientos dentro de la ventana seleccionada.`,
    },
    {
      title: "Inventario normalizado",
      body: `La diferencia fuente vs IDs únicos en movimientos es ${formatCompact(anomaly?.difference || 0)}. El modelo conserva la entidad única para análisis consistente.`,
    },
  ]);
}

function renderCommercial() {
  const docs = filteredCommercialDocs();
  const lines = filteredProductLines();

  const byMonth = aggregateBy(
    docs,
    (row) => monthBucket(row.date),
    (row) => ({ period: monthBucket(row.date), documentos: 1, monto_total: toNumber(row.total) }),
    (acc, row) => {
      acc.documentos += 1;
      acc.monto_total += toNumber(row.total);
    },
  ).sort((a, b) => a.period.localeCompare(b.period));

  setOption(
    "commercial-revenue-chart",
    lineComboOption({
      categories: byMonth.map((row) => formatDate(row.period)),
      bars: byMonth.map((row) => row.documentos),
      line: byMonth.map((row) => row.monto_total),
      barName: "Documentos",
      lineName: "Monto",
    }),
  );

  const typeStateMap = new Map();
  docs.forEach((row) => {
    const key = row.tipo_documento;
    if (!typeStateMap.has(key)) typeStateMap.set(key, {});
    const entry = typeStateMap.get(key);
    entry[row.estado] = (entry[row.estado] || 0) + 1;
  });
  const categories = [...typeStateMap.keys()];
  const states = [...new Set(docs.map((row) => row.estado))];
  const series = states.map((stateKey) => ({
    name: stateKey,
    data: categories.map((typeKey) => typeStateMap.get(typeKey)?.[stateKey] || 0),
  }));
  setOption("commercial-type-state-chart", stackedBarOption({ categories, series }));

  const topCustomers = sortByValueDescending(
    aggregateBy(
      docs,
      (row) => row.cliente_nombre,
      (row) => ({ label: row.cliente_nombre, value: toNumber(row.total) }),
      (acc, row) => {
        acc.value += toNumber(row.total);
      },
    ),
  ).slice(0, 12);
  setOption("commercial-top-customers-chart", horizontalBarOption({ labels: topCustomers.map((row) => row.label), values: topCustomers.map((row) => row.value) }));

  const topProducts = sortByValueDescending(
    aggregateBy(
      lines,
      (row) => row.producto_nombre,
      (row) => ({ label: row.producto_nombre, value: toNumber(row.importe) }),
      (acc, row) => {
        acc.value += toNumber(row.importe);
      },
    ),
  ).slice(0, 12);
  setOption(
    "commercial-top-products-chart",
    horizontalBarOption({ labels: topProducts.map((row) => row.label), values: topProducts.map((row) => row.value), color: "#4a9084" }),
  );
}

function renderCustomersAndProducts() {
  setOption(
    "customers-role-chart",
    donutOption({ data: snapshot.customers.role_mix.map((row) => ({ name: row.rol, value: toNumber(row.total) })) }),
  );

  const docs = filteredCommercialDocs();
  const concentrationBase = sortByValueDescending(
    aggregateBy(
      docs,
      (row) => row.cliente_nombre,
      (row) => ({ label: row.cliente_nombre, value: toNumber(row.total) }),
      (acc, row) => {
        acc.value += toNumber(row.total);
      },
    ),
  ).slice(0, 15);
  const total = concentrationBase.reduce((sum, row) => sum + row.value, 0) || 1;
  let running = 0;
  setOption(
    "customers-concentration-chart",
    paretoOption({
      labels: concentrationBase.map((row) => row.label),
      bars: concentrationBase.map((row) => row.value),
      line: concentrationBase.map((row) => {
        running += row.value;
        return Number(((running / total) * 100).toFixed(1));
      }),
    }),
  );

  const categoryRows = snapshot.products.category_stock.slice(0, 12);
  setOption(
    "products-category-chart",
    horizontalBarOption({ labels: categoryRows.map((row) => row.categoria_nombre), values: categoryRows.map((row) => row.stock_total), formatter: formatNumber, color: "#d79b41" }),
  );

  const brandRows = snapshot.products.brand_mix.slice(0, 12);
  setOption(
    "products-brand-chart",
    horizontalBarOption({ labels: brandRows.map((row) => row.marca_nombre), values: brandRows.map((row) => row.productos), formatter: formatNumber, color: "#8cbeb3" }),
  );
}

function renderInventory() {
  const movements = filteredMovements();
  const lines = filteredMovementLines();

  const byMonthAndType = aggregateBy(
    movements,
    (row) => `${monthBucket(row.date)}|${row.tipo}`,
    (row) => ({ period: monthBucket(row.date), tipo: row.tipo, count: 1 }),
    (acc) => {
      acc.count += 1;
    },
  );
  const months = [...new Set(byMonthAndType.map((row) => row.period))].sort();
  const movementTypes = [...new Set(byMonthAndType.map((row) => row.tipo))].sort();
  setOption(
    "inventory-flow-chart",
    stackedBarOption({
      categories: months.map((period) => formatDate(period)),
      series: movementTypes.map((tipo) => ({
        name: tipo,
        data: months.map((period) => byMonthAndType.find((row) => row.period === period && row.tipo === tipo)?.count || 0),
      })),
    }),
  );

  const bodegas = sortByValueDescending(
    aggregateBy(
      movements,
      (row) => row.bodega_nombre,
      (row) => ({ label: row.bodega_nombre, value: 1 }),
      (acc) => {
        acc.value += 1;
      },
    ),
  ).slice(0, 12);
  setOption(
    "inventory-bodega-chart",
    horizontalBarOption({ labels: bodegas.map((row) => row.label), values: bodegas.map((row) => row.value), formatter: formatNumber }),
  );

  const rotation = sortByValueDescending(
    aggregateBy(
      lines,
      (row) => row.producto_nombre,
      (row) => ({ label: row.producto_nombre, value: toNumber(row.cantidad) }),
      (acc, row) => {
        acc.value += toNumber(row.cantidad);
      },
    ),
  ).slice(0, 12);
  setOption(
    "inventory-rotation-chart",
    horizontalBarOption({ labels: rotation.map((row) => row.label), values: rotation.map((row) => row.value), formatter: formatNumber, color: "#4a9084" }),
  );

  const categories = sortByValueDescending(
    aggregateBy(
      lines,
      (row) => row.categoria_nombre,
      (row) => ({ label: row.categoria_nombre, value: toNumber(row.costo_total) }),
      (acc, row) => {
        acc.value += toNumber(row.costo_total);
      },
    ),
  ).slice(0, 12);
  setOption(
    "inventory-category-chart",
    horizontalBarOption({ labels: categories.map((row) => row.label), values: categories.map((row) => row.value), color: "#d79b41" }),
  );
}

function renderAccounting() {
  const monthlySummary = filteredAccountingSummary();
  const accountingFacts = filteredAccountingFacts();

  setOption(
    "accounting-monthly-chart",
    lineComboOption({
      categories: monthlySummary.map((row) => formatDate(row.period)),
      bars: monthlySummary.map((row) => toNumber(row.asientos)),
      line: monthlySummary.map((row) => toNumber(row.debe) + toNumber(row.haber)),
      barName: "Asientos",
      lineName: "Debe + Haber",
    }),
  );

  const accounts = sortByValueDescending(
    aggregateBy(
      accountingFacts,
      (row) => row.cuenta_nombre,
      (row) => ({ label: row.cuenta_nombre, value: toNumber(row.valor_total) }),
      (acc, row) => {
        acc.value += toNumber(row.valor_total);
      },
    ),
  ).slice(0, 12);
  setOption("accounting-accounts-chart", horizontalBarOption({ labels: accounts.map((row) => row.label), values: accounts.map((row) => row.value) }));

  const centers = sortByValueDescending(
    aggregateBy(
      accountingFacts,
      (row) => row.centro_costo_nombre,
      (row) => ({ label: row.centro_costo_nombre, value: toNumber(row.valor_total) }),
      (acc, row) => {
        acc.value += toNumber(row.valor_total);
      },
    ),
  ).slice(0, 12);
  setOption(
    "accounting-centers-chart",
    horizontalBarOption({ labels: centers.map((row) => row.label), values: centers.map((row) => row.value), color: "#8cbeb3" }),
  );

  const balanceByMonth = aggregateBy(
    accountingFacts,
    (row) => `${row.period}|${row.tipo}`,
    (row) => ({ period: row.period, tipo: row.tipo, value: toNumber(row.valor_total) }),
    (acc, row) => {
      acc.value += toNumber(row.valor_total);
    },
  );
  const months = [...new Set(balanceByMonth.map((row) => row.period))].sort();
  setOption(
    "accounting-balance-chart",
    balanceBarOption({
      labels: months.map((period) => formatDate(period)),
      debe: months.map((period) => balanceByMonth.find((row) => row.period === period && row.tipo === "D")?.value || 0),
      haber: months.map((period) => balanceByMonth.find((row) => row.period === period && row.tipo === "H")?.value || 0),
    }),
  );
}

function renderQuality() {
  const differenceRows = snapshot.quality.source_vs_core;
  setOption(
    "quality-difference-chart",
    horizontalBarOption({ labels: differenceRows.map((row) => row.resource), values: differenceRows.map((row) => row.difference), formatter: formatNumber, color: "#b94f44" }),
  );

  const fkRows = snapshot.quality.fk_health.slice(0, 12);
  setOption(
    "quality-fk-chart",
    horizontalBarOption({ labels: fkRows.map((row) => row.relation_name), values: fkRows.map((row) => row.orphan_count), formatter: formatNumber, color: "#4a9084" }),
  );

  const qualityMetrics = [
    ...snapshot.quality.placeholders.map((row) => ({
      label: `Placeholders ${row.table_name}`,
      value: formatCompact(row.placeholder_count),
      caption: "Referencias generadas para preservar integridad.",
    })),
    ...snapshot.quality.nulls_allowed.map((row) => ({
      label: row.metric,
      value: formatCompact(row.value),
      caption: "Nulos u observaciones permitidas dentro del modelo.",
    })),
  ].slice(0, 6);
  renderMetricCards(elements.qualityMetrics, qualityMetrics);
}

function renderTables() {
  const docs = filteredCommercialDocs();
  const lines = filteredProductLines();
  const movements = filteredMovements();
  const accountingFacts = filteredAccountingFacts();

  const topCustomers = sortByValueDescending(
    aggregateBy(
      docs,
      (row) => row.cliente_nombre,
      (row) => ({ cliente_nombre: row.cliente_nombre, documentos: 1, monto_total: toNumber(row.total) }),
      (acc, row) => {
        acc.documentos += 1;
        acc.monto_total += toNumber(row.total);
      },
    ),
    "monto_total",
  ).slice(0, 20);

  const topProducts = sortByValueDescending(
    aggregateBy(
      lines,
      (row) => row.producto_nombre,
      (row) => ({
        producto_nombre: row.producto_nombre,
        categoria_nombre: row.categoria_nombre,
        marca_nombre: row.marca_nombre,
        cantidad_total: toNumber(row.cantidad),
        importe_total: toNumber(row.importe),
      }),
      (acc, row) => {
        acc.cantidad_total += toNumber(row.cantidad);
        acc.importe_total += toNumber(row.importe);
      },
    ),
    "importe_total",
  ).slice(0, 20);

  const topBodegas = sortByValueDescending(
    aggregateBy(
      movements,
      (row) => row.bodega_nombre,
      (row) => ({ bodega_nombre: row.bodega_nombre, movimientos: 1, valor_total: toNumber(row.total) }),
      (acc, row) => {
        acc.movimientos += 1;
        acc.valor_total += toNumber(row.total);
      },
    ),
    "movimientos",
  ).slice(0, 20);

  const topAccounts = sortByValueDescending(
    aggregateBy(
      accountingFacts,
      (row) => row.cuenta_nombre,
      (row) => ({ cuenta_nombre: row.cuenta_nombre, lineas: toNumber(row.lineas), valor_total: toNumber(row.valor_total) }),
      (acc, row) => {
        acc.lineas += toNumber(row.lineas);
        acc.valor_total += toNumber(row.valor_total);
      },
    ),
    "valor_total",
  ).slice(0, 20);

  const customerColumns = [
    { key: "cliente_nombre", label: "Cliente" },
    { key: "documentos", label: "Documentos", formatter: formatNumber },
    { key: "monto_total", label: "Monto total", formatter: formatCurrency },
  ];
  const productColumns = [
    { key: "producto_nombre", label: "Producto" },
    { key: "categoria_nombre", label: "Categoría" },
    { key: "cantidad_total", label: "Cantidad", formatter: formatNumber },
    { key: "importe_total", label: "Importe", formatter: formatCurrency },
  ];
  const bodegaColumns = [
    { key: "bodega_nombre", label: "Bodega" },
    { key: "movimientos", label: "Movimientos", formatter: formatNumber },
    { key: "valor_total", label: "Valor total", formatter: formatCurrency },
  ];
  const accountColumns = [
    { key: "cuenta_nombre", label: "Cuenta" },
    { key: "lineas", label: "Líneas", formatter: formatNumber },
    { key: "valor_total", label: "Valor", formatter: formatCurrency },
  ];

  renderTable("top-customers-table", topCustomers, customerColumns);
  renderTable("top-products-table", topProducts, productColumns);
  renderTable("top-bodegas-table", topBodegas, bodegaColumns);
  renderTable("top-accounts-table", topAccounts, accountColumns);

  registerTableExport("top-customers", topCustomers, customerColumns);
  registerTableExport("top-products", topProducts, productColumns);
  registerTableExport("top-bodegas", topBodegas, bodegaColumns);
  registerTableExport("top-accounts", topAccounts, accountColumns);
}

function renderHeroText() {
  const anomaly = snapshot.quality.source_vs_core.find((row) => row.resource === "movimiento-inventario");
  const documents = snapshot.manifest.counts.find((row) => row.key === "documentos")?.value || 0;
  elements.heroText.textContent =
    `La lectura combina comercial, clientes, inventario, contabilidad y calidad del dato en una sola capa analítica. ` +
    `El histórico cubre desde ${snapshot.manifest.coverage_min} hasta ${snapshot.manifest.coverage_max}, con ${formatCompact(documents)} documentos y una anomalía de ${formatCompact(anomaly?.difference || 0)} filas en movimientos normalizada a IDs únicos.`;
}

function renderStaticMeta() {
  elements.heroGeneratedAt.textContent = `Actualización: ${snapshot.manifest.generated_at}`;
  elements.heroCoverage.textContent = `Cobertura: ${snapshot.manifest.coverage_min} a ${snapshot.manifest.coverage_max}`;
}

function showServerHelp() {
  elements.heroText.textContent =
    "El dashboard no puede cargarse correctamente si abres index.html directo con file://. Levántalo con servidor local y entra por http://127.0.0.1:8123.";
  elements.heroAlerts.innerHTML = `
    <article class="alert-card warning">
      <h3>Servidor requerido</h3>
      <p>Ejecuta <strong>python -m http.server 8123</strong> dentro de la carpeta <strong>dashboard</strong> y abre luego <strong>http://127.0.0.1:8123</strong>.</p>
    </article>
  `;
}

function renderAll() {
  renderActiveFilters();
  renderHeroText();
  renderOverview();
  renderCommercial();
  renderCustomersAndProducts();
  renderInventory();
  renderAccounting();
  renderQuality();
  renderTables();
}

function populateControls() {
  const filters = snapshot.manifest.filters_available;
  elements.filters.dateFrom.value = snapshot.manifest.coverage_min;
  elements.filters.dateTo.value = snapshot.manifest.coverage_max;
  state.global.dateFrom = snapshot.manifest.coverage_min;
  state.global.dateTo = snapshot.manifest.coverage_max;

  makeOptionList(elements.filters.documentType, filters.document_types, "value", "label");
  makeOptionList(elements.filters.documentState, filters.document_states, "value", "label");
  makeOptionList(elements.filters.bodega, filters.bodegas, "value", "label");
  makeOptionList(elements.filters.category, filters.categories, "value", "label");
  makeOptionList(
    elements.filters.vendor,
    sortByValueDescending(
      aggregateBy(
        snapshot.commercial.document_facts.filter((row) => row.vendedor_id),
        (row) => row.vendedor_id,
        (row) => ({ value: row.vendedor_id, label: row.vendedor_nombre, count: 1 }),
        (acc) => {
          acc.count += 1;
        },
      ),
      "count",
    ),
  );
  makeOptionList(
    elements.filters.inventoryType,
    [...new Set(snapshot.inventory.movement_facts.map((row) => row.tipo))]
      .sort()
      .map((value) => ({ value, label: value })),
  );
  makeOptionList(elements.filters.account, filters.accounts, "value", "label");
  makeOptionList(elements.filters.center, filters.cost_centers, "value", "label");
}

function bindEvents() {
  elements.filters.dateFrom.addEventListener("input", (event) => {
    state.global.dateFrom = event.target.value;
    renderAll();
  });
  elements.filters.dateTo.addEventListener("input", (event) => {
    state.global.dateTo = event.target.value;
    renderAll();
  });
  elements.filters.documentType.addEventListener("change", (event) => {
    state.global.documentType = event.target.value;
    renderAll();
  });
  elements.filters.documentState.addEventListener("change", (event) => {
    state.global.documentState = event.target.value;
    renderAll();
  });
  elements.filters.bodega.addEventListener("change", (event) => {
    state.global.bodega = event.target.value;
    renderAll();
  });
  elements.filters.category.addEventListener("change", (event) => {
    state.global.category = event.target.value;
    renderAll();
  });
  elements.filters.vendor.addEventListener("change", (event) => {
    state.local.vendor = event.target.value;
    renderAll();
  });
  elements.filters.inventoryType.addEventListener("change", (event) => {
    state.local.inventoryType = event.target.value;
    renderAll();
  });
  elements.filters.account.addEventListener("change", (event) => {
    state.local.account = event.target.value;
    renderAll();
  });
  elements.filters.center.addEventListener("change", (event) => {
    state.local.center = event.target.value;
    renderAll();
  });
  document.getElementById("reset-filters").addEventListener("click", () => {
    state.global = {
      dateFrom: snapshot.manifest.coverage_min,
      dateTo: snapshot.manifest.coverage_max,
      documentType: "",
      documentState: "",
      bodega: "",
      category: "",
    };
    state.local = {
      vendor: "",
      inventoryType: "",
      account: "",
      center: "",
    };
    Object.entries(elements.filters).forEach(([key, element]) => {
      if (key === "dateFrom") element.value = state.global.dateFrom;
      else if (key === "dateTo") element.value = state.global.dateTo;
      else element.value = "";
    });
    renderAll();
  });
  document.querySelectorAll("[data-export-table]").forEach((button) => {
    button.addEventListener("click", () => exportTable(button.dataset.exportTable));
  });
  window.addEventListener("resize", resizeCharts);
}

async function loadSnapshot() {
  const responses = await Promise.all(fileNames.map((file) => fetch(`./data/${file}`).then((response) => response.json())));
  const [manifest, overview, commercial, customers, products, inventory, accounting, quality, tables] = responses;
  return { manifest, overview, commercial, customers, products, inventory, accounting, quality, tables };
}

async function init() {
  if (window.location.protocol === "file:") {
    showServerHelp();
    return;
  }
  try {
    snapshot = await loadSnapshot();
    renderAlerts(snapshot.manifest.alerts);
    renderStaticMeta();
    populateControls();
    bindEvents();
    renderAll();
  } catch (error) {
    elements.heroText.textContent = `No fue posible cargar el snapshot del dashboard. ${error.message}`;
    showServerHelp();
    console.error(error);
  }
}

init();
