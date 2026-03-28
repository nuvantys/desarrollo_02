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
  "database.json",
  "tables.json",
];

const apiBase = `${window.location.protocol}//${window.location.hostname || "127.0.0.1"}:8130/api/technical`;

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
  ui: {
    activeTab: "technical-view",
  },
};

const technicalState = {
  data: null,
  apiAvailable: false,
  apiError: "",
  runtime: {
    current_job: null,
    last_job: null,
  },
  pollHandle: null,
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
  tabs: document.querySelectorAll("[data-tab-target]"),
  tabViews: document.querySelectorAll(".tab-view"),
  technical: {
    subtitle: document.getElementById("technical-subtitle"),
    refreshButton: document.getElementById("technical-refresh-button"),
    reloadButton: document.getElementById("technical-reload-button"),
    runtimeBadge: document.getElementById("technical-runtime-badge"),
    summaryMetrics: document.getElementById("technical-summary-metrics"),
    runtimeMessage: document.getElementById("technical-runtime-message"),
    progressBar: document.getElementById("technical-progress-bar"),
    runtimeMeta: document.getElementById("technical-runtime-meta"),
    alerts: document.getElementById("technical-alerts"),
    runsTable: document.getElementById("technical-runs-table"),
    loadTable: document.getElementById("technical-load-table"),
    fkTable: document.getElementById("technical-fk-table"),
    watermarksTable: document.getElementById("technical-watermarks-table"),
    healthMetrics: document.getElementById("technical-health-metrics"),
    storyCards: document.getElementById("technical-story-cards"),
    inventoryReview: document.getElementById("technical-inventory-review"),
    accountingReview: document.getElementById("technical-accounting-review"),
    seniorSummary: document.getElementById("technical-senior-summary"),
    sourceModes: document.getElementById("technical-source-modes"),
    detailedFindings: document.getElementById("technical-detailed-findings"),
    priorityMatrix: document.getElementById("technical-priority-matrix"),
  },
  database: {
    summaryMetrics: document.getElementById("database-summary-metrics"),
    storyCards: document.getElementById("database-story-cards"),
    schemaTable: document.getElementById("database-schema-table"),
    tablesTable: document.getElementById("database-tables-table"),
    columnTypesTable: document.getElementById("database-column-types-table"),
    relationshipsTable: document.getElementById("database-relationships-table"),
    assetsTable: document.getElementById("database-assets-table"),
    frontBackTable: document.getElementById("database-front-back-table"),
  },
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

function formatDateTime(value) {
  if (!value) return "--";
  try {
    return new Intl.DateTimeFormat("es-EC", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    }).format(new Date(value));
  } catch {
    return String(value);
  }
}

function formatDuration(seconds) {
  const total = Number(seconds || 0);
  if (!total) return "0 s";
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = total % 60;
  const parts = [];
  if (hours) parts.push(`${hours} h`);
  if (minutes) parts.push(`${minutes} min`);
  if (secs || !parts.length) parts.push(`${secs} s`);
  return parts.join(" ");
}

function formatBytes(value) {
  const bytes = Number(value || 0);
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const power = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const scaled = bytes / 1024 ** power;
  return `${scaled.toFixed(power === 0 ? 0 : 2)} ${units[power]}`;
}

function freshnessLabel(seconds) {
  const total = Number(seconds || 0);
  if (!Number.isFinite(total)) return "--";
  if (total < 60) return `${total} s`;
  if (total < 3600) return `${Math.floor(total / 60)} min`;
  if (total < 86400) return `${Math.floor(total / 3600)} h`;
  return `${Math.floor(total / 86400)} d`;
}

function stageProgress(stage, status) {
  if (status === "success") return 100;
  if (status === "error") return 100;
  const map = {
    extrayendo: 20,
    normalizando: 40,
    "cargando PostgreSQL": 68,
    "regenerando snapshot": 88,
    finalizado: 100,
  };
  return map[stage] || 8;
}

function runtimeBadgeClass(status) {
  if (status === "error") return "runtime-badge error";
  if (status === "running") return "runtime-badge warning";
  return "runtime-badge";
}

function setActiveTab(targetId) {
  state.ui.activeTab = targetId;
  elements.tabs.forEach((button) => {
    button.classList.toggle("active", button.dataset.tabTarget === targetId);
  });
  elements.tabViews.forEach((view) => {
    view.classList.toggle("active", view.id === targetId);
  });
  resizeCharts();
}

function renderTechnicalAlerts(alerts, runtime) {
  const cards = [...alerts];
  if (!technicalState.apiAvailable) {
    cards.unshift({
      level: "warning",
      title: "API tecnica no disponible",
      message:
        technicalState.apiError ||
        "Se muestra el ultimo technical.json estable. La vista analitica sigue operativa, pero el refresh bajo demanda no puede ejecutarse.",
      metric: 0,
    });
  }
  if (runtime?.status === "error" && runtime.error_text) {
    cards.unshift({
      level: "error",
      title: "Ultimo refresh con error",
      message: runtime.error_text,
      metric: 0,
    });
  }
  elements.technical.alerts.innerHTML = cards
    .map(
      (alert) => `
        <article class="technical-alert-card ${alert.level}">
          <h4>${alert.title}</h4>
          <p>${alert.message}</p>
          ${alert.metric ? `<p><strong>${formatCompact(alert.metric)}</strong></p>` : ""}
        </article>
      `,
    )
    .join("");
}

function formatSampleRow(row) {
  return Object.entries(row)
    .map(([key, value]) => `${key}: ${value ?? "--"}`)
    .join(" | ");
}

function renderReviewCards(target, cards) {
  if (!cards.length) {
    target.innerHTML = `<div class="table-empty">No se detectaron incoherencias para esta seccion.</div>`;
    return;
  }
  target.innerHTML = cards
    .map(
      (card) => `
        <article class="review-card ${card.severity}">
          <div class="review-card-header">
            <h4>${card.title}</h4>
            <span class="severity-pill ${card.severity}">${card.severity}</span>
          </div>
          <strong>${formatPreciseNumber(card.metric)}</strong>
          <p><b>Incoherencia:</b> ${card.issue}</p>
          <p><b>Impacto:</b> ${card.impact}</p>
          <p><b>Solucion o ajuste:</b> ${card.suggested_action}</p>
          ${
            (card.sample_rows || []).length
              ? `<div class="review-samples"><h5>Muestras</h5><ul>${card.sample_rows
                  .map((row) => `<li>${formatSampleRow(row)}</li>`)
                  .join("")}</ul></div>`
              : ""
          }
        </article>
      `,
    )
    .join("");
}

function renderSourceCards(target, sourceOverview) {
  const cards = [
    ...(sourceOverview.source_chain || []).map((entry) => ({
      title: entry.layer,
      body: entry.role,
    })),
    ...(sourceOverview.operating_modes || []).map((entry) => ({
      title: entry.mode,
      body: `${entry.description} Script: ${entry.script}`,
    })),
  ];
  target.innerHTML = cards
    .map(
      (card) => `
        <article class="source-card">
          <h4>${card.title}</h4>
          <p>${card.body}</p>
        </article>
      `,
    )
    .join("");
}

function renderDetailedFindings(target, cards) {
  if (!cards.length) {
    target.innerHTML = `<div class="table-empty">No se detectaron hallazgos para detallar.</div>`;
    return;
  }
  target.innerHTML = cards
    .map(
      (card) => `
        <article class="detailed-review-card ${card.severity}">
          <div class="detailed-review-card-header">
            <h4>${card.title}</h4>
            <span class="severity-pill ${card.severity}">${card.area}</span>
          </div>
          <strong>${formatPreciseNumber(card.metric)}</strong>
          <div class="detail-grid">
            <div class="detail-block">
              <h5>Incongruencia</h5>
              <p>${card.issue}</p>
            </div>
            <div class="detail-block">
              <h5>Perjuicio analitico</h5>
              <p>${card.analysis_risk || card.impact}</p>
            </div>
            <div class="detail-block">
              <h5>Riesgo para decision</h5>
              <p>${card.decision_risk || card.impact}</p>
            </div>
            <div class="detail-block">
              <h5>Solucion o ajuste</h5>
              <p>${card.suggested_action}</p>
            </div>
            <div class="detail-block">
              <h5>Enfoque positivo</h5>
              <p>${card.positive_outlook || "Corregir este punto mejora la confiabilidad del modelo y fortalece la toma de decisiones."}</p>
            </div>
            <div class="detail-block">
              <h5>Impacto resumido</h5>
              <p>${card.impact}</p>
            </div>
          </div>
          ${
            (card.sample_rows || []).length
              ? `<div class="review-samples"><h5>Muestras</h5><ul>${card.sample_rows
                  .map((row) => `<li>${formatSampleRow(row)}</li>`)
                  .join("")}</ul></div>`
              : ""
          }
        </article>
      `,
    )
    .join("");
}

function renderPriorityBadge(level, code) {
  return `<span class="priority-pill ${level || "bajo"}">${level || "bajo"}${code ? ` / ${code}` : ""}</span>`;
}

function renderAreaBadge(area) {
  return `<span class="area-pill">${String(area || "").replaceAll("_", " ")}</span>`;
}

function renderRelationBadge(value) {
  return `<span class="area-pill">${value || "--"}</span>`;
}

function yesNoLabel(value, yes = "Si", no = "No") {
  return value ? yes : no;
}

function renderDatabase() {
  const database = snapshot.database;
  if (!database) return;

  const summary = database.summary || {};
  renderMetricCards(elements.database.summaryMetrics, [
    { label: "Tamano total BD", value: formatBytes(summary.database_total_size_bytes), caption: "Peso consolidado de tablas fisicas en PostgreSQL." },
    { label: "Snapshot frontend", value: formatBytes(summary.frontend_total_size_bytes), caption: "Peso total de los JSON publicados para la interfaz web." },
    { label: "Tablas base", value: formatPreciseNumber(summary.table_count || 0), caption: "Tablas fisicas entre meta, raw, core y reporting." },
    { label: "Relaciones FK", value: formatPreciseNumber(summary.relationship_count || 0), caption: "Enlaces foraneos materializados dentro del modelo." },
    { label: "Filas backend", value: formatPreciseNumber(summary.backend_total_rows || 0), caption: "Volumen total preservado en PostgreSQL." },
    { label: "Registros frontend", value: formatPreciseNumber(summary.frontend_total_rows || 0), caption: "Puntos analiticos expuestos por el snapshot web." },
  ]);
  renderStoryCards(elements.database.storyCards, database.story_cards || []);

  const schemaRows = (database.schema_storage || []).map((row) => ({
    label: row.schema_name,
    value: row.total_size_bytes,
  }));
  setOption(
    "database-size-chart",
    horizontalBarOption({
      labels: schemaRows.map((row) => row.label),
      values: schemaRows.map((row) => row.value),
      formatter: formatBytes,
      color: "#126d84",
    }),
  );
  setOption(
    "database-relationship-chart",
    donutOption({
      data: (database.relationship_types || []).map((row) => ({
        name: row.relation_type,
        value: row.relation_count,
      })),
    }),
  );

  const densityRows = (database.relationship_density || [])
    .map((row) => ({
      ...row,
      connected: Number(row.incoming_fks || 0) + Number(row.outgoing_fks || 0),
    }))
    .sort((a, b) => b.connected - a.connected)
    .slice(0, 12);
  setOption(
    "database-density-chart",
    horizontalBarOption({
      labels: densityRows.map((row) => row.table_name.replace("core.", "").replace("meta.", "").replace("raw.", "")),
      values: densityRows.map((row) => row.connected),
      formatter: formatNumber,
      color: "#d79b41",
    }),
  );

  const frontBackRows = database.front_back_inventory || [];
  setOption(
    "database-front-back-chart",
    lineComboOption({
      categories: frontBackRows.map((row) => row.domain),
      bars: frontBackRows.map((row) => row.backend_rows),
      line: frontBackRows.map((row) => row.frontend_rows),
      barName: "Back",
      lineName: "Front",
      barFormatter: formatNumber,
      lineFormatter: formatNumber,
    }),
  );

  renderTable("database-schema-table", database.schema_storage || [], [
    { key: "schema_name", label: "Esquema" },
    { key: "table_count", label: "Tablas", formatter: formatNumber },
    { key: "view_count", label: "Vistas", formatter: formatNumber },
    { key: "total_rows", label: "Filas", formatter: formatNumber },
    { key: "total_size_bytes", label: "Tamano", formatter: formatBytes },
  ]);
  renderTable("database-tables-table", (database.table_inventory || []).slice(0, 20), [
    { key: "qualified_name", label: "Tabla" },
    { key: "row_count", label: "Filas", formatter: formatNumber },
    { key: "column_count", label: "Columnas", formatter: formatNumber },
    { key: "nullable_columns", label: "Nullable", formatter: formatNumber },
    { key: "pk_columns", label: "PK" },
    { key: "total_size_bytes", label: "Tamano total", formatter: formatBytes },
  ]);
  renderTable("database-column-types-table", database.column_types || [], [
    { key: "schema_name", label: "Esquema" },
    { key: "data_type", label: "Tipo SQL" },
    { key: "column_count", label: "Columnas", formatter: formatNumber },
  ]);
  renderTable("database-relationships-table", database.relationships || [], [
    { key: "constraint_name", label: "Constraint" },
    { key: "relation_type", label: "Tipo", formatter: renderRelationBadge },
    { key: "source_table", label: "Origen" },
    { key: "source_columns", label: "Cols origen" },
    { key: "target_table", label: "Destino" },
    { key: "target_columns", label: "Cols destino" },
    { key: "nullable_child", label: "Opcional", formatter: (value) => yesNoLabel(value, "Si", "No") },
    { key: "deferrable", label: "Deferrable", formatter: (value) => yesNoLabel(value, "Si", "No") },
    { key: "orphan_count", label: "Huerfanos", formatter: formatNumber },
  ]);
  renderTable("database-assets-table", database.frontend_assets || [], [
    { key: "file_name", label: "Archivo" },
    { key: "rows_exposed", label: "Filas expuestas", formatter: formatNumber },
    { key: "collection_count", label: "Colecciones", formatter: formatNumber },
    { key: "largest_collection", label: "Coleccion mayor" },
    { key: "largest_collection_rows", label: "Filas mayor", formatter: formatNumber },
    { key: "size_bytes", label: "Tamano", formatter: formatBytes },
  ]);
  renderTable("database-front-back-table", database.front_back_inventory || [], [
    { key: "domain", label: "Dominio" },
    { key: "backend_scope", label: "Scope backend" },
    { key: "backend_rows", label: "Filas back", formatter: formatNumber },
    { key: "frontend_file", label: "Archivo front" },
    { key: "frontend_rows", label: "Filas front", formatter: formatNumber },
  ]);
}

function renderTechnical() {
  if (!technicalState.data) return;
  const technical = technicalState.data;
  const runtime = technicalState.runtime.current_job || technicalState.runtime.last_job;
  const runtimeStatus = runtime?.status || technical.summary?.status || "success";

  elements.technical.subtitle.textContent = `Cobertura ${technical.coverage_min || "--"} a ${technical.coverage_max || "--"} con run_id ${technical.run_id || "--"}.`;
  elements.technical.runtimeBadge.className = runtimeBadgeClass(runtimeStatus);
  elements.technical.runtimeBadge.textContent = runtimeStatus === "running" ? "Actualizando" : runtimeStatus === "error" ? "Con error" : "Estable";
  elements.technical.runtimeMessage.textContent =
    runtime?.message || "No hay procesos activos. El estado expuesto corresponde al ultimo snapshot tecnico disponible.";
  elements.technical.progressBar.style.width = `${stageProgress(runtime?.stage, runtimeStatus)}%`;
  elements.technical.refreshButton.disabled = !technicalState.apiAvailable || runtimeStatus === "running";

  const summary = technical.summary || {};
  renderMetricCards(elements.technical.summaryMetrics, [
    { label: "Ultima actualizacion", value: formatDateTime(technical.generated_at), caption: "Hora efectiva del snapshot tecnico publicado." },
    { label: "Duracion ultima corrida", value: formatDuration(technical.last_refresh_duration_seconds), caption: "Tiempo total del ultimo backfill mas snapshot." },
    { label: "Filas core actualizadas", value: formatPreciseNumber(summary.core_rows_updated || 0), caption: "Suma de filas materializadas en tablas core." },
    { label: "Tablas actualizadas", value: formatPreciseNumber(summary.tables_updated || 0), caption: "Total de tablas impactadas por la ultima corrida." },
    { label: "Recursos procesados", value: formatPreciseNumber(summary.resources_processed || 0), caption: "Recursos de Contifico recorridos por el pipeline." },
    { label: "Freshness", value: freshnessLabel(technical.freshness_seconds), caption: "Tiempo transcurrido desde la ultima generacion del snapshot." },
  ]);

  const metaPills = [
    `Run ID: ${technical.run_id || "--"}`,
    `Inicio: ${formatDateTime(technical.last_refresh_started_at)}`,
    `Fin: ${formatDateTime(technical.last_refresh_finished_at)}`,
    `Exitosos: ${summary.resources_success || 0}`,
    `Fallidos: ${summary.resources_failed || 0}`,
    `Filas origen: ${formatCompact(summary.source_rows_processed || 0)}`,
  ];
  elements.technical.runtimeMeta.innerHTML = metaPills.map((item) => `<span class="technical-meta-pill">${item}</span>`).join("");
  renderTechnicalAlerts(technical.alerts || [], runtime);

  const resourceRows = sortByValueDescending(
    (technical.resource_metrics || []).map((row) => ({
      label: row.resource,
      value: toNumber(row.core_rows_loaded),
    })),
  ).slice(0, 10);
  setOption("technical-resource-chart", horizontalBarOption({ labels: resourceRows.map((row) => row.label), values: resourceRows.map((row) => row.value), formatter: formatNumber, color: "#0d6c5f" }));

  const differenceRows = sortByValueDescending(
    (technical.source_vs_core || []).map((row) => ({
      label: row.resource,
      value: Math.abs(toNumber(row.difference)),
    })),
  ).slice(0, 10);
  setOption("technical-difference-chart", horizontalBarOption({ labels: differenceRows.map((row) => row.label), values: differenceRows.map((row) => row.value), formatter: formatNumber, color: "#b94f44" }));

  renderTable("technical-runs-table", technical.recent_runs || [], [
    { key: "run_id", label: "Run ID" },
    { key: "status", label: "Estado" },
    { key: "started_at", label: "Inicio", formatter: formatDateTime },
    { key: "duration_seconds", label: "Duracion", formatter: formatDuration },
    { key: "resources_processed", label: "Recursos", formatter: formatNumber },
    { key: "source_rows", label: "Filas origen", formatter: formatNumber },
  ]);
  renderTable("technical-load-table", (technical.load_metrics || []).slice(0, 20), [
    { key: "stage", label: "Stage" },
    { key: "table_name", label: "Tabla" },
    { key: "row_count", label: "Filas", formatter: formatNumber },
    { key: "measured_at", label: "Medido", formatter: formatDateTime },
  ]);
  renderTable("technical-fk-table", technical.fk_health || [], [
    { key: "relation_name", label: "Relacion" },
    { key: "orphan_count", label: "Huerfanos", formatter: formatNumber },
  ]);
  renderTable("technical-watermarks-table", technical.watermarks || [], [
    { key: "resource", label: "Recurso" },
    { key: "min_record_date", label: "Desde" },
    { key: "max_record_date", label: "Hasta" },
    { key: "updated_at", label: "Actualizado", formatter: formatDateTime },
  ]);

  const healthMetrics = [
    ...(technical.placeholders || []).map((row) => ({
      label: `Placeholders ${row.table_name}`,
      value: formatPreciseNumber(row.placeholder_count),
      caption: "Registros auxiliares para sostener integridad referencial.",
    })),
    ...(technical.nulls_allowed || []).map((row) => ({
      label: row.metric,
      value: formatPreciseNumber(row.value),
      caption: "Nulos tolerados y hallazgos tecnicos del modelo.",
    })),
  ].slice(0, 6);
  renderMetricCards(elements.technical.healthMetrics, healthMetrics);
  renderStoryCards(
    elements.technical.storyCards,
    (technical.narrative || []).map((body, index) => ({
      title: `Hallazgo ${index + 1}`,
      body,
    })),
  );
  renderReviewCards(elements.technical.inventoryReview, technical.consistency_review?.inventory || []);
  renderReviewCards(elements.technical.accountingReview, technical.consistency_review?.accounting || []);
  renderStoryCards(elements.technical.seniorSummary, technical.source_overview?.senior_summary || []);
  renderSourceCards(elements.technical.sourceModes, technical.source_overview || { source_chain: [], operating_modes: [] });
  renderDetailedFindings(
    elements.technical.detailedFindings,
    [...(technical.consistency_review?.inventory || []), ...(technical.consistency_review?.accounting || [])],
  );
  renderTable("technical-priority-matrix", technical.priority_matrix || [], [
    { key: "correction_order", label: "Orden", formatter: formatNumber },
    { key: "priority_level", label: "Prioridad", formatter: (value, row) => renderPriorityBadge(value, row.priority_code) },
    { key: "title", label: "Hallazgo" },
    { key: "area", label: "Area", formatter: renderAreaBadge },
    { key: "metric", label: "Volumen", formatter: formatNumber },
    { key: "recommended_owner", label: "Responsable" },
    { key: "workstream", label: "Frente" },
    { key: "why_now", label: "Por que ahora" },
    { key: "success_criteria", label: "Criterio de cierre" },
  ]);
}

function showServerHelp() {
  elements.heroText.textContent =
    "El dashboard no puede cargarse correctamente si abres index.html directo con file://. Levántalo con servidor local y entra por http://127.0.0.1:8123.";
  elements.technical.subtitle.textContent =
    "El frontend requiere servidor HTTP local para leer snapshots JSON y conectarse a la API tecnica.";
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
  renderTechnical();
  renderDatabase();
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

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
}

async function loadTechnicalState() {
  try {
    const payload = await fetchJson(`${apiBase}/status`);
    technicalState.apiAvailable = true;
    technicalState.apiError = "";
    technicalState.runtime = payload.runtime || { current_job: null, last_job: null };
    technicalState.data = payload.technical;
  } catch (error) {
    technicalState.apiAvailable = false;
    technicalState.apiError = error.message;
    technicalState.runtime = { current_job: null, last_job: null };
    technicalState.data = await fetchJson("./data/technical.json");
  }
}

async function reloadTechnicalStatus(reloadAnalytics = false) {
  await loadTechnicalState();
  renderTechnical();
  if (reloadAnalytics) {
    snapshot = await loadSnapshot();
    renderAlerts(snapshot.manifest.alerts);
    renderStaticMeta();
    populateControls();
    renderAll();
  }
}

function clearPolling() {
  if (technicalState.pollHandle) {
    clearInterval(technicalState.pollHandle);
    technicalState.pollHandle = null;
  }
}

async function pollRefreshJob(jobId) {
  clearPolling();
  technicalState.pollHandle = window.setInterval(async () => {
    try {
      const payload = await fetchJson(`${apiBase}/refresh/${jobId}`);
      technicalState.runtime.current_job = payload.job?.status === "running" ? payload.job : null;
      technicalState.runtime.last_job = payload.job?.status === "running" ? technicalState.runtime.last_job : payload.job;
      renderTechnical();
      if (payload.job && payload.job.status !== "running") {
        clearPolling();
        await reloadTechnicalStatus(payload.job.status === "success");
      }
    } catch (error) {
      clearPolling();
      technicalState.apiError = error.message;
      renderTechnical();
    }
  }, 4000);
}

async function startTechnicalRefresh() {
  if (!technicalState.apiAvailable) {
    renderTechnical();
    return;
  }
  try {
    const payload = await fetchJson(`${apiBase}/refresh`, { method: "POST" });
    technicalState.runtime.current_job = payload.job;
    setActiveTab("technical-view");
    renderTechnical();
    await pollRefreshJob(payload.job.job_id);
  } catch (error) {
    technicalState.apiError = error.message;
    renderTechnical();
  }
}

async function reloadTechnicalSnapshotOnly() {
  if (technicalState.apiAvailable) {
    await fetchJson(`${apiBase}/reload`, { method: "POST" });
  }
  await reloadTechnicalStatus(true);
}

function bindEvents() {
  elements.tabs.forEach((button) => {
    button.addEventListener("click", () => setActiveTab(button.dataset.tabTarget));
  });
  elements.technical.refreshButton.addEventListener("click", startTechnicalRefresh);
  elements.technical.reloadButton.addEventListener("click", reloadTechnicalSnapshotOnly);

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
  const responses = await Promise.all(fileNames.map((file) => fetchJson(`./data/${file}`)));
  const [manifest, overview, commercial, customers, products, inventory, accounting, quality, database, tables] = responses;
  return { manifest, overview, commercial, customers, products, inventory, accounting, quality, database, tables };
}

async function init() {
  if (window.location.protocol === "file:") {
    showServerHelp();
    return;
  }
  try {
    [snapshot] = await Promise.all([loadSnapshot(), loadTechnicalState()]);
    renderAlerts(snapshot.manifest.alerts);
    renderStaticMeta();
    populateControls();
    renderTechnical();
    bindEvents();
    renderAll();
    setActiveTab("technical-view");
    if (technicalState.runtime.current_job?.job_id) {
      await pollRefreshJob(technicalState.runtime.current_job.job_id);
    }
  } catch (error) {
    elements.heroText.textContent = `No fue posible cargar el snapshot del dashboard. ${error.message}`;
    elements.technical.subtitle.textContent = `No fue posible cargar la revision tecnica. ${error.message}`;
    showServerHelp();
    console.error(error);
  }
}

init();
