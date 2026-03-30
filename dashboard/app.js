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

const appConfig = window.CONTIFICO_CONFIG || {};
const snapshotBase = (appConfig.snapshotBase || "./data").replace(/\/$/, "");
const snapshotApiUrl = appConfig.snapshotApiUrl || "";
const bootstrapApiUrl = appConfig.bootstrapApiUrl || "";
const refreshApiUrl = appConfig.refreshApiUrl || "";
const refreshStatusUrl = appConfig.refreshStatusUrl || "";
const supabaseUrl = appConfig.supabaseUrl || "";
const supabaseAnonKey = appConfig.supabaseAnonKey || "";
const simpleLoginConfig = appConfig.simpleLogin || {};
const simpleSessionKey = "contifico_dashboard_simple_login_v2";
const simpleAuthEnabled = Boolean(simpleLoginConfig.email && simpleLoginConfig.password);
const secureModeRequested = Boolean(!simpleAuthEnabled && (bootstrapApiUrl || snapshotApiUrl || refreshApiUrl || refreshStatusUrl || supabaseUrl));
const authEnabled = Boolean(
  simpleAuthEnabled ||
  ((bootstrapApiUrl || snapshotApiUrl || refreshApiUrl || refreshStatusUrl) && supabaseUrl && supabaseAnonKey && window.supabase?.createClient),
);

const analyticsFileNames = [
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

const bootstrapFallbackFiles = ["technical.json"];

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

const authState = {
  enabled: authEnabled,
  provider: simpleAuthEnabled ? "simple" : authEnabled ? "supabase" : "none",
  client: null,
  session: null,
  user: null,
  bootstrapped: false,
  bindingReady: false,
};

const dataState = {
  phase: "logged_out",
  logoutInFlight: false,
  bootstrapInFlight: null,
  analyticsInFlight: null,
  databaseInFlight: null,
  retryScheduled: false,
  slices: {
    bootstrapLoaded: false,
    analyticsLoaded: false,
    databaseLoaded: false,
  },
  requestControllers: {},
};

const tableExports = new Map();
const buttonLabels = new WeakMap();
const cacheKeys = {
  bootstrap: "contifico_dashboard_bootstrap_v3",
  analytics: "contifico_dashboard_analytics_v3",
  database: "contifico_dashboard_database_v3",
  legacySnapshot: "contifico_dashboard_snapshot_v1",
  legacyTechnical: "contifico_dashboard_technical_v1",
};

const elements = {
  appShell: document.getElementById("app-shell"),
  authShell: document.getElementById("auth-shell"),
  authForm: document.getElementById("auth-form"),
  authEmail: document.getElementById("auth-email"),
  authPassword: document.getElementById("auth-password"),
  authSubmit: document.getElementById("auth-submit"),
  authMessage: document.getElementById("auth-message"),
  authSignoutButton: document.getElementById("auth-signout-button"),
  sessionUserEmail: document.getElementById("session-user-email"),
  sessionUserStatus: document.getElementById("session-user-status"),
  heroText: document.getElementById("hero-text"),
  heroGeneratedAt: document.getElementById("hero-generated-at"),
  heroCoverage: document.getElementById("hero-coverage"),
  heroAlerts: document.getElementById("hero-alerts"),
  overviewMetrics: document.getElementById("overview-metrics"),
  storyCards: document.getElementById("story-cards"),
  qualityMetrics: document.getElementById("quality-metrics"),
  operationsMetrics: document.getElementById("operations-metrics"),
  operationsStoryCards: document.getElementById("operations-story-cards"),
  activeFilters: document.getElementById("active-filters"),
  tabs: document.querySelectorAll("[data-tab-target]"),
  tabViews: document.querySelectorAll(".tab-view"),
  technical: {
    subtitle: document.getElementById("technical-subtitle"),
    refreshQuickButton: document.getElementById("technical-refresh-quick-button"),
    refreshFullButton: document.getElementById("technical-refresh-full-button"),
    reloadButton: document.getElementById("technical-reload-button"),
    runtimeBadge: document.getElementById("technical-runtime-badge"),
    summaryMetrics: document.getElementById("technical-summary-metrics"),
    runtimeMessage: document.getElementById("technical-runtime-message"),
    progressStage: document.getElementById("technical-progress-stage"),
    progressPercent: document.getElementById("technical-progress-percent"),
    progressBar: document.getElementById("technical-progress-bar"),
    progressDetail: document.getElementById("technical-progress-detail"),
    progressSteps: document.getElementById("technical-progress-steps"),
    runtimeMeta: document.getElementById("technical-runtime-meta"),
    refreshGuide: document.getElementById("technical-refresh-guide"),
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
    performanceMetrics: document.getElementById("database-performance-metrics"),
    performanceStory: document.getElementById("database-performance-story"),
    performanceTable: document.getElementById("database-performance-table"),
    performanceRunsTable: document.getElementById("database-performance-runs-table"),
    performanceComparisonMetrics: document.getElementById("database-performance-comparison-metrics"),
    performanceComparisonStory: document.getElementById("database-performance-comparison-story"),
    performanceComparisonTable: document.getElementById("database-performance-comparison-table"),
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

function projectRefFromSupabaseUrl() {
  try {
    return new URL(supabaseUrl).hostname.split(".")[0] || "";
  } catch {
    return "";
  }
}

function readCachedJson(key) {
  try {
    const raw = window.localStorage.getItem(key);
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
}

function writeCachedJson(key, payload) {
  try {
    window.localStorage.setItem(key, JSON.stringify(payload));
  } catch {
    // Ignore storage quota or serialization errors.
  }
}

function clearCachedData() {
  try {
    [
      cacheKeys.bootstrap,
      cacheKeys.analytics,
      cacheKeys.database,
      "contifico_dashboard_bootstrap_v2",
      "contifico_dashboard_analytics_v2",
      "contifico_dashboard_database_v2",
      cacheKeys.legacySnapshot,
      cacheKeys.legacyTechnical,
    ].forEach((key) => window.localStorage.removeItem(key));
  } catch {
    // Ignore storage access errors.
  }
}

function clearSupabaseStoredSession() {
  const projectRef = projectRefFromSupabaseUrl();
  const stores = [window.localStorage, window.sessionStorage];
  for (const store of stores) {
    if (!store) continue;
    const toDelete = [];
    for (let index = 0; index < store.length; index += 1) {
      const key = store.key(index);
      if (!key) continue;
      const normalized = key.toLowerCase();
      if (
        (projectRef && normalized.includes(projectRef.toLowerCase())) ||
        normalized.includes("supabase.auth") ||
        normalized.includes("auth-token")
      ) {
        toDelete.push(key);
      }
    }
    toDelete.forEach((key) => store.removeItem(key));
  }
}

function readSimpleSession() {
  try {
    window.localStorage.removeItem("contifico_dashboard_simple_login_v1");
    const raw = window.sessionStorage.getItem(simpleSessionKey);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed?.email) return null;
    return parsed;
  } catch {
    return null;
  }
}

function writeSimpleSession(email) {
  const payload = {
    email,
    provider: "simple",
    issued_at: new Date().toISOString(),
  };
  try {
    window.sessionStorage.setItem(simpleSessionKey, JSON.stringify(payload));
  } catch {
    // Ignore storage errors in demo mode.
  }
  return payload;
}

function clearSimpleSession() {
  try {
    window.sessionStorage.removeItem(simpleSessionKey);
    window.localStorage.removeItem("contifico_dashboard_simple_login_v1");
  } catch {
    // Ignore storage errors in demo mode.
  }
}

function abortActiveRequests() {
  Object.values(dataState.requestControllers).forEach((controller) => controller?.abort?.());
  dataState.requestControllers = {};
}

function beginRequest(key) {
  dataState.requestControllers[key]?.abort?.();
  const controller = new AbortController();
  dataState.requestControllers[key] = controller;
  return controller;
}

function endRequest(key, controller) {
  if (dataState.requestControllers[key] === controller) {
    delete dataState.requestControllers[key];
  }
}

function setSessionPhase(phase) {
  dataState.phase = phase;
}

function resetUiToSignedOutState() {
  snapshot = null;
  technicalState.data = null;
  technicalState.apiError = "";
  technicalState.runtime = { current_job: null, last_job: null };
  authState.session = null;
  authState.user = null;
  authState.bootstrapped = false;
  dataState.slices = {
    bootstrapLoaded: false,
    analyticsLoaded: false,
    databaseLoaded: false,
  };
  dataState.bootstrapInFlight = null;
  dataState.analyticsInFlight = null;
  dataState.databaseInFlight = null;
  dataState.retryScheduled = false;
  setSessionPhase("logged_out");
  abortActiveRequests();
  clearPolling();
  updateSessionChrome();
  setAppVisibility(false);
  elements.authForm?.reset();
  if (authState.provider === "simple") {
    elements.authEmail.value = simpleLoginConfig.email || "";
  }
  elements.heroText.textContent = authState.provider === "simple"
    ? "Inicia sesion para abrir el snapshot publicado y explorar la analitica web."
    : "Inicia sesion para cargar el snapshot privado y habilitar el refresh cloud.";
  elements.technical.subtitle.textContent = authState.provider === "simple"
    ? "Inicia sesion para revisar el estado tecnico y la analitica del snapshot publicado."
    : "Inicia sesion para revisar el estado tecnico, la analitica y la base en Supabase.";
}

function primeAnalyticsUi() {
  if (!snapshot?.manifest) return;
  renderAlerts(snapshot.manifest.alerts);
  renderStaticMeta();
  populateControls();
  renderAnalyticsViews();
}

function primeDatabaseUi() {
  if (!snapshot?.database) return;
  renderDatabase();
}

function setAuthMessage(message, tone = "") {
  elements.authMessage.textContent = message;
  elements.authMessage.className = tone ? `auth-message ${tone}` : "auth-message";
}

function setAppVisibility(isAuthenticated) {
  elements.authShell.classList.toggle("hidden", isAuthenticated);
  elements.appShell.classList.toggle("app-shell-hidden", !isAuthenticated);
}

function setButtonBusy(button, isBusy, busyLabel = "Procesando...") {
  if (!button) return;
  if (!buttonLabels.has(button)) {
    buttonLabels.set(button, button.textContent);
  }
  button.disabled = isBusy;
  button.classList.toggle("is-busy", isBusy);
  button.textContent = isBusy ? busyLabel : buttonLabels.get(button);
}

function updateSessionChrome() {
  if (!authState.enabled) {
    elements.sessionUserEmail.textContent = "Modo sin login";
    elements.sessionUserStatus.textContent = "El dashboard esta usando el snapshot publicado del sitio.";
    return;
  }
  if (authState.provider === "simple" && !authState.user) {
    elements.sessionUserEmail.textContent = "Sesion web no iniciada";
    elements.sessionUserStatus.textContent = "Inicia sesion para abrir el dashboard publicado.";
    return;
  }
  if (authState.provider === "simple" && authState.user) {
    elements.sessionUserEmail.textContent = authState.user.email || "Usuario web";
    elements.sessionUserStatus.textContent = "Sesion web activa.";
    return;
  }
  if (!authState.user) {
    elements.sessionUserEmail.textContent = "Sesion no iniciada";
    elements.sessionUserStatus.textContent = "Inicia sesion para desbloquear snapshot y refresh cloud.";
    return;
  }
  elements.sessionUserEmail.textContent = authState.user.email || "Usuario autenticado";
  elements.sessionUserStatus.textContent = "Sesion activa con Supabase Auth.";
}

function authHeaders(extraHeaders = {}) {
  const headers = { ...extraHeaders };
  if (authState.provider !== "supabase") {
    return headers;
  }
  if (supabaseAnonKey) {
    headers.apikey = supabaseAnonKey;
  }
  if (authState.session?.access_token) {
    headers.Authorization = `Bearer ${authState.session.access_token}`;
  }
  return headers;
}

async function fetchSnapshotPayload(file, options = {}, timeoutMs = 15000) {
  if (authState.provider !== "supabase") {
    return fetchJson(`${snapshotBase}/${file}`, options, timeoutMs);
  }
  const { headers = {}, ...restOptions } = options;
  return fetchJson(`${snapshotApiUrl}?file=${encodeURIComponent(file)}`, {
    ...restOptions,
    headers: authHeaders(headers),
  }, timeoutMs);
}

async function signInWithPassword(email, password) {
  dataState.logoutInFlight = false;
  if (authState.provider === "simple") {
    const normalizedEmail = String(email || "").trim().toLowerCase();
    const expectedEmail = String(simpleLoginConfig.email || "").trim().toLowerCase();
    if (normalizedEmail !== expectedEmail || password !== simpleLoginConfig.password) {
      throw new Error("Credenciales incorrectas. Usa el correo y la contrasena configurados para este dashboard.");
    }
    const session = writeSimpleSession(simpleLoginConfig.email);
    authState.session = { access_token: "frontend-simple-login", provider: "simple" };
    authState.user = { email: session.email };
    updateSessionChrome();
    setAppVisibility(true);
    const ready = await bootstrapDashboard();
    if (!ready) {
      throw new Error("No fue posible cargar el snapshot publicado.");
    }
    if (state.ui.activeTab !== "technical-view") {
      await activateTabAndLoad(state.ui.activeTab);
    }
    return;
  }
  const { error } = await authState.client.auth.signInWithPassword({ email, password });
  if (error) {
    throw error;
  }
}

async function signOutSession() {
  if (dataState.logoutInFlight) {
    return;
  }
  dataState.logoutInFlight = true;
  const client = authState.client;
  clearPolling();
  abortActiveRequests();
  clearCachedData();
  clearSimpleSession();
  clearSupabaseStoredSession();
  resetUiToSignedOutState();
  setAuthMessage("Sesion cerrada correctamente.", "success");

  if (authState.provider !== "supabase" || !client) {
    dataState.logoutInFlight = false;
    return;
  }

  Promise.race([
      client.auth.signOut({ scope: "local" }),
      new Promise((resolve) => window.setTimeout(resolve, 1200)),
    ])
    .catch(() => {
      // Keep the local logout even if the remote invalidation takes too long or fails.
    })
    .finally(() => {
      clearSupabaseStoredSession();
      dataState.logoutInFlight = false;
    });
}

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

function normalizeCloudError(message) {
  const text = String(message || "");
  if (authState.provider === "simple") {
    return text || "No fue posible leer el snapshot publicado del dashboard.";
  }
  if (text.includes("Failed to fetch") || text.includes("NetworkError")) {
    return "No fue posible alcanzar la capa cloud. El proyecto de Supabase puede estar pausado o devolviendo un error temporal de red.";
  }
  if (text.includes("521")) {
    return "Supabase esta respondiendo con 521. El host del proyecto no esta atendiendo solicitudes en este momento.";
  }
  if (text.includes("404")) {
    return "Las funciones cloud de refresh todavia no estan desplegadas en Supabase. El dashboard sigue usando el ultimo snapshot estable.";
  }
  if (text.includes("401") || text.includes("403")) {
    return "La capa cloud respondio sin autorizacion. Revisa secrets y permisos del despliegue.";
  }
  return text || "La capa cloud no esta disponible en este momento.";
}

function normalizeAuthError(error) {
  const text = String(error?.message || error || "");
  if (authState.provider === "simple") {
    return text || "No fue posible abrir el dashboard con el login web.";
  }
  if (text.includes("Failed to fetch") || text.includes("NetworkError")) {
    return "Supabase Auth no responde en este momento. El dashboard no puede iniciar sesion hasta que el host del proyecto vuelva a estar disponible.";
  }
  return normalizeCloudError(text);
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

function filteredGuides() {
  const { from, to } = currentDateBounds();
  return (snapshot.inventory.guide_facts || []).filter((row) => {
    if (!inDateRange(row.date, from, to)) return false;
    if (state.global.bodega && row.bodega_id !== state.global.bodega) return false;
    return true;
  });
}

function filteredBankMovements() {
  const { from, to } = currentDateBounds();
  return (snapshot.accounting.bank_movement_facts || []).filter((row) => inDateRange(row.date, from, to));
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

function renderOperations() {
  const guides = filteredGuides();
  const bankMovements = filteredBankMovements();
  const linkedGuides = guides.filter((row) => row.linked_document);
  const bankTotal = bankMovements.reduce((sum, row) => sum + toNumber(row.monto_total), 0);
  const distinctBankAccounts = uniqueCount(bankMovements.map((row) => row.cuenta_bancaria_id));

  renderMetricCards(elements.operationsMetrics, [
    { label: "Guias filtradas", value: formatPreciseNumber(guides.length), caption: "Guias de remision visibles en la ventana activa." },
    { label: "Guias vinculadas", value: formatPreciseNumber(linkedGuides.length), caption: "Guias conectadas con un documento del modelo." },
    { label: "Movimientos bancarios", value: formatPreciseNumber(bankMovements.length), caption: "Eventos bancarios observados dentro del rango temporal." },
    { label: "Monto bancario", value: formatCurrency(bankTotal), caption: `${formatPreciseNumber(distinctBankAccounts)} cuentas bancarias activas en la vista.` },
  ]);

  const guideByMonth = aggregateBy(
    guides,
    (row) => monthBucket(row.date),
    (row) => ({ period: monthBucket(row.date), total_guias: 1, linked_guias: row.linked_document ? 1 : 0 }),
    (acc, row) => {
      acc.total_guias += 1;
      acc.linked_guias += row.linked_document ? 1 : 0;
    },
  ).sort((a, b) => a.period.localeCompare(b.period));

  setOption(
    "operations-guides-timeline-chart",
    lineComboOption({
      categories: guideByMonth.map((row) => formatDate(row.period)),
      bars: guideByMonth.map((row) => row.total_guias),
      line: guideByMonth.map((row) => row.linked_guias),
      barName: "Guias emitidas",
      lineName: "Guias vinculadas",
      barFormatter: formatNumber,
      lineFormatter: formatNumber,
    }),
  );

  const guideByBodega = sortByValueDescending(
    aggregateBy(
      guides,
      (row) => row.bodega_nombre,
      (row) => ({ label: row.bodega_nombre, guias: 1, cantidad_total: toNumber(row.cantidad_total) }),
      (acc, row) => {
        acc.guias += 1;
        acc.cantidad_total += toNumber(row.cantidad_total);
      },
    ),
    "guias",
  ).slice(0, 12);

  setOption(
    "operations-bodega-load-chart",
    lineComboOption({
      categories: guideByBodega.map((row) => row.label),
      bars: guideByBodega.map((row) => row.guias),
      line: guideByBodega.map((row) => row.cantidad_total),
      barName: "Guias",
      lineName: "Cantidad movilizada",
      barFormatter: formatNumber,
      lineFormatter: formatNumber,
    }),
  );

  const bankByMonthType = aggregateBy(
    bankMovements,
    (row) => `${monthBucket(row.date)}|${row.tipo_registro}`,
    (row) => ({ period: monthBucket(row.date), tipo_registro: row.tipo_registro, monto_total: toNumber(row.monto_total) }),
    (acc, row) => {
      acc.monto_total += toNumber(row.monto_total);
    },
  );
  const bankMonths = [...new Set(bankByMonthType.map((row) => row.period))].sort();
  const bankTypeLabels = [
    { code: "I", label: "Ingresos" },
    { code: "E", label: "Egresos" },
  ];
  setOption(
    "operations-bank-flow-chart",
    stackedBarOption({
      categories: bankMonths.map((period) => formatDate(period)),
      series: bankTypeLabels.map((entry) => ({
        name: entry.label,
        data: bankMonths.map(
          (period) => bankByMonthType.find((row) => row.period === period && row.tipo_registro === entry.code)?.monto_total || 0,
        ),
      })),
      formatter: formatCurrency,
    }),
  );

  const linkedShare = guides.length ? ((linkedGuides.length / guides.length) * 100).toFixed(1) : "0.0";
  const topBodega = guideByBodega[0];
  const topBankAccount = sortByValueDescending(
    aggregateBy(
      bankMovements,
      (row) => row.cuenta_bancaria_nombre,
      (row) => ({ label: row.cuenta_bancaria_nombre, value: toNumber(row.monto_total) }),
      (acc, row) => {
        acc.value += toNumber(row.monto_total);
      },
    ),
  )[0];
  renderStoryCards(elements.operationsStoryCards, [
    {
      title: "Trazabilidad logística",
      body: `${formatPreciseNumber(linkedGuides.length)} de ${formatPreciseNumber(guides.length)} guias filtradas se enlazan a un documento. Eso deja una cobertura de ${linkedShare}% para seguimiento entre salida fisica y documento comercial.`,
    },
    {
      title: "Presion por bodega",
      body: `La bodega con mayor carga en la vista activa es ${topBodega?.label || "--"}, con ${formatPreciseNumber(topBodega?.guias || 0)} guias y ${formatPreciseNumber(topBodega?.cantidad_total || 0)} unidades movilizadas.`,
    },
    {
      title: "Lectura de tesoreria",
      body: `El flujo bancario visible suma ${formatCurrency(bankTotal)} y se concentra principalmente en ${topBankAccount?.label || "--"}. Este dominio complementa cobros y contabilidad con evidencia bancaria real.`,
    },
  ]);
}

function renderAccounting() {
  const monthlySummary = filteredAccountingSummary();
  const accountingFacts = filteredAccountingFacts();
  const { from, to } = currentDateBounds();
  const reconciliationRows = (snapshot.accounting.bank_reconciliation_monthly || []).filter((row) => inDateRange(row.period, from, to));

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

  const reconciliationByMonth = aggregateBy(
    reconciliationRows,
    (row) => row.period,
    (row) => ({
      period: row.period,
      monto_cobros: toNumber(row.monto_cobros),
      ingresos_bancarios: toNumber(row.ingresos_bancarios),
    }),
    (acc, row) => {
      acc.monto_cobros += toNumber(row.monto_cobros);
      acc.ingresos_bancarios += toNumber(row.ingresos_bancarios);
    },
  ).sort((a, b) => a.period.localeCompare(b.period));
  setOption(
    "accounting-reconciliation-chart",
    lineComboOption({
      categories: reconciliationByMonth.map((row) => formatDate(row.period)),
      bars: reconciliationByMonth.map((row) => row.monto_cobros),
      line: reconciliationByMonth.map((row) => row.ingresos_bancarios),
      barName: "Cobros",
      lineName: "Ingresos bancarios",
      barFormatter: formatCurrency,
      lineFormatter: formatCurrency,
    }),
  );

  const accountGapRows = sortByValueDescending(
    (snapshot.accounting.bank_reconciliation_accounts || []).map((row) => ({
      label: row.cuenta_bancaria_nombre,
      value: toNumber(row.brecha_absoluta),
    })),
  ).slice(0, 12);
  setOption(
    "accounting-bank-gap-chart",
    horizontalBarOption({
      labels: accountGapRows.map((row) => row.label),
      values: accountGapRows.map((row) => row.value),
      formatter: formatCurrency,
      color: "#b94f44",
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
    queued: 6,
    setup: 18,
    dependencies: 34,
    sync: 72,
    publish: 92,
    extrayendo: 20,
    normalizando: 40,
    "cargando PostgreSQL": 68,
    "regenerando snapshot": 88,
    finalizado: 100,
  };
  return map[stage] || 8;
}

function elapsedSecondsSince(timestamp) {
  if (!timestamp) return 0;
  const value = Date.parse(timestamp);
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.round((Date.now() - value) / 1000));
}

function runtimeStageLabel(runtime, status) {
  if (runtime?.stage === "finalizado" || status === "success") return "Finalizado";
  if (runtime?.stage === "error" || status === "error") return "Con error";
  if (runtime?.active_step) return runtime.active_step;
  if (runtime?.stage_detail) return runtime.stage_detail;
  return "Esperando estado";
}

function inferProgressPercent(runtime, status, fallbackDurationSeconds = 0) {
  if (runtime?.progress_percent != null) return Number(runtime.progress_percent);
  if (status === "success" || runtime?.stage === "finalizado") return 100;
  if (status === "error" || runtime?.stage === "error") return 100;
  const explicit = stageProgress(runtime?.stage, status);
  if (runtime?.started_at && fallbackDurationSeconds > 0) {
    const elapsed = elapsedSecondsSince(runtime.started_at);
    const ratio = Math.min(0.96, elapsed / Math.max(fallbackDurationSeconds, 1));
    return Math.max(explicit, Math.round(ratio * 100));
  }
  return explicit;
}

function inferProgressSteps(runtime, status) {
  if (Array.isArray(runtime?.steps) && runtime.steps.length) {
    return runtime.steps;
  }
  const steps = [
    { key: "queued", label: "En cola", status: "completed" },
    { key: "setup", label: "Preparando entorno", status: "pending" },
    { key: "sync", label: "Actualizando Supabase", status: "pending" },
    { key: "publish", label: "Publicando snapshot", status: "pending" },
    { key: "final", label: "Finalizado", status: "pending" },
  ];
  const current = runtime?.stage || (status === "success" ? "finalizado" : status === "error" ? "error" : "queued");
  const indexMap = { queued: 0, running: 1, setup: 1, dependencies: 1, sync: 2, publish: 3, finalizado: 4, error: 4 };
  const activeIndex = indexMap[current] ?? 1;
  steps.forEach((step, index) => {
    if (status === "success") {
      step.status = "completed";
    } else if (status === "error" && index === steps.length - 1) {
      step.status = "error";
    } else if (index < activeIndex) {
      step.status = "completed";
    } else if (index === activeIndex && status === "running") {
      step.status = "active";
    }
  });
  return steps;
}

function renderProgressSteps(steps = []) {
  if (!steps.length) {
    elements.technical.progressSteps.innerHTML = "";
    return;
  }
  elements.technical.progressSteps.innerHTML = steps
    .map(
      (step, index) => `
        <article class="progress-step ${step.status || "pending"}">
          <span class="step-index">${index + 1}</span>
          <strong>${step.label}</strong>
          <span>${
            step.status === "completed"
              ? "Completado"
              : step.status === "active"
                ? "En curso"
                : step.status === "error"
                  ? "Con error"
                  : "Pendiente"
          }</span>
        </article>
      `,
    )
    .join("");
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
        normalizeCloudError(technicalState.apiError) ||
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

function runModeBadge(value) {
  const normalized = String(value || "").toLowerCase();
  if (normalized === "backfill" || normalized === "completo") {
    return `<span class="area-pill">Completo</span>`;
  }
  if (normalized === "refresh" || normalized === "rapido" || normalized === "rápido") {
    return `<span class="area-pill">Rapido</span>`;
  }
  return value || "--";
}

function formatSeconds(value) {
  return `${formatNumber(value)} s`;
}

function formatSignedNumber(value, suffix = "") {
  const number = Number(value || 0);
  const sign = number > 0 ? "+" : "";
  return `${sign}${formatNumber(number)}${suffix}`;
}

function comparisonBadge(status) {
  const label = status === "mejora" ? "mejora" : status === "regresion" ? "regresion" : "sin cambio";
  return `<span class="priority-pill ${status === "mejora" ? "bajo" : status === "regresion" ? "alto" : "medio"}">${label}</span>`;
}

function renderDatabase() {
  const database = snapshot.database;
  if (!database) return;

  const summary = database.summary || {};
  renderMetricCards(elements.database.summaryMetrics, [
    { label: "Tamano total BD", value: formatBytes(summary.database_total_size_bytes), caption: "Peso consolidado de tablas fisicas en Supabase Postgres." },
    { label: "Snapshot frontend", value: formatBytes(summary.frontend_total_size_bytes), caption: "Peso total de los JSON publicados para la interfaz web." },
    { label: "Tablas base", value: formatPreciseNumber(summary.table_count || 0), caption: "Tablas fisicas entre meta, raw, core y reporting." },
    { label: "Relaciones FK", value: formatPreciseNumber(summary.relationship_count || 0), caption: "Enlaces foraneos materializados dentro del modelo." },
    { label: "Filas backend", value: formatPreciseNumber(summary.backend_total_rows || 0), caption: "Volumen total preservado en Supabase." },
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

  const performanceSummary = database.performance_summary || {};
  renderMetricCards(elements.database.performanceMetrics, [
    { label: "Duracion ultimo refresh", value: formatDuration(performanceSummary.latest_total_duration_seconds), caption: "Tiempo total del ultimo refresh exitoso analizado." },
    { label: "Recurso mas lento", value: performanceSummary.slowest_resource || "--", caption: `Con ${formatSeconds(performanceSummary.slowest_duration_seconds || 0)} de costo total.` },
    { label: "Mayor paginacion", value: performanceSummary.highest_pages_resource || "--", caption: `${formatNumber(performanceSummary.highest_pages_fetched || 0)} paginas recorridas.` },
    { label: "Mayor fanout", value: performanceSummary.highest_fanout_resource || "--", caption: `${formatPreciseNumber(performanceSummary.highest_fanout_ratio || 0)} filas core por fila fuente.` },
    { label: "Filas fuente", value: formatPreciseNumber(performanceSummary.latest_total_source_rows || 0), caption: "Volumen fuente recuperado en el ultimo run." },
    { label: "Filas core", value: formatPreciseNumber(performanceSummary.latest_total_core_rows || 0), caption: "Filas materializadas tras normalizacion y carga." },
  ]);
  renderStoryCards(elements.database.performanceStory, database.performance_story_cards || []);

  const slowResources = (database.performance_resources || []).slice(0, 10);
  setOption(
    "database-recovery-duration-chart",
    horizontalBarOption({
      labels: slowResources.map((row) => row.resource),
      values: slowResources.map((row) => row.latest_duration_seconds),
      formatter: formatSeconds,
      color: "#b94f44",
    }),
  );
  setOption(
    "database-recovery-driver-chart",
    lineComboOption({
      categories: slowResources.map((row) => row.resource),
      bars: slowResources.map((row) => row.latest_pages_fetched),
      line: slowResources.map((row) => row.fanout_ratio),
      barName: "Paginas",
      lineName: "Fanout",
      barFormatter: formatNumber,
      lineFormatter: formatPreciseNumber,
    }),
  );

  renderTable("database-performance-table", database.performance_resources || [], [
    { key: "resource", label: "Recurso" },
    { key: "latest_duration_seconds", label: "Ultimo tiempo", formatter: formatDuration },
    { key: "avg_duration_seconds", label: "Promedio", formatter: formatSeconds },
    { key: "latest_pages_fetched", label: "Paginas", formatter: formatNumber },
    { key: "latest_source_count", label: "Filas fuente", formatter: formatNumber },
    { key: "latest_core_rows_loaded", label: "Filas core", formatter: formatNumber },
    { key: "fanout_ratio", label: "Fanout", formatter: formatPreciseNumber },
    { key: "source_rows_per_second", label: "Fuente/s", formatter: formatPreciseNumber },
    { key: "core_rows_per_second", label: "Core/s", formatter: formatPreciseNumber },
    { key: "reason", label: "Por que tarda" },
    { key: "optimization_hint", label: "Enfoque tecnico" },
  ]);
  renderTable("database-performance-runs-table", database.performance_runs || [], [
    { key: "run_id", label: "Run ID" },
    { key: "resources_processed", label: "Recursos", formatter: formatNumber },
    { key: "total_duration_seconds", label: "Duracion total", formatter: formatDuration },
    { key: "total_pages_fetched", label: "Paginas", formatter: formatNumber },
    { key: "total_source_rows", label: "Filas fuente", formatter: formatNumber },
    { key: "total_core_rows", label: "Filas core", formatter: formatNumber },
    { key: "slowest_resource", label: "Mas lento" },
    { key: "slowest_duration_seconds", label: "Tiempo mas lento", formatter: formatDuration },
  ]);

  const comparison = database.performance_comparison;
  if (comparison) {
    renderMetricCards(elements.database.performanceComparisonMetrics, [
      { label: "Ultimo run", value: comparison.latest_run_id, caption: `Duracion ${formatDuration(comparison.latest_total_duration_seconds)}.` },
      { label: "Run anterior", value: comparison.previous_run_id, caption: `Duracion ${formatDuration(comparison.previous_total_duration_seconds)}.` },
      { label: "Delta total", value: formatSignedNumber(comparison.total_delta_seconds, " s"), caption: `${formatSignedNumber(comparison.total_delta_pct, "%")} frente al run anterior.` },
      { label: "Recursos con mejora", value: formatPreciseNumber(comparison.improved_resources || 0), caption: "Recursos que bajaron su tiempo en la ultima corrida." },
      { label: "Recursos con regresion", value: formatPreciseNumber(comparison.regressed_resources || 0), caption: "Recursos que subieron su tiempo en la ultima corrida." },
      { label: "Cambio en el mas lento", value: `${comparison.previous_slowest_resource || "--"} -> ${comparison.latest_slowest_resource || "--"}`, caption: "Comparacion del principal cuello de botella entre corridas." },
    ]);
    renderStoryCards(elements.database.performanceComparisonStory, database.performance_comparison_story_cards || []);
    renderTable("database-performance-comparison-table", database.performance_resource_comparison || [], [
      { key: "resource", label: "Recurso" },
      { key: "status", label: "Estado", formatter: comparisonBadge },
      { key: "previous_duration_seconds", label: "Antes", formatter: formatSeconds },
      { key: "latest_duration_seconds", label: "Despues", formatter: formatSeconds },
      { key: "delta_seconds", label: "Delta", formatter: (value) => formatSignedNumber(value, " s") },
      { key: "delta_pct", label: "Delta %", formatter: (value) => formatSignedNumber(value, "%") },
      { key: "same_volume", label: "Mismo volumen", formatter: (value) => yesNoLabel(value, "Si", "No") },
      { key: "explanation", label: "Lectura tecnica" },
    ]);
  } else {
    elements.database.performanceComparisonMetrics.innerHTML = `<div class="table-empty">Aun no hay dos corridas exitosas comparables para mostrar ahorro o regresion.</div>`;
    elements.database.performanceComparisonStory.innerHTML = "";
    elements.database.performanceComparisonTable.innerHTML = `<div class="table-empty">Ejecuta al menos dos corridas exitosas para habilitar el comparativo antes vs despues.</div>`;
  }
}

function technicalPhaseSnapshot() {
  const phase = dataState.phase;
  const apiMessage = technicalState.apiError ? normalizeCloudError(technicalState.apiError) : "";
  const baseSteps = [
    { label: "Validando sesion", status: "pending" },
    { label: "Leyendo snapshot", status: "pending" },
    { label: "Consultando estado del refresh", status: "pending" },
    { label: "Listo", status: "pending" },
  ];

  if (phase === "logging_out" || dataState.logoutInFlight) {
    baseSteps[0].status = "completed";
    return {
      badgeLabel: "Cerrando",
      badgeClass: "runtime-badge warning",
      subtitle: authState.provider === "simple"
        ? "Cerrando sesion web y limpiando el snapshot cacheado del navegador."
        : "Cerrando sesion y limpiando el snapshot privado del navegador.",
      message: authState.provider === "simple"
        ? "El dashboard vuelve al login del sitio sin conservar la sesion de esta pestana."
        : "El dashboard vuelve al login sin esperar a que Supabase complete la invalidacion remota.",
      stage: "Cerrando sesion",
      percent: 96,
      detail: authState.provider === "simple"
        ? "Limpiando cache del navegador, polling activo y estado efimero del login web."
        : "Limpiando cache local, polling activo y storage asociado a la sesion de Supabase.",
      steps: baseSteps,
      alerts: [],
    };
  }

  if (!authState.session) {
    return {
      badgeLabel: "Bloqueado",
      badgeClass: "runtime-badge",
      subtitle: authState.provider === "simple"
        ? "Inicia sesion para abrir el sitio y revisar el snapshot publicado."
        : "Inicia sesion para revisar el estado tecnico, la analitica y la base en Supabase.",
      message: authState.provider === "simple"
        ? "El login simple solo abre la app. La lectura sale del snapshot publicado junto al sitio."
        : "El snapshot privado y el refresh cloud solo se habilitan con una sesion valida.",
      stage: "Esperando autenticacion",
      percent: 0,
      detail: authState.provider === "simple"
        ? "Todavia no hay una sesion activa para hidratar la vista web."
        : "Todavia no hay una sesion valida para consultar el bootstrap cloud.",
      steps: baseSteps,
      alerts: [],
    };
  }

  if (phase === "authenticating") {
    baseSteps[0].status = "active";
    return {
      badgeLabel: "Validando",
      badgeClass: "runtime-badge warning",
      subtitle: authState.provider === "simple"
        ? "Validando credenciales simples antes de abrir el dashboard publicado."
        : "Validando sesion en Supabase antes de abrir el snapshot privado.",
      message: authState.provider === "simple"
        ? "Comprobando el acceso simple configurado para este sitio."
        : "Comprobando credenciales y restaurando el contexto seguro del dashboard.",
      stage: "Validando sesion",
      percent: 12,
      detail: authState.provider === "simple"
        ? "Se esta validando la clave del sitio y preparando el arranque del snapshot."
        : "Se esta verificando el token activo y preparando el arranque del dashboard.",
      steps: baseSteps,
      alerts: [],
    };
  }

  if (phase === "bootstrapping") {
    baseSteps[0].status = "completed";
    baseSteps[1].status = "active";
    return {
      badgeLabel: "Cargando",
      badgeClass: "runtime-badge warning",
      subtitle: authState.provider === "simple"
        ? "Cargando el snapshot tecnico publicado y restaurando cache valida."
        : "Cargando estado tecnico del snapshot y de la base maestra en Supabase.",
      message: authState.provider === "simple"
        ? "Leyendo los JSON publicados del dashboard y preparando el shell tecnico."
        : "Leyendo bootstrap cloud, restaurando cache valida y preparando el shell tecnico.",
      stage: "Leyendo snapshot",
      percent: 42,
      detail: authState.provider === "simple"
        ? "El dashboard hidrata primero la vista tecnica desde el snapshot publicado y luego carga las demas pestanas bajo demanda."
        : "El dashboard primero intenta hidratar el shell tecnico; si el endpoint tarda, usa cache local o deja un estado degradado visible.",
      steps: baseSteps,
      alerts: apiMessage
        ? [
            {
              level: "warning",
              title: "Intentando recuperar el bootstrap",
              message: apiMessage,
            },
          ]
        : [],
    };
  }

  if (phase === "degraded") {
    baseSteps[0].status = "completed";
    baseSteps[1].status = "error";
    return {
      badgeLabel: "Degradado",
      badgeClass: "runtime-badge error",
      subtitle: authState.provider === "simple"
        ? "No fue posible completar la lectura del snapshot publicado en el tiempo esperado."
        : "No fue posible completar el bootstrap cloud con el tiempo esperado.",
      message: authState.provider === "simple"
        ? "El dashboard mantiene el ultimo estado valido disponible o queda listo para reintentar la lectura del snapshot publicado del sitio."
        : "El dashboard mantiene el ultimo estado valido disponible o queda listo para reintentar sin bloquear la sesion.",
      stage: "Bootstrap degradado",
      percent: 100,
      detail: apiMessage || (
        authState.provider === "simple"
          ? "La lectura del snapshot publicado no devolvio respuesta a tiempo. Usa Recargar estado para reintentar."
          : "El endpoint cloud no devolvio respuesta a tiempo. Usa Recargar estado para reintentar el bootstrap."
      ),
      steps: baseSteps.map((step, index) => ({
        ...step,
        status: index === 1 ? "error" : index === 0 ? "completed" : "pending",
      })),
      alerts: [
        {
          level: "error",
          title: "Bootstrap incompleto",
          message: apiMessage || (
            authState.provider === "simple"
              ? "No fue posible completar la carga inicial desde el snapshot publicado."
              : "No fue posible completar la carga inicial desde la nube."
          ),
        },
      ],
    };
  }

  baseSteps.forEach((step) => {
    step.status = "completed";
  });
  return {
    badgeLabel: "Listo",
      badgeClass: "runtime-badge",
      subtitle: authState.provider === "simple"
      ? "Snapshot tecnico listo en modo web."
      : "Bootstrap tecnico listo.",
      message: authState.provider === "simple"
      ? "El shell tecnico ya esta hidratado desde JSON publicados y las pestanas pesadas cargan bajo demanda."
      : "El shell tecnico ya tiene contexto suficiente para pintar datos o pedir cargas diferidas por pestana.",
    stage: "Listo",
    percent: 100,
      detail: authState.provider === "simple"
      ? "La sesion del sitio y el bootstrap basico ya estan disponibles."
      : "La sesion y el bootstrap basico ya estan disponibles.",
    steps: baseSteps,
    alerts: [],
  };
}

function renderTechnicalLoadingState() {
  const phaseState = technicalPhaseSnapshot();
  const canDispatchRefresh = Boolean(authState.session && !dataState.logoutInFlight && refreshApiUrl);
  elements.technical.subtitle.textContent = phaseState.subtitle;
  elements.technical.runtimeBadge.className = phaseState.badgeClass;
  elements.technical.runtimeBadge.textContent = phaseState.badgeLabel;
  elements.technical.runtimeMessage.textContent = phaseState.message;
  elements.technical.progressBar.style.width = `${phaseState.percent}%`;
  elements.technical.progressStage.textContent = phaseState.stage;
  elements.technical.progressPercent.textContent = `${phaseState.percent}%`;
  elements.technical.progressDetail.textContent = phaseState.detail;
  renderProgressSteps(phaseState.steps);
  elements.technical.refreshQuickButton.disabled = !canDispatchRefresh;
  elements.technical.refreshFullButton.disabled = !canDispatchRefresh;
  elements.technical.reloadButton.disabled = !authState.session || dataState.logoutInFlight;
  renderMetricCards(elements.technical.summaryMetrics, [
    { label: "Ultima actualizacion", value: "--", caption: "Se completara cuando el bootstrap tecnico llegue desde la nube." },
    { label: "Duracion ultima corrida", value: "--", caption: "Se mostrara cuando exista un snapshot tecnico disponible." },
    { label: "Modo ultima corrida", value: "--", caption: "Pendiente de lectura del estado tecnico." },
    { label: "Filas leidas en esta corrida", value: "--", caption: "Pendiente de lectura del bootstrap." },
    { label: "Historico core almacenado", value: "--", caption: "Pendiente de lectura del bootstrap." },
    { label: "Filas core actualizadas", value: "--", caption: "Pendiente de lectura del bootstrap." },
    { label: "Tablas actualizadas", value: "--", caption: "Pendiente de lectura del bootstrap." },
    { label: "Recursos procesados", value: "--", caption: "Pendiente de lectura del bootstrap." },
    { label: "Freshness", value: "--", caption: "Pendiente de lectura del bootstrap." },
  ]);
  const meta = [
    `Fase: ${phaseState.stage}`,
    `Sesion: ${authState.user?.email || "sin iniciar"}`,
    `Cache bootstrap: ${readCachedJson(cacheKeys.bootstrap) ? "disponible" : "vacia"}`,
    `Cache analitica: ${readCachedJson(cacheKeys.analytics) ? "disponible" : "vacia"}`,
  ];
  elements.technical.runtimeMeta.innerHTML = meta.map((item) => `<span class="technical-meta-pill">${item}</span>`).join("");
  renderTechnicalAlerts(phaseState.alerts, null);
  elements.technical.refreshGuide.innerHTML = `<div class="table-empty">El shell tecnico se esta preparando. Cuando el bootstrap termine, aqui se mostraran recomendaciones y narrativa tecnica.</div>`;
  elements.technical.healthMetrics.innerHTML = `<div class="table-empty">Esperando el snapshot tecnico para calcular salud de datos.</div>`;
  elements.technical.storyCards.innerHTML = `<div class="table-empty">Sin narrativa tecnica todavia.</div>`;
  elements.technical.inventoryReview.innerHTML = `<div class="table-empty">Sin hallazgos de inventario todavia.</div>`;
  elements.technical.accountingReview.innerHTML = `<div class="table-empty">Sin hallazgos de cuenta contable todavia.</div>`;
  elements.technical.seniorSummary.innerHTML = `<div class="table-empty">Sin resumen senior todavia.</div>`;
  elements.technical.sourceModes.innerHTML = `<div class="table-empty">Sin cadena de origen todavia.</div>`;
  elements.technical.detailedFindings.innerHTML = `<div class="table-empty">Sin detalles de inconsistencia todavia.</div>`;
  elements.technical.runsTable.innerHTML = `<div class="table-empty">Sin corridas visibles todavia.</div>`;
  elements.technical.loadTable.innerHTML = `<div class="table-empty">Sin metricas de carga todavia.</div>`;
  elements.technical.fkTable.innerHTML = `<div class="table-empty">Sin FK health todavia.</div>`;
  elements.technical.watermarksTable.innerHTML = `<div class="table-empty">Sin cobertura temporal todavia.</div>`;
  elements.technical.priorityMatrix.innerHTML = `<div class="table-empty">Sin matriz de prioridad todavia.</div>`;
}

function renderTechnical() {
  if (!technicalState.data) {
    renderTechnicalLoadingState();
    return;
  }
  const technical = technicalState.data;
  const runtime = technicalState.runtime.current_job || technicalState.runtime.last_job;
  const runtimeStatus = runtime?.status || technical.summary?.status || "success";
  const summary = technical.summary || {};
  const runtimeScopeLabel = runtime?.scope_label || summary.run_mode_label || "Modo no disponible";

  const canDispatchRefresh = Boolean(refreshApiUrl && authState.session && !dataState.logoutInFlight);
  elements.technical.subtitle.textContent = `Cobertura ${technical.coverage_min || "--"} a ${technical.coverage_max || "--"} con run_id ${technical.run_id || "--"} y ultimo modo ${summary.run_mode_label || "--"}.`;
  elements.technical.runtimeBadge.className = runtimeBadgeClass(runtimeStatus);
  elements.technical.runtimeBadge.textContent = runtimeStatus === "running" ? "Actualizando" : runtimeStatus === "error" ? "Con error" : "Estable";
  elements.technical.runtimeMessage.textContent =
    runtime?.message || "No hay procesos activos. El estado expuesto corresponde al ultimo snapshot tecnico disponible.";
  const progressPercent = inferProgressPercent(runtime, runtimeStatus, technical.last_refresh_duration_seconds);
  const progressSteps = inferProgressSteps(runtime, runtimeStatus);
  elements.technical.progressBar.style.width = `${progressPercent}%`;
  elements.technical.progressStage.textContent = runtimeStageLabel(runtime, runtimeStatus);
  elements.technical.progressPercent.textContent = runtime?.progress_label || `${Math.round(progressPercent)}%`;
  elements.technical.progressDetail.textContent =
    runtime?.stage_detail ||
    (runtimeStatus === "running"
      ? "El workflow esta en curso; el porcentaje se calcula con etapas reales si estan disponibles y, como respaldo, con el tiempo transcurrido frente a la ultima corrida conocida."
      : "No hay una corrida activa en este momento.");
  renderProgressSteps(progressSteps);
  elements.technical.reloadButton.disabled = !authState.session || dataState.logoutInFlight;
  elements.technical.refreshQuickButton.disabled = !canDispatchRefresh || runtimeStatus === "running";
  elements.technical.refreshFullButton.disabled = !canDispatchRefresh || runtimeStatus === "running";
  renderMetricCards(elements.technical.summaryMetrics, [
    { label: "Ultima actualizacion", value: formatDateTime(technical.generated_at), caption: "Hora efectiva del snapshot tecnico publicado." },
    { label: "Duracion ultima corrida", value: formatDuration(technical.last_refresh_duration_seconds), caption: "Tiempo total de la ultima actualizacion tecnica mas snapshot." },
    { label: "Modo ultima corrida", value: summary.run_mode_label || "--", caption: summary.read_scope_note || "Sin descripcion del alcance de la corrida." },
    { label: "Filas leidas en esta corrida", value: formatPreciseNumber(summary.source_rows_processed || 0), caption: "Filas recuperadas desde la API en esta ejecucion; puede bajar si el modo deja de releer todo el historico." },
    { label: "Historico core almacenado", value: formatPreciseNumber(summary.historical_core_rows_stored || 0), caption: "Filas que siguen persistidas en Supabase y disponibles para analitica aunque la corrida actual lea menos." },
    { label: "Filas core actualizadas", value: formatPreciseNumber(summary.core_rows_updated || 0), caption: "Suma de filas materializadas o reconciliadas en tablas core por esta corrida." },
    { label: "Tablas actualizadas", value: formatPreciseNumber(summary.tables_updated || 0), caption: "Total de tablas impactadas por la ultima corrida." },
    { label: "Recursos procesados", value: formatPreciseNumber(summary.resources_processed || 0), caption: "Recursos de Contifico recorridos por el pipeline." },
    { label: "Freshness", value: freshnessLabel(technical.freshness_seconds), caption: "Tiempo transcurrido desde la ultima generacion del snapshot." },
  ]);

  const metaPills = [
    `Run ID: ${technical.run_id || "--"}`,
    `Modo: ${runtimeScopeLabel}`,
    `Inicio: ${formatDateTime(technical.last_refresh_started_at)}`,
    `Fin: ${formatDateTime(technical.last_refresh_finished_at)}`,
    `Progreso: ${runtime?.progress_label || `${Math.round(progressPercent)}%`}`,
    `Exitosos: ${summary.resources_success || 0}`,
    `Fallidos: ${summary.resources_failed || 0}`,
    `Filas leidas: ${formatCompact(summary.source_rows_processed || 0)}`,
    `Historico core: ${formatCompact(summary.historical_core_rows_stored || 0)}`,
  ];
  elements.technical.runtimeMeta.innerHTML = metaPills.map((item) => `<span class="technical-meta-pill">${item}</span>`).join("");
  renderTechnicalAlerts(technical.alerts || [], runtime);
  renderStoryCards(elements.technical.refreshGuide, technical.refresh_guidance || []);

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
    { key: "run_mode", label: "Modo", formatter: runModeBadge },
    { key: "status", label: "Estado" },
    { key: "started_at", label: "Inicio", formatter: formatDateTime },
    { key: "duration_seconds", label: "Duracion", formatter: formatDuration },
    { key: "resources_processed", label: "Recursos", formatter: formatNumber },
    { key: "source_rows", label: "Filas leidas", formatter: formatNumber },
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
  showHostingHelp();
}

function showHostingHelp() {
  elements.heroText.textContent =
    "El dashboard no puede cargarse correctamente si abres index.html directo con file://. Publicalo en un hosting estatico o sirvelo por HTTP.";
  elements.technical.subtitle.textContent =
    "El frontend requiere HTTP para leer snapshots JSON y conectarse a la capa cloud de refresh.";
  elements.heroAlerts.innerHTML = `
    <article class="alert-card warning">
      <h3>Hosting requerido</h3>
      <p>${appConfig.hostedHint || "Publica esta carpeta en GitHub Pages, Netlify, Vercel o cualquier hosting estatico. Evita abrir index.html con file://."}</p>
    </article>
  `;
}

function hasAnalyticsData() {
  return Boolean(
    snapshot?.manifest &&
    snapshot?.overview &&
    snapshot?.commercial &&
    snapshot?.customers &&
    snapshot?.products &&
    snapshot?.inventory &&
    snapshot?.accounting &&
    snapshot?.quality &&
    snapshot?.tables,
  );
}

function hasDatabaseData() {
  return Boolean(snapshot?.database);
}

function renderAnalyticsViews() {
  if (!hasAnalyticsData()) return;
  renderActiveFilters();
  renderHeroText();
  renderOverview();
  renderCommercial();
  renderCustomersAndProducts();
  renderInventory();
  renderOperations();
  renderAccounting();
  renderQuality();
  renderTables();
}

function renderAll() {
  renderTechnical();
  renderAnalyticsViews();
  if (hasDatabaseData()) {
    renderDatabase();
  }
}

function populateControls() {
  if (!hasAnalyticsData()) return;
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

async function fetchJson(url, options = {}, timeoutMs = 15000) {
  const timeoutController = new AbortController();
  const timeoutHandle = window.setTimeout(() => timeoutController.abort(), timeoutMs);
  if (options.signal) {
    if (options.signal.aborted) {
      timeoutController.abort();
    } else {
      options.signal.addEventListener("abort", () => timeoutController.abort(), { once: true });
    }
  }
  let response;
  try {
    response = await fetch(url, { ...options, signal: timeoutController.signal });
  } catch (error) {
    window.clearTimeout(timeoutHandle);
    if (error?.name === "AbortError") {
      throw new Error("La solicitud excedio el tiempo esperado.");
    }
    throw error;
  }
  window.clearTimeout(timeoutHandle);
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    let message = text;
    try {
      const parsed = JSON.parse(text);
      message = parsed.message || parsed.error || text;
    } catch {
      // Keep raw response text when the body is not JSON.
    }
    throw new Error(message || `${response.status} ${response.statusText}`);
  }
  return response.json();
}

function clearPolling() {
  if (technicalState.pollHandle) {
    clearInterval(technicalState.pollHandle);
    technicalState.pollHandle = null;
  }
}

async function fetchRuntimeState(signal, silent = false, timeoutMs = 12000) {
  if (!refreshStatusUrl) {
    technicalState.apiAvailable = false;
    technicalState.apiError = "No hay endpoint cloud configurado para refresh.";
    return { current_job: null, last_job: null };
  }
  try {
    const payload = await fetchJson(
      refreshStatusUrl,
      {
        headers: authHeaders(),
        signal,
      },
      timeoutMs,
    );
    technicalState.apiAvailable = true;
    technicalState.apiError = "";
    return payload.runtime || { current_job: null, last_job: null };
  } catch (error) {
    technicalState.apiAvailable = false;
    technicalState.apiError = normalizeCloudError(error.message);
    if (!silent) {
      throw error;
    }
    return { current_job: null, last_job: null };
  }
}

function applyBootstrapPayload(payload) {
  if (!payload) return;
  if (payload.technical) {
    technicalState.data = payload.technical;
  }
  if (payload.runtime) {
    technicalState.runtime = payload.runtime;
  }
  if (payload.manifest || payload.overview) {
    snapshot = {
      ...(snapshot || {}),
      ...(payload.manifest ? { manifest: payload.manifest } : {}),
      ...(payload.overview ? { overview: payload.overview } : {}),
    };
  }
  dataState.slices.bootstrapLoaded = Boolean(technicalState.data);
}

function scheduleBootstrapRetry() {
  if (dataState.retryScheduled || !authState.session) return;
  dataState.retryScheduled = true;
  window.setTimeout(async () => {
    dataState.retryScheduled = false;
    if (!authState.session || dataState.logoutInFlight) return;
    try {
      await loadBootstrapState({ preferCache: false, silent: true });
    } catch {
      // Keep the last valid bootstrap if the retry fails.
    }
  }, 1600);
}

async function fetchBootstrapPayload(signal) {
  if (authState.provider === "simple") {
    const [technical, runtime] = await Promise.all([
      fetchSnapshotPayload("technical.json", { signal }, 8000),
      fetchRuntimeState(signal, true, 3000),
    ]);
    return {
      technical,
      runtime,
      bootstrap_generated_at: technical.generated_at || null,
      run_id: technical.run_id || null,
      coverage_min: technical.coverage_min || null,
      coverage_max: technical.coverage_max || null,
      fallback: true,
    };
  }

  if (bootstrapApiUrl) {
    try {
      const payload = await fetchJson(
        bootstrapApiUrl,
        {
          headers: authHeaders(),
          signal,
        },
        12000,
      );
      technicalState.apiAvailable = true;
      technicalState.apiError = "";
      return payload;
    } catch (error) {
      technicalState.apiError = normalizeCloudError(error.message);
    }
  }

  const technical = await fetchSnapshotPayload("technical.json", { signal }, 12000);
  const runtime = await fetchRuntimeState(signal, true);
  return {
    technical,
    runtime,
    bootstrap_generated_at: technical.generated_at || null,
    run_id: technical.run_id || null,
    coverage_min: technical.coverage_min || null,
    coverage_max: technical.coverage_max || null,
    fallback: true,
  };
}

async function loadBootstrapState({ preferCache = true, silent = false } = {}) {
  if (dataState.bootstrapInFlight) {
    return dataState.bootstrapInFlight;
  }

  const cached = preferCache ? readCachedJson(cacheKeys.bootstrap) : null;
  if (cached) {
    applyBootstrapPayload(cached);
    setSessionPhase("ready");
    renderTechnical();
    if (preferCache) {
      window.setTimeout(() => {
        if (!authState.session || dataState.logoutInFlight || dataState.bootstrapInFlight) return;
        void loadBootstrapState({ preferCache: false, silent: true });
      }, 0);
      return Promise.resolve(cached);
    }
  }

  setSessionPhase("bootstrapping");
  const controller = beginRequest("bootstrap");
  const task = (async () => {
    try {
      const payload = await fetchBootstrapPayload(controller.signal);
      writeCachedJson(cacheKeys.bootstrap, payload);
      applyBootstrapPayload(payload);
      renderTechnical();
      setSessionPhase("ready");
      return payload;
    } catch (error) {
      technicalState.apiError = normalizeCloudError(error.message);
      if (cached) {
        setSessionPhase("degraded");
        renderTechnical();
        scheduleBootstrapRetry();
        return cached;
      }
      setSessionPhase("degraded");
      renderTechnical();
      scheduleBootstrapRetry();
      return null;
    } finally {
      endRequest("bootstrap", controller);
      dataState.bootstrapInFlight = null;
    }
  })();

  dataState.bootstrapInFlight = task;
  return task;
}

async function loadAnalyticsData({ preferCache = true } = {}) {
  if (dataState.analyticsInFlight) {
    return dataState.analyticsInFlight;
  }

  const cached = preferCache ? readCachedJson(cacheKeys.analytics) : null;
  if (cached) {
    snapshot = { ...(snapshot || {}), ...cached };
    dataState.slices.analyticsLoaded = true;
    primeAnalyticsUi();
  }

  const controller = beginRequest("analytics");
  const task = (async () => {
    try {
      const responses = await Promise.all(
        analyticsFileNames.map((file) => fetchSnapshotPayload(file, { signal: controller.signal }, 15000)),
      );
      const [manifest, overview, commercial, customers, products, inventory, accounting, quality, tables] = responses;
      const payload = { manifest, overview, commercial, customers, products, inventory, accounting, quality, tables };
      snapshot = { ...(snapshot || {}), ...payload };
      writeCachedJson(cacheKeys.analytics, payload);
      dataState.slices.analyticsLoaded = true;
      primeAnalyticsUi();
      return payload;
    } catch (error) {
      if (cached) {
        setSessionPhase("degraded");
        primeAnalyticsUi();
        return cached;
      }
      throw error;
    } finally {
      endRequest("analytics", controller);
      dataState.analyticsInFlight = null;
    }
  })();

  dataState.analyticsInFlight = task;
  return task;
}

async function loadDatabaseData({ preferCache = true } = {}) {
  if (dataState.databaseInFlight) {
    return dataState.databaseInFlight;
  }

  const cached = preferCache ? readCachedJson(cacheKeys.database) : null;
  if (cached) {
    snapshot = { ...(snapshot || {}), ...cached };
    dataState.slices.databaseLoaded = true;
    primeDatabaseUi();
  }

  const controller = beginRequest("database");
  const task = (async () => {
    try {
      const database = await fetchSnapshotPayload("database.json", { signal: controller.signal }, 15000);
      const payload = { database };
      snapshot = { ...(snapshot || {}), ...payload };
      writeCachedJson(cacheKeys.database, payload);
      dataState.slices.databaseLoaded = true;
      primeDatabaseUi();
      return payload;
    } catch (error) {
      if (cached) {
        setSessionPhase("degraded");
        primeDatabaseUi();
        return cached;
      }
      throw error;
    } finally {
      endRequest("database", controller);
      dataState.databaseInFlight = null;
    }
  })();

  dataState.databaseInFlight = task;
  return task;
}

async function reloadTechnicalStatus(reloadVisibleSlices = false) {
  await loadBootstrapState({ preferCache: false });
  renderTechnical();
  if (!reloadVisibleSlices) return;

  if (dataState.slices.analyticsLoaded) {
    await loadAnalyticsData({ preferCache: false });
  }
  if (dataState.slices.databaseLoaded) {
    await loadDatabaseData({ preferCache: false });
  }
}

async function pollRefreshJob(jobId) {
  clearPolling();
  technicalState.pollHandle = window.setInterval(async () => {
    try {
      const payload = await fetchJson(
        `${refreshStatusUrl}?run_id=${encodeURIComponent(jobId)}`,
        {
          headers: authHeaders(),
        },
        12000,
      );
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

async function startTechnicalRefresh(scope = "refresh") {
  if (!refreshApiUrl) {
    technicalState.apiError = "La actualizacion remota no esta disponible en este acceso.";
    renderTechnical();
    return;
  }
  const button = scope === "backfill" ? elements.technical.refreshFullButton : elements.technical.refreshQuickButton;
  try {
    setButtonBusy(button, true, scope === "backfill" ? "Lanzando backfill..." : "Lanzando refresh...");
    const payload = await fetchJson(
      refreshApiUrl,
      {
        method: "POST",
        headers: authHeaders({ "Content-Type": "application/json" }),
        body: JSON.stringify({ scope }),
      },
      20000,
    );
    technicalState.runtime.current_job = payload.job;
    setActiveTab("technical-view");
    renderTechnical();
    await pollRefreshJob(payload.job.job_id);
  } catch (error) {
    technicalState.apiError = normalizeCloudError(error.message);
    renderTechnical();
  } finally {
    setButtonBusy(button, false);
  }
}

async function reloadTechnicalSnapshotOnly() {
  try {
    setButtonBusy(elements.technical.reloadButton, true, "Recargando...");
    await reloadTechnicalStatus(true);
  } finally {
    setButtonBusy(elements.technical.reloadButton, false);
  }
}

async function activateTabAndLoad(targetId) {
  setActiveTab(targetId);
  try {
    if (targetId === "technical-view") {
      if (!dataState.slices.bootstrapLoaded) {
        await loadBootstrapState({ preferCache: true, silent: true });
      }
      renderTechnical();
      return;
    }
    if (targetId === "analytic-view") {
      await loadAnalyticsData();
      renderAnalyticsViews();
      return;
    }
    if (targetId === "database-view") {
      await loadDatabaseData();
      renderDatabase();
      return;
    }
  } catch (error) {
    const message = normalizeCloudError(error.message);
    if (targetId === "analytic-view") {
      elements.heroText.textContent = `No fue posible cargar la vista analitica. ${message}`;
    } else if (targetId === "database-view") {
      elements.database.summaryMetrics.innerHTML = `<div class="table-empty">No fue posible cargar la vista DataBase. ${message}</div>`;
    } else {
      technicalState.apiError = message;
      setSessionPhase("degraded");
      renderTechnical();
    }
  }
}

function bindEvents() {
  if (authState.bindingReady) return;
  authState.bindingReady = true;
  elements.tabs.forEach((button) => {
    button.addEventListener("click", () => {
      void activateTabAndLoad(button.dataset.tabTarget);
    });
  });
  elements.authForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!authState.enabled) return;
    const email = elements.authEmail.value.trim();
    const password = elements.authPassword.value;
    if (!email || !password) {
      setAuthMessage("Completa correo y contrasena para ingresar.", "error");
      return;
    }
    setButtonBusy(elements.authSubmit, true, "Ingresando...");
    setAuthMessage(
      authState.provider === "simple"
        ? "Validando acceso del sitio..."
        : "Validando credenciales en Supabase...",
      "",
    );
    try {
      await signInWithPassword(email, password);
      elements.authPassword.value = "";
      setAuthMessage(
        authState.provider === "simple" ? "Sesion web iniciada correctamente." : "Sesion iniciada correctamente.",
        "success",
      );
    } catch (error) {
      setAuthMessage(`No fue posible iniciar sesion. ${normalizeAuthError(error)}`, "error");
    } finally {
      setButtonBusy(elements.authSubmit, false);
    }
  });
  elements.authSignoutButton.addEventListener("click", async () => {
    if (dataState.logoutInFlight) return;
    try {
      setButtonBusy(elements.authSignoutButton, true, "Cerrando...");
      await signOutSession();
    } finally {
      setButtonBusy(elements.authSignoutButton, false);
    }
  });
  elements.technical.refreshQuickButton.addEventListener("click", () => startTechnicalRefresh("refresh"));
  elements.technical.refreshFullButton.addEventListener("click", () => startTechnicalRefresh("backfill"));
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
    if (!hasAnalyticsData()) return;
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
    renderAnalyticsViews();
    if (hasDatabaseData()) {
      renderDatabase();
    }
  });
  document.querySelectorAll("[data-export-table]").forEach((button) => {
    button.addEventListener("click", () => exportTable(button.dataset.exportTable));
  });
  window.addEventListener("resize", resizeCharts);
}

async function bootstrapDashboard() {
  bindEvents();
  setActiveTab("technical-view");
  setSessionPhase("authenticating");
  renderTechnical();
  setSessionPhase("bootstrapping");
  const payload = await loadBootstrapState();
  authState.bootstrapped = Boolean(technicalState.data);
  renderTechnical();
  if (technicalState.runtime.current_job?.job_id) {
    void pollRefreshJob(technicalState.runtime.current_job.job_id);
  }
  return Boolean(payload || technicalState.data);
}

async function initAuth() {
  bindEvents();
  updateSessionChrome();
  if (authState.provider === "simple") {
    elements.authEmail.value = simpleLoginConfig.email || "";
    elements.authPassword.value = "";
    const savedSession = readSimpleSession();
    if (savedSession?.email && String(savedSession.email).toLowerCase() === String(simpleLoginConfig.email || "").toLowerCase()) {
      authState.session = { access_token: "frontend-simple-login", provider: "simple" };
      authState.user = { email: savedSession.email };
      updateSessionChrome();
      setAppVisibility(true);
      setAuthMessage("Sesion web activa.", "success");
      await bootstrapDashboard();
      return;
    }
    resetUiToSignedOutState();
    setAppVisibility(false);
    setAuthMessage("Ingresa con la clave configurada para abrir el dashboard publicado.", "");
    return;
  }
  if (!authState.enabled && !secureModeRequested) {
    setAppVisibility(true);
    setAuthMessage("Login no configurado todavia. Completa la configuracion del acceso simple para abrir el dashboard.", "error");
    await bootstrapDashboard();
    return;
  }
  if (!authState.enabled && secureModeRequested) {
    setAppVisibility(false);
    setAuthMessage("Falta configurar supabaseAnonKey en dashboard/config.js o desde el hosting. El dashboard queda bloqueado hasta completar ese dato.", "error");
    return;
  }

  authState.client = window.supabase.createClient(supabaseUrl, supabaseAnonKey);
  const { data, error } = await authState.client.auth.getSession();
  if (error) {
    setAuthMessage(`No fue posible leer la sesion actual. ${error.message}`, "error");
  }

  const applySession = async (event, session) => {
    if (dataState.logoutInFlight && event !== "SIGNED_OUT") {
      return;
    }

    authState.session = session;
    authState.user = session?.user || null;
    updateSessionChrome();
    if (!session || event === "SIGNED_OUT") {
      dataState.logoutInFlight = false;
      resetUiToSignedOutState();
      setAuthMessage("Inicia sesion para cargar el snapshot privado y habilitar el refresh cloud.", "");
      return;
    }

    setAppVisibility(true);
    if (event === "TOKEN_REFRESHED" && authState.bootstrapped) {
      setSessionPhase("ready");
      renderTechnical();
      setAuthMessage("Sesion activa.", "success");
      return;
    }

    if (dataState.bootstrapInFlight) {
      return;
    }

    setSessionPhase("authenticating");
    renderTechnical();
    setAuthMessage(authState.bootstrapped ? "Sesion activa." : "Validando sesion y leyendo bootstrap...", "success");
    const ready = await bootstrapDashboard();
    if (!ready || !authState.bootstrapped) {
      setSessionPhase("degraded");
      renderTechnical();
      setAuthMessage("La sesion es valida, pero el bootstrap cloud no termino. Usa Recargar estado para reintentar.", "error");
      return;
    }
    if (state.ui.activeTab === "analytic-view" && dataState.slices.analyticsLoaded) {
      await loadAnalyticsData({ preferCache: false });
    }
    if (state.ui.activeTab === "database-view" && dataState.slices.databaseLoaded) {
      await loadDatabaseData({ preferCache: false });
    }
    renderAll();
    setSessionPhase("ready");
    setAuthMessage("Sesion activa.", "success");
  };

  authState.client.auth.onAuthStateChange((event, session) => {
    if (dataState.logoutInFlight && event !== "SIGNED_OUT") {
      return;
    }
    Promise.resolve(applySession(event, session)).catch((error) => {
      setAuthMessage(`No fue posible aplicar la sesion. ${error.message}`, "error");
    });
  });

  await applySession("INITIAL_SESSION", data.session);
}

async function init() {
  if (window.location.protocol === "file:") {
    showHostingHelp();
    return;
  }
  await initAuth();
}

init();
