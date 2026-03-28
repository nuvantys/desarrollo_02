const charts = new Map();

const currency = new Intl.NumberFormat("es-EC", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const decimal = new Intl.NumberFormat("es-EC", {
  maximumFractionDigits: 2,
});

const preciseDecimal = new Intl.NumberFormat("es-EC", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

export function formatCurrency(value) {
  return currency.format(Number(value || 0));
}

export function formatNumber(value) {
  return decimal.format(Number(value || 0));
}

export function formatPreciseNumber(value) {
  return preciseDecimal.format(Number(value || 0));
}

export function formatCompact(value) {
  return new Intl.NumberFormat("es-EC", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(Number(value || 0));
}

export function formatDate(value) {
  return new Intl.DateTimeFormat("es-EC", {
    year: "numeric",
    month: "short",
  }).format(new Date(value));
}

function chartFor(id) {
  const element = document.getElementById(id);
  if (!element) {
    throw new Error(`Missing chart container: ${id}`);
  }
  let chart = charts.get(id);
  if (!chart) {
    chart = window.echarts.init(element, null, { renderer: "canvas" });
    charts.set(id, chart);
  }
  return chart;
}

export function setOption(id, option) {
  const chart = chartFor(id);
  chart.setOption(option, true);
}

export function resizeCharts() {
  charts.forEach((chart) => chart.resize());
}

export function disposeCharts() {
  charts.forEach((chart) => chart.dispose());
  charts.clear();
}

export function lineComboOption({
  categories,
  bars,
  line,
  barName,
  lineName,
  barFormatter = formatNumber,
  lineFormatter = formatCurrency,
}) {
  return {
    color: ["#8cbeb3", "#0d6c5f"],
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "cross" },
      formatter: (params) => {
        const items = Array.isArray(params) ? params : [params];
        const title = items[0]?.axisValueLabel || "";
        const body = items
          .map((item) => {
            const formatter = item.seriesIndex === 0 ? barFormatter : lineFormatter;
            return `${item.marker}${item.seriesName}: ${formatter(item.value)}`;
          })
          .join("<br/>");
        return `${title}<br/>${body}`;
      },
    },
    grid: { left: 42, right: 48, top: 24, bottom: 34 },
    legend: { top: 0, textStyle: { color: "#5d6e66" } },
    xAxis: {
      type: "category",
      data: categories,
      axisLabel: { color: "#5d6e66" },
      axisLine: { lineStyle: { color: "rgba(30,44,37,0.1)" } },
    },
    yAxis: [
      {
        type: "value",
        name: barName,
        axisLabel: { color: "#5d6e66" },
        splitLine: { lineStyle: { color: "rgba(30,44,37,0.08)" } },
      },
      {
        type: "value",
        name: lineName,
        axisLabel: { color: "#5d6e66" },
        splitLine: { show: false },
      },
    ],
    series: [
      {
        name: barName,
        type: "bar",
        data: bars,
        barMaxWidth: 28,
        borderRadius: [10, 10, 0, 0],
      },
      {
        name: lineName,
        type: "line",
        yAxisIndex: 1,
        smooth: true,
        symbolSize: 7,
        data: line,
      },
    ],
  };
}

export function stackedBarOption({ categories, series }) {
  return {
    color: ["#0d6c5f", "#4a9084", "#8cbeb3", "#cfdc85", "#d79b41", "#b94f44"],
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    legend: { top: 0, textStyle: { color: "#5d6e66" } },
    grid: { left: 36, right: 24, top: 48, bottom: 48 },
    xAxis: {
      type: "category",
      data: categories,
      axisLabel: { color: "#5d6e66", rotate: 25 },
      axisLine: { lineStyle: { color: "rgba(30,44,37,0.1)" } },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#5d6e66" },
      splitLine: { lineStyle: { color: "rgba(30,44,37,0.08)" } },
    },
    series: series.map((entry) => ({
      ...entry,
      type: "bar",
      stack: "stack",
      emphasis: { focus: "series" },
      barMaxWidth: 36,
    })),
  };
}

export function horizontalBarOption({ labels, values, formatter = formatCurrency, color = "#0d6c5f" }) {
  return {
    color: [color],
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" }, valueFormatter: (value) => formatter(value) },
    grid: { left: 160, right: 20, top: 20, bottom: 20 },
    xAxis: {
      type: "value",
      axisLabel: { color: "#5d6e66" },
      splitLine: { lineStyle: { color: "rgba(30,44,37,0.08)" } },
    },
    yAxis: {
      type: "category",
      data: labels,
      axisLabel: { color: "#5d6e66" },
      axisLine: { show: false },
      axisTick: { show: false },
    },
    series: [
      {
        type: "bar",
        data: values,
        barMaxWidth: 24,
        borderRadius: 8,
      },
    ],
  };
}

export function donutOption({ data }) {
  return {
    color: ["#0d6c5f", "#4a9084", "#cfdc85", "#d79b41", "#b94f44"],
    tooltip: { trigger: "item", valueFormatter: (value) => formatNumber(value) },
    legend: { bottom: 0, textStyle: { color: "#5d6e66" } },
    series: [
      {
        type: "pie",
        radius: ["48%", "74%"],
        center: ["50%", "42%"],
        label: {
          color: "#1e2c25",
          formatter: ({ name, percent }) => `${name}\n${percent}%`,
        },
        data,
      },
    ],
  };
}

export function paretoOption({ labels, bars, line }) {
  return {
    color: ["#0d6c5f", "#d79b41"],
    tooltip: { trigger: "axis", axisPointer: { type: "cross" } },
    grid: { left: 60, right: 50, top: 36, bottom: 60 },
    xAxis: {
      type: "category",
      data: labels,
      axisLabel: { color: "#5d6e66", rotate: 35 },
    },
    yAxis: [
      {
        type: "value",
        axisLabel: { color: "#5d6e66" },
        splitLine: { lineStyle: { color: "rgba(30,44,37,0.08)" } },
      },
      {
        type: "value",
        max: 100,
        axisLabel: { formatter: "{value}%", color: "#5d6e66" },
        splitLine: { show: false },
      },
    ],
    series: [
      { type: "bar", data: bars, barMaxWidth: 26, borderRadius: [8, 8, 0, 0] },
      { type: "line", yAxisIndex: 1, smooth: true, symbolSize: 8, data: line },
    ],
  };
}

export function balanceBarOption({ labels, debe, haber }) {
  return {
    color: ["#0d6c5f", "#b94f44"],
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" }, valueFormatter: (value) => formatCurrency(value) },
    legend: { top: 0, textStyle: { color: "#5d6e66" } },
    grid: { left: 44, right: 24, top: 40, bottom: 34 },
    xAxis: {
      type: "category",
      data: labels,
      axisLabel: { color: "#5d6e66" },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#5d6e66" },
      splitLine: { lineStyle: { color: "rgba(30,44,37,0.08)" } },
    },
    series: [
      { name: "Debe", type: "bar", data: debe, stack: "balance", barMaxWidth: 28, borderRadius: 8 },
      { name: "Haber", type: "bar", data: haber, stack: "balance", barMaxWidth: 28, borderRadius: 8 },
    ],
  };
}
