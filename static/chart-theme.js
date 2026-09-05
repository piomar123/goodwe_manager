// Shared Chart.js dark-theme building blocks, used by the RCE prices chart
// and the PV forecast chart so both stay visually consistent and only need
// updating in one place.
const ChartTheme = {
  axisTickColor: '#888',
  axisBorderColor: '#444',
  gridColor: 'rgba(128,128,128,0.2)',
  gridDash: [3, 4],

  legend: {
    display: true,
    labels: { color: '#aaa', boxWidth: 20, font: { size: 11 } }
  },

  // Base tooltip styling; pages add their own `callbacks` on top, e.g.:
  // tooltip: { ...ChartTheme.tooltip, callbacks: { ... } }
  tooltip: {
    backgroundColor: '#222',
    borderColor: '#555',
    borderWidth: 1,
    titleColor: '#aaa',
    bodyColor: '#fff',
    padding: 8,
  },

  // Palette for chart series like the forecast's per-orientation bars
  // (deliberately avoids red/green, which read as "alert" colors against a
  // dark background). Falls back to a muted HSL rotation past the curated
  // colors so it never repeats or runs out, however many series there are.
  seriesPalette: ['#6472a6', '#a57d42', '#8f7fc9', '#5fb8b0'],
  seriesColor(index) {
    if (index < this.seriesPalette.length) return this.seriesPalette[index];
    const hue = (index * 137.508) % 360; // golden-angle spacing
    return `hsl(${hue}, 40%, 55%)`; // lower saturation than a "loud" default
  },

  // Adds/overrides the alpha channel of a '#rrggbb' or 'hsl(...)' color.
  withAlpha(color, alpha) {
    if (color.startsWith('#')) {
      const r = parseInt(color.slice(1, 3), 16), g = parseInt(color.slice(3, 5), 16), b = parseInt(color.slice(5, 7), 16);
      return `rgba(${r}, ${g}, ${b}, ${alpha})`;
    }
    if (color.startsWith('hsl(')) return color.replace('hsl(', 'hsla(').replace(')', `, ${alpha})`);
    return color;
  },

  // Thins out x-axis tick labels so they don't overlap: every `stepWide`-th
  // label on a normal-width chart, every `stepNarrow`-th on a narrow one.
  thinTicksCallback(stepWide, stepNarrow) {
    return function (val, index) {
      const step = this.chart.width < 500 ? stepNarrow : stepWide;
      return index % step === 0 ? this.getLabelForValue(val) : '';
    };
  },
};
