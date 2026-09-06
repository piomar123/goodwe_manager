// Shared helper for rendering a plain HTML table from column names + row
// objects. Used by the history page (server-paginated raw data) and the
// forecast page (hourly PV production breakdown).
function renderTable(theadId, tbodyId, emptyId, columns, rows) {
  const thead = document.getElementById(theadId);
  const tbody = document.getElementById(tbodyId);
  const empty = document.getElementById(emptyId);
  thead.innerHTML = '<tr>' + columns.map(c => `<th>${c}</th>`).join('') + '</tr>';
  if (rows.length === 0) {
    tbody.innerHTML = '';
    empty.classList.remove('d-none');
    return;
  }
  empty.classList.add('d-none');
  tbody.innerHTML = rows.map(row =>
    '<tr>' + columns.map(c => `<td>${row[c] === null || row[c] === undefined ? '' : row[c]}</td>`).join('') + '</tr>'
  ).join('');
}

// Sticky table header via a fixed-position clone, for tables that live
// inside a horizontally-scrollable wrapper (`.history-table` in
// history.html). CSS `position: sticky` doesn't work there: the wrapper's
// `overflow-x: auto` makes the browser also treat it as a vertical scroll
// container (per the CSS overflow spec), which gives a sticky header that
// box to stick within instead of the page. `position: fixed` isn't subject
// to that rule - it's only affected by a transform/perspective/filter on an
// ancestor - so a fixed clone stays correctly pinned to the viewport.
//
// Visibility is driven by a requestAnimationFrame loop, not scroll/resize
// listeners or an IntersectionObserver - both were tried first and both
// showed the same real-device symptom: correct positioning, but laggy and
// direction-dependent (catching up only once scrolling slowed or reversed,
// screen-recording-confirmed at the time). That turned out to be a red
// herring caused by the *page* overflowing horizontally (see history.html's
// comment) rather than anything about event delivery, but the rAF loop is
// still the more robust choice generally, so it stays.
function initStickyTableHeader(table) {
  let cloneTable = null;
  let clone = null;
  let lastTheadHTML = null; // thead's *original* markup at the last rebuild,
  // captured before styling the clone's <th>s with explicit widths below -
  // comparing against the (now-mutated) clone itself would never match again
  // after the first rebuild, forcing a pointless rebuild on every frame.
  const thead = table.querySelector('thead');

  function rebuildClone() {
    if (cloneTable) cloneTable.remove();
    lastTheadHTML = thead.innerHTML;
    cloneTable = table.cloneNode(false);
    cloneTable.removeAttribute('id');
    cloneTable.setAttribute('aria-hidden', 'true'); // presentational duplicate of the live table's header
    Object.assign(cloneTable.style, {
      position: 'fixed', top: '0', margin: '0', zIndex: '10', display: 'none',
      tableLayout: 'fixed', backgroundColor: 'var(--bs-table-bg)',
    });
    clone = thead.cloneNode(true);
    cloneTable.appendChild(clone);
    document.body.appendChild(cloneTable);
  }

  function tick() {
    if (!cloneTable || thead.innerHTML !== lastTheadHTML) rebuildClone();
    const theadRect = thead.getBoundingClientRect();
    const tableRect = table.getBoundingClientRect();
    const shouldStick = theadRect.top < 0 && tableRect.bottom > theadRect.height;
    if (!shouldStick) {
      if (cloneTable.style.display !== 'none') cloneTable.style.display = 'none';
    } else {
      cloneTable.style.display = '';
      // table.getBoundingClientRect() already reflects the wrapper's
      // current horizontal scroll offset, so the clone tracks sideways
      // scrolling for free without needing to read wrapper.scrollLeft.
      cloneTable.style.left = tableRect.left + 'px';
      cloneTable.style.width = tableRect.width + 'px';
      const liveThs = thead.querySelectorAll('th');
      const cloneThs = clone.querySelectorAll('th');
      liveThs.forEach((th, i) => {
        if (cloneThs[i]) cloneThs[i].style.width = th.getBoundingClientRect().width + 'px';
      });
    }
    requestAnimationFrame(tick);
  }

  requestAnimationFrame(tick);
}
